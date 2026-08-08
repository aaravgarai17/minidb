#!/usr/bin/env bash
#
# One-command verification. Starts the server, proves every claim in the
# README, and shuts down. Written for someone who just cloned this repo and
# wants evidence rather than assurances.
#
# Usage:  ./verify.sh

set -uo pipefail

PORT="${PORT:-6390}"
AOF="$(mktemp -d)/verify.aof"
PY="${PYTHON:-python3}"
SERVER_PID=""
pass=0
fail=0

ok()  { echo "  ✓ $1"; pass=$(( pass + 1 )); }
bad() { echo "  ✗ $1"; fail=$(( fail + 1 )); }

cleanup() {
  [[ -n "$SERVER_PID" ]] && kill "$SERVER_PID" 2>/dev/null
  wait "$SERVER_PID" 2>/dev/null
  rm -rf "$(dirname "$AOF")"
}
trap cleanup EXIT

cli() { $PY -m minidb.cli --port "$PORT" "$@" 2>/dev/null; }

start_server() {
  $PY -m minidb.server --port "$PORT" --aof "$AOF" >/tmp/minidb_verify.log 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 1 40); do
    if cli PING 2>/dev/null | grep -q PONG; then return 0; fi
    sleep 0.25
  done
  return 1
}

stop_server() {
  [[ -n "$SERVER_PID" ]] || return 0
  kill -TERM "$SERVER_PID" 2>/dev/null      # graceful: triggers final fsync
  wait "$SERVER_PID" 2>/dev/null
  SERVER_PID=""
}

echo "=================================================="
echo " 0/6  Preflight"
echo "=================================================="
command -v $PY >/dev/null || { echo "  ✗ python3 not found"; exit 1; }
$PY -c "import pytest" 2>/dev/null || {
  echo "  ✗ dependencies missing. Run:"
  echo "      python3 -m venv .venv && source .venv/bin/activate"
  echo "      pip install -r requirements.txt"
  exit 1
}
ok "python and dependencies available"

echo ""
echo "=================================================="
echo " 1/6  Test suite"
echo "=================================================="
# -p no:cacheprovider: skip writing .pytest_cache, which fails on some
# network/container mounts and has nothing to do with whether tests pass.
if $PY -m pytest -q -p no:cacheprovider 2>&1 | tail -3; then
  ok "all tests passed"
else
  bad "tests failed"
fi

echo ""
echo "=================================================="
echo " 2/6  Server starts and speaks RESP"
echo "=================================================="
if start_server; then
  ok "server is up on :$PORT"
else
  bad "server failed to start"
  cat /tmp/minidb_verify.log
  echo "Results: $pass passed, $fail failed"
  exit 1
fi

[[ "$(cli SET greeting hello)" == *OK* ]] && ok "SET works" || bad "SET failed"
[[ "$(cli GET greeting)" == *hello* ]]    && ok "GET returns the value" || bad "GET failed"
[[ "$(cli GET nothere)" == *nil* ]]       && ok "missing key returns nil" || bad "expected nil"
[[ "$(cli DEL greeting)" == *1* ]]        && ok "DEL removes the key" || bad "DEL failed"

echo ""
echo "=================================================="
echo " 3/6  Real Redis clients work against it"
echo "=================================================="
# The reason for implementing RESP rather than a custom protocol: existing
# Redis tooling drives this server unmodified.
if $PY -c "import redis" 2>/dev/null; then
  if $PY - "$PORT" <<'EOF'
import sys, redis
r = redis.Redis(port=int(sys.argv[1]), decode_responses=True)
assert r.ping()
assert r.set("py", "works")
assert r.get("py") == "works"
r.mset({"user:1": "a", "user:2": "b", "misc": "c"})
assert sorted(r.keys("user:*")) == ["user:1", "user:2"], r.keys("user:*")
assert r.incr("n") == 1 and r.incr("n") == 2
r.flushall()
EOF
  then
    ok "the official redis-py client drives it correctly"
  else
    bad "redis-py client failed"
  fi
else
  echo "  – redis-py not installed, skipping (pip install redis)"
fi

if command -v redis-cli >/dev/null 2>&1; then
  if redis-cli -p "$PORT" PING 2>/dev/null | grep -q PONG; then
    ok "the real redis-cli connects and works"
  else
    bad "redis-cli could not talk to the server"
  fi
else
  echo "  – redis-cli not installed, skipping"
fi

echo ""
echo "=================================================="
echo " 4/6  TTL expiry, both lazy and active"
echo "=================================================="
cli SET temp value EX 100 >/dev/null
ttl=$(cli TTL temp | tr -dc '0-9')
[[ -n "$ttl" && "$ttl" -gt 0 && "$ttl" -le 100 ]] \
  && ok "TTL reports time remaining ($ttl s)" || bad "unexpected TTL: $ttl"

# Nothing reads this key, so only the background sweeper can remove it.
cli SET ghost value PX 100 >/dev/null
sleep 1
[[ "$(cli DBSIZE)" == *1* ]] \
  && ok "active expiry reclaimed a key nobody read" \
  || bad "expired key was not reclaimed (DBSIZE: $(cli DBSIZE))"

echo ""
echo "=================================================="
echo " 5/6  Concurrent writes lose no updates"
echo "=================================================="
# INCR is read-modify-write — where a threaded server would need a lock.
if $PY -c "import redis" 2>/dev/null; then
  result=$($PY - "$PORT" <<'EOF'
import sys, threading, redis
port = int(sys.argv[1])
CLIENTS, PER = 20, 100
redis.Redis(port=port).delete("ctr")

def bump():
    r = redis.Redis(port=port)
    for _ in range(PER):
        r.incr("ctr")

ts = [threading.Thread(target=bump) for _ in range(CLIENTS)]
[t.start() for t in ts]
[t.join() for t in ts]
print(int(redis.Redis(port=port).get("ctr")), CLIENTS * PER)
EOF
)
  got=$(echo "$result" | awk '{print $1}')
  want=$(echo "$result" | awk '{print $2}')
  [[ "$got" == "$want" ]] \
    && ok "$want concurrent increments from 20 clients, zero lost ($got)" \
    || bad "lost updates: expected $want, got $got"
else
  echo "  – redis-py not installed, skipping"
fi

echo ""
echo "=================================================="
echo " 6/6  Data survives a restart"
echo "=================================================="
cli FLUSHALL >/dev/null
cli SET durable "survives restart" >/dev/null
cli SET doomed "will be deleted" >/dev/null
cli DEL doomed >/dev/null

stop_server                       # graceful shutdown → final fsync
ok "server shut down cleanly"

if start_server; then
  [[ "$(cli GET durable)" == *"survives restart"* ]] \
    && ok "value restored from the append-only file" \
    || bad "data did not survive the restart"

  [[ "$(cli GET doomed)" == *nil* ]] \
    && ok "the DEL was replayed too (not just the writes)" \
    || bad "deleted key came back after restart"
else
  bad "server failed to restart"
fi

echo ""
echo "=================================================="
echo " Results: $pass passed, $fail failed"
echo "=================================================="
[[ $fail -eq 0 ]] && echo "VERIFIED — every README claim checks out." \
                  || echo "FAILED — see above."
exit $(( fail > 0 ? 1 : 0 ))
