FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY minidb ./minidb
COPY bench ./bench

# AOF lives here; mount a volume to persist across container restarts.
RUN mkdir -p /data
VOLUME ["/data"]

# Run as an unprivileged user. Containers default to root, which means any
# process escape starts with root inside the container. Nothing here needs
# elevated privileges, so drop them.
RUN useradd --create-home --uid 10001 appuser \
    && chown -R appuser:appuser /app /data
USER appuser

EXPOSE 6380

CMD ["python", "-m", "minidb.server", "--host", "0.0.0.0", "--port", "6380", "--aof", "/data/minidb.aof"]
