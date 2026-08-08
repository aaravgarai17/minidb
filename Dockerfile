FROM python:3.12-slim

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

EXPOSE 6380

CMD ["python", "-m", "minidb.server", "--host", "0.0.0.0", "--port", "6380", "--aof", "/data/minidb.aof"]
