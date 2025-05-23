FROM python:3.9-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    tzdata \
    curl \
    && pip install --no-cache-dir psycopg2-binary python-dotenv schedule \
    && mkdir -p /backups /app \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
    && echo $TZ > /etc/timezone \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY backup.py restore.py ./

ENTRYPOINT ["python3", "backup.py"]