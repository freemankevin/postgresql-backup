FROM debian:12-slim

# 设置时区和语言环境
ENV TZ=Asia/Shanghai \
    LANG=C.UTF-8 \
    PG_HOST=postgres \
    PG_PORT=5432 \
    PG_USER=postgres \
    PG_PASSWORD=postgres \
    PG_DATABASE=postgres \
    BACKUP_DIR=/backups \
    BACKUP_RETENTION_DAYS=7

# 安装必要的包并设置时区
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    postgresql-client \
    tzdata \
    curl \
    && pip3 install psycopg2-binary python-dotenv schedule fastapi uvicorn \
    && mkdir -p /backups /app \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
    && echo $TZ > /etc/timezone \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 复制应用文件
COPY backup.py restore.py app.py ./

# 复制静态文件
COPY static/index.html static/styles.css static/app.js ./static/

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/api/health || exit 1

ENTRYPOINT ["python3", "app.py"]