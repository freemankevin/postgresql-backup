FROM python:3.9-slim

ENV TZ=Asia/Shanghai

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

# 确保日志目录存在
RUN mkdir -p /backups/logs

# 添加执行权限
RUN chmod +x /app/backup.py

# 使用 -u 参数确保 Python 输出不被缓冲
ENTRYPOINT ["python3", "-u", "backup.py"]