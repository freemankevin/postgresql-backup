FROM python:3.13-slim

# 设置环境变量
ENV TZ=Asia/Shanghai
ENV PYTHONUNBUFFERED=1
ENV PYTHONIOENCODING=utf-8

# 设置工作目录
WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    postgresql-client-common \
    tzdata \
    curl \
    ca-certificates \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
    && echo $TZ > /etc/timezone \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# 复制依赖文件并安装Python包
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# 创建必要的目录
RUN mkdir -p /backups /app/scripts

# 复制应用文件
COPY backup.py restore.py ./

# 设置权限
RUN chmod +x backup.py restore.py

# 默认入口点
ENTRYPOINT ["python3", "backup.py"]