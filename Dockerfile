FROM python:3.13-slim

# 设置环境变量
ENV TZ=Asia/Shanghai
ENV PYTHONUNBUFFERED=1
ENV PYTHONIOENCODING=utf-8

# 设置工作目录
WORKDIR /app

# 安装系统依赖和PostgreSQL官方APT仓库
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    ca-certificates \
    gnupg \
    lsb-release \
    tzdata \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 添加PostgreSQL官方APT仓库
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - \
    && echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list

# 更新包列表并安装最新版本的PostgreSQL客户端工具
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client-17 \
    postgresql-client-common \
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

# 验证PostgreSQL客户端版本
RUN pg_dump --version && psql --version

# 默认入口点
ENTRYPOINT ["python3", "backup.py"]