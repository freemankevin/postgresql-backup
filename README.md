# 🐘 PostgreSQL 备份工具镜像

📦 一个自动备份的 PostgreSQL 数据库备份工具

## ✨ 功能
- ✅ 支持 PostgreSQL 13-18 所有主流版本
- 🖥️ 多平台支持（linux/amd64, linux/arm64）
- 🕒 支持定时自动备份 PostgreSQL 数据库
- 🔄 通过 GitHub Actions 自动更新与发布 Docker 镜像
- 📦 支持自定义 PostgreSQL 客户端版本

## 使用方法

### 快速启动

#### 方式一：使用预构建镜像
```bash
# 拉取代码
git clone https://github.com/freemankevin/postgresql-backup.git
cd postgresql-backup

# 拉取镜像（默认支持 PostgreSQL 18）
docker pull freelabspace/postgresql-backup:latest

# 或者拉取指定版本的镜像
docker pull freelabspace/postgresql-backup:pg18  # PostgreSQL 18
docker pull freelabspace/postgresql-backup:pg17  # PostgreSQL 17
docker pull freelabspace/postgresql-backup:pg16  # PostgreSQL 16

# 使用 docker-compose 启动
docker-compose up -d
```

#### 方式二：本地构建（推荐用于自定义版本）
```bash
# 拉取代码
git clone https://github.com/freemankevin/postgresql-backup.git
cd postgresql-backup

# 复制环境变量配置文件
cp .env.example .env

# 编辑 .env 文件，设置你的 PostgreSQL 版本
# PG_MAJOR_VERSION=18  # 改为你的 PostgreSQL 服务器版本

# 修改 docker-compose.yaml，启用本地构建
# 取消注释 build 部分，注释掉 image 部分

# 构建并启动
docker-compose up -d --build
```


### 本地构建指定版本

```bash
# 方式1：使用 docker build
docker build -t my-pg-backup:latest --build-arg PG_MAJOR_VERSION=17 .

# 方式2：使用 docker-compose（推荐）
# 编辑 docker-compose.yaml，取消注释 build 部分：
# build:
#   context: .
#   dockerfile: Dockerfile
#   args:
#     PG_MAJOR_VERSION: 17  # 指定版本

docker-compose up -d --build
```

### 恢复数据

```bash
# 进入备份容器
docker-compose exec pg-backup bash

# 在容器内执行恢复
python3 restore.py /backups/data/20241201/gis_20241201_030001.dump -d gis

# 查看可用的备份文件
python3 restore.py -l /backups/data
```

## 配置说明

### 环境变量

| 变量名 | 默认值 | 说明 |
|--------|--------|------|
| PG_HOST | postgres | PostgreSQL 主机地址 |
| PG_PORT | 5432 | PostgreSQL 端口 |
| PG_USER | postgres | PostgreSQL 用户名 |
| PG_PASSWORD | postgres | PostgreSQL 密码 |
| PG_DATABASE | postgres | 要备份的数据库（多个用逗号分隔） |
| BACKUP_TIME | 03:00 | 备份时间（24小时制） |
| BACKUP_INTERVAL | daily | 备份间隔（daily/hourly/分钟数） |
| BACKUP_RETENTION_DAYS | 7 | 备份文件保留天数 |
| ENABLE_COMPRESSION | true | 是否启用压缩 |
| BACKUP_FORMAT | both | 备份格式（both/dump/sql） |
