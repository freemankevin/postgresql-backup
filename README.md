# 🐘 PostgreSQL 备份工具

📦 一个自动备份的 PostgreSQL 数据库备份工具


## 快速启动

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
