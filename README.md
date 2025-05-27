# 🐘 PostgreSQL 备份工具镜像

🌍 **中文** | [ENGLISH](README_EN.md)

📦 一个自动备份的 PostgreSQL 数据库备份工具


## ✨ 功能
- ✅ 支持主流 PostgreSQL 绝大部分版本
- 🖥️ 多平台支持（linux/amd64, linux/arm64）
- 🕒 支持定时自动备份 PostgreSQL 数据库
- 🔄 通过 GitHub Actions 自动更新与发布Docker 镜像

## 使用方法

### 快速启动
```bash
# 拉取代码
git clone https://github.com/freemankevin/postgresql-backup.git
cd postgresql-backup

# 拉取镜像
docker pull freelabspace/postgresql-backup:v1

# 使用 docker-compose 启动
docker-compose up -d


# 恢复数据
# 进入备份容器
docker-compose exec pg-backup bash

# 在容器内执行恢复
python3 restore.py /backups/data/20241201/gis_20241201_030001.dump -d gis

```



### 配置说明

#### 环境变量
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
