# 🐘 PostgreSQL Backup Tool Image

🌍 **ENGLISH** | [中文](README.md)

📦 An automated PostgreSQL database backup tool


## ✨ Features
- ✅ Supports most major PostgreSQL versions
- 🖥️ Multi-platform support (linux/amd64, linux/arm64)
- 🕒 Scheduled automatic PostgreSQL database backups
- 🔄 Automatic updates and Docker image releases via GitHub Actions

## Usage

### Quick Start
```bash
# Clone the code
git clone https://github.com/freemankevin/postgresql-backup.git
cd postgresql-backup

# Pull the image
docker pull freelabspace/postgresql-backup:v1

# Start with docker-compose
docker-compose up -d
```

### Configuration

#### Environment Variables
| Variable | Default | Description |
|----------|---------|-------------|
| PG_HOST | postgres | PostgreSQL host address |
| PG_PORT | 5432 | PostgreSQL port |
| PG_USER | postgres | PostgreSQL username |
| PG_PASSWORD | postgres | PostgreSQL password |
| PG_DATABASE | postgres | Databases to backup (comma separated) |
| BACKUP_TIME | 03:00 | Backup time (24-hour format) |
| BACKUP_INTERVAL | daily | Backup interval (daily/hourly/minutes) |
| BACKUP_RETENTION_DAYS | 7 | Backup file retention days |
| ENABLE_COMPRESSION | true | Enable compression |
