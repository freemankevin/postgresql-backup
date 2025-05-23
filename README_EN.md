# 📂 PostgreSQL Backup Manager

🌍 **ENGLISH** | [中文](README.md)

🚀 A powerful PostgreSQL database backup tool.

## 🎯 Features

### 🔄 Automatic Backup
- 🕒 Scheduled backup for PostgreSQL databases
- ⚙️ Configurable backup interval (daily, hourly, or custom minutes)
- 🗜️ Automatic backup file compression
- 🗑️ Automatic cleanup of expired backup files

### 🌐 Web Management Interface
- 📊 Real-time backup status monitoring
- 📁 Backup file list
- 📜 Backup logs
- 📖 Pagination support

### 🐳 Container Deployment
- 🐋 Docker deployment support
- 🖥️ Multi-architecture support (amd64/arm64)
- 🏗️ Automatic Docker image building

## Usage

### Quick Start
```bash
docker-compose up -d
```

### Access Web UI
- Default URL: `http://localhost:8000`
- Default username: `admin`
- Default password: `Lzf@BzjGwv`

### Configuration

#### Environment Variables
| Variable | Default | Description |
|----------|---------|--------------|
| PG_HOST | postgres | PostgreSQL host address |
| PG_PORT | 5432 | PostgreSQL port |
| PG_USER | postgres | PostgreSQL username |
| PG_PASSWORD | postgres | PostgreSQL password |
| PG_DATABASE | postgres | Databases to backup (comma separated) |
| BACKUP_TIME | 03:00 | Backup time (24-hour format) |
| BACKUP_INTERVAL | daily | Backup interval (daily/hourly/minutes) |
| BACKUP_RETENTION_DAYS | 7 | Backup file retention days |
| ENABLE_COMPRESSION | true | Enable compression |
| WEB_UI_USERNAME | admin | Web UI username |
| WEB_UI_PASSWORD | Lzf@BzjGwv | Web UI password |