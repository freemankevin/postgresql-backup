# 📂 PostgreSQL 备份管理工具

🌍 **中文** | [ENGLISH](README_EN.md)

🚀 一个强大的 PostgreSQL 数据库备份工具

## 🎯 功能概述

### 🔄 自动备份
- 🕒 支持定时备份 PostgreSQL 数据库
- ⚙️ 可配置备份时间间隔（每天、每小时或自定义分钟数）
- 🗜️ 自动压缩备份文件
- 🗑️ 自动清理过期备份文件

### 🌐 Web 管理界面
- 📊 实时查看备份状态
- 📁 查看备份文件列表
- 📜 查看备份日志
- 📖 支持分页查询

### 🐳 容器化部署
- 🐋 支持 Docker 部署
- 🖥️ 支持多架构（amd64/arm64）
- 🏗️ 自动构建 Docker 镜像

## 🎯 功能概述

### 🔄 自动备份
- 🕒 支持定时备份 PostgreSQL 数据库
- ⚙️ 可配置备份时间间隔（每天、每小时或自定义分钟数）
- 🗜️ 自动压缩备份文件
- 🗑️ 自动清理过期备份文件

### 🌐 Web 管理界面
- 📊 实时查看备份状态
- 📁 查看备份文件列表
- 📜 查看备份日志
- 📖 支持分页查询

### 🐳 容器化部署
- 🐋 支持 Docker 部署
- 🖥️ 支持多架构（amd64/arm64）
- 🏗️ 自动构建 Docker 镜像

## 使用方法

### 快速启动
```bash
docker-compose up -d
```

### 访问 Web 界面
- 默认地址：`http://localhost:8000`
- 默认用户名：`admin`
- 默认密码：`Lzf@BzjGwv`

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
| WEB_UI_USERNAME | admin | Web 界面用户名 |
| WEB_UI_PASSWORD | Lzf@BzjGwv | Web 界面密码 |
