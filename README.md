# 🐘 PostgreSQL 备份工具

📦 一个自动备份的 PostgreSQL 数据库备份工具，支持定时备份、压缩、校验、并发备份等功能。


## 快速启动

```bash
# 拉取代码
git clone https://github.com/freemankevin/postgresql-backup.git
cd postgresql-backup

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
| BACKUP_PARALLEL_WORKERS | CPU核心数 | 并发备份线程数（默认等于CPU可用核心数） |
| ENABLE_VERIFY | true | 是否验证备份文件可用性 |
| CONNECTION_RETRIES | 5 | 连接失败重试次数 |
| ENABLE_PARALLEL | true | 是否启用并发备份（多数据库） |
| STARTUP_MAX_WAIT | 60 | 启动时等待数据库就绪秒数 |


## 使用方式

### 备份操作

#### 1. 定时备份服务（默认模式）

容器启动后自动进入定时备份模式：

```bash
# 启动定时备份服务
docker-compose up -d

# 查看备份日志
docker logs pg-backup -f
```

#### 2. 手动执行备份

```bash
# 执行一次性备份
docker exec pg-backup python3 main.py backup --once

# 执行备份并验证
docker exec pg-backup python3 main.py backup --once --verify

# 启用并发备份（多数据库）
docker exec pg-backup python3 main.py backup --once --parallel
```

#### 3. 查看备份文件

```bash
# 列出所有备份文件
docker exec pg-backup python3 main.py list

# 列出指定目录的备份文件
docker exec pg-backup python3 main.py list --dir /backups/data/20260427
```

### 恢复操作

#### 1. 恢复到原数据库

```bash
# 恢复 SQL 格式备份
docker exec pg-backup python3 main.py restore /backups/data/20260427/postgres_20260427.sql.gz

# 恢复 dump 格式备份
docker exec pg-backup python3 main.py restore /backups/data/20260427/postgres_20260427.dump.gz
```

#### 2. 恢复到新数据库

```bash
# 恢复到指定数据库
docker exec pg-backup python3 main.py restore /backups/data/20260427/postgres_20260427.dump.gz -d new_database
```

#### 3. 恢复选项

```bash
# 恢复前清理现有数据
docker exec pg-backup python3 main.py restore <backup_file> -d mydb --clean

# 仅恢复数据（不恢复架构）
docker exec pg-backup python3 main.py restore <backup_file> -d mydb --data-only

# 仅恢复架构（不恢复数据）
docker exec pg-backup python3 main.py restore <backup_file> -d mydb --schema-only

# 恢复后验证数据完整性
docker exec pg-backup python3 main.py restore <backup_file> -d mydb --verify-data

# 跳过 checksum 验证
docker exec pg-backup python3 main.py restore <backup_file> -d mydb --no-verify-checksum
```

#### 4. 流式恢复（无需临时文件）

压缩备份文件支持流式恢复，直接解压传输到数据库，无需创建临时文件：

```bash
# 流式恢复（自动识别压缩格式）
docker exec pg-backup python3 main.py restore /backups/data/20260427/postgres_20260427.dump.gz
```

### 备份文件结构

```
/backups/
├── data/
│   └── 20260427/
│       ├── postgres_20260427_103000.dump.gz      # dump 格式备份
│       ├── postgres_20260427_103000.dump.gz.sha256 # checksum 文件
│       ├── postgres_20260427_103000.sql.gz       # SQL 格式备份
│       └── postgres_20260427_103000.sql.gz.sha256  # checksum 文件
└── logs/
    └── 20260427/
        └── backup_20260427_103000.log            # 备份日志
```


## 命令行帮助

```bash
# 查看主命令帮助
docker exec pg-backup python3 main.py --help

# 查看备份子命令帮助
docker exec pg-backup python3 main.py backup --help

# 查看恢复子命令帮助
docker exec pg-backup python3 main.py restore --help
```


## 功能特性

| 功能 | 说明 |
|------|------|
| ✅ 定时备份 | 支持每日、每小时或自定义分钟间隔 |
| ✅ 多数据库备份 | 支持同时备份多个数据库（逗号分隔） |
| ✅ 并发备份 | 多数据库时可启用并发提升效率 |
| ✅ 双格式备份 | dump（自定义格式）和 sql（纯文本格式） |
| ✅ 自动压缩 | gzip 压缩节省存储空间 |
| ✅ SHA256 校验 | 每个备份文件生成 checksum |
| ✅ 备份验证 | 可验证备份恢复到临时库 |
| ✅ 流式恢复 | 压缩文件直接流式恢复，无临时文件 |
| ✅ 连接重试 | 启动和备份时自动重试连接 |
| ✅ 版本兼容检查 | pg_dump 与服务器版本兼容性检查 |
| ✅ 自动清理 | 按保留天数自动清理过期备份 |
| ✅ 颜色日志 | 终端输出带颜色高亮，清晰美观 |


## 日志输出示例

```
2026-04-27 10:30:00 │ INFO    │ ───────────────────────────────────────────────────────
2026-04-27 10:30:00 │ INFO    │   备份任务开始
2026-04-27 10:30:00 │ INFO    │ ───────────────────────────────────────────────────────
2026-04-27 10:30:00 │ INFO    │ PostgreSQL 主机 : postgres:5432
2026-04-27 10:30:00 │ INFO    │ 目标数据库     : postgres
2026-04-27 10:30:00 │ SUCCESS │ 数据库 postgres 连接成功
2026-04-27 10:30:01 │ INFO    │ 创建 dump 备份: /backups/data/20260427/postgres.dump.gz
2026-04-27 10:30:02 │ SUCCESS │ dump 备份成功: 16701 bytes
2026-04-27 10:30:02 │ SUCCESS │ Checksum: a1b2c3d4...
```
