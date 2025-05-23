from fastapi import FastAPI, HTTPException, Depends, status, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pathlib import Path
import logging
import json
from datetime import datetime
import os
import threading
from backup import create_backup, main as backup_main
from typing import Optional
import secrets

# 配置默认用户名和密码
DEFAULT_USERNAME = "admin"
DEFAULT_PASSWORD = "Lzf@BzjGwv"

# 从环境变量获取用户名和密码，如果未设置则使用默认值
USERNAME = os.environ.get("WEB_UI_USERNAME", DEFAULT_USERNAME)
PASSWORD = os.environ.get("WEB_UI_PASSWORD", DEFAULT_PASSWORD)

# 打印认证信息
print(f"Web UI 认证信息：")
print(f"用户名: {USERNAME}")
print(f"密码: {PASSWORD}")

security = HTTPBasic()

def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)) -> bool:
    is_username_correct = secrets.compare_digest(credentials.username.encode("utf8"), USERNAME.encode("utf8"))
    is_password_correct = secrets.compare_digest(credentials.password.encode("utf8"), PASSWORD.encode("utf8"))
    
    if not (is_username_correct and is_password_correct):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="认证失败",
            headers={"WWW-Authenticate": "Basic"},
        )
    return True

app = FastAPI(title="PostgreSQL Backup Monitor")

@app.on_event("startup")
async def startup_event():
    # 在后台线程中启动备份服务
    threading.Thread(target=backup_main, daemon=True).start()

# 挂载静态文件目录
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def read_root(authenticated: bool = Depends(verify_credentials)):
    return FileResponse("static/index.html")

@app.get("/api/health")
async def health_check(authenticated: bool = Depends(verify_credentials)):
    try:
        backup_dir = os.environ.get("BACKUP_DIR", "/backups")
        if not os.path.exists(backup_dir):
            raise HTTPException(status_code=503, detail="Backup directory not found")
        return {"status": "healthy", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))

@app.get("/api/backups")
async def list_backups(
    authenticated: bool = Depends(verify_credentials),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100)
):
    backup_dir = os.environ.get("BACKUP_DIR", "/backups")
    backups = []
    try:
        for file in Path(backup_dir).glob("backup_*"):
            if file.is_file():
                stats = file.stat()
                backups.append({
                    "name": file.name,
                    "size": stats.st_size,
                    "created_at": datetime.fromtimestamp(stats.st_mtime).isoformat(),
                })
        # 按创建时间排序
        sorted_backups = sorted(backups, key=lambda x: x["created_at"], reverse=True)
        
        # 计算分页
        total = len(sorted_backups)
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        
        return {
            "items": sorted_backups[start_idx:end_idx],
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/logs")
async def get_logs(authenticated: bool = Depends(verify_credentials)):
    log_dir = os.path.join(os.environ.get("BACKUP_DIR", "/backups"), "logs")
    logs = []
    try:
        for file in Path(log_dir).glob("backup_*.log"):
            if file.is_file():
                with open(file, "r") as f:
                    last_lines = list(f)[-100:]  # 获取最后100行
                    logs.extend(last_lines)
        return {"logs": logs}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))