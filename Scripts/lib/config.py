import os
import multiprocessing
from typing import List, Optional
from pathlib import Path


class Config:
    PG_HOST: str = 'localhost'
    PG_PORT: str = '5432'
    PG_USER: str = 'postgres'
    PG_PASSWORD: str = 'postgres'
    PG_DATABASE: str = 'postgres'
    
    BACKUP_DIR: str = '/backups'
    BACKUP_TIME: str = '03:00'
    BACKUP_INTERVAL: str = 'daily'
    BACKUP_RETENTION_DAYS: int = 7
    BACKUP_FORMAT: str = 'both'
    BACKUP_PARALLEL_WORKERS: int = 0
    
    ENABLE_COMPRESSION: bool = True
    ENABLE_VERIFY: bool = True
    ENABLE_PARALLEL: bool = True
    
    RESTORE_VERIFY_CHECKSUM: bool = True
    RESTORE_VERIFY_DATA: bool = False
    
    CONNECTION_RETRIES: int = 5
    CONNECTION_RETRY_DELAY: int = 5
    STARTUP_MAX_WAIT: int = 180
    
    BACKUP_TIMEOUT: int = 3600
    RESTORE_TIMEOUT: int = 7200
    
    def __init__(self):
        self._load_from_env()
    
    def _load_from_env(self):
        self.PG_HOST = os.environ.get('PG_HOST', self.PG_HOST)
        self.PG_PORT = os.environ.get('PG_PORT', self.PG_PORT)
        self.PG_USER = os.environ.get('PG_USER', self.PG_USER)
        self.PG_PASSWORD = os.environ.get('PG_PASSWORD', self.PG_PASSWORD)
        self.PG_DATABASE = os.environ.get('PG_DATABASE', self.PG_DATABASE)
        
        self.BACKUP_DIR = os.environ.get('BACKUP_DIR', self.BACKUP_DIR)
        self.BACKUP_TIME = os.environ.get('BACKUP_TIME', self.BACKUP_TIME)
        self.BACKUP_INTERVAL = os.environ.get('BACKUP_INTERVAL', self.BACKUP_INTERVAL)
        self.BACKUP_RETENTION_DAYS = int(os.environ.get('BACKUP_RETENTION_DAYS', str(self.BACKUP_RETENTION_DAYS)))
        self.BACKUP_FORMAT = os.environ.get('BACKUP_FORMAT', self.BACKUP_FORMAT)
        
        env_workers = os.environ.get('BACKUP_PARALLEL_WORKERS')
        if env_workers:
            self.BACKUP_PARALLEL_WORKERS = int(env_workers)
        elif self.BACKUP_PARALLEL_WORKERS == 0:
            self.BACKUP_PARALLEL_WORKERS = multiprocessing.cpu_count()
        
        self.ENABLE_COMPRESSION = os.environ.get('ENABLE_COMPRESSION', 'true').lower() == 'true'
        self.ENABLE_VERIFY = os.environ.get('ENABLE_VERIFY', 'true').lower() == 'true'
        self.ENABLE_PARALLEL = os.environ.get('ENABLE_PARALLEL', 'true').lower() == 'true'
        
        self.RESTORE_VERIFY_CHECKSUM = os.environ.get('RESTORE_VERIFY_CHECKSUM', 'true').lower() == 'true'
        self.RESTORE_VERIFY_DATA = os.environ.get('RESTORE_VERIFY_DATA', 'false').lower() == 'true'
        
        self.CONNECTION_RETRIES = int(os.environ.get('CONNECTION_RETRIES', str(self.CONNECTION_RETRIES)))
        self.CONNECTION_RETRY_DELAY = int(os.environ.get('CONNECTION_RETRY_DELAY', str(self.CONNECTION_RETRY_DELAY)))
        self.STARTUP_MAX_WAIT = int(os.environ.get('STARTUP_MAX_WAIT', str(self.STARTUP_MAX_WAIT)))
        
        self.BACKUP_TIMEOUT = int(os.environ.get('BACKUP_TIMEOUT', str(self.BACKUP_TIMEOUT)))
        self.RESTORE_TIMEOUT = int(os.environ.get('RESTORE_TIMEOUT', str(self.RESTORE_TIMEOUT)))
    
    def get_databases(self) -> List[str]:
        return [db.strip() for db in self.PG_DATABASE.split(',') if db.strip()]
    
    def get_pg_env(self) -> dict:
        env = os.environ.copy()
        env['PGPASSWORD'] = self.PG_PASSWORD
        return env
    
    def ensure_dirs(self) -> dict:
        date_dir = self._get_date_dir()
        paths = {
            'backup_root': self.BACKUP_DIR,
            'backup_data': Path(self.BACKUP_DIR) / 'data' / date_dir,
            'backup_logs': Path(self.BACKUP_DIR) / 'logs' / date_dir,
        }
        
        for path in paths.values():
            Path(path).mkdir(parents=True, exist_ok=True)
        
        return paths
    
    def _get_date_dir(self) -> str:
        from datetime import datetime
        return datetime.now().strftime('%Y%m%d')
    
    def get_timestamp(self) -> str:
        from datetime import datetime
        return datetime.now().strftime('%Y%m%d_%H%M%S')
    
    def print_config(self, logger):
        items = {
            'PostgreSQL 主机': f"{self.PG_HOST}:{self.PG_PORT}",
            'PostgreSQL 用户': self.PG_USER,
            'PostgreSQL 密码': '***',
            '目标数据库': self.PG_DATABASE,
            '备份目录': self.BACKUP_DIR,
            '备份格式': self.BACKUP_FORMAT,
            '压缩': '启用' if self.ENABLE_COMPRESSION else '禁用',
            '并行备份': '启用' if self.ENABLE_PARALLEL else '禁用',
            '并发数': f"{self.BACKUP_PARALLEL_WORKERS} (CPU核心)",
            '备份验证': '启用' if self.ENABLE_VERIFY else '禁用',
            '保留天数': self.BACKUP_RETENTION_DAYS,
            '备份时间': self.BACKUP_TIME,
            '备份间隔': self.BACKUP_INTERVAL,
            '连接重试': self.CONNECTION_RETRIES,
            '启动等待': f"{self.STARTUP_MAX_WAIT}s",
        }
        
        logger.print_summary("配置信息", items)


def get_config() -> Config:
    return Config()