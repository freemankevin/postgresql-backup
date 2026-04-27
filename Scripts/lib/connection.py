import os
import subprocess
import time
import re
from typing import Tuple, Optional
from pathlib import Path

from .logger import get_logger
from .config import Config


class ConnectionManager:
    def __init__(self, config: Config = None):
        self.config = config or Config()
        self.logger = get_logger()
    
    def test_connection(self, database: str, retries: int = None, retry_delay: int = None) -> bool:
        retries = retries or self.config.CONNECTION_RETRIES
        retry_delay = retry_delay or self.config.CONNECTION_RETRY_DELAY
        
        env = self.config.get_pg_env()
        cmd = [
            'psql', '-h', self.config.PG_HOST, '-p', self.config.PG_PORT,
            '-U', self.config.PG_USER, '-d', database, '-c', 'SELECT 1;'
        ]
        
        for attempt in range(1, retries + 1):
            try:
                result = subprocess.run(
                    cmd, env=env, capture_output=True, text=True, timeout=30
                )
                
                if result.returncode == 0:
                    self.logger.success(f"数据库 {database} 连接成功")
                    return True
                
                self.logger.warning(
                    f"数据库 {database} 连接失败 (尝试 {attempt}/{retries})"
                )
                if attempt < retries:
                    self.logger.info(f"等待 {retry_delay} 秒后重试...")
                    time.sleep(retry_delay)
                    
            except subprocess.TimeoutExpired:
                self.logger.warning(
                    f"数据库 {database} 连接超时 (尝试 {attempt}/{retries})"
                )
                if attempt < retries:
                    self.logger.info(f"等待 {retry_delay} 秒后重试...")
                    time.sleep(retry_delay)
            except Exception as e:
                self.logger.warning(
                    f"数据库 {database} 连接异常 (尝试 {attempt}/{retries}): {e}"
                )
                if attempt < retries:
                    self.logger.info(f"等待 {retry_delay} 秒后重试...")
                    time.sleep(retry_delay)
        
        self.logger.error(f"数据库 {database} 连接最终失败，已重试 {retries} 次")
        return False
    
    def wait_for_startup(self, database: str, max_wait: int = None) -> bool:
        max_wait = max_wait or self.config.STARTUP_MAX_WAIT
        
        self.logger.info(f"等待数据库 {database} 就绪，最长等待 {max_wait} 秒...")
        
        env = self.config.get_pg_env()
        cmd = [
            'psql', '-h', self.config.PG_HOST, '-p', self.config.PG_PORT,
            '-U', self.config.PG_USER, '-d', database, '-c', 'SELECT 1;'
        ]
        
        start_time = time.time()
        attempt = 0
        
        while time.time() - start_time < max_wait:
            attempt += 1
            elapsed = int(time.time() - start_time)
            
            try:
                result = subprocess.run(
                    cmd, env=env, capture_output=True, text=True, timeout=10
                )
                if result.returncode == 0:
                    self.logger.success(f"数据库 {database} 已就绪 (等待 {elapsed} 秒，尝试 {attempt} 次)")
                    return True
                else:
                    self.logger.subtask(f"连接尝试 {attempt} 失败，继续等待... ({elapsed}s/{max_wait}s)")
            except subprocess.TimeoutExpired:
                self.logger.subtask(f"连接尝试 {attempt} 超时，继续等待... ({elapsed}s/{max_wait}s)")
            except Exception as e:
                self.logger.subtask(f"连接尝试 {attempt} 异常: {e}，继续等待... ({elapsed}s/{max_wait}s)")
            
            time.sleep(5)
        
        self.logger.error(f"数据库 {database} 在 {max_wait} 秒内未能就绪 (尝试 {attempt} 次)")
        return False
    
    def get_server_version(self, database: str) -> Optional[Tuple[int, int]]:
        try:
            env = self.config.get_pg_env()
            cmd = [
                'psql', '-h', self.config.PG_HOST, '-p', self.config.PG_PORT,
                '-U', self.config.PG_USER, '-d', database, '-t', '-c',
                'SHOW server_version;'
            ]
            result = subprocess.run(
                cmd, env=env, capture_output=True, text=True, timeout=30
            )
            
            if result.returncode == 0:
                version_str = result.stdout.strip()
                match = re.search(r'(\d+)\.(\d+)', version_str)
                if match:
                    major = int(match.group(1))
                    minor = int(match.group(2))
                    self.logger.info(f"PostgreSQL 服务器版本: {major}.{minor}")
                    return major, minor
            return None
        except Exception as e:
            self.logger.warning(f"获取服务器版本失败: {e}")
            return None
    
    def get_pg_dump_version(self) -> Optional[Tuple[int, int]]:
        try:
            result = subprocess.run(
                ['pg_dump', '--version'], capture_output=True, text=True, timeout=10
            )
            if result.returncode == 0:
                match = re.search(r'(\d+)\.(\d+)', result.stdout)
                if match:
                    major = int(match.group(1))
                    minor = int(match.group(2))
                    self.logger.info(f"pg_dump 版本: {major}.{minor}")
                    return major, minor
            return None
        except Exception as e:
            self.logger.warning(f"获取 pg_dump 版本失败: {e}")
            return None
    
    def check_version_compatibility(self, database: str) -> bool:
        dump_version = self.get_pg_dump_version()
        if dump_version is None:
            self.logger.error("无法确定 pg_dump 版本")
            return False
        
        server_version = self.get_server_version(database)
        if server_version is None:
            self.logger.warning("无法确定服务器版本，将尝试继续")
            return True
        
        dump_major, dump_minor = dump_version
        server_major, server_minor = server_version
        
        self.logger.info(
            f"版本检查: pg_dump {dump_major}.{dump_minor} vs Server {server_major}.{server_minor}"
        )
        
        if dump_major < server_major:
            self.logger.error(
                f"版本不兼容: pg_dump {dump_major}.{dump_minor} "
                f"不能备份 PostgreSQL {server_major}.{server_minor}"
            )
            self.logger.error(
                f"请升级 postgresql-client 至 {server_major} 或更高版本"
            )
            return False
        
        if dump_major == server_major and dump_minor < server_minor:
            self.logger.warning(
                f"次版本较低: pg_dump {dump_major}.{dump_minor} 备份 "
                f"PostgreSQL {server_major}.{server_minor}，可能有问题"
            )
        
        self.logger.success("版本兼容性检查通过")
        return True
    
    def get_database_size(self, database: str) -> str:
        try:
            env = self.config.get_pg_env()
            cmd = [
                'psql', '-h', self.config.PG_HOST, '-p', self.config.PG_PORT,
                '-U', self.config.PG_USER, '-d', database, '-t', '-c',
                "SELECT pg_size_pretty(pg_database_size(current_database()));"
            ]
            result = subprocess.run(
                cmd, env=env, capture_output=True, text=True, timeout=30
            )
            
            if result.returncode == 0:
                size = result.stdout.strip()
                self.logger.info(f"数据库 {database} 大小: {size}")
                return size
            return "未知"
        except Exception as e:
            self.logger.warning(f"获取数据库大小失败: {e}")
            return "未知"
    
    def create_database(self, database: str) -> bool:
        try:
            env = self.config.get_pg_env()
            cmd = [
                'psql', '-h', self.config.PG_HOST, '-p', self.config.PG_PORT,
                '-U', self.config.PG_USER, '-d', 'postgres', '-c',
                f'CREATE DATABASE "{database}";'
            ]
            result = subprocess.run(cmd, env=env, capture_output=True, text=True)
            
            if result.returncode == 0:
                self.logger.success(f"数据库 {database} 创建成功")
                return True
            elif 'already exists' in result.stderr:
                self.logger.info(f"数据库 {database} 已存在")
                return True
            
            self.logger.error(f"创建数据库失败: {result.stderr}")
            return False
        except Exception as e:
            self.logger.error(f"创建数据库异常: {e}")
            return False
    
    def get_pg_dump_path(self) -> Optional[str]:
        import shutil
        path = shutil.which('pg_dump')
        if path:
            self.logger.info(f"pg_dump 路径: {path}")
            return path
        self.logger.error("pg_dump 命令未找到")
        return None


def get_connection_manager(config: Config = None) -> ConnectionManager:
    return ConnectionManager(config)