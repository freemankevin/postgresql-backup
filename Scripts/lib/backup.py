import os
import gzip
import subprocess
import shutil
import schedule
import signal
import threading
import time
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from .logger import get_logger, Logger
from .config import Config
from .connection import ConnectionManager
from .checksum import ChecksumManager


class BackupManager:
    def __init__(self, config: Config = None):
        self.config = config or Config()
        self.logger = get_logger()
        self.conn = ConnectionManager(self.config)
        self.checksum = ChecksumManager()
        self.shutdown_event = threading.Event()
    
    def compress_file(self, file_path: str, show_progress: bool = False) -> str:
        try:
            if not os.path.exists(file_path):
                self.logger.error(f"文件不存在: {file_path}")
                return file_path
            
            file_size = os.path.getsize(file_path)
            gz_path = f'{file_path}.gz'
            
            if show_progress and file_size > 10 * 1024 * 1024:
                self.logger.info(f"压缩大文件 ({file_size / (1024*1024):.2f} MB)")
            
            with open(file_path, 'rb') as f_in:
                with gzip.open(gz_path, 'wb') as f_out:
                    if show_progress and file_size > 50 * 1024 * 1024:
                        copied = 0
                        chunk_size = 8192
                        while True:
                            chunk = f_in.read(chunk_size)
                            if not chunk:
                                break
                            f_out.write(chunk)
                            copied += len(chunk)
                            if copied % (10 * 1024 * 1024) == 0:
                                progress = (copied / file_size) * 100
                                self.logger.info(f"压缩进度: {progress:.1f}%")
                    else:
                        shutil.copyfileobj(f_in, f_out)
            
            if os.path.exists(gz_path) and os.path.getsize(gz_path) > 0:
                os.remove(file_path)
                self.logger.success(f"压缩完成: {gz_path}")
                self.checksum.calculate(gz_path)
                return gz_path
            
            self.logger.error(f"压缩失败: {gz_path}")
            return file_path
            
        except Exception as e:
            self.logger.error(f"压缩异常: {e}")
            return file_path
    
    def backup_single_database(self, database: str, backup_dir: str, 
                                 timestamp: str, pg_dump_path: str) -> dict:
        result = {
            'database': database,
            'success': False,
            'files': [],
            'size': 0,
            'error': None
        }
        
        self.logger.section(f"备份数据库: {database}")
        
        if not self.conn.test_connection(database):
            result['error'] = '连接失败'
            return result
        
        self.conn.get_database_size(database)
        
        env = self.config.get_pg_env()
        
        try:
            if self.config.BACKUP_FORMAT in ['both', 'dump']:
                dump_file = Path(backup_dir) / f'{database}_{timestamp}.dump'
                self.logger.info(f"创建 dump 备份: {dump_file}")
                
                cmd = [
                    pg_dump_path, '-h', self.config.PG_HOST, '-p', self.config.PG_PORT,
                    '-U', self.config.PG_USER, '-d', database,
                    '-F', 'c', '-b', '-v', '-f', str(dump_file)
                ]
                
                dump_result = subprocess.run(
                    cmd, env=env, capture_output=True, text=True, 
                    timeout=self.config.BACKUP_TIMEOUT
                )
                
                if dump_result.returncode == 0:
                    file_size = os.path.getsize(dump_file)
                    result['size'] += file_size
                    self.logger.success(f"dump 备份成功: {file_size} bytes")
                    
                    if self.config.ENABLE_COMPRESSION:
                        dump_file = self.compress_file(str(dump_file), show_progress=True)
                    else:
                        self.checksum.calculate(str(dump_file))
                    
                    result['files'].append(dump_file)
                else:
                    self.logger.error(f"dump 备份失败: {dump_result.stderr}")
            
            if self.config.BACKUP_FORMAT in ['both', 'sql']:
                sql_file = Path(backup_dir) / f'{database}_{timestamp}.sql'
                self.logger.info(f"创建 SQL 备份: {sql_file}")
                
                cmd = [
                    pg_dump_path, '-h', self.config.PG_HOST, '-p', self.config.PG_PORT,
                    '-U', self.config.PG_USER, '-d', database,
                    '-b', '-v', '-f', str(sql_file)
                ]
                
                sql_result = subprocess.run(
                    cmd, env=env, capture_output=True, text=True,
                    timeout=self.config.BACKUP_TIMEOUT
                )
                
                if sql_result.returncode == 0:
                    file_size = os.path.getsize(sql_file)
                    result['size'] += file_size
                    self.logger.success(f"SQL 备份成功: {file_size} bytes")
                    
                    if self.config.ENABLE_COMPRESSION:
                        sql_file = self.compress_file(str(sql_file), show_progress=True)
                    else:
                        self.checksum.calculate(str(sql_file))
                    
                    result['files'].append(sql_file)
                else:
                    self.logger.error(f"SQL 备份失败: {sql_result.stderr}")
            
            result['success'] = True
            self.logger.success(f"数据库 {database} 备份完成")
            
        except subprocess.TimeoutExpired:
            result['error'] = '备份超时'
            self.logger.error(f"备份超时: {database}")
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"备份异常: {database} - {e}")
        
        return result
    
    def verify_backup(self, backup_file: str, database: str) -> bool:
        try:
            self.logger.section(f"验证备份: {backup_file}")
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            verify_db = f'_verify_{timestamp}'
            
            if not self.conn.create_database(verify_db):
                self.logger.error("创建验证数据库失败")
                return False
            
            env = self.config.get_pg_env()
            
            actual_file = backup_file
            if backup_file.endswith('.gz'):
                subprocess.run(['gzip', '-d', '-k', backup_file], capture_output=True)
                actual_file = backup_file[:-3]
            
            try:
                if actual_file.endswith('.dump') or '.dump' in actual_file:
                    cmd = [
                        'pg_restore', '-h', self.config.PG_HOST, '-p', self.config.PG_PORT,
                        '-U', self.config.PG_USER, '-d', verify_db,
                        '--verbose', '--no-owner', '--no-acl', actual_file
                    ]
                else:
                    cmd = [
                        'psql', '-h', self.config.PG_HOST, '-p', self.config.PG_PORT,
                        '-U', self.config.PG_USER, '-d', verify_db, '-f', actual_file
                    ]
                
                result = subprocess.run(
                    cmd, env=env, capture_output=True, text=True, timeout=1800
                )
                
                if backup_file.endswith('.gz') and os.path.exists(actual_file):
                    os.remove(actual_file)
                
                has_fatal_error = False
                if result.returncode != 0:
                    for line in result.stderr.split('\n'):
                        if 'ERROR:' in line and 'already exists' not in line.lower():
                            has_fatal_error = True
                            break
                
                if not has_fatal_error:
                    self.logger.success("备份验证成功")
                    
                    cmd_count = [
                        'psql', '-h', self.config.PG_HOST, '-p', self.config.PG_PORT,
                        '-U', self.config.PG_USER, '-d', verify_db, '-t', '-c',
                        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';"
                    ]
                    count_result = subprocess.run(
                        cmd_count, env=env, capture_output=True, text=True, timeout=30
                    )
                    table_count = count_result.stdout.strip() if count_result.returncode == 0 else '未知'
                    self.logger.info(f"验证数据库表数量: {table_count}")
                    return True
                
                self.logger.error(f"备份验证失败: {result.stderr}")
                return False
                
            finally:
                cmd_drop = [
                    'psql', '-h', self.config.PG_HOST, '-p', self.config.PG_PORT,
                    '-U', self.config.PG_USER, '-d', 'postgres', '-c',
                    f'DROP DATABASE IF EXISTS "{verify_db}";'
                ]
                subprocess.run(cmd_drop, env=env, capture_output=True, text=True)
                self.logger.info(f"清理验证数据库: {verify_db}")
                
        except Exception as e:
            self.logger.error(f"验证异常: {e}")
            return False
    
    def cleanup_old_files(self, retention_days: int):
        self.logger.section("清理过期文件")
        
        cutoff_time = time.time() - (retention_days * 86400)
        
        for subdir in ['data', 'logs']:
            dir_path = Path(self.config.BACKUP_DIR) / subdir
            if not dir_path.exists():
                continue
            
            deleted_count = 0
            deleted_size = 0
            
            for file_path in dir_path.rglob('*'):
                if file_path.is_file():
                    try:
                        if file_path.stat().st_mtime < cutoff_time:
                            file_size = file_path.stat().st_size
                            file_path.unlink()
                            deleted_count += 1
                            deleted_size += file_size
                            self.logger.info(f"删除: {file_path.name}")
                    except Exception as e:
                        self.logger.warning(f"删除失败: {file_path} - {e}")
            
            for dir_path in dir_path.rglob('*'):
                if dir_path.is_dir() and not any(dir_path.iterdir()):
                    try:
                        dir_path.rmdir()
                        self.logger.info(f"删除空目录: {dir_path.name}")
                    except:
                        pass
            
            if deleted_count > 0:
                self.logger.success(
                    f"清理完成: 删除 {deleted_count} 个文件，释放 {deleted_size} bytes"
                )
            else:
                self.logger.info("无过期文件")
    
    def run_backup(self, verify: bool = False, parallel: bool = False) -> bool:
        start_time = datetime.now()
        
        self.logger.header("  备份任务开始")
        self.logger.info(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        self.config.print_config(self.logger)
        
        paths = self.config.ensure_dirs()
        backup_dir = str(paths['backup_data'])
        timestamp = self.config.get_timestamp()
        
        pg_dump_path = self.conn.get_pg_dump_path()
        if not pg_dump_path:
            return False
        
        databases = self.config.get_databases()
        
        if not self.conn.wait_for_startup(databases[0]):
            return False
        
        if not self.conn.check_version_compatibility(databases[0]):
            return False
        
        results = []
        backup_files = []
        success_count = 0
        total_size = 0
        
        enable_parallel = parallel or self.config.ENABLE_PARALLEL
        
        if enable_parallel and len(databases) > 1:
            self.logger.info(f"启用并发备份 (并发数: {self.config.BACKUP_PARALLEL_WORKERS})")
            
            with ThreadPoolExecutor(max_workers=self.config.BACKUP_PARALLEL_WORKERS) as executor:
                futures = {
                    executor.submit(
                        self.backup_single_database, db, backup_dir, timestamp, pg_dump_path
                    ): db for db in databases
                }
                
                for future in as_completed(futures):
                    db = futures[future]
                    try:
                        result = future.result()
                        results.append(result)
                        if result['success']:
                            success_count += 1
                            total_size += result['size']
                            backup_files.extend(result['files'])
                    except Exception as e:
                        self.logger.error(f"并发备份异常: {db} - {e}")
                        results.append({'database': db, 'success': False, 'error': str(e)})
        else:
            for database in databases:
                result = self.backup_single_database(
                    database, backup_dir, timestamp, pg_dump_path
                )
                results.append(result)
                if result['success']:
                    success_count += 1
                    total_size += result['size']
                    backup_files.extend(result['files'])
        
        if verify and backup_files:
            for backup_file in backup_files:
                if '.dump' in backup_file:
                    self.verify_backup(backup_file, databases[0])
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        self.logger.print_summary("备份任务完成", {
            '开始时间': start_time.strftime('%Y-%m-%d %H:%M:%S'),
            '结束时间': end_time.strftime('%Y-%m-%d %H:%M:%S'),
            '耗时': str(duration),
            '并发模式': '启用' if enable_parallel else '禁用',
            '成功数量': f"{success_count}/{len(databases)}",
            '文件数量': len(backup_files),
            '总大小': f"{total_size} bytes",
        })
        
        if backup_files:
            self.logger.print_list("备份文件列表", backup_files, 
                lambda i, f: f"  {i}. {Path(f).name} ({os.path.getsize(f)} bytes)")
        
        for result in results:
            if not result['success']:
                self.logger.warning(
                    f"数据库 {result['database']} 备份失败: {result.get('error', '未知')}"
                )
        
        if success_count == 0:
            self.logger.error("没有成功完成任何备份")
            return False
        
        self.cleanup_old_files(self.config.BACKUP_RETENTION_DAYS)
        
        return True
    
    def signal_handler(self, signum, frame):
        self.logger.info(f"收到信号 {signum}，准备退出...")
        self.shutdown_event.set()
    
    def run_scheduler(self):
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)
        
        self.logger.header("  PostgreSQL 备份服务启动")
        self.logger.info(f"备份时间: {self.config.BACKUP_TIME}")
        self.logger.info(f"备份间隔: {self.config.BACKUP_INTERVAL}")
        
        if self.config.BACKUP_INTERVAL == 'daily':
            schedule.every().day.at(self.config.BACKUP_TIME).do(self.run_backup)
            self.logger.info(f"已设置每日备份: {self.config.BACKUP_TIME}")
        elif self.config.BACKUP_INTERVAL == 'hourly':
            schedule.every().hour.do(self.run_backup)
            self.logger.info("已设置每小时备份")
        elif self.config.BACKUP_INTERVAL.isdigit():
            minutes = int(self.config.BACKUP_INTERVAL)
            schedule.every(minutes).minutes.do(self.run_backup)
            self.logger.info(f"已设置每 {minutes} 分钟备份")
        else:
            self.logger.error(f"不支持的备份间隔: {self.config.BACKUP_INTERVAL}")
            return
        
        self.logger.info("执行启动时备份...")
        self.run_backup()
        
        self.logger.info("进入定时任务循环...")
        while not self.shutdown_event.is_set():
            try:
                schedule.run_pending()
                self.shutdown_event.wait(60)
            except KeyboardInterrupt:
                break
            except Exception as e:
                self.logger.error(f"定时任务异常: {e}")
                time.sleep(60)
        
        self.logger.success("备份服务正常退出")


def get_backup_manager(config: Config = None) -> BackupManager:
    return BackupManager(config)