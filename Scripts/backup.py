import os
import sys
import time
import gzip
import logging
import schedule
from datetime import datetime, timedelta
import subprocess
import shutil
from pathlib import Path
import threading
import signal
import re
import hashlib
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

class BackupManager:
    def __init__(self):
        self.logger = None
        self.log_dir = None
        self.shutdown_event = threading.Event()
        
    def setup_logging(self, backup_dir):
        """设置日志配置"""
        # 获取当前日期作为目录
        date_dir = datetime.now().strftime('%Y%m%d')
        self.log_dir = os.path.join(backup_dir, 'logs', date_dir)
        Path(self.log_dir).mkdir(parents=True, exist_ok=True)
        
        # 生成日志文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = os.path.join(self.log_dir, f'backup_{timestamp}.log')
        
        # 清除现有的handlers
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # 创建自定义logger
        self.logger = logging.getLogger('backup_logger')
        self.logger.setLevel(logging.INFO)
        
        # 创建formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # 文件handler
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        
        # 控制台handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        
        # 添加handlers
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        # 确保立即写入
        for handler in self.logger.handlers:
            if isinstance(handler, logging.FileHandler):
                handler.flush()
        
        self.logger.info(f"日志系统已初始化，日志文件: {log_file}")
        return self.log_dir

    def get_version(self, command):
        """获取工具版本号"""
        try:
            result = subprocess.run([command, '--version'], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                # 提取版本号，如 "pg_dump (PostgreSQL) 18.1"
                match = re.search(r'(\d+)\.(\d+)', result.stdout)
                if match:
                    return int(match.group(1)), int(match.group(2))
            return None, None
        except Exception as e:
            self.logger.warning(f"无法获取 {command} 版本: {e}")
            return None, None

    def get_server_version(self, host, port, user, password, database):
        """获取PostgreSQL服务器版本"""
        try:
            env = os.environ.copy()
            env['PGPASSWORD'] = password
            
            cmd = [
                'psql', '-h', host, '-p', port, '-U', user, '-d', database, '-t', '-c',
                'SHOW server_version;'
            ]
            result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                version_str = result.stdout.strip()
                match = re.search(r'(\d+)\.(\d+)', version_str)
                if match:
                    major = int(match.group(1))
                    minor = int(match.group(2))
                    self.logger.info(f"PostgreSQL 服务器版本: {major}.{minor}")
                    return major, minor
            return None, None
        except Exception as e:
            self.logger.warning(f"无法获取服务器版本: {e}")
            return None, None

    def check_version_compatibility(self, host, port, user, password, database):
        """检查pg_dump与服务器版本兼容性"""
        try:
            # 获取pg_dump版本
            dump_major, dump_minor = self.get_version('pg_dump')
            if dump_major is None:
                self.logger.error("无法确定 pg_dump 版本")
                return False
            
            # 获取服务器版本
            server_major, server_minor = self.get_server_version(host, port, user, password, database)
            if server_major is None:
                self.logger.warning("无法确定服务器版本，将尝试继续备份")
                return True
            
            self.logger.info(f"版本检查: pg_dump {dump_major}.{dump_minor} vs PostgreSQL Server {server_major}.{server_minor}")
            
            # pg_dump 版本应该 >= 服务器版本
            if dump_major < server_major:
                self.logger.error(
                    f"版本不兼容: pg_dump {dump_major}.{dump_minor} 不能备份 PostgreSQL {server_major}.{server_minor} 服务器"
                )
                self.logger.error(
                    f"解决方案: 请升级 Dockerfile 中的 postgresql-client 版本至 postgresql-client-{server_major} 或更高"
                )
                return False
            elif dump_major == server_major and dump_minor < server_minor:
                self.logger.warning(
                    f"次版本号较低: pg_dump {dump_major}.{dump_minor} 备份 PostgreSQL {server_major}.{server_minor}，可能会有问题"
                )
                return True
            
            self.logger.info("版本兼容性检查通过")
            return True
            
        except Exception as e:
            self.logger.warning(f"版本兼容性检查异常: {e}，将尝试继续备份")
            return True

    def calculate_checksum(self, file_path):
        """计算文件 SHA256 checksum"""
        try:
            sha256_hash = hashlib.sha256()
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(8192), b''):
                    sha256_hash.update(chunk)
            
            checksum = sha256_hash.hexdigest()
            checksum_file = f'{file_path}.sha256'
            
            with open(checksum_file, 'w') as f:
                f.write(f'{checksum}  {Path(file_path).name}\n')
            
            self.logger.info(f"Checksum 计算完成: {checksum}")
            self.logger.info(f"Checksum 文件: {checksum_file}")
            return checksum, checksum_file
            
        except Exception as e:
            self.logger.error(f'Checksum 计算失败 {file_path}: {e}')
            return None, None
    
    def verify_checksum(self, file_path):
        """验证文件 checksum"""
        try:
            checksum_file = f'{file_path}.sha256'
            if not os.path.exists(checksum_file):
                self.logger.warning(f"Checksum 文件不存在: {checksum_file}")
                return True
            
            with open(checksum_file, 'r') as f:
                expected_checksum = f.read().split()[0]
            
            sha256_hash = hashlib.sha256()
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(8192), b''):
                    sha256_hash.update(chunk)
            
            actual_checksum = sha256_hash.hexdigest()
            
            if actual_checksum == expected_checksum:
                self.logger.info(f"Checksum 验证成功: {file_path}")
                return True
            else:
                self.logger.error(f"Checksum 验证失败: {file_path}")
                self.logger.error(f"期望: {expected_checksum}")
                self.logger.error(f"实际: {actual_checksum}")
                return False
                
        except Exception as e:
            self.logger.error(f'Checksum 验证异常 {file_path}: {e}')
            return False
    
    def compress_file(self, file_path, show_progress=False):
        """压缩文件，支持进度显示"""
        try:
            if not os.path.exists(file_path):
                self.logger.error(f"要压缩的文件不存在: {file_path}")
                return file_path, None
                
            gz_path = f'{file_path}.gz'
            file_size = os.path.getsize(file_path)
            
            if show_progress and file_size > 10 * 1024 * 1024:
                self.logger.info(f"压缩大文件 ({file_size / (1024*1024):.2f} MB): {file_path}")
            
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
                self.logger.info(f"文件压缩成功: {file_path} -> {gz_path}")
                checksum, checksum_file = self.calculate_checksum(gz_path)
                return gz_path, checksum
            else:
                self.logger.error(f"压缩文件创建失败: {gz_path}")
                return file_path, None
                
        except Exception as e:
            self.logger.error(f'压缩文件失败 {file_path}: {e}')
            return file_path, None

    def get_env(self, key, default=None):
        """获取环境变量"""
        value = os.environ.get(key, default)
        if self.logger:
            self.logger.info(f"环境变量 {key}: {'***' if 'PASSWORD' in key else value}")
        return value

    def test_connection(self, host, port, user, password, database, retries=3, retry_delay=5):
        """测试数据库连接，支持重试"""
        env = os.environ.copy()
        env['PGPASSWORD'] = password
        cmd = ['psql', '-h', host, '-p', port, '-U', user, '-d', database, '-c', 'SELECT 1;']
        
        for attempt in range(1, retries + 1):
            try:
                result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=30)
                
                if result.returncode == 0:
                    self.logger.info(f"数据库连接测试成功: {database}")
                    return True
                else:
                    self.logger.warning(f"数据库连接测试失败 {database} (尝试 {attempt}/{retries}): {result.stderr}")
                    if attempt < retries:
                        self.logger.info(f"等待 {retry_delay} 秒后重试...")
                        time.sleep(retry_delay)
                        
            except subprocess.TimeoutExpired:
                self.logger.warning(f"数据库连接测试超时 {database} (尝试 {attempt}/{retries})")
                if attempt < retries:
                    self.logger.info(f"等待 {retry_delay} 秒后重试...")
                    time.sleep(retry_delay)
            except Exception as e:
                self.logger.warning(f"数据库连接测试异常 {database} (尝试 {attempt}/{retries}): {e}")
                if attempt < retries:
                    self.logger.info(f"等待 {retry_delay} 秒后重试...")
                    time.sleep(retry_delay)
        
        self.logger.error(f"数据库连接测试最终失败: {database}，已重试 {retries} 次")
        return False
    
    def test_startup_connection(self, host, port, user, password, database, max_wait=60):
        """启动时等待数据库就绪"""
        env = os.environ.copy()
        env['PGPASSWORD'] = password
        cmd = ['psql', '-h', host, '-p', port, '-U', user, '-d', database, '-c', 'SELECT 1;']
        
        self.logger.info(f"等待数据库 {database} 就绪，最长等待 {max_wait} 秒...")
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            try:
                result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=10)
                if result.returncode == 0:
                    self.logger.info(f"数据库 {database} 已就绪，等待时间: {int(time.time() - start_time)} 秒")
                    return True
            except:
                pass
            time.sleep(2)
        
        self.logger.error(f"数据库 {database} 在 {max_wait} 秒内未能就绪")
        return False

    def verify_backup(self, backup_file, host, port, user, password, original_database):
        """验证备份文件可用性（恢复到临时数据库）"""
        try:
            self.logger.info(f"开始验证备份文件: {backup_file}")
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            verify_database = f'_verify_{timestamp}'
            
            env = os.environ.copy()
            env['PGPASSWORD'] = password
            
            cmd_create = [
                'psql', '-h', host, '-p', port, '-U', user, '-d', 'postgres', '-c',
                f'CREATE DATABASE "{verify_database}";'
            ]
            result = subprocess.run(cmd_create, env=env, capture_output=True, text=True, timeout=30)
            
            if result.returncode != 0 and 'already exists' not in result.stderr:
                self.logger.error(f"创建验证数据库失败: {result.stderr}")
                return False
            
            self.logger.info(f"创建临时验证数据库: {verify_database}")
            
            try:
                if backup_file.endswith('.gz'):
                    decompress_cmd = ['gzip', '-d', '-k', backup_file]
                    subprocess.run(decompress_cmd, capture_output=True, timeout=120)
                    actual_file = backup_file[:-3]
                else:
                    actual_file = backup_file
                
                if actual_file.endswith('.dump') or '.dump' in actual_file:
                    restore_cmd = [
                        'pg_restore', '-h', host, '-p', port, '-U', user,
                        '-d', verify_database, '-v', '--no-owner', '--no-acl',
                        actual_file
                    ]
                else:
                    restore_cmd = [
                        'psql', '-h', host, '-p', port, '-U', user,
                        '-d', verify_database, '-f', actual_file
                    ]
                
                result = subprocess.run(restore_cmd, env=env, capture_output=True, text=True, timeout=1800)
                
                if backup_file.endswith('.gz') and os.path.exists(actual_file):
                    os.remove(actual_file)
                
                if result.returncode == 0 or 'ERROR' not in result.stderr:
                    self.logger.info("备份验证成功：恢复到临时数据库完成")
                    
                    cmd_count = [
                        'psql', '-h', host, '-p', port, '-U', user, '-d', verify_database, '-t', '-c',
                        f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';"
                    ]
                    result = subprocess.run(cmd_count, env=env, capture_output=True, text=True, timeout=30)
                    table_count = result.stdout.strip() if result.returncode == 0 else '未知'
                    self.logger.info(f"验证数据库表数量: {table_count}")
                    
                    cmd_drop = [
                        'psql', '-h', host, '-p', port, '-U', user, '-d', 'postgres', '-c',
                        f'DROP DATABASE IF EXISTS "{verify_database}";'
                    ]
                    subprocess.run(cmd_drop, env=env, capture_output=True, text=True, timeout=30)
                    self.logger.info(f"清理验证数据库: {verify_database}")
                    
                    return True
                else:
                    self.logger.error(f"备份验证失败: {result.stderr}")
                    cmd_drop = [
                        'psql', '-h', host, '-p', port, '-U', user, '-d', 'postgres', '-c',
                        f'DROP DATABASE IF EXISTS "{verify_database}";'
                    ]
                    subprocess.run(cmd_drop, env=env, capture_output=True, text=True, timeout=30)
                    return False
                    
            except Exception as e:
                self.logger.error(f"验证过程异常: {e}")
                cmd_drop = [
                    'psql', '-h', host, '-p', port, '-U', user, '-d', 'postgres', '-c',
                    f'DROP DATABASE IF EXISTS "{verify_database}";'
                ]
                subprocess.run(cmd_drop, env=env, capture_output=True, text=True, timeout=30)
                return False
                
        except Exception as e:
            self.logger.error(f"验证备份异常: {e}")
            return False
    
    def get_database_size(self, host, port, user, password, database):
        """获取数据库大小"""
        try:
            env = os.environ.copy()
            env['PGPASSWORD'] = password
            
            cmd = [
                'psql', '-h', host, '-p', port, '-U', user, '-d', database, '-t', '-c',
                "SELECT pg_size_pretty(pg_database_size(current_database()));"
            ]
            result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                size = result.stdout.strip()
                self.logger.info(f"数据库 {database} 大小: {size}")
                return size
            else:
                self.logger.warning(f"无法获取数据库大小 {database}: {result.stderr}")
                return "未知"
                
        except Exception as e:
            self.logger.warning(f"获取数据库大小异常 {database}: {e}")
            return "未知"

    def backup_single_database(self, database, host, port, user, password, backup_date_dir, 
                               timestamp, pg_dump_path, backup_format, enable_compression, env):
        """备份单个数据库（支持并发调用）"""
        db_result = {
            'database': database,
            'success': False,
            'files': [],
            'size': 0,
            'error': None
        }
        
        db_logger_prefix = f"[{database}]"
        self.logger.info(f"{db_logger_prefix} 开始备份")
        
        if not self.test_connection(host, port, user, password, database):
            db_result['error'] = '连接失败'
            self.logger.error(f"{db_logger_prefix} 连接失败，跳过")
            return db_result
        
        db_size = self.get_database_size(host, port, user, password, database)
        
        try:
            if backup_format in ['both', 'dump']:
                dump_file = os.path.join(backup_date_dir, f'{database}_{timestamp}.dump')
                self.logger.info(f"{db_logger_prefix} 创建dump备份: {dump_file}")
                
                dump_cmd = [
                    pg_dump_path, '-h', host, '-p', port, '-U', user,
                    '-d', database, '-F', 'c', '-b', '-v', '-f', dump_file
                ]
                
                dump_result = subprocess.run(
                    dump_cmd, env=env, capture_output=True, text=True, timeout=3600
                )
                
                if dump_result.returncode == 0:
                    file_size = os.path.getsize(dump_file)
                    db_result['size'] += file_size
                    self.logger.info(f"{db_logger_prefix} dump备份成功: {file_size} bytes")
                    
                    if enable_compression:
                        dump_file, checksum = self.compress_file(dump_file, show_progress=True)
                    else:
                        checksum, _ = self.calculate_checksum(dump_file)
                    db_result['files'].append(dump_file)
                else:
                    self.logger.error(f"{db_logger_prefix} dump备份失败: {dump_result.stderr}")
            
            if backup_format in ['both', 'sql']:
                sql_file = os.path.join(backup_date_dir, f'{database}_{timestamp}.sql')
                self.logger.info(f"{db_logger_prefix} 创建SQL备份: {sql_file}")
                
                sql_cmd = [
                    pg_dump_path, '-h', host, '-p', port, '-U', user,
                    '-d', database, '-b', '-v', '-f', sql_file
                ]
                
                sql_result = subprocess.run(
                    sql_cmd, env=env, capture_output=True, text=True, timeout=3600
                )
                
                if sql_result.returncode == 0:
                    file_size = os.path.getsize(sql_file)
                    db_result['size'] += file_size
                    self.logger.info(f"{db_logger_prefix} SQL备份成功: {file_size} bytes")
                    
                    if enable_compression:
                        sql_file, checksum = self.compress_file(sql_file, show_progress=True)
                    else:
                        checksum, _ = self.calculate_checksum(sql_file)
                    db_result['files'].append(sql_file)
                else:
                    self.logger.error(f"{db_logger_prefix} SQL备份失败: {sql_result.stderr}")
            
            db_result['success'] = True
            self.logger.info(f"{db_logger_prefix} 备份完成")
            
        except subprocess.TimeoutExpired:
            db_result['error'] = '备份超时'
            self.logger.error(f"{db_logger_prefix} 备份超时")
        except Exception as e:
            db_result['error'] = str(e)
            self.logger.error(f"{db_logger_prefix} 备份异常: {e}")
        
        return db_result
    
    def create_backup(self, verify=False, parallel=False):
        """执行备份任务"""
        start_time = datetime.now()
        self.logger.info("=" * 60)
        self.logger.info(f"备份任务开始: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("=" * 60)
        
        try:
            host = self.get_env('PG_HOST', 'localhost')
            port = self.get_env('PG_PORT', '5432')
            user = self.get_env('PG_USER', 'postgres')
            password = self.get_env('PG_PASSWORD', 'postgres')
            databases = [db.strip() for db in self.get_env('PG_DATABASE', 'postgres').split(',') if db.strip()]
            backup_dir = self.get_env('BACKUP_DIR', '/backups')
            retention_days = int(self.get_env('BACKUP_RETENTION_DAYS', '7'))
            enable_compression = self.get_env('ENABLE_COMPRESSION', 'true').lower() == 'true'
            backup_format = self.get_env('BACKUP_FORMAT', 'both')
            max_workers = int(self.get_env('BACKUP_PARALLEL_WORKERS', '2'))
            enable_verify = self.get_env('ENABLE_VERIFY', 'false').lower() == 'true' or verify
            enable_parallel = self.get_env('ENABLE_PARALLEL', 'false').lower() == 'true' or parallel

            Path(backup_dir).mkdir(parents=True, exist_ok=True)
            self.logger.info(f"备份根目录: {backup_dir}")
            
            date_dir = datetime.now().strftime('%Y%m%d')
            backup_date_dir = os.path.join(backup_dir, 'data', date_dir)
            Path(backup_date_dir).mkdir(parents=True, exist_ok=True)
            self.logger.info(f"备份目标目录: {backup_date_dir}")

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            pg_dump_path = shutil.which('pg_dump')
            if not pg_dump_path:
                raise Exception('pg_dump命令未找到，请确保PostgreSQL客户端工具已安装')
            self.logger.info(f"使用pg_dump: {pg_dump_path}")

            if databases:
                if not self.test_startup_connection(host, port, user, password, databases[0]):
                    self.logger.error("数据库未就绪，备份任务终止")
                    return False
                if not self.check_version_compatibility(host, port, user, password, databases[0]):
                    self.logger.error("版本兼容性检查失败，备份任务终止")
                    return False

            env = os.environ.copy()
            env['PGPASSWORD'] = password

            backup_files = []
            success_count = 0
            total_size = 0
            db_results = []
            
            if enable_parallel and len(databases) > 1:
                self.logger.info(f"启用并发备份，最大并发数: {max_workers}")
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {
                        executor.submit(
                            self.backup_single_database, db, host, port, user, password,
                            backup_date_dir, timestamp, pg_dump_path, backup_format,
                            enable_compression, env
                        ): db for db in databases if db
                    }
                    
                    for future in as_completed(futures):
                        db = futures[future]
                        try:
                            result = future.result()
                            db_results.append(result)
                            if result['success']:
                                success_count += 1
                                total_size += result['size']
                                backup_files.extend(result['files'])
                        except Exception as e:
                            self.logger.error(f"并发备份异常 [{db}]: {e}")
                            db_results.append({'database': db, 'success': False, 'error': str(e)})
            else:
                for database in databases:
                    if not database:
                        continue
                    result = self.backup_single_database(
                        database, host, port, user, password,
                        backup_date_dir, timestamp, pg_dump_path, backup_format,
                        enable_compression, env
                    )
                    db_results.append(result)
                    if result['success']:
                        success_count += 1
                        total_size += result['size']
                        backup_files.extend(result['files'])

            for handler in self.logger.handlers:
                if hasattr(handler, 'flush'):
                    handler.flush()

            if enable_verify and backup_files:
                self.logger.info("\n开始备份验证...")
                for backup_file in backup_files:
                    if backup_file.endswith('.dump.gz') or backup_file.endswith('.dump'):
                        verify_result = self.verify_backup(
                            backup_file, host, port, user, password, databases[0] if databases else 'postgres'
                        )
                        if verify_result:
                            self.logger.info(f"验证成功: {backup_file}")
                        else:
                            self.logger.warning(f"验证失败: {backup_file}")

            end_time = datetime.now()
            duration = end_time - start_time
            
            self.logger.info("=" * 60)
            self.logger.info("备份任务完成总结:")
            self.logger.info(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            self.logger.info(f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            self.logger.info(f"耗时: {duration}")
            self.logger.info(f"并发模式: {'启用' if enable_parallel else '禁用'}")
            self.logger.info(f"成功备份数据库数量: {success_count}/{len(databases)}")
            self.logger.info(f"生成备份文件数量: {len(backup_files)}")
            self.logger.info(f"备份文件总大小: {total_size} bytes")
            
            if backup_files:
                self.logger.info("备份文件列表:")
                for file_path in backup_files:
                    file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
                    checksum_file = f'{file_path}.sha256'
                    checksum_status = '有' if os.path.exists(checksum_file) else '无'
                    self.logger.info(f"  - {file_path} ({file_size} bytes, checksum: {checksum_status})")
            
            for result in db_results:
                if not result['success']:
                    self.logger.warning(f"数据库 {result['database']} 备份失败: {result.get('error', '未知错误')}")
            
            if success_count == 0:
                self.logger.error('没有成功完成任何数据库的备份')
                return False

            self.logger.info("\n开始清理过期文件...")
            self.cleanup_old_files(os.path.join(backup_dir, 'data'), retention_days, 'backup')
            self.cleanup_old_files(os.path.join(backup_dir, 'logs'), retention_days, 'logs')
            
            self.logger.info("=" * 60)
            return True

        except Exception as e:
            self.logger.error(f"备份任务执行异常: {e}")
            return False
        finally:
            for handler in self.logger.handlers:
                if hasattr(handler, 'flush'):
                    handler.flush()

    def cleanup_old_files(self, directory, retention_days, file_type):
        """清理过期文件"""
        if not os.path.exists(directory):
            self.logger.info(f"清理目录不存在，跳过: {directory}")
            return
            
        current_time = time.time()
        cutoff_time = current_time - (retention_days * 86400)
        deleted_count = 0
        deleted_size = 0
        
        try:
            for root, dirs, files in os.walk(directory):
                # 删除过期文件
                for file in files:
                    file_path = Path(root) / file
                    try:
                        if file_path.stat().st_mtime < cutoff_time:
                            file_size = file_path.stat().st_size
                            file_path.unlink()
                            deleted_count += 1
                            deleted_size += file_size
                            self.logger.info(f'删除过期{file_type}文件: {file_path}')
                    except Exception as e:
                        self.logger.error(f'删除文件失败 {file_path}: {e}')
                
                # 删除空目录
                for dir_name in dirs:
                    dir_path = Path(root) / dir_name
                    try:
                        if dir_path.exists() and not any(dir_path.iterdir()):
                            dir_path.rmdir()
                            self.logger.info(f'删除空目录: {dir_path}')
                    except Exception as e:
                        self.logger.error(f'删除目录失败 {dir_path}: {e}')
            
            if deleted_count > 0:
                self.logger.info(f'清理完成: 删除了 {deleted_count} 个{file_type}文件，释放空间 {deleted_size} bytes')
            else:
                self.logger.info(f'没有需要清理的过期{file_type}文件')
                
        except Exception as e:
            self.logger.error(f'清理{file_type}文件时发生异常: {e}')

    def signal_handler(self, signum, frame):
        """信号处理器"""
        self.logger.info(f"收到信号 {signum}，准备退出...")
        self.shutdown_event.set()

    def run(self):
        """主运行方法"""
        # 注册信号处理器
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)
        
        # 获取配置
        backup_dir = self.get_env('BACKUP_DIR', '/backups')
        backup_time = self.get_env('BACKUP_TIME', '03:00')
        backup_interval = self.get_env('BACKUP_INTERVAL', 'daily')
        
        # 设置日志
        self.setup_logging(backup_dir)
        
        self.logger.info("PostgreSQL备份服务启动")
        self.logger.info(f"备份时间: {backup_time}")
        self.logger.info(f"备份间隔: {backup_interval}")
        
        # 配置定时任务
        try:
            if backup_interval == 'daily':
                schedule.every().day.at(backup_time).do(self.create_backup)
                self.logger.info(f"已设置每日备份任务，执行时间: {backup_time}")
            elif backup_interval == 'hourly':
                schedule.every().hour.do(self.create_backup)
                self.logger.info("已设置每小时备份任务")
            elif backup_interval.isdigit():
                minutes = int(backup_interval)
                schedule.every(minutes).minutes.do(self.create_backup)
                self.logger.info(f"已设置每 {minutes} 分钟备份任务")
            else:
                raise ValueError(f'不支持的备份间隔配置: {backup_interval}')
        except Exception as e:
            self.logger.error(f"配置定时任务失败: {e}")
            sys.exit(1)
        
        # 启动时执行一次备份
        self.logger.info("执行启动时备份...")
        self.create_backup()
        
        # 主循环
        self.logger.info("进入定时任务循环...")
        while not self.shutdown_event.is_set():
            try:
                schedule.run_pending()
                self.shutdown_event.wait(60)  # 等待60秒或直到收到停止信号
            except KeyboardInterrupt:
                break
            except Exception as e:
                self.logger.error(f"定时任务执行异常: {e}")
                time.sleep(60)
        
        self.logger.info("备份服务正常退出")

def main():
    parser = argparse.ArgumentParser(description='PostgreSQL数据库备份工具')
    parser.add_argument('--verify', action='store_true', help='备份后验证备份文件可用性')
    parser.add_argument('--parallel', action='store_true', help='启用并发备份（多数据库）')
    parser.add_argument('--once', action='store_true', help='仅执行一次备份，不进入定时循环')
    
    args = parser.parse_args()
    
    backup_manager = BackupManager()
    
    if args.once:
        backup_dir = os.environ.get('BACKUP_DIR', '/backups')
        backup_manager.setup_logging(backup_dir)
        success = backup_manager.create_backup(verify=args.verify, parallel=args.parallel)
        sys.exit(0 if success else 1)
    else:
        backup_manager.run()

if __name__ == '__main__':
    main()