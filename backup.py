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

    def compress_file(self, file_path):
        """压缩文件"""
        try:
            if not os.path.exists(file_path):
                self.logger.error(f"要压缩的文件不存在: {file_path}")
                return file_path
                
            gz_path = f'{file_path}.gz'
            
            with open(file_path, 'rb') as f_in:
                with gzip.open(gz_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            # 验证压缩文件是否创建成功
            if os.path.exists(gz_path) and os.path.getsize(gz_path) > 0:
                os.remove(file_path)  # 删除原文件
                self.logger.info(f"文件压缩成功: {file_path} -> {gz_path}")
                return gz_path
            else:
                self.logger.error(f"压缩文件创建失败: {gz_path}")
                return file_path
                
        except Exception as e:
            self.logger.error(f'压缩文件失败 {file_path}: {e}')
            return file_path

    def get_env(self, key, default=None):
        """获取环境变量"""
        value = os.environ.get(key, default)
        if self.logger:
            self.logger.info(f"环境变量 {key}: {'***' if 'PASSWORD' in key else value}")
        return value

    def test_connection(self, host, port, user, password, database):
        """测试数据库连接"""
        try:
            env = os.environ.copy()
            env['PGPASSWORD'] = password
            
            cmd = ['psql', '-h', host, '-p', port, '-U', user, '-d', database, '-c', 'SELECT 1;']
            result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                self.logger.info(f"数据库连接测试成功: {database}")
                return True
            else:
                self.logger.error(f"数据库连接测试失败 {database}: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            self.logger.error(f"数据库连接测试超时: {database}")
            return False
        except Exception as e:
            self.logger.error(f"数据库连接测试异常 {database}: {e}")
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

    def create_backup(self):
        """执行备份任务"""
        start_time = datetime.now()
        self.logger.info("=" * 60)
        self.logger.info(f"备份任务开始: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("=" * 60)
        
        try:
            # 获取环境变量
            host = self.get_env('PG_HOST', 'localhost')
            port = self.get_env('PG_PORT', '5432')
            user = self.get_env('PG_USER', 'postgres')
            password = self.get_env('PG_PASSWORD', 'postgres')
            databases = [db.strip() for db in self.get_env('PG_DATABASE', 'postgres').split(',') if db.strip()]
            backup_dir = self.get_env('BACKUP_DIR', '/backups')
            retention_days = int(self.get_env('BACKUP_RETENTION_DAYS', '7'))
            enable_compression = self.get_env('ENABLE_COMPRESSION', 'true').lower() == 'true'
            backup_format = self.get_env('BACKUP_FORMAT', 'both')  # both, dump, sql

            # 确保备份根目录存在
            Path(backup_dir).mkdir(parents=True, exist_ok=True)
            self.logger.info(f"备份根目录: {backup_dir}")
            
            # 获取当前日期作为备份目录
            date_dir = datetime.now().strftime('%Y%m%d')
            backup_date_dir = os.path.join(backup_dir, 'data', date_dir)
            Path(backup_date_dir).mkdir(parents=True, exist_ok=True)
            self.logger.info(f"备份目标目录: {backup_date_dir}")

            # 生成时间戳
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # 检查pg_dump路径
            pg_dump_path = shutil.which('pg_dump')
            if not pg_dump_path:
                raise Exception('pg_dump命令未找到，请确保PostgreSQL客户端工具已安装')
            self.logger.info(f"使用pg_dump: {pg_dump_path}")

            # 设置环境变量
            env = os.environ.copy()
            env['PGPASSWORD'] = password

            backup_files = []
            success_count = 0
            total_size = 0
            
            for database in databases:
                if not database:
                    continue

                self.logger.info(f"\n开始备份数据库: {database}")
                
                # 测试连接
                if not self.test_connection(host, port, user, password, database):
                    self.logger.error(f"跳过数据库 {database}，连接失败")
                    continue
                
                # 获取数据库大小
                db_size = self.get_database_size(host, port, user, password, database)
                
                try:
                    # 根据配置决定备份格式
                    if backup_format in ['both', 'dump']:
                        # 生成 .dump 文件（自定义格式）
                        dump_file = os.path.join(backup_date_dir, f'{database}_{timestamp}.dump')
                        self.logger.info(f"创建dump格式备份: {dump_file}")
                        
                        dump_cmd = [
                            pg_dump_path, '-h', host, '-p', port, '-U', user,
                            '-d', database, '-F', 'c', '-b', '-v', '-f', dump_file
                        ]
                        
                        dump_result = subprocess.run(
                            dump_cmd, env=env, capture_output=True, text=True, timeout=3600
                        )
                        
                        if dump_result.returncode == 0:
                            file_size = os.path.getsize(dump_file)
                            total_size += file_size
                            self.logger.info(f"dump备份成功，文件大小: {file_size} bytes")
                            
                            if enable_compression:
                                dump_file = self.compress_file(dump_file)
                            backup_files.append(dump_file)
                        else:
                            self.logger.error(f"dump备份失败: {dump_result.stderr}")
                            continue

                    if backup_format in ['both', 'sql']:
                        # 生成 .sql 文件（纯文本格式）
                        sql_file = os.path.join(backup_date_dir, f'{database}_{timestamp}.sql')
                        self.logger.info(f"创建SQL格式备份: {sql_file}")
                        
                        sql_cmd = [
                            pg_dump_path, '-h', host, '-p', port, '-U', user,
                            '-d', database, '-b', '-v', '-f', sql_file
                        ]
                        
                        sql_result = subprocess.run(
                            sql_cmd, env=env, capture_output=True, text=True, timeout=3600
                        )
                        
                        if sql_result.returncode == 0:
                            file_size = os.path.getsize(sql_file)
                            total_size += file_size
                            self.logger.info(f"SQL备份成功，文件大小: {file_size} bytes")
                            
                            if enable_compression:
                                sql_file = self.compress_file(sql_file)
                            backup_files.append(sql_file)
                        else:
                            self.logger.error(f"SQL备份失败: {sql_result.stderr}")

                    success_count += 1
                    self.logger.info(f"数据库 {database} 备份完成")

                except subprocess.TimeoutExpired:
                    self.logger.error(f'数据库 {database} 备份超时')
                except Exception as e:
                    self.logger.error(f'数据库 {database} 备份异常: {e}')

                # 强制刷新日志
                for handler in self.logger.handlers:
                    if hasattr(handler, 'flush'):
                        handler.flush()

            # 备份总结
            end_time = datetime.now()
            duration = end_time - start_time
            
            self.logger.info("=" * 60)
            self.logger.info("备份任务完成总结:")
            self.logger.info(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            self.logger.info(f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            self.logger.info(f"耗时: {duration}")
            self.logger.info(f"成功备份数据库数量: {success_count}/{len(databases)}")
            self.logger.info(f"生成备份文件数量: {len(backup_files)}")
            self.logger.info(f"备份文件总大小: {total_size} bytes")
            
            if backup_files:
                self.logger.info("备份文件列表:")
                for file_path in backup_files:
                    file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
                    self.logger.info(f"  - {file_path} ({file_size} bytes)")
            
            if success_count == 0:
                self.logger.error('没有成功完成任何数据库的备份')
                return False

            # 清理旧文件
            self.logger.info("\n开始清理过期文件...")
            self.cleanup_old_files(os.path.join(backup_dir, 'data'), retention_days, 'backup')
            self.cleanup_old_files(os.path.join(backup_dir, 'logs'), retention_days, 'logs')
            
            self.logger.info("=" * 60)
            return True

        except Exception as e:
            self.logger.error(f"备份任务执行异常: {e}")
            return False
        finally:
            # 确保日志被写入
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
    backup_manager = BackupManager()
    backup_manager.run()

if __name__ == '__main__':
    main()