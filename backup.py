import os
import sys
import time
import gzip
import logging
import schedule
from datetime import datetime
import subprocess
import shutil
from pathlib import Path

def setup_logging(backup_dir):
    # 获取当前日期作为目录
    date_dir = datetime.now().strftime('%Y%m%d')
    log_dir = os.path.join(backup_dir, date_dir, 'logs')
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    # 生成日志文件名
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f'backup_{timestamp}.log')
    
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    logging.info(f"日志目录已初始化: {log_dir}")
    return log_dir

def compress_file(file_path):
    try:
        with open(file_path, 'rb') as f_in:
            gz_path = f'{file_path}.gz'
            with gzip.open(gz_path, 'wb') as f_out:
                f_out.writelines(f_in)
        os.remove(file_path)  # 删除原文件
        logging.info(f"文件 {file_path} 已成功压缩为 {gz_path}")
        return gz_path
    except Exception as e:
        logging.error(f'压缩文件 {file_path} 失败: {e}')
        return file_path

def get_env(key, default=None):
    return os.environ.get(key, default)

def create_backup():
    # 获取环境变量
    host = get_env('PG_HOST', 'localhost')
    port = get_env('PG_PORT', '5432')
    user = get_env('PG_USER', 'postgres')
    password = get_env('PG_PASSWORD', 'postgres')
    databases = get_env('PG_DATABASE', 'postgres').split(',')
    backup_dir = get_env('BACKUP_DIR', '/backups')
    retention_days = int(get_env('BACKUP_RETENTION_DAYS', '7'))
    enable_compression = get_env('ENABLE_COMPRESSION', 'true').lower() == 'true'

    # 设置日志，自动创建日期目录
    log_dir = setup_logging(backup_dir)
    
    # 确保备份根目录存在
    Path(backup_dir).mkdir(parents=True, exist_ok=True)
    
    # 获取当前日期作为备份目录
    date_dir = datetime.now().strftime('%Y%m%d')
    backup_date_dir = os.path.join(backup_dir, date_dir)
    Path(backup_date_dir).mkdir(parents=True, exist_ok=True)

    # 生成时间戳
    timestamp = datetime.now().strftime('%H%M%S')

    # 设置环境变量
    env = os.environ.copy()
    env['PGPASSWORD'] = password

    backup_files = []
    for database in databases:
        database = database.strip()
        if not database:
            continue

        try:
            # 为每个数据库生成备份文件名，存储在日期目录下
            backup_file = os.path.join(backup_date_dir, f'backup_{database}_{timestamp}.dump')

            # 检查pg_dump路径
            pg_dump_path = shutil.which('pg_dump')
            if not pg_dump_path:
                raise Exception('pg_dump命令未找到，请确保PostgreSQL客户端工具已安装')
                
            # 执行pg_dump
            cmd = [
                pg_dump_path,
                '-h', host,
                '-p', port,
                '-U', user,
                '-d', database,
                '-F', 'c',
                '-f', backup_file
            ]
            logging.info(f"开始备份数据库 {database} 到 {backup_file}")
            subprocess.run(cmd, env=env, check=True)
            
            # 压缩备份文件
            if enable_compression:
                backup_file = compress_file(backup_file)
                logging.info(f'数据库 {database} 备份已压缩: {backup_file}')
            
            logging.info(f'数据库 {database} 备份成功: {backup_file}')
            backup_files.append(backup_file)

        except subprocess.CalledProcessError as e:
            logging.error(f'数据库 {database} 备份失败: {e}')
            # 继续备份其他数据库
        except Exception as e:
            logging.error(f'数据库 {database} 处理过程中出错: {e}')

    if not backup_files:
        logging.error('没有成功完成任何数据库的备份')
        sys.exit(1)

    # 清理旧备份和日志
    cleanup_old_files(backup_dir, retention_days)
    cleanup_old_files(log_dir, retention_days, pattern='backup_*.log')

def cleanup_old_files(directory, retention_days, pattern='backup_*'):
    current_time = time.time()
    for file_path in Path(directory).rglob(pattern):  # 使用 rglob 处理嵌套目录
        file_time = file_path.stat().st_mtime
        if (current_time - file_time) > (retention_days * 86400):
            try:
                file_path.unlink()
                logging.info(f'已删除过期文件: {file_path}')
            except Exception as e:
                logging.error(f'删除文件失败 {file_path}: {e}')

def main():
    # 从环境变量获取定时配置
    backup_time = get_env('BACKUP_TIME', '03:00')  # 默认凌晨3点
    backup_interval = get_env('BACKUP_INTERVAL', 'daily')  # 默认每天执行

    # 根据配置设置定时任务
    if backup_interval == 'daily':
        schedule.every().day.at(backup_time).do(create_backup)
    elif backup_interval == 'hourly':
        schedule.every().hour.do(create_backup)
    elif backup_interval.isdigit():
        # 如果是数字，表示每隔多少分钟执行一次
        schedule.every(int(backup_interval)).minutes.do(create_backup)
    else:
        print(f'不支持的备份间隔配置: {backup_interval}')
        sys.exit(1)
    
    # 启动时执行一次备份
    logging.info("程序启动，执行首次备份...")
    create_backup()
    
    # 保持程序运行并执行定时任务
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == '__main__':
    main()