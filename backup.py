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
    log_dir = os.path.join(backup_dir, date_dir)
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
    # 强制刷新日志文件
    for handler in logging.getLogger().handlers:
        if isinstance(handler, logging.FileHandler):
            handler.flush()
    
    logging.info(f"日志文件已初始化: {log_file}")
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
    value = os.environ.get(key, default)
    logging.info(f"获取环境变量 {key}: {value}")
    return value

def create_backup():
    logging.info("开始执行备份任务...")
    
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
    logging.info(f"备份根目录已确保存在: {backup_dir}")
    
    # 获取当前日期作为备份目录
    date_dir = datetime.now().strftime('%Y%m%d')
    backup_date_dir = os.path.join(backup_dir, date_dir)
    Path(backup_date_dir).mkdir(parents=True, exist_ok=True)
    logging.info(f"备份日期目录已创建: {backup_date_dir}")

    # 生成时间戳（包含日期和时间）
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    logging.info(f"当前时间戳: {timestamp}")

    # 设置环境变量
    env = os.environ.copy()
    env['PGPASSWORD'] = password
    logging.info(f"已设置环境变量 PGPASSWORD")

    backup_files = []
    for database in databases:
        database = database.strip()
        if not database:
            logging.info("跳过空数据库名称")
            continue

        try:
            # 检查pg_dump路径
            pg_dump_path = shutil.which('pg_dump')
            if not pg_dump_path:
                raise Exception('pg_dump命令未找到，请确保PostgreSQL客户端工具已安装')
            logging.info(f"找到 pg_dump 命令: {pg_dump_path}")

            # 1. 生成 .dump 文件（自定义格式）
            dump_file = os.path.join(backup_date_dir, f'backup_{database}_{timestamp}.dump')
            logging.info(f"准备生成 .dump 备份文件: {dump_file}")
            dump_cmd = [
                pg_dump_path,
                '-h', host,
                '-p', port,
                '-U', user,
                '-d', database,
                '-F', 'c',  # 自定义格式
                '-f', dump_file
            ]
            logging.info(f"执行 .dump 备份命令: {' '.join(dump_cmd)}")
            dump_result = subprocess.run(dump_cmd, env=env, check=True, capture_output=True, text=True)
            logging.info(f"pg_dump (.dump) 输出: {dump_result.stdout}")

            # 压缩 .dump 文件
            if enable_compression:
                dump_file = compress_file(dump_file)
                logging.info(f'数据库 {database} 的 .dump 备份已压缩: {dump_file}')
            backup_files.append(dump_file)

            # 2. 生成 .sql 文件（纯文本 SQL 格式）
            sql_file = os.path.join(backup_date_dir, f'backup_{database}_{timestamp}.sql')
            logging.info(f"准备生成 .sql 备份文件: {sql_file}")
            sql_cmd = [
                pg_dump_path,
                '-h', host,
                '-p', port,
                '-U', user,
                '-d', database,
                '-f', sql_file  # 默认格式为纯文本 SQL
            ]
            logging.info(f"执行 .sql 备份命令: {' '.join(sql_cmd)}")
            sql_result = subprocess.run(sql_cmd, env=env, check=True, capture_output=True, text=True)
            logging.info(f"pg_dump (.sql) 输出: {sql_result.stdout}")

            # 可选：压缩 .sql 文件（如果需要）
            if enable_compression:
                sql_file = compress_file(sql_file)
                logging.info(f'数据库 {database} 的 .sql 备份已压缩: {sql_file}')
            backup_files.append(sql_file)

            logging.info(f'数据库 {database} 备份成功: {dump_file} 和 {sql_file}')

        except subprocess.CalledProcessError as e:
            logging.error(f'数据库 {database} 备份失败: {e}')
            logging.error(f'pg_dump 错误输出: {e.stderr}')
            # 继续备份其他数据库
        except Exception as e:
            logging.error(f'数据库 {database} 处理过程中出错: {e}')

        # 强制刷新日志
        for handler in logging.getLogger().handlers:
            if isinstance(handler, logging.FileHandler):
                handler.flush()

    if not backup_files:
        logging.error('没有成功完成任何数据库的备份')
        sys.exit(1)

    # 清理旧备份和日志
    logging.info("开始清理旧备份和日志...")
    cleanup_old_files(backup_dir, retention_days)
    cleanup_old_files(backup_dir, retention_days, pattern='backup_*.log')

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