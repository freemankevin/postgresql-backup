#!/usr/bin/env python3
"""
PostgreSQL 备份恢复工具主入口

用法:
    python main.py backup [--verify] [--parallel] [--once]
    python main.py restore <backup_file> [-d database] [--verify-data]
    python main.py list [--dir <backup_dir>]
    python main.py --help
"""

import os
import sys
import argparse
import signal

from lib.logger import get_logger, Logger
from lib.config import get_config, Config
from lib.backup import get_backup_manager, BackupManager
from lib.restore import get_restore_manager, RestoreManager


def setup_logger(config: Config) -> Logger:
    logger = get_logger()
    logger.setup(config.BACKUP_DIR, enable_color=True)
    return logger


def cmd_backup(args):
    config = get_config()
    logger = setup_logger(config)
    
    manager = get_backup_manager(config)
    
    if args.once:
        logger.header("  单次备份模式")
        success = manager.run_backup(verify=args.verify, parallel=args.parallel)
        sys.exit(0 if success else 1)
    else:
        manager.run_scheduler()


def cmd_restore(args):
    config = get_config()
    logger = setup_logger(config)
    
    if not args.backup_file:
        logger.error("请指定备份文件路径")
        sys.exit(1)
    
    manager = get_restore_manager(config)
    
    success = manager.restore_backup(
        args.backup_file,
        target_database=args.database,
        clean=args.clean,
        data_only=args.data_only,
        schema_only=args.schema_only,
        verify_checksum=not args.no_verify_checksum,
        verify_data=args.verify_data
    )
    
    sys.exit(0 if success else 1)


def cmd_list(args):
    config = get_config()
    logger = setup_logger(config)
    
    manager = get_restore_manager(config)
    
    backup_dir = args.dir or config.BACKUP_DIR
    manager.list_backups(backup_dir)


def main():
    parser = argparse.ArgumentParser(
        prog='pg_backup',
        description='PostgreSQL 备份恢复工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  启动定时备份服务:
    python main.py backup
  
  执行一次性备份:
    python main.py backup --once
  
  执行备份并验证:
    python main.py backup --once --verify
  
  恢复备份到指定数据库:
    python main.py restore /backups/data/20260427/postgres_20260427.dump.gz -d mydb
  
  恢复并验证数据:
    python main.py restore /backups/data/20260427/postgres_20260427.dump.gz --verify-data
  
  列出备份文件:
    python main.py list
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='子命令')
    
    backup_parser = subparsers.add_parser('backup', help='执行备份任务')
    backup_parser.add_argument('--once', action='store_true', help='仅执行一次备份')
    backup_parser.add_argument('--verify', action='store_true', help='备份后验证')
    backup_parser.add_argument('--parallel', action='store_true', help='启用并发备份')
    
    restore_parser = subparsers.add_parser('restore', help='恢复备份')
    restore_parser.add_argument('backup_file', nargs='?', help='备份文件路径')
    restore_parser.add_argument('-d', '--database', help='目标数据库')
    restore_parser.add_argument('-c', '--clean', action='store_true', help='恢复前清理')
    restore_parser.add_argument('--data-only', action='store_true', help='仅恢复数据')
    restore_parser.add_argument('--schema-only', action='store_true', help='仅恢复架构')
    restore_parser.add_argument('--no-verify-checksum', action='store_true', help='跳过checksum验证')
    restore_parser.add_argument('--verify-data', action='store_true', help='恢复后验证数据')
    
    list_parser = subparsers.add_parser('list', help='列出备份文件')
    list_parser.add_argument('--dir', help='备份目录路径')
    
    args = parser.parse_args()
    
    if args.command == 'backup':
        cmd_backup(args)
    elif args.command == 'restore':
        cmd_restore(args)
    elif args.command == 'list':
        cmd_list(args)
    else:
        parser.print_help()
        sys.exit(0)


if __name__ == '__main__':
    main()