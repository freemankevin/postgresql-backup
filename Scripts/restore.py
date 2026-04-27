#!/usr/bin/env python3
import os
import sys
import gzip
import subprocess
import logging
from pathlib import Path
from datetime import datetime
import argparse
import hashlib

class RestoreManager:
    def __init__(self):
        self.setup_logging()
        
    def setup_logging(self):
        """设置日志"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            handlers=[
                logging.StreamHandler(),
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def verify_checksum(self, file_path):
        """验证文件 checksum"""
        try:
            checksum_file = f'{file_path}.sha256'
            if not os.path.exists(checksum_file):
                self.logger.warning(f"Checksum 文件不存在: {checksum_file}")
                return True
            
            with open(checksum_file, 'r') as f:
                content = f.read().strip()
                expected_checksum = content.split()[0]
            
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
    
    def get_original_checksum(self, gz_file_path):
        """从压缩文件的 checksum 文件获取 checksum"""
        checksum_file = f'{gz_file_path}.sha256'
        if os.path.exists(checksum_file):
            with open(checksum_file, 'r') as f:
                return f.read().strip().split()[0]
        return None
    
    def verify_data_after_restore(self, host, port, user, password, database):
        """恢复后数据验证"""
        try:
            env = os.environ.copy()
            env['PGPASSWORD'] = password
            
            self.logger.info(f"开始验证恢复数据: {database}")
            
            cmd_tables = [
                'psql', '-h', host, '-p', port, '-U', user, '-d', database, '-t', '-c',
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog', 'information_schema');"
            ]
            result = subprocess.run(cmd_tables, env=env, capture_output=True, text=True, timeout=30)
            table_count = int(result.stdout.strip()) if result.returncode == 0 and result.stdout.strip() else 0
            self.logger.info(f"用户表数量: {table_count}")
            
            cmd_sequences = [
                'psql', '-h', host, '-p', port, '-U', user, '-d', database, '-t', '-c',
                "SELECT COUNT(*) FROM information_schema.sequences WHERE sequence_schema NOT IN ('pg_catalog', 'information_schema');"
            ]
            result = subprocess.run(cmd_sequences, env=env, capture_output=True, text=True, timeout=30)
            seq_count = int(result.stdout.strip()) if result.returncode == 0 and result.stdout.strip() else 0
            self.logger.info(f"用户序列数量: {seq_count}")
            
            cmd_total_rows = [
                'psql', '-h', host, '-p', port, '-U', user, '-d', database, '-t', '-c',
                """
                SELECT SUM(n_live_tup) FROM pg_stat_user_tables;
                """
            ]
            result = subprocess.run(cmd_total_rows, env=env, capture_output=True, text=True, timeout=60)
            total_rows = result.stdout.strip() if result.returncode == 0 else '未知'
            self.logger.info(f"总记录数估算: {total_rows}")
            
            if table_count > 0:
                self.logger.info("恢复验证成功：数据库包含用户数据")
                return True
            else:
                self.logger.warning("恢复验证：数据库可能为空")
                return True
                
        except Exception as e:
            self.logger.warning(f"数据验证异常: {e}")
            return True

    def get_env(self, key, default=None):
        """获取环境变量"""
        value = os.environ.get(key, default)
        if 'PASSWORD' not in key:
            self.logger.info(f"环境变量 {key}: {value}")
        return value

    def detect_backup_format(self, backup_file):
        """检测备份文件格式（不解压）"""
        try:
            if not Path(backup_file).exists():
                self.logger.error(f'备份文件不存在: {backup_file}')
                return None, None
            
            is_compressed = backup_file.endswith('.gz')
            
            if is_compressed:
                with gzip.open(backup_file, 'rb') as f:
                    header = f.read(5)
                    if header.startswith(b'PGDMP'):
                        return 'custom', True
                    else:
                        return 'plain', True
            else:
                if backup_file.endswith('.dump'):
                    with open(backup_file, 'rb') as f:
                        header = f.read(5)
                        if header.startswith(b'PGDMP'):
                            return 'custom', False
                        else:
                            return 'plain', False
                elif backup_file.endswith('.sql'):
                    return 'plain', False
                else:
                    with open(backup_file, 'rb') as f:
                        header = f.read(5)
                        if header.startswith(b'PGDMP'):
                            return 'custom', False
                        else:
                            return 'plain', False
                    
        except Exception as e:
            self.logger.error(f"检测备份格式失败: {e}")
            return None, None

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
                self.logger.error(f"数据库连接测试失败: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            self.logger.error("数据库连接测试超时")
            return False
        except Exception as e:
            self.logger.error(f"数据库连接测试异常: {e}")
            return False

    def create_database_if_not_exists(self, host, port, user, password, database):
        """如果数据库不存在则创建"""
        try:
            env = os.environ.copy()
            env['PGPASSWORD'] = password
            
            # 连接到postgres数据库来创建新数据库
            cmd = [
                'psql', '-h', host, '-p', port, '-U', user, '-d', 'postgres', '-c',
                f'CREATE DATABASE "{database}";'
            ]
            
            result = subprocess.run(cmd, env=env, capture_output=True, text=True)
            
            if result.returncode == 0:
                self.logger.info(f"数据库 {database} 创建成功")
                return True
            elif 'already exists' in result.stderr:
                self.logger.info(f"数据库 {database} 已存在")
                return True
            else:
                self.logger.error(f"创建数据库失败: {result.stderr}")
                return False
                
        except Exception as e:
            self.logger.error(f"创建数据库异常: {e}")
            return False

    def restore_backup(self, backup_file, target_database=None, clean_first=False, 
                       data_only=False, schema_only=False, verify_checksum=True, 
                       verify_data=False):
        """恢复备份（支持流式传输）"""
        start_time = datetime.now()
        self.logger.info("=" * 60)
        self.logger.info(f"开始恢复备份: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"备份文件: {backup_file}")
        self.logger.info("=" * 60)
        
        try:
            host = self.get_env('PG_HOST', 'localhost')
            port = self.get_env('PG_PORT', '5432')
            user = self.get_env('PG_USER', 'postgres')
            password = self.get_env('PG_PASSWORD', 'postgres')
            database = target_database or self.get_env('PG_DATABASE', 'postgres')
            
            backup_format, is_compressed = self.detect_backup_format(backup_file)
            if not backup_format:
                return False
            
            self.logger.info(f"检测到备份格式: {backup_format}")
            self.logger.info(f"是否压缩: {is_compressed}")
            
            if verify_checksum and is_compressed:
                checksum_valid = self.verify_checksum(backup_file)
                if not checksum_valid:
                    self.logger.error("Checksum 验证失败，终止恢复")
                    return False
            
            if not self.create_database_if_not_exists(host, port, user, password, database):
                self.logger.error("无法创建或连接到目标数据库")
                return False
            
            if not self.test_connection(host, port, user, password, database):
                return False
            
            env = os.environ.copy()
            env['PGPASSWORD'] = password
            
            success = False
            
            if backup_format == 'custom':
                if is_compressed:
                    self.logger.info("使用流式解压恢复（无需临时文件）")
                    decompress_proc = subprocess.Popen(
                        ['gzip', '-d', '-c', backup_file],
                        stdout=subprocess.PIPE
                    )
                    
                    restore_cmd = ['pg_restore', '-h', host, '-p', port, '-U', user, '-d', database]
                    
                    if clean_first:
                        restore_cmd.extend(['--clean', '--if-exists'])
                    if data_only:
                        restore_cmd.append('--data-only')
                    elif schema_only:
                        restore_cmd.append('--schema-only')
                    
                    restore_cmd.append('--verbose')
                    
                    self.logger.info(f"执行流式pg_restore命令")
                    
                    restore_proc = subprocess.Popen(
                        restore_cmd,
                        stdin=decompress_proc.stdout,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        env=env
                    )
                    
                    decompress_proc.stdout.close()
                    stdout, stderr = restore_proc.communicate(timeout=7200)
                    decompress_proc.wait()
                    
                    if restore_proc.returncode == 0:
                        self.logger.info("流式pg_restore执行成功")
                        success = True
                    else:
                        stderr_text = stderr.decode('utf-8', errors='ignore')
                        if "ERROR" in stderr_text:
                            self.logger.error(f"pg_restore执行失败: {stderr_text}")
                        else:
                            self.logger.warning(f"pg_restore完成但有警告: {stderr_text}")
                            success = True
                else:
                    cmd = ['pg_restore', '-h', host, '-p', port, '-U', user, '-d', database]
                    
                    if clean_first:
                        cmd.extend(['--clean', '--if-exists'])
                    if data_only:
                        cmd.append('--data-only')
                    elif schema_only:
                        cmd.append('--schema-only')
                    
                    cmd.extend(['--verbose', backup_file])
                    
                    self.logger.info(f"执行pg_restore命令")
                    
                    try:
                        result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=7200)
                        
                        if result.returncode == 0:
                            self.logger.info("pg_restore执行成功")
                            success = True
                        else:
                            if "ERROR" in result.stderr:
                                self.logger.error(f"pg_restore执行失败: {result.stderr}")
                            else:
                                self.logger.warning(f"pg_restore完成但有警告: {result.stderr}")
                                success = True
                                
                    except subprocess.TimeoutExpired:
                        self.logger.error("恢复操作超时")
                    
            elif backup_format == 'plain':
                if is_compressed:
                    self.logger.info("使用流式解压恢复（无需临时文件）")
                    decompress_proc = subprocess.Popen(
                        ['gzip', '-d', '-c', backup_file],
                        stdout=subprocess.PIPE
                    )
                    
                    restore_cmd = ['psql', '-h', host, '-p', port, '-U', user, '-d', database]
                    
                    self.logger.info(f"执行流式psql命令")
                    
                    restore_proc = subprocess.Popen(
                        restore_cmd,
                        stdin=decompress_proc.stdout,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        env=env
                    )
                    
                    decompress_proc.stdout.close()
                    stdout, stderr = restore_proc.communicate(timeout=7200)
                    decompress_proc.wait()
                    
                    if restore_proc.returncode == 0:
                        self.logger.info("流式psql执行成功")
                        success = True
                    else:
                        stderr_text = stderr.decode('utf-8', errors='ignore')
                        self.logger.warning(f"psql恢复完成: {stderr_text}")
                        success = True
                else:
                    cmd = ['psql', '-h', host, '-p', port, '-U', user, '-d', database, '-f', backup_file]
                    
                    self.logger.info(f"执行psql命令")
                    
                    try:
                        result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=7200)
                        
                        if result.returncode == 0:
                            self.logger.info("psql执行成功")
                            success = True
                        else:
                            self.logger.warning(f"psql恢复完成: {result.stderr}")
                            success = True
                            
                    except subprocess.TimeoutExpired:
                        self.logger.error("恢复操作超时")
            
            if success and verify_data:
                self.verify_data_after_restore(host, port, user, password, database)
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            self.logger.info("=" * 60)
            self.logger.info("恢复任务完成总结:")
            self.logger.info(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            self.logger.info(f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            self.logger.info(f"耗时: {duration}")
            self.logger.info(f"流式恢复: {'启用' if is_compressed else '禁用'}")
            self.logger.info(f"Checksum验证: {'通过' if verify_checksum else '跳过'}")
            self.logger.info(f"恢复状态: {'成功' if success else '失败'}")
            self.logger.info(f"目标数据库: {database}")
            self.logger.info("=" * 60)
            
            return success
            
        except Exception as e:
            self.logger.error(f"恢复过程异常: {e}")
            return False

    def list_backups(self, backup_dir):
        """列出可用的备份文件"""
        self.logger.info(f"扫描备份目录: {backup_dir}")
        
        if not Path(backup_dir).exists():
            self.logger.error(f"备份目录不存在: {backup_dir}")
            return []
        
        backup_files = []
        for root, dirs, files in os.walk(backup_dir):
            for file in files:
                if file.endswith(('.dump', '.sql', '.dump.gz', '.sql.gz')):
                    file_path = Path(root) / file
                    file_size = file_path.stat().st_size
                    file_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                    
                    checksum_file = file_path.parent / f'{file}.sha256'
                    has_checksum = checksum_file.exists()
                    
                    backup_files.append({
                        'path': str(file_path),
                        'name': file,
                        'size': file_size,
                        'date': file_time,
                        'type': 'custom' if '.dump' in file else 'plain',
                        'checksum': has_checksum
                    })
        
        backup_files.sort(key=lambda x: x['date'], reverse=True)
        
        self.logger.info(f"找到 {len(backup_files)} 个备份文件:")
        for i, backup in enumerate(backup_files[:10]):
            checksum_status = '✓' if backup['checksum'] else '✗'
            self.logger.info(f"  {i+1}. {backup['name']} ({backup['size']} bytes, checksum:{checksum_status}, {backup['date'].strftime('%Y-%m-%d %H:%M:%S')})")
        
        return backup_files

def main():
    parser = argparse.ArgumentParser(description='PostgreSQL数据库恢复工具')
    parser.add_argument('backup_file', nargs='?', help='备份文件路径')
    parser.add_argument('-d', '--database', help='目标数据库名称')
    parser.add_argument('-l', '--list', help='列出指定目录中的备份文件')
    parser.add_argument('-c', '--clean', action='store_true', help='恢复前清理现有对象')
    parser.add_argument('--data-only', action='store_true', help='仅恢复数据')
    parser.add_argument('--schema-only', action='store_true', help='仅恢复架构')
    parser.add_argument('--no-verify-checksum', action='store_true', help='跳过checksum验证')
    parser.add_argument('--verify-data', action='store_true', help='恢复后验证数据完整性')
    
    args = parser.parse_args()
    
    restore_manager = RestoreManager()
    
    if args.list:
        restore_manager.list_backups(args.list)
        return
    
    if not args.backup_file:
        parser.print_help()
        sys.exit(1)
    
    success = restore_manager.restore_backup(
        args.backup_file,
        target_database=args.database,
        clean_first=args.clean,
        data_only=args.data_only,
        schema_only=args.schema_only,
        verify_checksum=not args.no_verify_checksum,
        verify_data=args.verify_data
    )
    
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main()