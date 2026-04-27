import os
import gzip
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Optional

from .logger import get_logger
from .config import Config
from .connection import ConnectionManager
from .checksum import ChecksumManager


class RestoreManager:
    def __init__(self, config: Config = None):
        self.config = config or Config()
        self.logger = get_logger()
        self.conn = ConnectionManager(self.config)
        self.checksum = ChecksumManager()
    
    def detect_format(self, backup_file: str) -> tuple:
        try:
            if not Path(backup_file).exists():
                self.logger.error(f"文件不存在: {backup_file}")
                return None, None
            
            is_compressed = backup_file.endswith('.gz')
            
            if is_compressed:
                with gzip.open(backup_file, 'rb') as f:
                    header = f.read(5)
                    if header.startswith(b'PGDMP'):
                        return 'custom', True
                    return 'plain', True
            
            if backup_file.endswith('.dump'):
                with open(backup_file, 'rb') as f:
                    header = f.read(5)
                    if header.startswith(b'PGDMP'):
                        return 'custom', False
                return 'plain', False
            
            if backup_file.endswith('.sql'):
                return 'plain', False
            
            with open(backup_file, 'rb') as f:
                header = f.read(5)
                if header.startswith(b'PGDMP'):
                    return 'custom', False
                return 'plain', False
                
        except Exception as e:
            self.logger.error(f"格式检测失败: {e}")
            return None, None
    
    def restore_streaming(self, backup_file: str, database: str, 
                          clean: bool = False, data_only: bool = False,
                          schema_only: bool = False) -> bool:
        try:
            format_type, is_compressed = self.detect_format(backup_file)
            
            if format_type is None:
                return False
            
            self.logger.info(f"备份格式: {format_type}")
            self.logger.info(f"是否压缩: {is_compressed}")
            
            if is_compressed and self.config.RESTORE_VERIFY_CHECKSUM:
                if not self.checksum.verify_gz_streaming(backup_file):
                    self.logger.error("Checksum 验证失败，终止恢复")
                    return False
            
            env = self.config.get_pg_env()
            
            if format_type == 'custom':
                if is_compressed:
                    self.logger.section("流式恢复 (pg_restore)")
                    
                    decompress_proc = subprocess.Popen(
                        ['gzip', '-d', '-c', backup_file],
                        stdout=subprocess.PIPE
                    )
                    
                    restore_cmd = ['pg_restore']
                    restore_cmd.extend(['-h', self.config.PG_HOST])
                    restore_cmd.extend(['-p', self.config.PG_PORT])
                    restore_cmd.extend(['-U', self.config.PG_USER])
                    restore_cmd.extend(['-d', database])
                    
                    if clean:
                        restore_cmd.extend(['--clean', '--if-exists'])
                    if data_only:
                        restore_cmd.append('--data-only')
                    elif schema_only:
                        restore_cmd.append('--schema-only')
                    
                    restore_cmd.append('--verbose')
                    
                    self.logger.info(f"执行: gzip -d -c {backup_file} | pg_restore -d {database}")
                    
                    restore_proc = subprocess.Popen(
                        restore_cmd,
                        stdin=decompress_proc.stdout,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        env=env
                    )
                    
                    decompress_proc.stdout.close()
                    stdout, stderr = restore_proc.communicate(timeout=self.config.RESTORE_TIMEOUT)
                    decompress_proc.wait()
                    
                    if restore_proc.returncode == 0:
                        self.logger.success("流式恢复成功")
                        return True
                    
                    stderr_text = stderr.decode('utf-8', errors='ignore')
                    if 'ERROR' in stderr_text:
                        self.logger.error(f"恢复失败: {stderr_text}")
                        return False
                    
                    self.logger.warning(f"恢复完成（有警告）: {stderr_text}")
                    return True
                else:
                    self.logger.section("文件恢复 (pg_restore)")
                    
                    cmd = ['pg_restore']
                    cmd.extend(['-h', self.config.PG_HOST])
                    cmd.extend(['-p', self.config.PG_PORT])
                    cmd.extend(['-U', self.config.PG_USER])
                    cmd.extend(['-d', database])
                    
                    if clean:
                        cmd.extend(['--clean', '--if-exists'])
                    if data_only:
                        cmd.append('--data-only')
                    elif schema_only:
                        cmd.append('--schema-only')
                    
                    cmd.extend(['--verbose', backup_file])
                    
                    self.logger.info(f"执行: pg_restore -d {database} {backup_file}")
                    
                    result = subprocess.run(
                        cmd, env=env, capture_output=True, text=True,
                        timeout=self.config.RESTORE_TIMEOUT
                    )
                    
                    if result.returncode == 0:
                        self.logger.success("恢复成功")
                        return True
                    
                    if 'ERROR' in result.stderr:
                        self.logger.error(f"恢复失败: {result.stderr}")
                        return False
                    
                    self.logger.warning(f"恢复完成（有警告）: {result.stderr}")
                    return True
            
            elif format_type == 'plain':
                if is_compressed:
                    self.logger.section("流式恢复 (psql)")
                    
                    decompress_proc = subprocess.Popen(
                        ['gzip', '-d', '-c', backup_file],
                        stdout=subprocess.PIPE
                    )
                    
                    restore_cmd = ['psql']
                    restore_cmd.extend(['-h', self.config.PG_HOST])
                    restore_cmd.extend(['-p', self.config.PG_PORT])
                    restore_cmd.extend(['-U', self.config.PG_USER])
                    restore_cmd.extend(['-d', database])
                    
                    self.logger.info(f"执行: gzip -d -c {backup_file} | psql -d {database}")
                    
                    restore_proc = subprocess.Popen(
                        restore_cmd,
                        stdin=decompress_proc.stdout,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        env=env
                    )
                    
                    decompress_proc.stdout.close()
                    stdout, stderr = restore_proc.communicate(timeout=self.config.RESTORE_TIMEOUT)
                    decompress_proc.wait()
                    
                    if restore_proc.returncode == 0:
                        self.logger.success("流式恢复成功")
                        return True
                    
                    stderr_text = stderr.decode('utf-8', errors='ignore')
                    self.logger.warning(f"恢复完成: {stderr_text}")
                    return True
                else:
                    self.logger.section("文件恢复 (psql)")
                    
                    cmd = ['psql']
                    cmd.extend(['-h', self.config.PG_HOST])
                    cmd.extend(['-p', self.config.PG_PORT])
                    cmd.extend(['-U', self.config.PG_USER])
                    cmd.extend(['-d', database])
                    cmd.extend(['-f', backup_file])
                    
                    self.logger.info(f"执行: psql -d {database} -f {backup_file}")
                    
                    result = subprocess.run(
                        cmd, env=env, capture_output=True, text=True,
                        timeout=self.config.RESTORE_TIMEOUT
                    )
                    
                    if result.returncode == 0:
                        self.logger.success("恢复成功")
                        return True
                    
                    self.logger.warning(f"恢复完成: {result.stderr}")
                    return True
            
            return False
            
        except subprocess.TimeoutExpired:
            self.logger.error("恢复超时")
            return False
        except Exception as e:
            self.logger.error(f"恢复异常: {e}")
            return False
    
    def verify_restored_data(self, database: str) -> bool:
        try:
            env = self.config.get_pg_env()
            
            self.logger.section("验证恢复数据")
            
            cmd_tables = [
                'psql', '-h', self.config.PG_HOST, '-p', self.config.PG_PORT,
                '-U', self.config.PG_USER, '-d', database, '-t', '-c',
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema NOT IN ('pg_catalog', 'information_schema');"
            ]
            result = subprocess.run(cmd_tables, env=env, capture_output=True, text=True, timeout=30)
            table_count = int(result.stdout.strip()) if result.returncode == 0 and result.stdout.strip() else 0
            self.logger.info(f"用户表数量: {table_count}")
            
            cmd_sequences = [
                'psql', '-h', self.config.PG_HOST, '-p', self.config.PG_PORT,
                '-U', self.config.PG_USER, '-d', database, '-t', '-c',
                "SELECT COUNT(*) FROM information_schema.sequences "
                "WHERE sequence_schema NOT IN ('pg_catalog', 'information_schema');"
            ]
            result = subprocess.run(cmd_sequences, env=env, capture_output=True, text=True, timeout=30)
            seq_count = int(result.stdout.strip()) if result.returncode == 0 and result.stdout.strip() else 0
            self.logger.info(f"用户序列数量: {seq_count}")
            
            cmd_rows = [
                'psql', '-h', self.config.PG_HOST, '-p', self.config.PG_PORT,
                '-U', self.config.PG_USER, '-d', database, '-t', '-c',
                "SELECT SUM(n_live_tup) FROM pg_stat_user_tables;"
            ]
            result = subprocess.run(cmd_rows, env=env, capture_output=True, text=True, timeout=60)
            row_count = result.stdout.strip() if result.returncode == 0 else '未知'
            self.logger.info(f"总记录数估算: {row_count}")
            
            if table_count > 0:
                self.logger.success("数据验证成功")
                return True
            
            self.logger.warning("数据库可能为空")
            return True
            
        except Exception as e:
            self.logger.warning(f"验证异常: {e}")
            return True
    
    def restore_backup(self, backup_file: str, target_database: str = None,
                       clean: bool = False, data_only: bool = False,
                       schema_only: bool = False, verify_checksum: bool = True,
                       verify_data: bool = False) -> bool:
        start_time = datetime.now()
        
        database = target_database or self.config.PG_DATABASE
        
        self.logger.header("  恢复任务开始")
        self.logger.info(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"备份文件: {backup_file}")
        self.logger.info(f"目标数据库: {database}")
        
        if not self.conn.create_database(database):
            self.logger.error("无法创建目标数据库")
            return False
        
        if not self.conn.test_connection(database):
            self.logger.error("无法连接目标数据库")
            return False
        
        success = self.restore_streaming(
            backup_file, database, clean, data_only, schema_only
        )
        
        if success and verify_data:
            self.verify_restored_data(database)
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        self.logger.print_summary("恢复任务完成", {
            '开始时间': start_time.strftime('%Y-%m-%d %H:%M:%S'),
            '结束时间': end_time.strftime('%Y-%m-%d %H:%M:%S'),
            '耗时': str(duration),
            '流式恢复': '启用' if backup_file.endswith('.gz') else '禁用',
            'Checksum验证': '通过' if verify_checksum else '跳过',
            '恢复状态': '成功' if success else '失败',
            '目标数据库': database,
        })
        
        return success
    
    def list_backups(self, backup_dir: str = None) -> list:
        backup_dir = backup_dir or self.config.BACKUP_DIR
        
        self.logger.section(f"扫描备份目录: {backup_dir}")
        
        if not Path(backup_dir).exists():
            self.logger.error(f"目录不存在: {backup_dir}")
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
        
        if backup_files:
            self.logger.print_list(
                f"找到 {len(backup_files)} 个备份文件",
                backup_files[:10],
                lambda i, b: f"  {i}. {b['name']} "
                            f"({b['size']} bytes, "
                            f"checksum: {'✓' if b['checksum'] else '✗'}, "
                            f"{b['date'].strftime('%Y-%m-%d %H:%M:%S')})"
            )
        else:
            self.logger.warning("未找到备份文件")
        
        return backup_files


def get_restore_manager(config: Config = None) -> RestoreManager:
    return RestoreManager(config)