#!/usr/bin/env python3
import os
import sys
import gzip
import subprocess
import logging
from pathlib import Path
from datetime import datetime
import argparse

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

    def get_env(self, key, default=None):
        """获取环境变量"""
        value = os.environ.get(key, default)
        if 'PASSWORD' not in key:
            self.logger.info(f"环境变量 {key}: {value}")
        return value

    def decompress_file(self, gz_file_path):
        """解压.gz文件"""
        try:
            if not gz_file_path.endswith('.gz'):
                return gz_file_path
                
            decompressed_path = gz_file_path[:-3]  # 移除.gz后缀
            
            self.logger.info(f"解压文件: {gz_file_path} -> {decompressed_path}")
            
            with gzip.open(gz_file_path, 'rb') as f_in:
                with open(decompressed_path, 'wb') as f_out:
                    f_out.writelines(f_in)
            
            self.logger.info(f"文件解压成功: {decompressed_path}")
            return decompressed_path
            
        except Exception as e:
            self.logger.error(f"文件解压失败: {e}")
            return None

    def detect_backup_format(self, backup_file):
        """检测备份文件格式"""
        try:
            # 处理压缩文件
            original_file = backup_file
            if backup_file.endswith('.gz'):
                backup_file = self.decompress_file(backup_file)
                if not backup_file:
                    return None, None
            
            # 检查文件是否存在
            if not Path(backup_file).exists():
                self.logger.error(f'备份文件不存在: {backup_file}')
                return None, None
            
            # 通过文件扩展名判断格式
            if backup_file.endswith('.dump'):
                return 'custom', backup_file
            elif backup_file.endswith('.sql'):
                return 'plain', backup_file
            else:
                # 尝试通过文件内容判断
                try:
                    with open(backup_file, 'rb') as f:
                        header = f.read(10)
                        if header.startswith(b'PGDMP'):
                            return 'custom', backup_file
                        else:
                            return 'plain', backup_file
                except:
                    return 'plain', backup_file
                    
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

    def restore_backup(self, backup_file, target_database=None, clean_first=False, data_only=False, schema_only=False):
        """恢复备份"""
        start_time = datetime.now()
        self.logger.info("=" * 60)
        self.logger.info(f"开始恢复备份: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"备份文件: {backup_file}")
        self.logger.info("=" * 60)
        
        try:
            # 获取环境变量
            host = self.get_env('PG_HOST', 'localhost')
            port = self.get_env('PG_PORT', '5432')
            user = self.get_env('PG_USER', 'postgres')
            password = self.get_env('PG_PASSWORD', 'postgres')
            database = target_database or self.get_env('PG_DATABASE', 'postgres')
            
            # 检测备份格式
            backup_format, processed_file = self.detect_backup_format(backup_file)
            if not backup_format:
                return False
                
            self.logger.info(f"检测到备份格式: {backup_format}")
            self.logger.info(f"处理后的文件: {processed_file}")
            
            # 创建数据库（如果不存在）
            if not self.create_database_if_not_exists(host, port, user, password, database):
                self.logger.error("无法创建或连接到目标数据库")
                return False
            
            # 测试连接
            if not self.test_connection(host, port, user, password, database):
                return False
            
            # 设置环境变量
            env = os.environ.copy()
            env['PGPASSWORD'] = password
            
            success = False
            
            if backup_format == 'custom':
                # 使用pg_restore恢复自定义格式
                cmd = ['pg_restore', '-h', host, '-p', port, '-U', user, '-d', database, '-v']
                
                if clean_first:
                    cmd.append('-c')  # 清理现有对象
                if data_only:
                    cmd.append('-a')  # 仅数据
                elif schema_only:
                    cmd.append('-s')  # 仅架构
                    
                cmd.append(processed_file)
                
                self.logger.info(f"执行pg_restore命令: {' '.join(cmd[:-1])} [文件路径]")
                
                try:
                    result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=7200)
                    
                    if result.returncode == 0:
                        self.logger.info("pg_restore执行成功")
                        success = True
                    else:
                        # pg_restore可能会有警告但仍然成功
                        if "ERROR" in result.stderr:
                            self.logger.error(f"pg_restore执行失败: {result.stderr}")
                        else:
                            self.logger.warning(f"pg_restore完成但有警告: {result.stderr}")
                            success = True
                            
                except subprocess.TimeoutExpired:
                    self.logger.error("恢复操作超时")
                    
            elif backup_format == 'plain':
                # 使用psql恢复SQL格式
                cmd = ['psql', '-h', host, '-p', port, '-U', user, '-d', database, '-f', processed_file]
                
                self.logger.info(f"执行psql命令: {' '.join(cmd[:-1])} [文件路径]")
                
                try:
                    result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=7200)
                    
                    if result.returncode == 0:
                        self.logger.info("psql执行成功")
                        success = True
                    else:
                        self.logger.error(f"psql执行失败: {result.stderr}")
                        
                except subprocess.TimeoutExpired:
                    self.logger.error("恢复操作超时")
            
            # 清理临时解压文件
            if backup_file.endswith('.gz') and processed_file != backup_file:
                try:
                    os.remove(processed_file)
                    self.logger.info(f"已清理临时文件: {processed_file}")
                except:
                    pass
            
            # 恢复总结
            end_time = datetime.now()
            duration = end_time - start_time
            
            self.logger.info("=" * 60)
            self.logger.info("恢复任务完成总结:")
            self.logger.info(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            self.logger.info(f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            self.logger.info(f"耗时: {duration}")
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
                    
                    backup_files.append({
                        'path': str(file_path),
                        'name': file,
                        'size': file_size,
                        'date': file_time,
                        'type': 'custom' if '.dump' in file else 'plain'
                    })
        
        # 按日期排序
        backup_files.sort(key=lambda x: x['date'], reverse=True)
        
        self.logger.info(f"找到 {len(backup_files)} 个备份文件:")
        for i, backup in enumerate(backup_files[:10]):  # 只显示最新的10个
            self.logger.info(f"  {i+1}. {backup['name']} ({backup['size']} bytes, {backup['date'].strftime('%Y-%m-%d %H:%M:%S')})")
        
        return backup_files

def main():
    parser = argparse.ArgumentParser(description='PostgreSQL数据库恢复工具')
    parser.add_argument('backup_file', nargs='?', help='备份文件路径')
    parser.add_argument('-d', '--database', help='目标数据库名称')
    parser.add_argument('-l', '--list', help='列出指定目录中的备份文件')
    parser.add_argument('-c', '--clean', action='store_true', help='恢复前清理现有对象')
    parser.add_argument('--data-only', action='store_true', help='仅恢复数据')
    parser.add_argument('--schema-only', action='store_true', help='仅恢复架构')
    
    args = parser.parse_args()
    
    restore_manager = RestoreManager()
    
    # 列出备份文件
    if args.list:
        restore_manager.list_backups(args.list)
        return
    
    # 恢复备份
    if not args.backup_file:
        parser.print_help()
        sys.exit(1)
    
    success = restore_manager.restore_backup(
        args.backup_file,
        target_database=args.database,
        clean_first=args.clean,
        data_only=args.data_only,
        schema_only=args.schema_only
    )
    
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main()