import os
import sys
import subprocess
from pathlib import Path

def restore_backup(backup_file):
    # 获取环境变量
    host = os.environ.get('PG_HOST', 'localhost')
    port = os.environ.get('PG_PORT', '5432')
    user = os.environ.get('PG_USER')
    password = os.environ.get('PG_PASSWORD')
    database = os.environ.get('PG_DATABASE')

    if not Path(backup_file).exists():
        print(f'备份文件不存在: {backup_file}')
        sys.exit(1)

    # 设置环境变量
    env = os.environ.copy()
    env['PGPASSWORD'] = password

    try:
        # 执行pg_restore
        cmd = [
            'pg_restore',
            '-h', host,
            '-p', port,
            '-U', user,
            '-d', database,
            '-c',
            backup_file
        ]
        subprocess.run(cmd, env=env, check=True)
        print(f'恢复成功: {backup_file}')

    except subprocess.CalledProcessError as e:
        print(f'恢复失败: {e}')
        sys.exit(1)

def main():
    if len(sys.argv) != 2:
        print('使用方法: python3 restore.py <backup_file>')
        sys.exit(1)

    backup_file = sys.argv[1]
    restore_backup(backup_file)

if __name__ == '__main__':
    main()