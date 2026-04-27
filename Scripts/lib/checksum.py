import os
import hashlib
from pathlib import Path
from typing import Tuple, Optional

from .logger import get_logger


class ChecksumManager:
    def __init__(self):
        self.logger = get_logger()
    
    def calculate(self, file_path: str) -> Tuple[str, str]:
        try:
            if not os.path.exists(file_path):
                self.logger.error(f"文件不存在: {file_path}")
                return None, None
            
            self.logger.info(f"计算 checksum: {file_path}")
            
            sha256_hash = hashlib.sha256()
            file_size = os.path.getsize(file_path)
            
            with open(file_path, 'rb') as f:
                chunk_size = 8192
                if file_size > 100 * 1024 * 1024:
                    self.logger.info(f"大文件 ({file_size / (1024*1024):.2f} MB)，计算中...")
                    processed = 0
                    while True:
                        chunk = f.read(chunk_size)
                        if not chunk:
                            break
                        sha256_hash.update(chunk)
                        processed += len(chunk)
                        if processed % (50 * 1024 * 1024) == 0:
                            progress = (processed / file_size) * 100
                            self.logger.info(f"Checksum 进度: {progress:.1f}%")
                else:
                    while True:
                        chunk = f.read(chunk_size)
                        if not chunk:
                            break
                        sha256_hash.update(chunk)
            
            checksum = sha256_hash.hexdigest()
            checksum_file = f'{file_path}.sha256'
            
            with open(checksum_file, 'w') as f:
                f.write(f'{checksum}  {Path(file_path).name}\n')
            
            self.logger.success(f"Checksum: {checksum}")
            self.logger.info(f"Checksum 文件: {checksum_file}")
            
            return checksum, checksum_file
            
        except Exception as e:
            self.logger.error(f"Checksum 计算失败: {e}")
            return None, None
    
    def verify(self, file_path: str) -> bool:
        try:
            checksum_file = f'{file_path}.sha256'
            
            if not os.path.exists(checksum_file):
                self.logger.warning(f"Checksum 文件不存在: {checksum_file}")
                return True
            
            if not os.path.exists(file_path):
                self.logger.error(f"备份文件不存在: {file_path}")
                return False
            
            self.logger.info(f"验证 checksum: {file_path}")
            
            with open(checksum_file, 'r') as f:
                content = f.read().strip()
                expected = content.split()[0]
            
            sha256_hash = hashlib.sha256()
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(8192), b''):
                    sha256_hash.update(chunk)
            
            actual = sha256_hash.hexdigest()
            
            if actual == expected:
                self.logger.success("Checksum 验证通过")
                return True
            
            self.logger.error("Checksum 验证失败")
            self.logger.error(f"期望: {expected}")
            self.logger.error(f"实际: {actual}")
            return False
            
        except Exception as e:
            self.logger.error(f"Checksum 验证异常: {e}")
            return False
    
    def verify_gz_streaming(self, gz_file_path: str) -> bool:
        import gzip
        
        try:
            checksum_file = f'{gz_file_path}.sha256'
            
            if not os.path.exists(checksum_file):
                self.logger.warning(f"Checksum 文件不存在: {checksum_file}")
                return True
            
            if not os.path.exists(gz_file_path):
                self.logger.error(f"压缩文件不存在: {gz_file_path}")
                return False
            
            self.logger.info(f"流式验证 checksum: {gz_file_path}")
            
            with open(checksum_file, 'r') as f:
                content = f.read().strip()
                expected = content.split()[0]
            
            sha256_hash = hashlib.sha256()
            with gzip.open(gz_file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(8192), b''):
                    sha256_hash.update(chunk)
            
            actual = sha256_hash.hexdigest()
            
            if actual == expected:
                self.logger.success("Checksum 验证通过")
                return True
            
            self.logger.error("Checksum 验证失败")
            self.logger.error(f"期望: {expected}")
            self.logger.error(f"实际: {actual}")
            return False
            
        except Exception as e:
            self.logger.error(f"Checksum 流式验证异常: {e}")
            return False


def get_checksum_manager() -> ChecksumManager:
    return ChecksumManager()