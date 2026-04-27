import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from enum import Enum


class LogLevel(Enum):
    DEBUG = 'DEBUG'
    INFO = 'INFO'
    WARNING = 'WARNING'
    ERROR = 'ERROR'
    SUCCESS = 'SUCCESS'


class ColorCodes:
    RESET = '\033[0m'
    BOLD = '\033[1m'
    
    DEBUG = '\033[36m'
    INFO = '\033[37m'
    WARNING = '\033[33m'
    ERROR = '\033[31m'
    SUCCESS = '\033[32m'
    
    TIMESTAMP = '\033[90m'
    MODULE = '\033[34m'
    DATABASE = '\033[35m'
    FILE = '\033[36m'
    SIZE = '\033[33m'
    DURATION = '\033[32m'
    COUNT = '\033[35m'
    
    HEADER = '\033[1;36m'
    SEPARATOR = '\033[90m'


class ColoredFormatter(logging.Formatter):
    def format(self, record):
        if not hasattr(record, 'level_type'):
            record.level_type = record.levelname
        
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        colored_timestamp = f"{ColorCodes.TIMESTAMP}{timestamp}{ColorCodes.RESET}"
        
        level_color = self._get_level_color(record.levelname)
        colored_level = f"{level_color}{record.levelname:7}{ColorCodes.RESET}"
        
        colored_message = self._colorize_message(record.getMessage(), record.levelname)
        
        return f"{colored_timestamp} │ {colored_level} │ {colored_message}"
    
    def _get_level_color(self, level):
        colors = {
            'DEBUG': ColorCodes.DEBUG,
            'INFO': ColorCodes.INFO,
            'WARNING': ColorCodes.WARNING,
            'ERROR': ColorCodes.ERROR,
            'SUCCESS': ColorCodes.SUCCESS,
        }
        return colors.get(level, ColorCodes.INFO)
    
    def _colorize_message(self, message, level):
        message = self._highlight_patterns(message)
        
        if level == 'ERROR':
            return f"{ColorCodes.ERROR}{message}{ColorCodes.RESET}"
        elif level == 'WARNING':
            return f"{ColorCodes.WARNING}{message}{ColorCodes.RESET}"
        elif level == 'SUCCESS':
            return f"{ColorCodes.SUCCESS}{message}{ColorCodes.RESET}"
        
        return message
    
    def _highlight_patterns(self, message):
        patterns = {
            r'数据库\s+(\w+)': f'数据库 {ColorCodes.DATABASE}\\1{ColorCodes.RESET}',
            r'备份文件[:\s]+(.+\.gz)': f'备份文件: {ColorCodes.FILE}\\1{ColorCodes.RESET}',
            r'恢复文件[:\s]+(.+\.gz)': f'恢复文件: {ColorCodes.FILE}\\1{ColorCodes.RESET}',
            r'(\d+)\s*bytes': f'{ColorCodes.SIZE}\\1 bytes{ColorCodes.RESET}',
            r'(\d+)\s*MB': f'{ColorCodes.SIZE}\\1 MB{ColorCodes.RESET}',
            r'(\d+)\s*KB': f'{ColorCodes.SIZE}\\1 KB{ColorCodes.RESET}',
            r'耗时[:\s]+(\d+:\d+:\d+)': f'耗时: {ColorCodes.DURATION}\\1{ColorCodes.RESET}',
            r'成功[:\s]+(\d+)': f'成功: {ColorCodes.SUCCESS}\\1{ColorCodes.RESET}',
            r'失败[:\s]+(\d+)': f'失败: {ColorCodes.ERROR}\\1{ColorCodes.RESET}',
            r'数量[:\s]+(\d+)': f'数量: {ColorCodes.COUNT}\\1{ColorCodes.RESET}',
        }
        
        import re
        for pattern, replacement in patterns.items():
            message = re.sub(pattern, replacement, message)
        
        return message


class Logger:
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if Logger._initialized:
            return
        Logger._initialized = True
        
        self.logger = None
        self.log_dir = None
        self.log_file = None
        self.console_handler = None
        self.file_handler = None
    
    def setup(self, backup_dir: str, enable_color: bool = True) -> str:
        date_dir = datetime.now().strftime('%Y%m%d')
        self.log_dir = os.path.join(backup_dir, 'logs', date_dir)
        Path(self.log_dir).mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.log_file = os.path.join(self.log_dir, f'backup_{timestamp}.log')
        
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        self.logger = logging.getLogger('pg_backup')
        self.logger.setLevel(logging.INFO)
        self.logger.handlers = []
        
        if enable_color and sys.stdout.isatty():
            formatter = ColoredFormatter()
        else:
            formatter = logging.Formatter(
                '%(asctime)s │ %(levelname)-7s │ %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
        
        self.console_handler = logging.StreamHandler(sys.stdout)
        self.console_handler.setLevel(logging.INFO)
        self.console_handler.setFormatter(formatter)
        self.logger.addHandler(self.console_handler)
        
        file_formatter = logging.Formatter(
            '%(asctime)s │ %(levelname)-7s │ %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.file_handler = logging.FileHandler(self.log_file, encoding='utf-8')
        self.file_handler.setLevel(logging.INFO)
        self.file_handler.setFormatter(file_formatter)
        self.logger.addHandler(self.file_handler)
        
        self.info(f"日志系统初始化完成")
        self.info(f"日志文件: {self.log_file}")
        
        return self.log_dir
    
    def _log(self, level: str, message: str):
        if self.logger is None:
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} │ {level:7} │ {message}")
            return
        
        if level == 'SUCCESS':
            self.logger.info(message, extra={'level_type': 'SUCCESS'})
        else:
            method = getattr(self.logger, level.lower(), self.logger.info)
            method(message)
        
        self.flush()
    
    def debug(self, message: str):
        self._log('DEBUG', message)
    
    def info(self, message: str):
        self._log('INFO', message)
    
    def warning(self, message: str):
        self._log('WARNING', message)
    
    def error(self, message: str):
        self._log('ERROR', message)
    
    def success(self, message: str):
        self._log('SUCCESS', message)
    
    def header(self, message: str):
        separator = "─" * 60
        self.info(separator)
        self.info(message)
        self.info(separator)
    
    def section(self, title: str):
        self.info(f"\n{'─' * 20} {title} {'─' * 20}")
    
    def flush(self):
        if self.file_handler:
            self.file_handler.flush()
        if self.console_handler:
            self.console_handler.flush()
    
    def print_summary(self, title: str, items: dict):
        self.header(f"  {title}")
        max_key_len = max(len(k) for k in items.keys())
        for key, value in items.items():
            self.info(f"  {key:<{max_key_len}} : {value}")
        self.info("─" * 60)
    
    def print_list(self, title: str, items: list, formatter=None):
        self.info(f"\n{title}:")
        for i, item in enumerate(items, 1):
            if formatter:
                line = formatter(i, item)
            else:
                line = f"  {i}. {item}"
            self.info(line)


def get_logger() -> Logger:
    return Logger()