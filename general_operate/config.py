"""Application configuration"""

import os
from pathlib import Path

from general_operate.core.logging import configure_logging, LogLevel


def setup_logging():
    """根據環境設定日誌"""
    env = os.getenv("ENVIRONMENT", "development")
    
    if env == "production":
        configure_logging(
            level=LogLevel.INFO,
            use_json=True,
            log_file=Path("/var/log/general_operate/app.log"),
            add_caller_info=False  # 生產環境可以關閉以提升效能
        )
    elif env == "staging":
        configure_logging(
            level=LogLevel.DEBUG,
            use_json=True,
            log_file=Path("logs/staging.log")
        )
    else:  # development
        configure_logging(
            level=LogLevel.DEBUG,
            use_json=False,  # 開發環境使用人類可讀格式
            add_caller_info=True
        )