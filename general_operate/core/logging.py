"""Unified logging configuration and utilities"""

import sys
import logging
import functools
import time
from enum import Enum
from pathlib import Path
from typing import Any, TypeVar, Callable
from contextvars import ContextVar

import structlog
import orjson


T = TypeVar('T')

# 請求 ID 上下文變數
request_id_var: ContextVar[str | None] = ContextVar('request_id', default=None)


class LogLevel(Enum):
    """日誌級別"""
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL


def configure_logging(
    level: LogLevel = LogLevel.INFO,
    log_file: Path | None = None,
    use_json: bool = False,
    add_timestamp: bool = True,
    add_caller_info: bool = True
) -> None:
    """
    配置統一的日誌系統
    
    Args:
        level: 日誌級別
        log_file: 日誌檔案路徑（可選）
        use_json: 是否使用 JSON 格式
        add_timestamp: 是否添加時間戳
        add_caller_info: 是否添加呼叫者資訊
    """
    # 共享的處理器
    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
    ]
    
    if add_timestamp:
        shared_processors.append(
            structlog.processors.TimeStamper(fmt="iso", utc=True)
        )
    
    if add_caller_info:
        shared_processors.append(
            structlog.processors.CallsiteParameterAdder(
                {
                    structlog.processors.CallsiteParameter.FILENAME,
                    structlog.processors.CallsiteParameter.FUNC_NAME,
                    structlog.processors.CallsiteParameter.LINENO,
                }
            )
        )
    
    # 根據環境選擇渲染器
    if use_json or not sys.stderr.isatty():
        # 生產環境或需要 JSON 格式
        processors = shared_processors + [
            structlog.processors.dict_tracebacks,
            structlog.processors.JSONRenderer(serializer=orjson.dumps),
        ]
    else:
        # 開發環境
        processors = shared_processors + [
            structlog.dev.ConsoleRenderer(
                colors=True,
                exception_formatter=structlog.dev.RichTracebackFormatter()
            ),
        ]
    
    # 配置 structlog
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(level.value),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    # 配置標準庫 logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=level.value,
    )
    
    # 如果需要寫入檔案
    if log_file:
        # 確保日誌目錄存在
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(level.value)
        
        # 檔案總是使用 JSON 格式
        file_processors = shared_processors + [
            structlog.processors.dict_tracebacks,
            structlog.processors.JSONRenderer(serializer=orjson.dumps),
        ]
        
        formatter = structlog.stdlib.ProcessorFormatter(
            processors=file_processors,
        )
        file_handler.setFormatter(formatter)
        
        logging.getLogger().addHandler(file_handler)


def get_logger(name: str | None = None) -> structlog.BoundLogger:
    """
    獲取日誌記錄器
    
    Args:
        name: 記錄器名稱
    
    Returns:
        配置好的日誌記錄器
    """
    if name:
        return structlog.get_logger(name)
    return structlog.get_logger()


class LogContext:
    """日誌上下文管理器"""
    
    def __init__(self, **kwargs: Any):
        self.context = kwargs
    
    def __enter__(self):
        structlog.contextvars.bind_contextvars(**self.context)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        structlog.contextvars.unbind_contextvars(*self.context.keys())


def with_request_id(request_id: str) -> LogContext:
    """
    添加請求 ID 到日誌上下文
    
    Args:
        request_id: 請求 ID
    
    Returns:
        日誌上下文管理器
    """
    request_id_var.set(request_id)
    return LogContext(request_id=request_id)


def log_performance(
    operation: str,
    threshold_ms: float = 1000.0
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    效能日誌裝飾器
    
    Args:
        operation: 操作名稱
        threshold_ms: 警告閾值（毫秒）
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs) -> T:
            logger = get_logger()
            
            start_time = time.perf_counter()
            
            try:
                result = await func(*args, **kwargs)
                duration_ms = (time.perf_counter() - start_time) * 1000
                
                log_method = logger.info
                if duration_ms > threshold_ms:
                    log_method = logger.warning
                
                log_method(
                    "operation_completed",
                    operation=operation,
                    duration_ms=round(duration_ms, 2),
                    slow=duration_ms > threshold_ms
                )
                
                return result
            except Exception as e:
                duration_ms = (time.perf_counter() - start_time) * 1000
                logger.error(
                    "operation_failed",
                    operation=operation,
                    duration_ms=round(duration_ms, 2),
                    error=str(e)
                )
                raise
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs) -> T:
            logger = get_logger()
            
            start_time = time.perf_counter()
            
            try:
                result = func(*args, **kwargs)
                duration_ms = (time.perf_counter() - start_time) * 1000
                
                log_method = logger.info
                if duration_ms > threshold_ms:
                    log_method = logger.warning
                
                log_method(
                    "operation_completed",
                    operation=operation,
                    duration_ms=round(duration_ms, 2),
                    slow=duration_ms > threshold_ms
                )
                
                return result
            except Exception as e:
                duration_ms = (time.perf_counter() - start_time) * 1000
                logger.error(
                    "operation_failed",
                    operation=operation,
                    duration_ms=round(duration_ms, 2),
                    error=str(e)
                )
                raise
        
        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator