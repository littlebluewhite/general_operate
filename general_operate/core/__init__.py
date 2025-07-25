"""Core utilities for GeneralOperate"""

from .exceptions import (
    ErrorCode,
    ErrorContext,
    GeneralOperateException,
    DatabaseException,
    CacheException,
    KafkaException,
    InfluxDBException,
    ValidationException,
)
from .decorators import handle_errors, retry_on_error
from .logging import (
    LogLevel,
    configure_logging,
    get_logger,
    LogContext,
    with_request_id,
    log_performance,
)

__all__ = [
    # Exceptions
    "ErrorCode",
    "ErrorContext", 
    "GeneralOperateException",
    "DatabaseException",
    "CacheException",
    "KafkaException",
    "InfluxDBException",
    "ValidationException",
    # Decorators
    "handle_errors",
    "retry_on_error",
    # Logging
    "LogLevel",
    "configure_logging",
    "get_logger",
    "LogContext",
    "with_request_id",
    "log_performance",
]