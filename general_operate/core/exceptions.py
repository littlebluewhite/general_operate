"""Unified exception handling for GeneralOperate"""

from dataclasses import dataclass
from enum import Enum
from typing import Any


class ErrorCode(Enum):
    """統一的錯誤代碼"""
    # 通用錯誤 (1000-1999)
    UNKNOWN_ERROR = 1000
    VALIDATION_ERROR = 1001
    CONFIGURATION_ERROR = 1002
    
    # 資料庫錯誤 (2000-2999)
    DB_CONNECTION_ERROR = 2000
    DB_QUERY_ERROR = 2001
    DB_TRANSACTION_ERROR = 2002
    
    # Cache 錯誤 (3000-3999)
    CACHE_CONNECTION_ERROR = 3000
    CACHE_KEY_ERROR = 3001
    CACHE_SERIALIZATION_ERROR = 3002
    
    # Kafka 錯誤 (4000-4999)
    KAFKA_CONNECTION_ERROR = 4000
    KAFKA_PRODUCER_ERROR = 4001
    KAFKA_CONSUMER_ERROR = 4002
    KAFKA_SERIALIZATION_ERROR = 4003
    KAFKA_CONFIGURATION_ERROR = 4004
    
    # InfluxDB 錯誤 (5000-5999)
    INFLUXDB_CONNECTION_ERROR = 5000
    INFLUXDB_WRITE_ERROR = 5001
    INFLUXDB_QUERY_ERROR = 5002


@dataclass
class ErrorContext:
    """錯誤上下文資訊"""
    operation: str
    resource: str | None = None
    details: dict[str, Any] | None = None
    
    
class GeneralOperateException(Exception):
    """基礎例外類別"""
    
    def __init__(
        self,
        code: ErrorCode,
        message: str,
        context: ErrorContext | None = None,
        cause: Exception | None = None
    ):
        self.code = code
        self.message = message
        self.context = context
        self.cause = cause
        super().__init__(self._build_message())
    
    def _build_message(self) -> str:
        """建構完整的錯誤訊息"""
        msg = f"[{self.code.name}] {self.message}"
        if self.context:
            msg += f" (Operation: {self.context.operation}"
            if self.context.resource:
                msg += f", Resource: {self.context.resource}"
            msg += ")"
        return msg
    
    def to_dict(self) -> dict[str, Any]:
        """轉換為字典格式，用於日誌記錄"""
        result: dict = {
            "error_code": self.code.value,
            "error_name": self.code.name,
            "message": self.message,
        }
        if self.context:
            result["context"] = {
                "operation": self.context.operation,
                "resource": self.context.resource,
                "details": self.context.details
            }
        if self.cause:
            result["cause"] = str(self.cause)
        return result


class DatabaseException(GeneralOperateException):
    """資料庫相關錯誤"""
    pass


class CacheException(GeneralOperateException):
    """快取相關錯誤"""
    pass


class KafkaException(GeneralOperateException):
    """Kafka 相關錯誤"""
    pass


class InfluxDBException(GeneralOperateException):
    """InfluxDB 相關錯誤"""
    pass


class ValidationException(GeneralOperateException):
    """驗證錯誤"""
    
    def __init__(self, field: str, value: Any, message: str):
        context = ErrorContext(
            operation="validation",
            details={"field": field, "value": value}
        )
        super().__init__(
            ErrorCode.VALIDATION_ERROR,
            message,
            context
        )