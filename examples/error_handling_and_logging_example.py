"""
Error handling and logging example for GeneralOperate

This example demonstrates:
1. Setting up logging configuration
2. Using error handling decorators
3. Structured logging with context
4. Custom exception handling
"""

import asyncio
from pathlib import Path

from general_operate.config import setup_logging
from general_operate.core import (
    configure_logging,
    get_logger,
    LogLevel,
    handle_errors,
    retry_on_error,
    log_performance,
    with_request_id,
    ErrorCode,
    ErrorContext,
    DatabaseException,
    CacheException,
)


# 設定日誌（使用環境配置）
setup_logging()

# 或手動配置
# configure_logging(
#     level=LogLevel.DEBUG,
#     use_json=False,
#     log_file=Path("logs/example.log")
# )

# 獲取 logger
logger = get_logger(__name__)


class ExampleService:
    """範例服務類別"""
    
    def __init__(self, db_name: str = "example_db"):
        self.db_name = db_name
        self.cache = {}
    
    @handle_errors(
        operation="db_query",
        resource_getter=lambda self, query, *args, **kwargs: self.db_name,
        default_error_code=ErrorCode.DB_QUERY_ERROR
    )
    @retry_on_error(max_attempts=3, exceptions=(DatabaseException,))
    @log_performance("database_query", threshold_ms=100)
    async def query_database(self, query: str) -> list[dict]:
        """模擬資料庫查詢"""
        logger.info("executing_query", query=query, database=self.db_name)
        
        # 模擬查詢延遲
        await asyncio.sleep(0.05)
        
        # 模擬偶爾的錯誤
        import random
        if random.random() < 0.3:
            raise DatabaseException(
                code=ErrorCode.DB_QUERY_ERROR,
                message="Connection timeout",
                context=ErrorContext(
                    operation="query",
                    resource=self.db_name,
                    details={"query": query}
                )
            )
        
        return [{"id": 1, "name": "Example"}]
    
    @handle_errors(
        operation="cache_get",
        default_error_code=ErrorCode.CACHE_KEY_ERROR
    )
    def get_from_cache(self, key: str) -> any:
        """從快取獲取資料"""
        logger.debug("cache_lookup", key=key)
        
        if key not in self.cache:
            raise CacheException(
                code=ErrorCode.CACHE_KEY_ERROR,
                message=f"Key '{key}' not found in cache",
                context=ErrorContext(
                    operation="get",
                    resource="memory_cache",
                    details={"key": key}
                )
            )
        
        return self.cache[key]
    
    @log_performance("cache_set")
    def set_cache(self, key: str, value: any) -> None:
        """設定快取"""
        logger.info("cache_set", key=key, value_type=type(value).__name__)
        self.cache[key] = value


async def main():
    """主函數"""
    # 使用請求 ID 上下文
    with with_request_id("req-123"):
        logger.info("application_started", version="1.0.0")
        
        service = ExampleService()
        
        # 測試成功的查詢
        try:
            result = await service.query_database("SELECT * FROM users")
            logger.info("query_success", result_count=len(result))
        except DatabaseException as e:
            logger.error("query_failed", error=e.to_dict())
        
        # 測試快取
        service.set_cache("user:1", {"id": 1, "name": "John"})
        
        try:
            user = service.get_from_cache("user:1")
            logger.info("cache_hit", user=user)
        except CacheException as e:
            logger.error("cache_miss", error=e.to_dict())
        
        # 測試不存在的快取鍵
        try:
            service.get_from_cache("user:999")
        except CacheException as e:
            logger.warning("expected_cache_miss", error=e.to_dict())
        
        logger.info("application_completed")


async def batch_processing_example():
    """批次處理範例"""
    logger = get_logger("batch_processor")
    
    # 使用結構化日誌記錄批次處理進度
    batch_id = "batch-001"
    total_items = 100
    
    with with_request_id(batch_id):
        logger.info(
            "batch_started",
            batch_id=batch_id,
            total_items=total_items
        )
        
        processed = 0
        errors = 0
        
        for i in range(total_items):
            try:
                # 模擬處理
                await asyncio.sleep(0.01)
                processed += 1
                
                # 每10筆記錄一次進度
                if processed % 10 == 0:
                    logger.info(
                        "batch_progress",
                        processed=processed,
                        total=total_items,
                        percentage=processed/total_items*100
                    )
            except Exception as e:
                errors += 1
                logger.error(
                    "item_processing_failed",
                    item_id=i,
                    error=str(e)
                )
        
        logger.info(
            "batch_completed",
            batch_id=batch_id,
            processed=processed,
            errors=errors,
            success_rate=(processed-errors)/total_items*100
        )


def sync_example():
    """同步函數範例"""
    logger = get_logger("sync_example")
    
    @handle_errors(operation="sync_operation")
    @log_performance("sync_task")
    def process_data(data: dict) -> dict:
        logger.info("processing_data", data_keys=list(data.keys()))
        
        # 模擬處理
        import time
        time.sleep(0.1)
        
        return {"processed": True, **data}
    
    # 測試同步函數
    result = process_data({"name": "test", "value": 42})
    logger.info("sync_result", result=result)


if __name__ == "__main__":
    # 執行主範例
    asyncio.run(main())
    
    # 執行批次處理範例
    asyncio.run(batch_processing_example())
    
    # 執行同步範例
    sync_example()