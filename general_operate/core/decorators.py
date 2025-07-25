"""Error handling and utility decorators"""

import functools
import asyncio
import time
from collections.abc import Callable
from typing import TypeVar


import structlog

from .exceptions import GeneralOperateException, ErrorCode, ErrorContext


T = TypeVar('T')
logger = structlog.get_logger()


def handle_errors(
    operation: str,
    resource_getter: Callable[..., str] | None = None,
    default_error_code: ErrorCode = ErrorCode.UNKNOWN_ERROR,
    log_errors: bool = True,
    raise_on_error: bool = True
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    統一的錯誤處理裝飾器
    
    Args:
        operation: 操作名稱
        resource_getter: 用於獲取資源名稱的函數
        default_error_code: 預設錯誤代碼
        log_errors: 是否記錄錯誤
        raise_on_error: 是否重新拋出錯誤
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs) -> T:
            resource = None
            if resource_getter:
                resource = resource_getter(*args, **kwargs)
            
            try:
                return await func(*args, **kwargs)
            except GeneralOperateException:
                raise
            except Exception as e:
                context = ErrorContext(
                    operation=operation,
                    resource=resource,
                    details={"args": args, "kwargs": kwargs}
                )
                
                exc = GeneralOperateException(
                    code=default_error_code,
                    message=str(e),
                    context=context,
                    cause=e
                )
                
                if log_errors:
                    logger.error(
                        "operation_failed",
                        **exc.to_dict(),
                        exc_info=True
                    )
                
                if raise_on_error:
                    raise exc from e
                return None  # type: ignore
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs) -> T:
            resource = None
            if resource_getter:
                resource = resource_getter(*args, **kwargs)
            
            try:
                return func(*args, **kwargs)
            except GeneralOperateException:
                raise
            except Exception as e:
                context = ErrorContext(
                    operation=operation,
                    resource=resource,
                    details={"args": args, "kwargs": kwargs}
                )
                
                exc = GeneralOperateException(
                    code=default_error_code,
                    message=str(e),
                    context=context,
                    cause=e
                )
                
                if log_errors:
                    logger.error(
                        "operation_failed",
                        **exc.to_dict(),
                        exc_info=True
                    )
                
                if raise_on_error:
                    raise exc from e
                return None  # type: ignore
        
        # 根據函數是否為異步來選擇包裝器
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


def retry_on_error(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple[type[Exception], ...] = (Exception,)
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    重試裝飾器
    
    Args:
        max_attempts: 最大重試次數
        delay: 初始延遲時間（秒）
        backoff: 延遲時間的倍數
        exceptions: 需要重試的例外類型
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs) -> T:
            last_exception = None
            current_delay = delay
            
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        logger.warning(
                            "retry_attempt",
                            attempt=attempt + 1,
                            max_attempts=max_attempts,
                            delay=current_delay,
                            error=str(e)
                        )
                        await asyncio.sleep(current_delay)
                        current_delay *= backoff
            
            raise last_exception  # type: ignore
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs) -> T:
            last_exception = None
            current_delay = delay
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        logger.warning(
                            "retry_attempt",
                            attempt=attempt + 1,
                            max_attempts=max_attempts,
                            delay=current_delay,
                            error=str(e)
                        )
                        time.sleep(current_delay)
                        current_delay *= backoff
            
            raise last_exception  # type: ignore
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator