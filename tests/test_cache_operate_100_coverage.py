"""
Comprehensive tests for cache_operate.py to achieve 100% coverage.
Tests all methods, exception handlers, and edge cases.
"""

import pytest
import asyncio
import json
import re
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone

import redis
from redis.exceptions import RedisError, ConnectionError as RedisConnectionError, TimeoutError as RedisTimeoutError, ResponseError as RedisResponseError

from general_operate.app.cache_operate import CacheOperate
from general_operate.core.exceptions import CacheException, ErrorCode, ErrorContext


class AsyncContextManager:
    """Mock async context manager for Redis pipeline."""
    def __init__(self, pipe_mock):
        self.pipe_mock = pipe_mock
    
    async def __aenter__(self):
        return self.pipe_mock
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return None

@pytest.fixture
def mock_redis():
    """Mock Redis client."""
    mock = AsyncMock()
    # Create a separate mock pipeline
    mock_pipe = AsyncMock()
    # Make pipeline() return a proper async context manager
    mock.pipeline = MagicMock(return_value=AsyncContextManager(mock_pipe))
    return mock


@pytest.fixture
def cache_operate(mock_redis):
    """CacheOperate instance with mocked Redis."""
    return CacheOperate(mock_redis)


class TestCacheOperateInit:
    """Test CacheOperate initialization."""
    
    def test_init(self):
        """Test CacheOperate initialization."""
        mock_redis = AsyncMock()
        cache = CacheOperate(mock_redis)
        
        assert cache.redis is mock_redis
        assert cache._CacheOperate__exc is CacheException
        assert hasattr(cache, 'logger')


class TestCacheOperateExceptionHandler:
    """Test exception handler decorator and all exception branches."""
    
    @pytest.mark.asyncio
    async def test_exception_handler_redis_connection_error(self, cache_operate):
        """Test exception handler for Redis ConnectionError."""
        
        # Create a test method with the decorator
        @cache_operate.exception_handler
        async def test_method(self):
            raise redis.ConnectionError("Connection failed")
        
        with pytest.raises(CacheException) as exc_info:
            await test_method(cache_operate)
        
        assert exc_info.value.code == ErrorCode.CACHE_CONNECTION_ERROR
        assert "Redis connection error" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_exception_handler_redis_timeout_error(self, cache_operate):
        """Test exception handler for Redis TimeoutError."""
        
        @cache_operate.exception_handler
        async def test_method(self):
            raise redis.TimeoutError("Timeout occurred")
        
        with pytest.raises(CacheException) as exc_info:
            await test_method(cache_operate)
        
        assert exc_info.value.code == ErrorCode.CACHE_CONNECTION_ERROR
        assert "Redis timeout error" in str(exc_info.value)
        assert exc_info.value.context.details["error_type"] == "timeout"
    
    @pytest.mark.asyncio
    async def test_exception_handler_redis_response_error(self, cache_operate):
        """Test exception handler for Redis ResponseError."""
        
        @cache_operate.exception_handler
        async def test_method(self):
            raise redis.ResponseError("Invalid response")
        
        with pytest.raises(CacheException) as exc_info:
            await test_method(cache_operate)
        
        assert exc_info.value.code == ErrorCode.CACHE_KEY_ERROR
        assert "Redis response error" in str(exc_info.value)
        assert exc_info.value.context.details["error_type"] == "response"
    
    @pytest.mark.asyncio
    async def test_exception_handler_redis_general_error_with_code(self, cache_operate):
        """Test exception handler for general RedisError with error code."""
        
        @cache_operate.exception_handler
        async def test_method(self):
            raise RedisError("Error 123 occurred")
        
        with pytest.raises(CacheException) as exc_info:
            await test_method(cache_operate)
        
        assert exc_info.value.code == ErrorCode.CACHE_CONNECTION_ERROR
        assert "Redis error" in str(exc_info.value)
        assert exc_info.value.context.details["redis_error_code"] == "123"
    
    @pytest.mark.asyncio
    async def test_exception_handler_redis_general_error_no_code(self, cache_operate):
        """Test exception handler for general RedisError without error code."""
        
        @cache_operate.exception_handler
        async def test_method(self):
            raise RedisError("Some redis error")
        
        with pytest.raises(CacheException) as exc_info:
            await test_method(cache_operate)
        
        assert exc_info.value.code == ErrorCode.CACHE_CONNECTION_ERROR
        assert "Redis error" in str(exc_info.value)
        assert exc_info.value.context.details["error_type"] == "redis_general"
        assert "redis_error_code" not in exc_info.value.context.details
    
    @pytest.mark.asyncio
    async def test_exception_handler_json_decode_error(self, cache_operate):
        """Test exception handler for JSON decode error."""
        
        @cache_operate.exception_handler
        async def test_method(self):
            raise json.JSONDecodeError("Invalid JSON", "test", 0)
        
        with pytest.raises(CacheException) as exc_info:
            await test_method(cache_operate)
        
        assert exc_info.value.code == ErrorCode.CACHE_SERIALIZATION_ERROR
        assert "JSON decode error" in str(exc_info.value)
        assert exc_info.value.context.details["error_type"] == "json_decode"
    
    @pytest.mark.asyncio
    async def test_exception_handler_value_error_with_sql(self, cache_operate):
        """Test exception handler for ValueError with SQL in message."""
        
        @cache_operate.exception_handler
        async def test_method(self):
            raise ValueError("SQL validation failed")
        
        with pytest.raises(CacheException) as exc_info:
            await test_method(cache_operate)
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "SQL operation validation error" in str(exc_info.value)
        assert exc_info.value.context.details["error_type"] == "sql_validation"
    
    @pytest.mark.asyncio
    async def test_exception_handler_type_error_with_sql(self, cache_operate):
        """Test exception handler for TypeError with SQL in message."""
        
        @cache_operate.exception_handler
        async def test_method(self):
            raise TypeError("SQL type error")
        
        with pytest.raises(CacheException) as exc_info:
            await test_method(cache_operate)
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "SQL operation validation error" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_exception_handler_attribute_error_with_sql(self, cache_operate):
        """Test exception handler for AttributeError with SQL in message."""
        
        @cache_operate.exception_handler
        async def test_method(self):
            raise AttributeError("SQL attribute error")
        
        with pytest.raises(CacheException) as exc_info:
            await test_method(cache_operate)
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "SQL operation validation error" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_exception_handler_cache_exception_passthrough(self, cache_operate):
        """Test exception handler passes through CacheException."""
        
        original_exc = CacheException(
            code=ErrorCode.CACHE_KEY_ERROR,
            message="Original error"
        )
        
        @cache_operate.exception_handler
        async def test_method(self):
            raise original_exc
        
        with pytest.raises(CacheException) as exc_info:
            await test_method(cache_operate)
        
        assert exc_info.value is original_exc
    
    @pytest.mark.asyncio
    async def test_exception_handler_unknown_error(self, cache_operate):
        """Test exception handler for unknown error."""
        
        @cache_operate.exception_handler
        async def test_method(self):
            raise RuntimeError("Unknown error")
        
        with pytest.raises(CacheException) as exc_info:
            await test_method(cache_operate)
        
        assert exc_info.value.code == ErrorCode.UNKNOWN_ERROR
        assert "Operation error" in str(exc_info.value)
        assert exc_info.value.context.details["error_type"] == "unexpected"
    
    def test_exception_handler_sync_wrapper(self, cache_operate):
        """Test exception handler sync wrapper."""
        
        @cache_operate.exception_handler
        def sync_method(self):
            raise ValueError("Sync error SQL")
        
        with pytest.raises(CacheException) as exc_info:
            sync_method(cache_operate)
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR


class TestCacheOperateGetCaches:
    """Test get_caches method."""
    
    @pytest.mark.asyncio
    async def test_get_caches_success(self, cache_operate, mock_redis):
        """Test successful batch cache retrieval."""
        # Setup the mock pipe that will be returned by the context manager
        mock_pipe = AsyncMock()
        mock_pipe.execute.return_value = ['{"id": 1}', '{"id": 2}', None]
        
        # Make sure the context manager returns our mock_pipe
        async_context = AsyncContextManager(mock_pipe)
        mock_redis.pipeline.return_value = async_context
        
        result = await cache_operate.get_caches("test", {"1", "2", "3"})
        
        assert len(result) == 2
        assert result[0] == {"id": 1}
        assert result[1] == {"id": 2}
        
        # Verify pipeline usage
        mock_redis.pipeline.assert_called_once_with(transaction=False)
        assert mock_pipe.get.call_count == 3
        mock_pipe.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_caches_json_decode_error(self, cache_operate, mock_redis):
        """Test get_caches with JSON decode error."""
        mock_context = mock_redis.pipeline(transaction=False)
        mock_pipe = mock_context.pipe_mock
        mock_pipe.execute.return_value = ['invalid json', '{"id": 2}']
        
        with patch.object(cache_operate.logger, 'warning') as mock_warning:
            result = await cache_operate.get_caches("test", {"1", "2"})
        
        assert len(result) == 1
        assert result[0] == {"id": 2}
        mock_warning.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_caches_empty_identifiers(self, cache_operate):
        """Test get_caches with empty identifiers."""
        result = await cache_operate.get_caches("test", set())
        assert result == []


class TestCacheOperateGetCache:
    """Test get_cache method."""
    
    @pytest.mark.asyncio
    async def test_get_cache_success(self, cache_operate, mock_redis):
        """Test successful single cache retrieval."""
        mock_redis.get.return_value = '{"id": 1, "name": "test"}'
        
        result = await cache_operate.get_cache("test", "1")
        
        assert result == {"id": 1, "name": "test"}
        mock_redis.get.assert_called_once_with("test:1")
    
    @pytest.mark.asyncio
    async def test_get_cache_not_found(self, cache_operate, mock_redis):
        """Test get_cache when key doesn't exist."""
        mock_redis.get.return_value = None
        
        result = await cache_operate.get_cache("test", "1")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_cache_json_decode_error(self, cache_operate, mock_redis):
        """Test get_cache with JSON decode error."""
        mock_redis.get.return_value = 'invalid json'
        
        with patch.object(cache_operate.logger, 'error') as mock_error:
            result = await cache_operate.get_cache("test", "1")
        
        assert result is None
        mock_error.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_cache_general_exception(self, cache_operate, mock_redis):
        """Test get_cache with general exception."""
        mock_redis.get.side_effect = Exception("General error")
        
        with patch.object(cache_operate.logger, 'error') as mock_error:
            result = await cache_operate.get_cache("test", "1")
        
        assert result is None
        mock_error.assert_called_once()


class TestCacheOperateStoreCaches:
    """Test store_caches method."""
    
    @pytest.mark.asyncio
    async def test_store_caches_success(self, cache_operate, mock_redis):
        """Test successful batch cache storage."""
        mock_context = mock_redis.pipeline(transaction=False)
        mock_pipe = mock_context.pipe_mock
        
        data = {"1": {"name": "test1"}, "2": {"name": "test2"}}
        
        with patch.object(CacheOperate, '_CacheOperate__store_cache_inner', new=AsyncMock(return_value=("key", 300, "serialized"))) as mock_inner:
            result = await cache_operate.store_caches("test", data, 300)
        
        assert result is True
        assert mock_inner.call_count == 2
        assert mock_pipe.setex.call_count == 2
        mock_pipe.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_store_caches_empty_data(self, cache_operate):
        """Test store_caches with empty data."""
        result = await cache_operate.store_caches("test", {})
        assert result is False


class TestCacheOperateStoreCache:
    """Test store_cache method."""
    
    @pytest.mark.asyncio
    async def test_store_cache_success(self, cache_operate, mock_redis):
        """Test successful single cache storage."""
        data = {"name": "test"}
        
        with patch.object(CacheOperate, '_CacheOperate__store_cache_inner', new=AsyncMock(return_value=("test:1", 300, '{"name":"test"}'))) as mock_inner:
            await cache_operate.store_cache("test", "1", data, 300)
        
        mock_inner.assert_called_once_with("test", "1", data, 300)
        mock_redis.setex.assert_called_once_with("test:1", 300, '{"name":"test"}')


class TestCacheOperateStoreCacheInner:
    """Test __store_cache_inner static method."""
    
    @pytest.mark.asyncio
    async def test_store_cache_inner_with_ttl(self):
        """Test __store_cache_inner with specified TTL."""
        data = {"name": "test"}
        
        key, ttl, serialized = await CacheOperate._CacheOperate__store_cache_inner("test", "1", data, 300)
        
        assert key == "test:1"
        assert ttl == 300
        
        # Parse serialized data to verify structure
        parsed = json.loads(serialized)
        assert parsed["name"] == "test"
        assert parsed["prefix"] == "test"
        assert parsed["_identifier"] == "1"
        assert "_created_at" in parsed
    
    @pytest.mark.asyncio
    async def test_store_cache_inner_random_ttl(self):
        """Test __store_cache_inner with random TTL."""
        data = {"name": "test"}
        
        key, ttl, serialized = await CacheOperate._CacheOperate__store_cache_inner("test", "1", data, None)
        
        assert key == "test:1"
        assert 2000 <= ttl <= 5000
        
        # Parse serialized data
        parsed = json.loads(serialized)
        assert parsed["name"] == "test"


class TestCacheOperateDeleteCaches:
    """Test delete_caches method."""
    
    @pytest.mark.asyncio
    async def test_delete_caches_success(self, cache_operate, mock_redis):
        """Test successful batch cache deletion."""
        mock_redis.delete.return_value = 2
        
        result = await cache_operate.delete_caches("test", {"1", "2"})
        
        assert result == 2
        mock_redis.delete.assert_called_once()
        # Check that the call was made with the correct keys
        call_args = mock_redis.delete.call_args[0]
        assert set(call_args) == {"test:1", "test:2"}
    
    @pytest.mark.asyncio
    async def test_delete_caches_empty_identifiers(self, cache_operate):
        """Test delete_caches with empty identifiers."""
        result = await cache_operate.delete_caches("test", set())
        assert result == 0


class TestCacheOperateDeleteCache:
    """Test delete_cache method."""
    
    @pytest.mark.asyncio
    async def test_delete_cache_success(self, cache_operate, mock_redis):
        """Test successful single cache deletion."""
        mock_redis.delete.return_value = 1
        
        result = await cache_operate.delete_cache("test", "1")
        
        assert result is True
        mock_redis.delete.assert_called_once_with("test:1")
    
    @pytest.mark.asyncio
    async def test_delete_cache_not_found(self, cache_operate, mock_redis):
        """Test delete_cache when key doesn't exist."""
        mock_redis.delete.return_value = 0
        
        result = await cache_operate.delete_cache("test", "1")
        
        assert result is False


class TestCacheOperateCacheExists:
    """Test cache_exists method."""
    
    @pytest.mark.asyncio
    async def test_cache_exists_true(self, cache_operate, mock_redis):
        """Test cache_exists when key exists."""
        mock_redis.exists.return_value = 1
        
        result = await cache_operate.cache_exists("test", "1")
        
        assert result is True
        mock_redis.exists.assert_called_once_with("test:1")
    
    @pytest.mark.asyncio
    async def test_cache_exists_false(self, cache_operate, mock_redis):
        """Test cache_exists when key doesn't exist."""
        mock_redis.exists.return_value = 0
        
        result = await cache_operate.cache_exists("test", "1")
        
        assert result is False


class TestCacheOperateCacheExtendTTL:
    """Test cache_extend_ttl method."""
    
    @pytest.mark.asyncio
    async def test_cache_extend_ttl_success(self, cache_operate, mock_redis):
        """Test successful TTL extension."""
        mock_redis.ttl.return_value = 100
        mock_redis.expire.return_value = True
        
        result = await cache_operate.cache_extend_ttl("test", "1", 50)
        
        assert result is True
        mock_redis.ttl.assert_called_once_with("test:1")
        mock_redis.expire.assert_called_once_with("test:1", 150)
    
    @pytest.mark.asyncio
    async def test_cache_extend_ttl_key_not_exists(self, cache_operate, mock_redis):
        """Test TTL extension when key doesn't exist."""
        mock_redis.ttl.return_value = -2  # Key doesn't exist
        
        result = await cache_operate.cache_extend_ttl("test", "1", 50)
        
        assert result is False
        mock_redis.expire.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_cache_extend_ttl_key_no_expiry(self, cache_operate, mock_redis):
        """Test TTL extension when key has no expiry."""
        mock_redis.ttl.return_value = -1  # Key exists but no expiry
        
        result = await cache_operate.cache_extend_ttl("test", "1", 50)
        
        assert result is False
        mock_redis.expire.assert_not_called()


class TestCacheOperateNullKeys:
    """Test null key operations."""
    
    @pytest.mark.asyncio
    async def test_set_null_key_success(self, cache_operate, mock_redis):
        """Test successful null key setting."""
        mock_redis.setex.return_value = True
        
        result = await cache_operate.set_null_key("test:1:null", 300)
        
        assert result is True
        mock_redis.setex.assert_called_once_with("test:1:null", 300, "1")
    
    @pytest.mark.asyncio
    async def test_set_null_key_default_expiry(self, cache_operate, mock_redis):
        """Test null key setting with default expiry."""
        mock_redis.setex.return_value = True
        
        result = await cache_operate.set_null_key("test:1:null")
        
        assert result is True
        mock_redis.setex.assert_called_once_with("test:1:null", 300, "1")
    
    @pytest.mark.asyncio
    async def test_delete_null_key_success(self, cache_operate, mock_redis):
        """Test successful null key deletion."""
        mock_redis.delete.return_value = 1
        
        result = await cache_operate.delete_null_key("test:1:null")
        
        assert result is True
        mock_redis.delete.assert_called_once_with("test:1:null")
    
    @pytest.mark.asyncio
    async def test_delete_null_key_not_found(self, cache_operate, mock_redis):
        """Test null key deletion when key doesn't exist."""
        mock_redis.delete.return_value = 0
        
        result = await cache_operate.delete_null_key("test:1:null")
        
        assert result is False


class TestCacheOperateHealthCheck:
    """Test health_check method."""
    
    @pytest.mark.asyncio
    async def test_health_check_success(self, cache_operate, mock_redis):
        """Test successful health check."""
        mock_redis.ping.return_value = True
        
        result = await cache_operate.health_check()
        
        assert result is True
        mock_redis.ping.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_health_check_connection_error(self, cache_operate, mock_redis):
        """Test health check with connection error."""
        mock_redis.ping.side_effect = redis.ConnectionError("Connection failed")
        
        with patch('structlog.get_logger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            result = await cache_operate.health_check()
        
        assert result is False
        mock_logger.warning.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_health_check_timeout_error(self, cache_operate, mock_redis):
        """Test health check with timeout error."""
        mock_redis.ping.side_effect = redis.TimeoutError("Timeout")
        
        with patch('structlog.get_logger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            result = await cache_operate.health_check()
        
        assert result is False
        mock_logger.warning.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_health_check_response_error(self, cache_operate, mock_redis):
        """Test health check with response error."""
        mock_redis.ping.side_effect = redis.ResponseError("Response error")
        
        with patch('structlog.get_logger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            result = await cache_operate.health_check()
        
        assert result is False
        mock_logger.warning.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_health_check_redis_error(self, cache_operate, mock_redis):
        """Test health check with general Redis error."""
        mock_redis.ping.side_effect = RedisError("Redis error")
        
        with patch('structlog.get_logger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            result = await cache_operate.health_check()
        
        assert result is False
        mock_logger.warning.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_health_check_unexpected_error(self, cache_operate, mock_redis):
        """Test health check with unexpected error."""
        mock_redis.ping.side_effect = Exception("Unexpected error")
        
        with patch('structlog.get_logger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            result = await cache_operate.health_check()
        
        assert result is False
        mock_logger.error.assert_called_once()


# Test coverage for edge cases and specific patterns
class TestCacheOperateEdgeCases:
    """Test edge cases and specific patterns."""
    
    @pytest.mark.asyncio
    async def test_regex_pattern_matching_error_code(self, cache_operate):
        """Test regex pattern matching for error codes in exception handler."""
        
        @cache_operate.exception_handler
        async def test_method(self):
            raise RedisError("Some error Error 456 details")
        
        with pytest.raises(CacheException) as exc_info:
            await test_method(cache_operate)
        
        assert exc_info.value.context.details["redis_error_code"] == "456"
    
    def test_exception_handler_function_detection(self, cache_operate):
        """Test that exception handler properly detects async vs sync functions."""
        
        @cache_operate.exception_handler
        async def async_func(self):
            return "async"
        
        @cache_operate.exception_handler
        def sync_func(self):
            return "sync"
        
        assert asyncio.iscoroutinefunction(async_func)
        assert not asyncio.iscoroutinefunction(sync_func)
    
    @pytest.mark.asyncio
    async def test_logger_error_called_in_exception_handler(self, cache_operate):
        """Test that logger.error is called in exception handler."""
        
        @cache_operate.exception_handler
        async def test_method(self):
            raise ValueError("Test error")
        
        with patch.object(cache_operate.logger, 'error') as mock_error:
            with pytest.raises(CacheException):
                await test_method(cache_operate)
        
        mock_error.assert_called_once()
        # Verify exc_info=True was passed
        call_args = mock_error.call_args
        assert call_args[1]['exc_info'] is True