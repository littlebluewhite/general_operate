"""Comprehensive unit tests for cache_operate.py to achieve 100% coverage."""

import pytest
import asyncio
import json
import random
from unittest.mock import AsyncMock, MagicMock, patch, call
from datetime import datetime, UTC
import redis
from redis import RedisError
import structlog

from general_operate.app.cache_operate import CacheOperate
from general_operate.core.exceptions import CacheException, ErrorCode, ErrorContext


@pytest.fixture
def mock_redis():
    """Mock Redis client."""
    mock = AsyncMock(spec=redis.asyncio.Redis)
    # Ensure all common Redis async methods are properly mocked
    mock.get = AsyncMock()
    mock.setex = AsyncMock()
    mock.delete = AsyncMock()
    mock.exists = AsyncMock()
    mock.keys = AsyncMock()
    mock.ttl = AsyncMock()
    mock.expire = AsyncMock()
    mock.ping = AsyncMock()
    mock.pipeline = MagicMock()
    return mock


@pytest.fixture
def cache_operate(mock_redis):
    """Create a CacheOperate instance for testing."""
    return CacheOperate(mock_redis)


class TestCacheOperateInit:
    """Test CacheOperate initialization."""
    
    def test_init(self, mock_redis):
        """Test initialization with Redis client."""
        cache_op = CacheOperate(mock_redis)
        
        assert cache_op.redis == mock_redis
        assert cache_op._CacheOperate__exc == CacheException
        assert hasattr(cache_op, 'logger')


class TestCacheOperateExceptionHandler:
    """Test exception handler functionality."""
    
    def test_exception_handler_decorator_async(self, cache_operate):
        """Test exception handler decorator on async function."""
        
        @CacheOperate.exception_handler
        async def test_async_func(self):
            raise redis.ConnectionError("Connection failed")
        
        with pytest.raises(CacheException) as exc_info:
            asyncio.run(test_async_func(cache_operate))
        
        assert exc_info.value.code == ErrorCode.CACHE_CONNECTION_ERROR
        assert "Redis connection error" in str(exc_info.value)
    
    def test_exception_handler_decorator_sync(self, cache_operate):
        """Test exception handler decorator on sync function."""
        
        @CacheOperate.exception_handler
        def test_sync_func(self):
            raise redis.TimeoutError("Timeout occurred")
        
        with pytest.raises(CacheException) as exc_info:
            test_sync_func(cache_operate)
        
        assert exc_info.value.code == ErrorCode.CACHE_CONNECTION_ERROR
        assert "Redis timeout error" in str(exc_info.value)
    
    def test_handle_connection_error(self, cache_operate):
        """Test handling of Redis connection errors."""
        
        @CacheOperate.exception_handler
        async def test_func(self):
            raise redis.ConnectionError("Connection failed")
        
        with pytest.raises(CacheException) as exc_info:
            asyncio.run(test_func(cache_operate))
        
        assert exc_info.value.code == ErrorCode.CACHE_CONNECTION_ERROR
        assert "Redis connection error" in str(exc_info.value)
        assert exc_info.value.context.operation == "cache_operation"
        assert exc_info.value.context.resource == "redis"
    
    def test_handle_timeout_error(self, cache_operate):
        """Test handling of Redis timeout errors."""
        
        @CacheOperate.exception_handler
        async def test_func(self):
            raise redis.TimeoutError("Operation timed out")
        
        with pytest.raises(CacheException) as exc_info:
            asyncio.run(test_func(cache_operate))
        
        assert exc_info.value.code == ErrorCode.CACHE_CONNECTION_ERROR
        assert "Redis timeout error" in str(exc_info.value)
        assert exc_info.value.context.details["error_type"] == "timeout"
    
    def test_handle_response_error(self, cache_operate):
        """Test handling of Redis response errors."""
        
        @CacheOperate.exception_handler
        async def test_func(self):
            raise redis.ResponseError("Invalid response")
        
        with pytest.raises(CacheException) as exc_info:
            asyncio.run(test_func(cache_operate))
        
        assert exc_info.value.code == ErrorCode.CACHE_KEY_ERROR
        assert "Redis response error" in str(exc_info.value)
        assert exc_info.value.context.details["error_type"] == "response"
    
    def test_handle_redis_error_with_code(self, cache_operate):
        """Test handling of generic Redis errors with error code."""
        
        @CacheOperate.exception_handler
        async def test_func(self):
            raise RedisError("Error 123 occurred")
        
        with pytest.raises(CacheException) as exc_info:
            asyncio.run(test_func(cache_operate))
        
        assert exc_info.value.code == ErrorCode.CACHE_CONNECTION_ERROR
        assert "Redis error" in str(exc_info.value)
        assert exc_info.value.context.details["redis_error_code"] == "123"
    
    def test_handle_redis_error_without_code(self, cache_operate):
        """Test handling of generic Redis errors without error code."""
        
        @CacheOperate.exception_handler
        async def test_func(self):
            raise RedisError("Generic redis error")
        
        with pytest.raises(CacheException) as exc_info:
            asyncio.run(test_func(cache_operate))
        
        assert exc_info.value.code == ErrorCode.CACHE_CONNECTION_ERROR
        assert "Redis error" in str(exc_info.value)
        assert exc_info.value.context.details["error_type"] == "redis_general"
    
    def test_handle_json_decode_error(self, cache_operate):
        """Test handling of JSON decode errors."""
        
        @CacheOperate.exception_handler
        async def test_func(self):
            raise json.JSONDecodeError("Invalid JSON", "doc", 0)
        
        with pytest.raises(CacheException) as exc_info:
            asyncio.run(test_func(cache_operate))
        
        assert exc_info.value.code == ErrorCode.CACHE_SERIALIZATION_ERROR
        assert "JSON decode error" in str(exc_info.value)
        assert exc_info.value.context.details["error_type"] == "json_decode"
    
    def test_handle_sql_validation_error(self, cache_operate):
        """Test handling of SQL-related validation errors."""
        
        @CacheOperate.exception_handler
        async def test_func(self):
            raise ValueError("SQL operation validation error")
        
        with pytest.raises(CacheException) as exc_info:
            asyncio.run(test_func(cache_operate))
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "SQL operation validation error" in str(exc_info.value)
        assert exc_info.value.context.details["error_type"] == "sql_validation"
    
    def test_handle_cache_exception_passthrough(self, cache_operate):
        """Test that CacheException is passed through unchanged."""
        original_exception = CacheException(
            code=ErrorCode.CACHE_KEY_ERROR,
            message="Original error",
            context=ErrorContext(operation="test")
        )
        
        @CacheOperate.exception_handler
        async def test_func(self):
            raise original_exception
        
        with pytest.raises(CacheException) as exc_info:
            asyncio.run(test_func(cache_operate))
        
        assert exc_info.value is original_exception
    
    def test_handle_unknown_error(self, cache_operate):
        """Test handling of unknown errors."""
        
        @CacheOperate.exception_handler
        async def test_func(self):
            raise RuntimeError("Unknown error")
        
        with pytest.raises(CacheException) as exc_info:
            asyncio.run(test_func(cache_operate))
        
        assert exc_info.value.code == ErrorCode.UNKNOWN_ERROR
        assert "Operation error" in str(exc_info.value)
        assert exc_info.value.context.details["error_type"] == "unexpected"


class TestCacheOperateGetCaches:
    """Test get_caches functionality."""
    
    @pytest.mark.asyncio
    async def test_get_caches_success(self, cache_operate):
        """Test successful get_caches operation."""
        identifiers = {"1", "2", "3"}
        mock_values = [
            '{"id": 1, "name": "item1"}',
            None,  # Missing value
            '{"id": 3, "name": "item3"}'
        ]
        
        mock_pipeline = AsyncMock()
        mock_pipeline.execute.return_value = mock_values
        cache_operate.redis.pipeline.return_value.__aenter__ = AsyncMock(return_value=mock_pipeline)
        cache_operate.redis.pipeline.return_value.__aexit__ = AsyncMock(return_value=None)
        
        result = await cache_operate.get_caches("prefix", identifiers)
        
        assert len(result) == 2  # Only non-None values
        assert {"id": 1, "name": "item1"} in result
        assert {"id": 3, "name": "item3"} in result
    
    @pytest.mark.asyncio
    async def test_get_caches_invalid_json(self, cache_operate):
        """Test get_caches with invalid JSON data."""
        identifiers = {"1", "2"}
        mock_values = [
            '{"id": 1, "name": "item1"}',
            'invalid json'  # Invalid JSON
        ]
        
        mock_pipeline = AsyncMock()
        mock_pipeline.execute.return_value = mock_values
        cache_operate.redis.pipeline.return_value.__aenter__ = AsyncMock(return_value=mock_pipeline)
        cache_operate.redis.pipeline.return_value.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(cache_operate.logger, 'warning') as mock_warning:
            result = await cache_operate.get_caches("prefix", identifiers)
        
        assert len(result) == 1  # Only valid JSON
        assert {"id": 1, "name": "item1"} in result
        mock_warning.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_caches_empty_identifiers(self, cache_operate):
        """Test get_caches with empty identifiers."""
        result = await cache_operate.get_caches("prefix", set())
        assert result == []
    
    @pytest.mark.asyncio
    async def test_get_caches_all_none_values(self, cache_operate):
        """Test get_caches when all values are None."""
        identifiers = {"1", "2"}
        mock_values = [None, None]
        
        mock_pipeline = AsyncMock()
        mock_pipeline.execute.return_value = mock_values
        cache_operate.redis.pipeline.return_value.__aenter__ = AsyncMock(return_value=mock_pipeline)
        cache_operate.redis.pipeline.return_value.__aexit__ = AsyncMock(return_value=None)
        
        result = await cache_operate.get_caches("prefix", identifiers)
        assert result == []


class TestCacheOperateGetCache:
    """Test get_cache functionality."""
    
    @pytest.mark.asyncio
    async def test_get_cache_success(self, cache_operate):
        """Test successful get_cache operation."""
        cache_data = {"id": 1, "name": "test", "_created_at": "2023-01-01T00:00:00Z"}
        cache_operate.redis.get = AsyncMock(return_value=json.dumps(cache_data))
        
        result = await cache_operate.get_cache("prefix", "123")
        
        assert result == cache_data
        cache_operate.redis.get.assert_called_once_with("prefix:123")
    
    @pytest.mark.asyncio
    async def test_get_cache_not_found(self, cache_operate):
        """Test get_cache when key not found."""
        cache_operate.redis.get = AsyncMock(return_value=None)
        
        result = await cache_operate.get_cache("prefix", "123")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_cache_json_decode_error(self, cache_operate):
        """Test get_cache with JSON decode error."""
        cache_operate.redis.get = AsyncMock(return_value="invalid json")
        
        with patch.object(cache_operate.logger, 'error') as mock_error:
            result = await cache_operate.get_cache("prefix", "123")
        
        assert result is None
        mock_error.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_cache_redis_error(self, cache_operate):
        """Test get_cache with Redis error."""
        cache_operate.redis.get = AsyncMock(side_effect=redis.ConnectionError("Connection failed"))
        
        with patch.object(cache_operate.logger, 'error') as mock_error:
            result = await cache_operate.get_cache("prefix", "123")
        
        assert result is None
        mock_error.assert_called_once()


class TestCacheOperateStoreCaches:
    """Test store_caches functionality."""
    
    @pytest.mark.asyncio
    async def test_store_caches_success(self, cache_operate):
        """Test successful store_caches operation."""
        data = {
            "1": {"id": 1, "name": "item1"},
            "2": {"id": 2, "name": "item2"}
        }
        
        mock_pipeline = AsyncMock()
        cache_operate.redis.pipeline.return_value.__aenter__ = AsyncMock(return_value=mock_pipeline)
        cache_operate.redis.pipeline.return_value.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(cache_operate, '_CacheOperate__store_cache_inner') as mock_inner:
            mock_inner.side_effect = [
                ("prefix:1", 3000, '{"id": 1, "name": "item1"}'),
                ("prefix:2", 3500, '{"id": 2, "name": "item2"}')
            ]
            
            result = await cache_operate.store_caches("prefix", data, ttl_seconds=300)
        
        assert result is True
        assert mock_inner.call_count == 2
        assert mock_pipeline.setex.call_count == 2
        mock_pipeline.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_store_caches_empty_data(self, cache_operate):
        """Test store_caches with empty data."""
        result = await cache_operate.store_caches("prefix", {})
        assert result is False
    
    @pytest.mark.asyncio
    async def test_store_caches_with_custom_ttl(self, cache_operate):
        """Test store_caches with custom TTL."""
        data = {"1": {"id": 1, "name": "item1"}}
        
        mock_pipeline = AsyncMock()
        cache_operate.redis.pipeline.return_value.__aenter__ = AsyncMock(return_value=mock_pipeline)
        cache_operate.redis.pipeline.return_value.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(cache_operate, '_CacheOperate__store_cache_inner') as mock_inner:
            mock_inner.return_value = ("prefix:1", 600, '{"id": 1, "name": "item1"}')
            
            result = await cache_operate.store_caches("prefix", data, ttl_seconds=600)
        
        assert result is True
        mock_inner.assert_called_once_with("prefix", "1", {"id": 1, "name": "item1"}, 600)


class TestCacheOperateStoreCache:
    """Test store_cache functionality."""
    
    @pytest.mark.asyncio
    async def test_store_cache_success(self, cache_operate):
        """Test successful store_cache operation."""
        data = {"id": 1, "name": "test"}
        
        with patch.object(cache_operate, '_CacheOperate__store_cache_inner') as mock_inner:
            mock_inner.return_value = ("prefix:123", 3000, '{"id": 1, "name": "test"}')
            
            await cache_operate.store_cache("prefix", "123", data, 300)
        
        cache_operate.redis.setex.assert_called_once_with("prefix:123", 3000, '{"id": 1, "name": "test"}')
        mock_inner.assert_called_once_with("prefix", "123", data, 300)
    
    @pytest.mark.asyncio
    async def test_store_cache_no_ttl(self, cache_operate):
        """Test store_cache without TTL."""
        data = {"id": 1, "name": "test"}
        
        with patch.object(cache_operate, '_CacheOperate__store_cache_inner') as mock_inner:
            mock_inner.return_value = ("prefix:123", 2500, '{"id": 1, "name": "test"}')
            
            await cache_operate.store_cache("prefix", "123", data)
        
        mock_inner.assert_called_once_with("prefix", "123", data, None)


class TestCacheOperateStoreCacheInner:
    """Test __store_cache_inner functionality."""
    
    @pytest.mark.asyncio
    async def test_store_cache_inner_with_ttl(self):
        """Test __store_cache_inner with specified TTL."""
        data = {"id": 1, "name": "test"}
        
        with patch('general_operate.app.cache_operate.datetime') as mock_datetime:
            mock_datetime.now.return_value.isoformat.return_value = "2023-01-01T00:00:00Z"
            
            key, ttl, serialized = await CacheOperate._CacheOperate__store_cache_inner(
                "prefix", "123", data, 600
            )
        
        assert key == "prefix:123"
        assert ttl == 600
        
        # Parse serialized data to verify enrichment
        parsed_data = json.loads(serialized)
        assert parsed_data["id"] == 1
        assert parsed_data["name"] == "test"
        assert parsed_data["_created_at"] == "2023-01-01T00:00:00Z"
        assert parsed_data["prefix"] == "prefix"
        assert parsed_data["_identifier"] == "123"
    
    @pytest.mark.asyncio
    async def test_store_cache_inner_random_ttl(self):
        """Test __store_cache_inner with random TTL."""
        data = {"id": 1, "name": "test"}
        
        with patch('general_operate.app.cache_operate.random.randint') as mock_randint:
            mock_randint.return_value = 3500
            
            key, ttl, serialized = await CacheOperate._CacheOperate__store_cache_inner(
                "prefix", "123", data, None
            )
        
        assert ttl == 3500
        mock_randint.assert_called_once_with(2000, 5000)


class TestCacheOperateDeleteCaches:
    """Test delete_caches functionality."""
    
    @pytest.mark.asyncio
    async def test_delete_caches_success(self, cache_operate):
        """Test successful delete_caches operation."""
        identifiers = {"1", "2", "3"}
        cache_operate.redis.delete = AsyncMock(return_value=3)
        
        result = await cache_operate.delete_caches("prefix", identifiers)
        
        assert result == 3
        cache_operate.redis.delete.assert_called_once()
        # Check that the call contains the expected keys, regardless of order
        call_args = cache_operate.redis.delete.call_args[0]
        expected_keys = {"prefix:1", "prefix:2", "prefix:3"}
        assert set(call_args) == expected_keys
    
    @pytest.mark.asyncio
    async def test_delete_caches_empty_identifiers(self, cache_operate):
        """Test delete_caches with empty identifiers."""
        result = await cache_operate.delete_caches("prefix", set())
        assert result == 0
        cache_operate.redis.delete.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_delete_caches_partial_success(self, cache_operate):
        """Test delete_caches with partial success."""
        identifiers = {"1", "2", "3"}
        cache_operate.redis.delete.return_value = 2  # Only 2 out of 3 deleted
        
        result = await cache_operate.delete_caches("prefix", identifiers)
        
        assert result == 2


class TestCacheOperateDeleteCache:
    """Test delete_cache functionality."""
    
    @pytest.mark.asyncio
    async def test_delete_cache_success(self, cache_operate):
        """Test successful delete_cache operation."""
        cache_operate.redis.delete.return_value = 1
        
        result = await cache_operate.delete_cache("prefix", "123")
        
        assert result is True
        cache_operate.redis.delete.assert_called_once_with("prefix:123")
    
    @pytest.mark.asyncio
    async def test_delete_cache_not_found(self, cache_operate):
        """Test delete_cache when key not found."""
        cache_operate.redis.delete.return_value = 0
        
        result = await cache_operate.delete_cache("prefix", "123")
        
        assert result is False


class TestCacheOperateCacheExists:
    """Test cache_exists functionality."""
    
    @pytest.mark.asyncio
    async def test_cache_exists_true(self, cache_operate):
        """Test cache_exists when key exists."""
        cache_operate.redis.exists.return_value = 1
        
        result = await cache_operate.cache_exists("prefix", "123")
        
        assert result is True
        cache_operate.redis.exists.assert_called_once_with("prefix:123")
    
    @pytest.mark.asyncio
    async def test_cache_exists_false(self, cache_operate):
        """Test cache_exists when key doesn't exist."""
        cache_operate.redis.exists.return_value = 0
        
        result = await cache_operate.cache_exists("prefix", "123")
        
        assert result is False


class TestCacheOperateCacheExtendTtl:
    """Test cache_extend_ttl functionality."""
    
    @pytest.mark.asyncio
    async def test_cache_extend_ttl_success(self, cache_operate):
        """Test successful cache_extend_ttl operation."""
        cache_operate.redis.ttl = AsyncMock(return_value=600)  # Current TTL
        cache_operate.redis.expire = AsyncMock(return_value=True)
        
        result = await cache_operate.cache_extend_ttl("prefix", "123", 300)
        
        assert result is True
        cache_operate.redis.ttl.assert_called_once_with("prefix:123")
        cache_operate.redis.expire.assert_called_once_with("prefix:123", 900)  # 600 + 300
    
    @pytest.mark.asyncio
    async def test_cache_extend_ttl_key_not_exists(self, cache_operate):
        """Test cache_extend_ttl when key doesn't exist."""
        cache_operate.redis.ttl = AsyncMock(return_value=-2)  # Key doesn't exist
        
        result = await cache_operate.cache_extend_ttl("prefix", "123", 300)
        
        assert result is False
        cache_operate.redis.expire.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_cache_extend_ttl_key_no_expiry(self, cache_operate):
        """Test cache_extend_ttl when key has no expiry."""
        cache_operate.redis.ttl = AsyncMock(return_value=-1)  # Key exists but has no expiry
        
        result = await cache_operate.cache_extend_ttl("prefix", "123", 300)
        
        assert result is False
        cache_operate.redis.expire.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_cache_extend_ttl_zero_current_ttl(self, cache_operate):
        """Test cache_extend_ttl when current TTL is zero."""
        cache_operate.redis.ttl.return_value = 0  # About to expire
        
        result = await cache_operate.cache_extend_ttl("prefix", "123", 300)
        
        assert result is False
        cache_operate.redis.expire.assert_not_called()


class TestCacheOperateSetNullKey:
    """Test set_null_key functionality."""
    
    @pytest.mark.asyncio
    async def test_set_null_key_success(self, cache_operate):
        """Test successful set_null_key operation."""
        cache_operate.redis.setex.return_value = True
        
        result = await cache_operate.set_null_key("null:key:123", 600)
        
        assert result is True
        cache_operate.redis.setex.assert_called_once_with("null:key:123", 600, "1")
    
    @pytest.mark.asyncio
    async def test_set_null_key_default_expiry(self, cache_operate):
        """Test set_null_key with default expiry."""
        cache_operate.redis.setex.return_value = True
        
        result = await cache_operate.set_null_key("null:key:123")
        
        assert result is True
        cache_operate.redis.setex.assert_called_once_with("null:key:123", 300, "1")
    
    @pytest.mark.asyncio
    async def test_set_null_key_failure(self, cache_operate):
        """Test set_null_key failure."""
        cache_operate.redis.setex.return_value = False
        
        result = await cache_operate.set_null_key("null:key:123")
        
        assert result is False


class TestCacheOperateDeleteNullKey:
    """Test delete_null_key functionality."""
    
    @pytest.mark.asyncio
    async def test_delete_null_key_success(self, cache_operate):
        """Test successful delete_null_key operation."""
        cache_operate.redis.delete.return_value = 1
        
        result = await cache_operate.delete_null_key("null:key:123")
        
        assert result is True
        cache_operate.redis.delete.assert_called_once_with("null:key:123")
    
    @pytest.mark.asyncio
    async def test_delete_null_key_not_found(self, cache_operate):
        """Test delete_null_key when key not found."""
        cache_operate.redis.delete.return_value = 0
        
        result = await cache_operate.delete_null_key("null:key:123")
        
        assert result is False


class TestCacheOperateHealthCheck:
    """Test health_check functionality."""
    
    @pytest.mark.asyncio
    async def test_health_check_success(self, cache_operate):
        """Test successful health check."""
        cache_operate.redis.ping.return_value = True
        
        result = await cache_operate.health_check()
        
        assert result is True
        cache_operate.redis.ping.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_health_check_ping_false(self, cache_operate):
        """Test health check when ping returns False."""
        cache_operate.redis.ping.return_value = False
        
        result = await cache_operate.health_check()
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_health_check_connection_error(self, cache_operate):
        """Test health check with connection error."""
        cache_operate.redis.ping.side_effect = redis.ConnectionError("Connection failed")
        
        with patch('structlog.get_logger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            result = await cache_operate.health_check()
        
        assert result is False
        mock_logger.warning.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_health_check_timeout_error(self, cache_operate):
        """Test health check with timeout error."""
        cache_operate.redis.ping.side_effect = redis.TimeoutError("Operation timed out")
        
        with patch('structlog.get_logger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            result = await cache_operate.health_check()
        
        assert result is False
        mock_logger.warning.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_health_check_response_error(self, cache_operate):
        """Test health check with response error."""
        cache_operate.redis.ping.side_effect = redis.ResponseError("Invalid response")
        
        with patch('structlog.get_logger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            result = await cache_operate.health_check()
        
        assert result is False
        mock_logger.warning.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_health_check_redis_error(self, cache_operate):
        """Test health check with generic Redis error."""
        cache_operate.redis.ping.side_effect = RedisError("Generic Redis error")
        
        with patch('structlog.get_logger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            result = await cache_operate.health_check()
        
        assert result is False
        mock_logger.warning.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_health_check_unexpected_error(self, cache_operate):
        """Test health check with unexpected error."""
        cache_operate.redis.ping.side_effect = Exception("Unexpected error")
        
        with patch('structlog.get_logger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            result = await cache_operate.health_check()
        
        assert result is False
        mock_logger.error.assert_called_once()


class TestCacheOperateIntegration:
    """Integration tests for CacheOperate."""
    
    @pytest.mark.asyncio
    async def test_full_cache_lifecycle(self, cache_operate):
        """Test complete cache lifecycle: store, get, exists, extend, delete."""
        # Setup mock Redis responses
        cache_operate.redis.setex = AsyncMock(return_value=True)
        cache_operate.redis.get = AsyncMock(return_value=None)
        cache_operate.redis.exists = AsyncMock(return_value=1)
        cache_operate.redis.ttl = AsyncMock(return_value=600)
        cache_operate.redis.expire = AsyncMock(return_value=True)
        cache_operate.redis.delete = AsyncMock(return_value=1)
        
        data = {"id": 1, "name": "test", "status": "active"}
        
        # Store data
        await cache_operate.store_cache("test", "123", data, 300)
        
        # Simulate getting data back
        with patch.object(cache_operate, '_CacheOperate__store_cache_inner') as mock_inner:
            mock_inner.return_value = ("test:123", 300, json.dumps({**data, "_created_at": "2023-01-01T00:00:00Z"}))
            stored_data = json.loads(mock_inner.return_value[2])
            cache_operate.redis.get.return_value = json.dumps(stored_data)
        
        # Get data
        result = await cache_operate.get_cache("test", "123")
        assert result["id"] == 1
        assert result["name"] == "test"
        
        # Check existence
        exists = await cache_operate.cache_exists("test", "123")
        assert exists is True
        
        # Extend TTL
        extended = await cache_operate.cache_extend_ttl("test", "123", 300)
        assert extended is True
        
        # Delete cache
        deleted = await cache_operate.delete_cache("test", "123")
        assert deleted is True
    
    @pytest.mark.asyncio
    async def test_batch_operations(self, cache_operate):
        """Test batch cache operations."""
        data = {
            "1": {"id": 1, "name": "item1"},
            "2": {"id": 2, "name": "item2"},
            "3": {"id": 3, "name": "item3"}
        }
        
        # Setup mock pipeline
        mock_pipeline = AsyncMock()
        cache_operate.redis.pipeline.return_value.__aenter__ = AsyncMock(return_value=mock_pipeline)
        cache_operate.redis.pipeline.return_value.__aexit__ = AsyncMock(return_value=None)
        
        # Store batch data
        with patch.object(cache_operate, '_CacheOperate__store_cache_inner') as mock_inner:
            mock_inner.side_effect = [
                ("test:1", 3000, json.dumps(data["1"])),
                ("test:2", 3000, json.dumps(data["2"])),
                ("test:3", 3000, json.dumps(data["3"]))
            ]
            
            result = await cache_operate.store_caches("test", data)
            assert result is True
        
        # Get batch data
        mock_pipeline.execute.return_value = [
            json.dumps(data["1"]),
            json.dumps(data["2"]),
            json.dumps(data["3"])
        ]
        
        results = await cache_operate.get_caches("test", {"1", "2", "3"})
        assert len(results) == 3
        
        # Delete batch data
        cache_operate.redis.delete.return_value = 3
        deleted_count = await cache_operate.delete_caches("test", {"1", "2", "3"})
        assert deleted_count == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--cov=general_operate.app.cache_operate", "--cov-report=term-missing"])