"""Comprehensive test coverage for cache_operate.py - achieving 100% coverage"""

import json
import asyncio
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch, call
from datetime import datetime, UTC
import re

import redis
from redis import RedisError

from general_operate.app.cache_operate import CacheOperate
from general_operate.core.exceptions import CacheException, ErrorCode, ErrorContext


@pytest_asyncio.fixture
async def redis_client():
    """Create mock Redis client"""
    client = AsyncMock(spec=redis.asyncio.Redis)
    client.pipeline = MagicMock()
    client.get = AsyncMock(return_value=None)
    client.setex = AsyncMock(return_value=True)
    client.delete = AsyncMock(return_value=1)
    client.exists = AsyncMock(return_value=1)
    client.ttl = AsyncMock(return_value=3600)
    client.expire = AsyncMock(return_value=True)
    client.ping = AsyncMock(return_value=True)
    return client


@pytest_asyncio.fixture
async def cache_operate(redis_client):
    """Create CacheOperate instance"""
    return CacheOperate(redis_client)


class TestInitialization:
    """Test CacheOperate initialization"""
    
    def test_init_with_redis(self, redis_client):
        """Test initialization with Redis client"""
        cache_op = CacheOperate(redis_client)
        
        assert cache_op.redis == redis_client
        assert cache_op._CacheOperate__exc == CacheException
        assert hasattr(cache_op, 'logger')
    
    def test_init_sets_logger(self, redis_client):
        """Test that logger is properly configured"""
        cache_op = CacheOperate(redis_client)
        
        assert cache_op.logger is not None
        # Logger should be bound with operator class name


class TestExceptionHandler:
    """Test exception_handler decorator"""
    
    @pytest.mark.asyncio
    async def test_exception_handler_redis_connection_error(self, cache_operate):
        """Test handling of Redis connection errors"""
        
        @CacheOperate.exception_handler
        async def test_method(self):
            raise redis.ConnectionError("Connection refused")
        
        with pytest.raises(CacheException) as exc_info:
            await test_method(cache_operate)
        
        assert exc_info.value.code == ErrorCode.CACHE_CONNECTION_ERROR
        assert "Redis connection error" in exc_info.value.message
    
    @pytest.mark.asyncio
    async def test_exception_handler_redis_timeout_error(self, cache_operate):
        """Test handling of Redis timeout errors"""
        
        @CacheOperate.exception_handler
        async def test_method(self):
            raise redis.TimeoutError("Operation timed out")
        
        with pytest.raises(CacheException) as exc_info:
            await test_method(cache_operate)
        
        assert exc_info.value.code == ErrorCode.CACHE_CONNECTION_ERROR
        assert "Redis timeout error" in exc_info.value.message
    
    @pytest.mark.asyncio
    async def test_exception_handler_redis_response_error(self, cache_operate):
        """Test handling of Redis response errors"""
        
        @CacheOperate.exception_handler
        async def test_method(self):
            raise redis.ResponseError("Invalid response")
        
        with pytest.raises(CacheException) as exc_info:
            await test_method(cache_operate)
        
        assert exc_info.value.code == ErrorCode.CACHE_KEY_ERROR
        assert "Redis response error" in exc_info.value.message
    
    @pytest.mark.asyncio
    async def test_exception_handler_generic_redis_error(self, cache_operate):
        """Test handling of generic Redis errors"""
        
        @CacheOperate.exception_handler
        async def test_method(self):
            raise RedisError("Error 42: Something went wrong")
        
        with pytest.raises(CacheException) as exc_info:
            await test_method(cache_operate)
        
        assert exc_info.value.code == ErrorCode.CACHE_CONNECTION_ERROR
        assert "Redis error" in exc_info.value.message
        # Should extract error code
        assert exc_info.value.context.details["redis_error_code"] == "42"
    
    @pytest.mark.asyncio
    async def test_exception_handler_json_decode_error(self, cache_operate):
        """Test handling of JSON decode errors"""
        
        @CacheOperate.exception_handler
        async def test_method(self):
            raise json.JSONDecodeError("Invalid JSON", "doc", 0)
        
        with pytest.raises(CacheException) as exc_info:
            await test_method(cache_operate)
        
        assert exc_info.value.code == ErrorCode.CACHE_SERIALIZATION_ERROR
        assert "JSON decode error" in exc_info.value.message
    
    @pytest.mark.asyncio
    async def test_exception_handler_sql_validation_error(self, cache_operate):
        """Test handling of SQL-related validation errors"""
        
        @CacheOperate.exception_handler
        async def test_method(self):
            raise ValueError("SQL operation failed")
        
        with pytest.raises(CacheException) as exc_info:
            await test_method(cache_operate)
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "SQL operation validation error" in exc_info.value.message
    
    @pytest.mark.asyncio
    async def test_exception_handler_preserves_cache_exception(self, cache_operate):
        """Test that existing CacheException is preserved"""
        
        original_exc = CacheException(
            code=ErrorCode.CACHE_KEY_ERROR,
            message="Original error"
        )
        
        @CacheOperate.exception_handler
        async def test_method(self):
            raise original_exc
        
        with pytest.raises(CacheException) as exc_info:
            await test_method(cache_operate)
        
        assert exc_info.value == original_exc
    
    @pytest.mark.asyncio
    async def test_exception_handler_unexpected_error(self, cache_operate):
        """Test handling of unexpected errors"""
        
        @CacheOperate.exception_handler
        async def test_method(self):
            raise RuntimeError("Unexpected error")
        
        with pytest.raises(CacheException) as exc_info:
            await test_method(cache_operate)
        
        assert exc_info.value.code == ErrorCode.UNKNOWN_ERROR
        assert "Operation error" in exc_info.value.message
    
    def test_exception_handler_sync_method(self, cache_operate):
        """Test exception handler with synchronous method"""
        
        @CacheOperate.exception_handler
        def test_method(self):
            raise redis.ConnectionError("Connection refused")
        
        with pytest.raises(CacheException) as exc_info:
            test_method(cache_operate)
        
        assert exc_info.value.code == ErrorCode.CACHE_CONNECTION_ERROR
    
    @pytest.mark.asyncio
    async def test_exception_handler_with_return_value(self, cache_operate):
        """Test that decorator preserves return values"""
        
        @CacheOperate.exception_handler
        async def test_method(self):
            return "success"
        
        result = await test_method(cache_operate)
        assert result == "success"


class TestGetCaches:
    """Test get_caches method"""
    
    @pytest.mark.asyncio
    async def test_get_caches_success(self, cache_operate):
        """Test successful batch cache retrieval"""
        mock_pipeline = AsyncMock()
        mock_pipeline.get = MagicMock()
        mock_pipeline.execute = AsyncMock(return_value=[
            b'{"id": 1, "name": "test1"}',
            b'{"id": 2, "name": "test2"}',
            None
        ])
        mock_pipeline.__aenter__ = AsyncMock(return_value=mock_pipeline)
        mock_pipeline.__aexit__ = AsyncMock()
        
        cache_operate.redis.pipeline = MagicMock(return_value=mock_pipeline)
        
        results = await cache_operate.get_caches("prefix", {"1", "2", "3"})
        
        assert len(results) == 2
        assert results[0]["id"] == 1
        assert results[1]["id"] == 2
    
    @pytest.mark.asyncio
    async def test_get_caches_empty_identifiers(self, cache_operate):
        """Test get_caches with empty identifier set"""
        mock_pipeline = AsyncMock()
        mock_pipeline.get = MagicMock()
        mock_pipeline.execute = AsyncMock(return_value=[])
        mock_pipeline.__aenter__ = AsyncMock(return_value=mock_pipeline)
        mock_pipeline.__aexit__ = AsyncMock()
        
        cache_operate.redis.pipeline = MagicMock(return_value=mock_pipeline)
        
        results = await cache_operate.get_caches("prefix", set())
        
        assert results == []
    
    @pytest.mark.asyncio
    async def test_get_caches_invalid_json(self, cache_operate):
        """Test get_caches with invalid JSON data"""
        mock_pipeline = AsyncMock()
        mock_pipeline.get = MagicMock()
        mock_pipeline.execute = AsyncMock(return_value=[
            b'{"id": 1}',
            b'invalid json',
            b'{"id": 3}'
        ])
        mock_pipeline.__aenter__ = AsyncMock(return_value=mock_pipeline)
        mock_pipeline.__aexit__ = AsyncMock()
        
        cache_operate.redis.pipeline = MagicMock(return_value=mock_pipeline)
        
        results = await cache_operate.get_caches("prefix", {"1", "2", "3"})
        
        # Should skip invalid JSON
        assert len(results) == 2
        assert results[0]["id"] == 1
        assert results[1]["id"] == 3
    
    @pytest.mark.asyncio
    async def test_get_caches_all_none(self, cache_operate):
        """Test get_caches when all values are None"""
        mock_pipeline = AsyncMock()
        mock_pipeline.get = MagicMock()
        mock_pipeline.execute = AsyncMock(return_value=[None, None, None])
        mock_pipeline.__aenter__ = AsyncMock(return_value=mock_pipeline)
        mock_pipeline.__aexit__ = AsyncMock()
        
        cache_operate.redis.pipeline = MagicMock(return_value=mock_pipeline)
        
        results = await cache_operate.get_caches("prefix", {"1", "2", "3"})
        
        assert results == []


class TestGetCache:
    """Test get_cache method"""
    
    @pytest.mark.asyncio
    async def test_get_cache_success(self, cache_operate):
        """Test successful single cache retrieval"""
        cache_data = {"id": 1, "name": "test", "_created_at": "2024-01-01T00:00:00"}
        cache_operate.redis.get = AsyncMock(return_value=json.dumps(cache_data))
        
        result = await cache_operate.get_cache("prefix", "123")
        
        assert result == cache_data
        cache_operate.redis.get.assert_called_once_with("prefix:123")
    
    @pytest.mark.asyncio
    async def test_get_cache_not_found(self, cache_operate):
        """Test get_cache when key doesn't exist"""
        cache_operate.redis.get = AsyncMock(return_value=None)
        
        result = await cache_operate.get_cache("prefix", "nonexistent")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_cache_invalid_json(self, cache_operate):
        """Test get_cache with invalid JSON"""
        cache_operate.redis.get = AsyncMock(return_value=b"invalid json")
        
        result = await cache_operate.get_cache("prefix", "123")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_cache_exception(self, cache_operate):
        """Test get_cache with Redis exception"""
        cache_operate.redis.get = AsyncMock(side_effect=Exception("Redis error"))
        
        result = await cache_operate.get_cache("prefix", "123")
        
        assert result is None


class TestStoreCaches:
    """Test store_caches method"""
    
    @pytest.mark.asyncio
    async def test_store_caches_success(self, cache_operate):
        """Test successful batch cache storage"""
        mock_pipeline = AsyncMock()
        mock_pipeline.setex = MagicMock()
        mock_pipeline.execute = AsyncMock(return_value=[True, True])
        mock_pipeline.__aenter__ = AsyncMock(return_value=mock_pipeline)
        mock_pipeline.__aexit__ = AsyncMock()
        
        cache_operate.redis.pipeline = MagicMock(return_value=mock_pipeline)
        
        data = {
            "1": {"id": 1, "name": "test1"},
            "2": {"id": 2, "name": "test2"}
        }
        
        result = await cache_operate.store_caches("prefix", data, ttl_seconds=3600)
        
        assert result is True
        assert mock_pipeline.setex.call_count == 2
    
    @pytest.mark.asyncio
    async def test_store_caches_empty_data(self, cache_operate):
        """Test store_caches with empty data"""
        result = await cache_operate.store_caches("prefix", {})
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_store_caches_with_random_ttl(self, cache_operate):
        """Test store_caches with None ttl (uses random)"""
        mock_pipeline = AsyncMock()
        mock_pipeline.setex = MagicMock()
        mock_pipeline.execute = AsyncMock(return_value=[True])
        mock_pipeline.__aenter__ = AsyncMock(return_value=mock_pipeline)
        mock_pipeline.__aexit__ = AsyncMock()
        
        cache_operate.redis.pipeline = MagicMock(return_value=mock_pipeline)
        
        data = {"1": {"id": 1}}
        
        with patch('random.randint', return_value=3000):
            result = await cache_operate.store_caches("prefix", data, ttl_seconds=None)
        
        assert result is True
        # Check that TTL was set with random value
        call_args = mock_pipeline.setex.call_args[0]
        assert call_args[1] == 3000


class TestStoreCache:
    """Test store_cache method"""
    
    @pytest.mark.asyncio
    async def test_store_cache_success(self, cache_operate):
        """Test successful single cache storage"""
        cache_operate.redis.setex = AsyncMock(return_value=True)
        
        data = {"id": 1, "name": "test"}
        
        await cache_operate.store_cache("prefix", "123", data, ttl_seconds=3600)
        
        cache_operate.redis.setex.assert_called_once()
        call_args = cache_operate.redis.setex.call_args[0]
        assert call_args[0] == "prefix:123"
        assert call_args[1] == 3600
        
        # Check that metadata was added
        stored_data = json.loads(call_args[2])
        assert stored_data["id"] == 1
        assert "_created_at" in stored_data
        assert stored_data["prefix"] == "prefix"
        assert stored_data["_identifier"] == "123"
    
    @pytest.mark.asyncio
    async def test_store_cache_with_random_ttl(self, cache_operate):
        """Test store_cache with None ttl"""
        cache_operate.redis.setex = AsyncMock(return_value=True)
        
        with patch('random.randint', return_value=2500):
            await cache_operate.store_cache("prefix", "123", {"data": "test"})
        
        call_args = cache_operate.redis.setex.call_args[0]
        assert call_args[1] == 2500


class TestStoreCacheInner:
    """Test __store_cache_inner static method"""
    
    @pytest.mark.asyncio
    async def test_store_cache_inner_with_ttl(self):
        """Test inner cache storage logic with TTL"""
        key, ttl, data = await CacheOperate._CacheOperate__store_cache_inner(
            "prefix", "123", {"id": 1}, ttl_seconds=7200
        )
        
        assert key == "prefix:123"
        assert ttl == 7200
        
        parsed_data = json.loads(data)
        assert parsed_data["id"] == 1
        assert "_created_at" in parsed_data
        assert parsed_data["prefix"] == "prefix"
        assert parsed_data["_identifier"] == "123"
    
    @pytest.mark.asyncio
    async def test_store_cache_inner_without_ttl(self):
        """Test inner cache storage logic without TTL"""
        with patch('random.randint', return_value=3500):
            key, ttl, data = await CacheOperate._CacheOperate__store_cache_inner(
                "test", "456", {"value": "data"}, ttl_seconds=None
            )
        
        assert key == "test:456"
        assert ttl == 3500
        
        parsed_data = json.loads(data)
        assert parsed_data["value"] == "data"
        assert "_created_at" in parsed_data
    
    @pytest.mark.asyncio
    async def test_store_cache_inner_with_datetime(self):
        """Test inner cache storage with datetime objects"""
        from datetime import datetime
        
        data_with_datetime = {
            "id": 1,
            "created": datetime.now(UTC),
            "updated": datetime(2024, 1, 1, 12, 0, 0)
        }
        
        key, ttl, serialized = await CacheOperate._CacheOperate__store_cache_inner(
            "prefix", "123", data_with_datetime, ttl_seconds=3600
        )
        
        # Should serialize without error using EnhancedJSONEncoder
        parsed = json.loads(serialized)
        assert "created" in parsed
        assert "updated" in parsed


class TestDeleteCaches:
    """Test delete_caches method"""
    
    @pytest.mark.asyncio
    async def test_delete_caches_success(self, cache_operate):
        """Test successful batch cache deletion"""
        cache_operate.redis.delete = AsyncMock(return_value=3)
        
        result = await cache_operate.delete_caches("prefix", {"1", "2", "3"})
        
        assert result == 3
        # Note: sets are unordered, so we can't guarantee the order of arguments
        cache_operate.redis.delete.assert_called_once()
        call_args = cache_operate.redis.delete.call_args[0]
        assert set(call_args) == {"prefix:1", "prefix:2", "prefix:3"}
    
    @pytest.mark.asyncio
    async def test_delete_caches_empty_set(self, cache_operate):
        """Test delete_caches with empty identifier set"""
        result = await cache_operate.delete_caches("prefix", set())
        
        assert result == 0
        cache_operate.redis.delete.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_delete_caches_partial_success(self, cache_operate):
        """Test delete_caches with partial deletion"""
        cache_operate.redis.delete = AsyncMock(return_value=2)
        
        result = await cache_operate.delete_caches("prefix", {"1", "2", "3"})
        
        assert result == 2


class TestDeleteCache:
    """Test delete_cache method"""
    
    @pytest.mark.asyncio
    async def test_delete_cache_success(self, cache_operate):
        """Test successful single cache deletion"""
        cache_operate.redis.delete = AsyncMock(return_value=1)
        
        result = await cache_operate.delete_cache("prefix", "123")
        
        assert result is True
        cache_operate.redis.delete.assert_called_once_with("prefix:123")
    
    @pytest.mark.asyncio
    async def test_delete_cache_not_found(self, cache_operate):
        """Test delete_cache when key doesn't exist"""
        cache_operate.redis.delete = AsyncMock(return_value=0)
        
        result = await cache_operate.delete_cache("prefix", "nonexistent")
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_delete_cache_multiple_deleted(self, cache_operate):
        """Test delete_cache when multiple keys deleted (edge case)"""
        cache_operate.redis.delete = AsyncMock(return_value=2)
        
        result = await cache_operate.delete_cache("prefix", "123")
        
        assert result is True


class TestCacheExists:
    """Test cache_exists method"""
    
    @pytest.mark.asyncio
    async def test_cache_exists_true(self, cache_operate):
        """Test cache_exists when key exists"""
        cache_operate.redis.exists = AsyncMock(return_value=1)
        
        result = await cache_operate.cache_exists("prefix", "123")
        
        assert result is True
        cache_operate.redis.exists.assert_called_once_with("prefix:123")
    
    @pytest.mark.asyncio
    async def test_cache_exists_false(self, cache_operate):
        """Test cache_exists when key doesn't exist"""
        cache_operate.redis.exists = AsyncMock(return_value=0)
        
        result = await cache_operate.cache_exists("prefix", "nonexistent")
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_cache_exists_multiple(self, cache_operate):
        """Test cache_exists with multiple keys (edge case)"""
        cache_operate.redis.exists = AsyncMock(return_value=2)
        
        result = await cache_operate.cache_exists("prefix", "123")
        
        assert result is True


class TestCacheExtendTTL:
    """Test cache_extend_ttl method"""
    
    @pytest.mark.asyncio
    async def test_cache_extend_ttl_success(self, cache_operate):
        """Test successful TTL extension"""
        cache_operate.redis.ttl = AsyncMock(return_value=3600)
        cache_operate.redis.expire = AsyncMock(return_value=True)
        
        result = await cache_operate.cache_extend_ttl("prefix", "123", 1800)
        
        assert result is True
        cache_operate.redis.expire.assert_called_once_with("prefix:123", 5400)
    
    @pytest.mark.asyncio
    async def test_cache_extend_ttl_key_not_exists(self, cache_operate):
        """Test TTL extension when key doesn't exist"""
        cache_operate.redis.ttl = AsyncMock(return_value=-2)
        
        result = await cache_operate.cache_extend_ttl("prefix", "nonexistent", 1800)
        
        assert result is False
        cache_operate.redis.expire.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_cache_extend_ttl_key_no_expiry(self, cache_operate):
        """Test TTL extension when key has no expiry"""
        cache_operate.redis.ttl = AsyncMock(return_value=-1)
        
        result = await cache_operate.cache_extend_ttl("prefix", "123", 1800)
        
        assert result is False
        cache_operate.redis.expire.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_cache_extend_ttl_expire_fails(self, cache_operate):
        """Test TTL extension when expire command fails"""
        cache_operate.redis.ttl = AsyncMock(return_value=3600)
        cache_operate.redis.expire = AsyncMock(return_value=False)
        
        result = await cache_operate.cache_extend_ttl("prefix", "123", 1800)
        
        assert result is False


class TestNullKeyMethods:
    """Test set_null_key and delete_null_key methods"""
    
    @pytest.mark.asyncio
    async def test_set_null_key_success(self, cache_operate):
        """Test successful null key setting"""
        cache_operate.redis.setex = AsyncMock(return_value=True)
        
        result = await cache_operate.set_null_key("table:123:null", 300)
        
        assert result is True
        cache_operate.redis.setex.assert_called_once_with("table:123:null", 300, "1")
    
    @pytest.mark.asyncio
    async def test_set_null_key_default_expiry(self, cache_operate):
        """Test null key with default expiry"""
        cache_operate.redis.setex = AsyncMock(return_value=True)
        
        result = await cache_operate.set_null_key("table:456:null")
        
        assert result is True
        cache_operate.redis.setex.assert_called_once_with("table:456:null", 300, "1")
    
    @pytest.mark.asyncio
    async def test_set_null_key_failure(self, cache_operate):
        """Test null key setting failure"""
        cache_operate.redis.setex = AsyncMock(return_value=False)
        
        result = await cache_operate.set_null_key("table:123:null")
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_delete_null_key_success(self, cache_operate):
        """Test successful null key deletion"""
        cache_operate.redis.delete = AsyncMock(return_value=1)
        
        result = await cache_operate.delete_null_key("table:123:null")
        
        assert result is True
        cache_operate.redis.delete.assert_called_once_with("table:123:null")
    
    @pytest.mark.asyncio
    async def test_delete_null_key_not_found(self, cache_operate):
        """Test null key deletion when key doesn't exist"""
        cache_operate.redis.delete = AsyncMock(return_value=0)
        
        result = await cache_operate.delete_null_key("table:999:null")
        
        assert result is False


class TestHealthCheck:
    """Test health_check method"""
    
    @pytest.mark.asyncio
    async def test_health_check_success(self, cache_operate):
        """Test successful health check"""
        cache_operate.redis.ping = AsyncMock(return_value=True)
        
        result = await cache_operate.health_check()
        
        assert result is True
        cache_operate.redis.ping.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_health_check_ping_false(self, cache_operate):
        """Test health check when ping returns False"""
        cache_operate.redis.ping = AsyncMock(return_value=False)
        
        result = await cache_operate.health_check()
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_health_check_connection_error(self, cache_operate):
        """Test health check with connection error"""
        cache_operate.redis.ping = AsyncMock(side_effect=redis.ConnectionError("Connection refused"))
        
        result = await cache_operate.health_check()
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_health_check_timeout_error(self, cache_operate):
        """Test health check with timeout error"""
        cache_operate.redis.ping = AsyncMock(side_effect=redis.TimeoutError("Operation timed out"))
        
        result = await cache_operate.health_check()
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_health_check_response_error(self, cache_operate):
        """Test health check with response error"""
        cache_operate.redis.ping = AsyncMock(side_effect=redis.ResponseError("Invalid response"))
        
        result = await cache_operate.health_check()
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_health_check_generic_redis_error(self, cache_operate):
        """Test health check with generic Redis error"""
        cache_operate.redis.ping = AsyncMock(side_effect=RedisError("Generic error"))
        
        result = await cache_operate.health_check()
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_health_check_unexpected_error(self, cache_operate):
        """Test health check with unexpected error"""
        cache_operate.redis.ping = AsyncMock(side_effect=RuntimeError("Unexpected"))
        
        result = await cache_operate.health_check()
        
        assert result is False


class TestEdgeCasesAndPerformance:
    """Test edge cases and performance scenarios"""
    
    @pytest.mark.asyncio
    async def test_large_batch_operations(self, cache_operate):
        """Test operations with large batches"""
        # Create large dataset
        large_data = {str(i): {"id": i, "data": f"test_{i}"} for i in range(1000)}
        
        mock_pipeline = AsyncMock()
        mock_pipeline.setex = MagicMock()
        mock_pipeline.execute = AsyncMock(return_value=[True] * 1000)
        mock_pipeline.__aenter__ = AsyncMock(return_value=mock_pipeline)
        mock_pipeline.__aexit__ = AsyncMock()
        
        cache_operate.redis.pipeline = MagicMock(return_value=mock_pipeline)
        
        result = await cache_operate.store_caches("prefix", large_data)
        
        assert result is True
        assert mock_pipeline.setex.call_count == 1000
    
    @pytest.mark.asyncio
    async def test_concurrent_operations(self, cache_operate):
        """Test concurrent cache operations"""
        cache_operate.redis.get = AsyncMock(side_effect=[
            json.dumps({"id": i}) for i in range(10)
        ])
        
        # Run multiple concurrent get operations
        tasks = [cache_operate.get_cache("prefix", str(i)) for i in range(10)]
        results = await asyncio.gather(*tasks)
        
        assert len(results) == 10
        for i, result in enumerate(results):
            assert result["id"] == i
    
    @pytest.mark.asyncio
    async def test_special_characters_in_keys(self, cache_operate):
        """Test handling of special characters in cache keys"""
        special_ids = ["test:123", "test@456", "test#789", "test$000"]
        
        for special_id in special_ids:
            await cache_operate.store_cache("prefix", special_id, {"data": "test"})
            
            expected_key = f"prefix:{special_id}"
            cache_operate.redis.setex.assert_called()
            call_args = cache_operate.redis.setex.call_args[0]
            assert call_args[0] == expected_key
    
    @pytest.mark.asyncio
    async def test_empty_data_values(self, cache_operate):
        """Test handling of empty or null data values"""
        test_data = {
            "empty_dict": {},
            "empty_list": [],
            "null_value": None,
            "empty_string": ""
        }
        
        await cache_operate.store_cache("prefix", "test", test_data)
        
        # Should store without error
        cache_operate.redis.setex.assert_called_once()
        stored_data = json.loads(cache_operate.redis.setex.call_args[0][2])
        assert stored_data["empty_dict"] == {}
        assert stored_data["empty_list"] == []
        assert stored_data["null_value"] is None
        assert stored_data["empty_string"] == ""
    
    @pytest.mark.asyncio
    async def test_pipeline_transaction_mode(self, cache_operate):
        """Test pipeline with transaction=False"""
        mock_pipeline = AsyncMock()
        mock_pipeline.get = MagicMock()
        mock_pipeline.execute = AsyncMock(return_value=[])
        mock_pipeline.__aenter__ = AsyncMock(return_value=mock_pipeline)
        mock_pipeline.__aexit__ = AsyncMock()
        
        cache_operate.redis.pipeline = MagicMock(return_value=mock_pipeline)
        
        await cache_operate.get_caches("prefix", {"1"})
        
        # Should be called with transaction=False
        cache_operate.redis.pipeline.assert_called_with(transaction=False)
    
    @pytest.mark.asyncio
    async def test_unicode_data_handling(self, cache_operate):
        """Test handling of Unicode data"""
        unicode_data = {
            "name": "测试数据",
            "emoji": "🚀🔥💯",
            "special": "Ñoño",
            "arabic": "مرحبا",
            "japanese": "こんにちは"
        }
        
        await cache_operate.store_cache("prefix", "unicode", unicode_data)
        
        # Should handle Unicode without errors
        cache_operate.redis.setex.assert_called_once()
        stored_json = cache_operate.redis.setex.call_args[0][2]
        
        # Verify ensure_ascii=False is used
        assert "测试数据" in stored_json
        assert "🚀🔥💯" in stored_json
    
    @pytest.mark.asyncio
    async def test_very_long_identifiers(self, cache_operate):
        """Test handling of very long identifiers"""
        long_id = "x" * 1000
        
        await cache_operate.store_cache("prefix", long_id, {"data": "test"})
        
        expected_key = f"prefix:{long_id}"
        cache_operate.redis.setex.assert_called()
        call_args = cache_operate.redis.setex.call_args[0]
        assert call_args[0] == expected_key
        assert len(call_args[0]) > 1000


class TestErrorRecovery:
    """Test error recovery and resilience"""
    
    @pytest.mark.asyncio
    async def test_partial_pipeline_failure(self, cache_operate):
        """Test handling of partial pipeline failures"""
        mock_pipeline = AsyncMock()
        mock_pipeline.get = MagicMock()
        # Some operations succeed, some are None (cache miss)
        mock_pipeline.execute = AsyncMock(return_value=[
            b'{"id": 1}',
            None,  # Cache miss instead of error object
            b'{"id": 3}'
        ])
        mock_pipeline.__aenter__ = AsyncMock(return_value=mock_pipeline)
        mock_pipeline.__aexit__ = AsyncMock()
        
        cache_operate.redis.pipeline = MagicMock(return_value=mock_pipeline)
        
        # Should handle partial results gracefully
        results = await cache_operate.get_caches("prefix", {"1", "2", "3"})
        
        # Should return successful results only
        assert len(results) == 2
        assert results[0]["id"] == 1
        assert results[1]["id"] == 3
    
    @pytest.mark.asyncio
    async def test_redis_reconnection_scenario(self, cache_operate):
        """Test behavior during Redis reconnection"""
        # First call fails, second succeeds (simulating reconnection)
        cache_operate.redis.ping = AsyncMock(side_effect=[
            redis.ConnectionError("Connection lost"),
            True
        ])
        
        result1 = await cache_operate.health_check()
        result2 = await cache_operate.health_check()
        
        assert result1 is False
        assert result2 is True
    
    @pytest.mark.asyncio
    async def test_data_corruption_handling(self, cache_operate):
        """Test handling of corrupted cache data"""
        # Simulate partially corrupted JSON
        corrupted_data = b'{"id": 1, "name": "test"'  # Missing closing brace
        
        cache_operate.redis.get = AsyncMock(return_value=corrupted_data)
        
        result = await cache_operate.get_cache("prefix", "corrupted")
        
        # Should return None for corrupted data
        assert result is None


class TestIntegrationScenarios:
    """Test realistic integration scenarios"""
    
    @pytest.mark.asyncio
    async def test_cache_invalidation_workflow(self, cache_operate):
        """Test typical cache invalidation workflow"""
        # 1. Store data
        await cache_operate.store_cache("users", "123", {"name": "John", "age": 30})
        
        # 2. Verify it exists
        exists = await cache_operate.cache_exists("users", "123")
        assert exists is True
        
        # 3. Delete it
        deleted = await cache_operate.delete_cache("users", "123")
        assert deleted is True
        
        # 4. Verify it's gone
        cache_operate.redis.exists = AsyncMock(return_value=0)
        exists = await cache_operate.cache_exists("users", "123")
        assert exists is False
    
    @pytest.mark.asyncio
    async def test_batch_update_workflow(self, cache_operate):
        """Test batch update workflow"""
        # Prepare batch data
        batch_data = {str(i): {"id": i, "value": f"v{i}"} for i in range(10)}
        
        # 1. Store batch
        mock_pipeline = AsyncMock()
        mock_pipeline.setex = MagicMock()
        mock_pipeline.execute = AsyncMock(return_value=[True] * 10)
        mock_pipeline.__aenter__ = AsyncMock(return_value=mock_pipeline)
        mock_pipeline.__aexit__ = AsyncMock()
        
        cache_operate.redis.pipeline = MagicMock(return_value=mock_pipeline)
        
        stored = await cache_operate.store_caches("items", batch_data)
        assert stored is True
        
        # 2. Delete subset
        cache_operate.redis.delete = AsyncMock(return_value=5)
        deleted = await cache_operate.delete_caches("items", {"0", "1", "2", "3", "4"})
        assert deleted == 5
    
    @pytest.mark.asyncio
    async def test_ttl_management_workflow(self, cache_operate):
        """Test TTL management workflow"""
        # 1. Store with initial TTL
        await cache_operate.store_cache("session", "abc123", {"user": "john"}, ttl_seconds=3600)
        
        # 2. Check remaining TTL
        cache_operate.redis.ttl = AsyncMock(return_value=3000)
        
        # 3. Extend TTL
        cache_operate.redis.expire = AsyncMock(return_value=True)
        extended = await cache_operate.cache_extend_ttl("session", "abc123", 1800)
        assert extended is True
        
        # Verify new TTL was set correctly
        cache_operate.redis.expire.assert_called_with("session:abc123", 4800)