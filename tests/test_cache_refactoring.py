"""
Test file to validate cache refactoring - ensures no regression
"""

import pytest
import pytest_asyncio
from unittest.mock import Mock, AsyncMock, MagicMock
import redis

from general_operate.app.cache_operate import CacheOperate
from general_operate.general_operate import GeneralOperate


class TestModule:
    """Mock module for testing"""
    table_name = "test_table"
    main_schemas = Mock()
    create_schemas = Mock()
    update_schemas = Mock()


class TestGeneralOperateSubclass(GeneralOperate):
    """Test subclass of GeneralOperate"""
    
    def get_module(self):
        return TestModule()


@pytest_asyncio.fixture
async def mock_redis():
    """Create mock Redis client"""
    mock = AsyncMock(spec=redis.asyncio.Redis)
    mock.ping = AsyncMock(return_value=True)
    mock.get = AsyncMock(return_value=None)
    mock.setex = AsyncMock(return_value=True)
    mock.delete = AsyncMock(return_value=1)
    mock.exists = AsyncMock(return_value=0)
    mock.keys = AsyncMock(return_value=[])
    mock.pipeline = MagicMock()
    
    # Setup pipeline mock
    pipe = AsyncMock()
    pipe.__aenter__ = AsyncMock(return_value=pipe)
    pipe.__aexit__ = AsyncMock(return_value=None)
    pipe.execute = AsyncMock(return_value=[])
    mock.pipeline.return_value = pipe
    
    return mock


@pytest_asyncio.fixture
async def cache_operate(mock_redis):
    """Create CacheOperate instance"""
    return CacheOperate(mock_redis)


@pytest_asyncio.fixture
async def general_operate(mock_redis):
    """Create GeneralOperate instance with cache"""
    mock_db = AsyncMock()
    instance = TestGeneralOperateSubclass(
        database_client=mock_db,
        redis_client=mock_redis
    )
    # Setup mock for read_sql
    instance.read_sql = AsyncMock(return_value=[])
    return instance


class TestCacheOperateRefactoring:
    """Test refactored CacheOperate methods"""
    
    def test_build_cache_key(self, cache_operate):
        """Test cache key building"""
        key = cache_operate.build_cache_key("users", 123)
        assert key == "users:123"
    
    def test_build_null_marker_key(self, cache_operate):
        """Test null marker key building"""
        key = cache_operate.build_null_marker_key("users", 123)
        assert key == "users:123:null"
    
    def test_process_in_batches(self):
        """Test batch processing utility"""
        items = list(range(10))
        batches = list(CacheOperate.process_in_batches(items, batch_size=3))
        
        assert len(batches) == 4
        assert batches[0] == [0, 1, 2]
        assert batches[1] == [3, 4, 5]
        assert batches[2] == [6, 7, 8]
        assert batches[3] == [9]
    
    @pytest.mark.asyncio
    async def test_cache_warming(self, cache_operate):
        """Test cache warming operation"""
        # Mock fetch function
        async def mock_fetch(table_name, limit, offset):
            if offset == 0:
                return [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
            return []
        
        result = await cache_operate.cache_warming(
            table_name="users",
            fetch_function=mock_fetch,
            limit=10,
            offset=0
        )
        
        assert result["success"] is True
        assert result["records_loaded"] == 2
    
    @pytest.mark.asyncio
    async def test_cache_clear(self, cache_operate, mock_redis):
        """Test cache clear operation"""
        mock_redis.keys.return_value = ["users:1:null", "users:2:null"]
        
        result = await cache_operate.cache_clear("users")
        
        assert result["success"] is True
        assert result["message"] == "Cache cleared successfully"
        mock_redis.delete.assert_called()
    
    @pytest.mark.asyncio
    async def test_process_cache_lookups(self, cache_operate, mock_redis):
        """Test processing cache lookups"""
        # Setup mock responses
        mock_redis.exists.side_effect = [0, 0]  # No null markers
        # Mock get_caches to return data for id 1 but not id 2
        async def mock_get_caches(table_name, ids):
            if "1" in ids:
                return [{"id": 1, "name": "cached"}]
            return []
        cache_operate.get_caches = mock_get_caches
        
        results, miss_ids, null_ids, failed_ops = await cache_operate.process_cache_lookups(
            table_name="users",
            id_values={1, 2}
        )
        
        assert len(results) == 1
        assert results[0]["name"] == "cached"
        assert 2 in miss_ids
    
    @pytest.mark.asyncio
    async def test_handle_cache_misses(self, cache_operate):
        """Test handling cache misses"""
        # Mock fetch function
        async def mock_fetch(ids):
            return [{"id": 1, "name": "fetched"}]
        
        results, found_ids = await cache_operate.handle_cache_misses(
            table_name="users",
            cache_miss_ids={1, 2},
            failed_cache_ops=[],
            fetch_function=mock_fetch
        )
        
        assert len(results) == 1
        assert results[0]["name"] == "fetched"
        assert 1 in found_ids
    
    @pytest.mark.asyncio
    async def test_refresh_cache(self, cache_operate):
        """Test cache refresh operation"""
        # Mock fetch function
        async def mock_fetch(table_name, filters):
            return [{"id": 1, "name": "refreshed"}]
        
        result = await cache_operate.refresh_cache(
            table_name="users",
            id_values={1, 2},
            fetch_function=mock_fetch
        )
        
        assert result["refreshed"] == 1
        assert result["not_found"] == 1
        assert result["errors"] == 0


class TestGeneralOperateIntegration:
    """Test GeneralOperate integration with refactored cache"""
    
    @pytest.mark.asyncio
    async def test_cache_warming_delegation(self, general_operate):
        """Test that cache_warming delegates to CacheOperate"""
        general_operate.read_sql.return_value = []
        
        result = await general_operate.cache_warming(limit=100)
        
        assert result["success"] is True
        assert result["records_loaded"] == 0
    
    @pytest.mark.asyncio
    async def test_cache_clear_delegation(self, general_operate, mock_redis):
        """Test that cache_clear delegates to CacheOperate"""
        result = await general_operate.cache_clear()
        
        assert result["success"] is True
        mock_redis.delete.assert_called()
    
    @pytest.mark.asyncio
    async def test_refresh_cache_delegation(self, general_operate):
        """Test that refresh_cache delegates to CacheOperate"""
        general_operate.read_sql.return_value = [{"id": 1, "name": "test"}]
        
        result = await general_operate.refresh_cache({1})
        
        assert result["refreshed"] == 1
    
    @pytest.mark.asyncio
    async def test_backward_compatibility(self, general_operate):
        """Test backward compatibility methods"""
        # Test store_cache_data
        await general_operate.store_cache_data("prefix", "key", {"data": "test"})
        
        # Test get_cache_data
        result = await general_operate.get_cache_data("prefix", "key")
        
        # Test delete_cache_data
        deleted = await general_operate.delete_cache_data("prefix", "key")
        assert isinstance(deleted, bool)
    
    def test_build_cache_key_delegation(self, general_operate):
        """Test cache key building delegation"""
        key = general_operate._build_cache_key(123)
        assert key == "test_table:123"
    
    def test_build_null_marker_delegation(self, general_operate):
        """Test null marker key building delegation"""
        key = general_operate._build_null_marker_key(123)
        assert key == "test_table:123:null"
    
    def test_process_in_batches_delegation(self, general_operate):
        """Test batch processing delegation"""
        items = list(range(5))
        batches = list(general_operate._process_in_batches(items, batch_size=2))
        
        assert len(batches) == 3
        assert batches[0] == [0, 1]
        assert batches[1] == [2, 3]
        assert batches[2] == [4]


class TestErrorHandling:
    """Test error handling in refactored code"""
    
    @pytest.mark.asyncio
    async def test_cache_operate_without_redis(self):
        """Test CacheOperate handles missing Redis gracefully"""
        operate = TestGeneralOperateSubclass(database_client=Mock())
        
        # Should not raise error, just return default values
        result = await operate.cache_warming()
        assert result["success"] is False
        
        result = await operate.cache_clear()
        assert result["success"] is False
        
        result = await operate.refresh_cache({1, 2})
        assert result["errors"] == 2
    
    @pytest.mark.asyncio
    async def test_cache_failures_dont_break_operations(self, general_operate, mock_redis):
        """Test that cache failures don't break main operations"""
        # Make cache operations fail
        mock_redis.get.side_effect = redis.RedisError("Connection failed")
        mock_redis.setex.side_effect = redis.RedisError("Connection failed")
        
        # Operations should still work through SQL fallback
        general_operate.read_sql.return_value = [{"id": 1, "name": "test"}]
        general_operate._fetch_from_sql = AsyncMock(return_value=[{"id": 1, "name": "test"}])
        
        # This should not raise an error
        results, miss_ids, null_ids, failed_ops = await general_operate._process_cache_lookups({1})
        
        # Should fall back to SQL
        assert 1 in miss_ids


if __name__ == "__main__":
    pytest.main([__file__, "-v"])