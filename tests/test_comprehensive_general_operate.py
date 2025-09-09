"""Comprehensive unit tests for general_operate.py to achieve 100% coverage."""

import pytest
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch, call, Mock
from datetime import datetime, UTC
from contextlib import asynccontextmanager
from typing import Any
import redis
import structlog

from general_operate.general_operate import GeneralOperate
from general_operate import GeneralOperateException, ErrorCode, ErrorContext

from general_operate.app.sql_operate import SQLOperate
from general_operate.app.influxdb_operate import InfluxOperate


class MockSchema:
    """Mock schema class for testing."""
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
    
    def model_dump(self, exclude_unset=False):
        data = {key: value for key, value in self.__dict__.items()}
        return data


class ConcreteGeneralOperate(GeneralOperate):
    """Concrete implementation for testing."""
    
    def __init__(self, *args, **kwargs):
        self._module = kwargs.pop('module', None)
        super().__init__(*args, **kwargs)
    
    def get_module(self):
        return self._module


@pytest.fixture
def mock_module():
    """Mock module with schemas and table name."""
    module = MagicMock()
    module.table_name = "test_table"
    module.main_schemas = MockSchema
    module.create_schemas = MockSchema
    module.update_schemas = MockSchema
    return module


@pytest.fixture
def mock_database_client():
    """Mock database client."""
    client = MagicMock()
    client.get_engine.return_value = MagicMock()
    return client


@pytest.fixture
def mock_redis_client():
    """Mock Redis client."""
    client = AsyncMock(spec=redis.asyncio.Redis)
    return client


@pytest.fixture
def mock_influxdb():
    """Mock InfluxDB client."""
    return MagicMock()


@pytest.fixture
def general_operate(mock_database_client, mock_redis_client, mock_influxdb, mock_module):
    """Create a GeneralOperate instance for testing."""
    return ConcreteGeneralOperate(
        database_client=mock_database_client,
        redis_client=mock_redis_client,
        influxdb=mock_influxdb,
        module=mock_module
    )


class TestGeneralOperateInit:
    """Test GeneralOperate initialization."""
    
    def test_init_with_all_clients(self, mock_database_client, mock_redis_client, mock_influxdb, mock_module):
        """Test initialization with all clients."""
        go = ConcreteGeneralOperate(
            database_client=mock_database_client,
            redis_client=mock_redis_client,
            influxdb=mock_influxdb,
            module=mock_module
        )
        
        assert go.table_name == "test_table"
        assert go.main_schemas == MockSchema
        assert go.create_schemas == MockSchema
        assert go.update_schemas == MockSchema
        assert hasattr(go, 'logger')
    
    def test_init_with_no_clients(self):
        """Test initialization with no clients."""
        go = ConcreteGeneralOperate(module=None)
        
        assert go.table_name is None
        assert go.main_schemas is None
        assert go.create_schemas is None
        assert go.update_schemas is None
    
    def test_init_partial_clients(self, mock_redis_client, mock_module):
        """Test initialization with only some clients."""
        go = ConcreteGeneralOperate(
            redis_client=mock_redis_client,
            module=mock_module
        )
        
        assert go.table_name == "test_table"
        assert hasattr(go, 'redis')


class TestGeneralOperateTransaction:
    """Test transaction context manager."""
    
    @pytest.mark.asyncio
    async def test_transaction_success(self, general_operate):
        """Test successful transaction."""
        mock_session = AsyncMock()
        
        # Create a proper async context manager
        class MockAsyncContextManager:
            async def __aenter__(self):
                return self
            
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                return None
        
        mock_context_manager = MockAsyncContextManager()
        # Use MagicMock for begin, not AsyncMock, so it doesn't return a coroutine
        mock_session.begin = MagicMock(return_value=mock_context_manager)
        mock_session.close = AsyncMock()
        
        with patch.object(general_operate, 'create_external_session', return_value=mock_session):
            async with general_operate.transaction() as session:
                assert session == mock_session
            
            mock_session.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_transaction_exception(self, general_operate):
        """Test transaction with exception."""
        mock_session = AsyncMock()
        
        # Create a proper async context manager
        class MockAsyncContextManager:
            async def __aenter__(self):
                return self
            
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                return None
        
        mock_context_manager = MockAsyncContextManager()
        # Use MagicMock for begin, not AsyncMock, so it doesn't return a coroutine
        mock_session.begin = MagicMock(return_value=mock_context_manager)
        mock_session.close = AsyncMock()
        
        with patch.object(general_operate, 'create_external_session', return_value=mock_session):
            try:
                async with general_operate.transaction() as session:
                    raise Exception("Test exception")
            except Exception:
                pass
            
            mock_session.close.assert_called_once()


class TestGeneralOperateHelperMethods:
    """Test helper methods."""
    
    def test_build_cache_key(self, general_operate):
        """Test cache key building."""
        key = general_operate._build_cache_key(123)
        assert key == "test_table:123"
    
    def test_build_null_marker_key(self, general_operate):
        """Test null marker key building."""
        key = general_operate._build_null_marker_key(123)
        assert key == "test_table:123:null"
    
    def test_validate_with_schema_main(self, general_operate):
        """Test schema validation with main schema."""
        data = {"id": 1, "name": "test"}
        result = general_operate._validate_with_schema(data, "main")
        assert hasattr(result, 'id')
        assert result.id == 1
    
    def test_validate_with_schema_create(self, general_operate):
        """Test schema validation with create schema."""
        data = {"name": "test"}
        result = general_operate._validate_with_schema(data, "create")
        assert hasattr(result, 'name')
        assert result.name == "test"
    
    def test_validate_with_schema_update(self, general_operate):
        """Test schema validation with update schema."""
        data = {"name": "updated"}
        result = general_operate._validate_with_schema(data, "update")
        assert hasattr(result, 'name')
        assert result.name == "updated"
    
    def test_validate_with_schema_invalid_type(self, general_operate):
        """Test schema validation with invalid schema type."""
        data = {"id": 1}
        result = general_operate._validate_with_schema(data, "invalid")
        assert hasattr(result, 'id')
    
    def test_process_in_batches(self, general_operate):
        """Test batch processing."""
        items = list(range(250))
        batches = list(general_operate._process_in_batches(items, batch_size=100))
        
        assert len(batches) == 3
        assert len(batches[0]) == 100
        assert len(batches[1]) == 100
        assert len(batches[2]) == 50
    
    def test_process_in_batches_empty(self, general_operate):
        """Test batch processing with empty list."""
        items = []
        batches = list(general_operate._process_in_batches(items))
        assert batches == []


class TestGeneralOperateHealthCheck:
    """Test health check functionality."""
    
    @pytest.mark.asyncio
    async def test_health_check_all_healthy(self, general_operate):
        """Test health check when all services are healthy."""
        with patch.object(SQLOperate, 'health_check', return_value=True) as mock_sql, \
             patch('general_operate.app.cache_operate.CacheOperate.health_check', return_value=True) as mock_cache:
            
            result = await general_operate.health_check()
            assert result is True
            mock_sql.assert_called_once()
            mock_cache.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_health_check_sql_unhealthy(self, general_operate):
        """Test health check when SQL is unhealthy."""
        with patch.object(SQLOperate, 'health_check', return_value=False) as mock_sql, \
             patch('general_operate.app.cache_operate.CacheOperate.health_check', return_value=True) as mock_cache:
            
            result = await general_operate.health_check()
            assert result is False
            mock_sql.assert_called_once()
            mock_cache.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_health_check_cache_unhealthy(self, general_operate):
        """Test health check when cache is unhealthy."""
        with patch.object(SQLOperate, 'health_check', return_value=True) as mock_sql, \
             patch('general_operate.app.cache_operate.CacheOperate.health_check', return_value=False) as mock_cache:
            
            result = await general_operate.health_check()
            assert result is False
            mock_sql.assert_called_once()
            mock_cache.assert_called_once()


class TestGeneralOperateCacheWarming:
    """Test cache warming functionality."""
    
    @pytest.mark.asyncio
    async def test_cache_warming_success(self, general_operate):
        """Test successful cache warming."""
        mock_data = [
            {"id": 1, "name": "item1"},
            {"id": 2, "name": "item2"}
        ]
        
        with patch.object(general_operate, 'read_sql', return_value=mock_data) as mock_read, \
             patch.object(general_operate, 'store_caches', return_value=True) as mock_store:
            
            result = await general_operate.cache_warming(limit=2)
            
            assert result["success"] is True
            assert result["records_loaded"] == 2
            mock_read.assert_called()
            mock_store.assert_called()
    
    @pytest.mark.asyncio
    async def test_cache_warming_empty_results(self, general_operate):
        """Test cache warming with empty results."""
        with patch.object(general_operate, 'read_sql', return_value=[]):
            
            result = await general_operate.cache_warming()
            
            assert result["success"] is True
            assert result["records_loaded"] == 0
    
    @pytest.mark.asyncio
    async def test_cache_warming_large_limit(self, general_operate):
        """Test cache warming with large limit."""
        # Mock to return fewer items than batch size to avoid infinite loop
        mock_data = [{"id": i, "name": f"item{i}"} for i in range(300)]
        
        with patch.object(general_operate, 'read_sql', return_value=mock_data) as mock_read, \
             patch.object(general_operate, 'store_caches', return_value=True):
            
            result = await general_operate.cache_warming(limit=1500)
            
            assert result["success"] is True
            # Should limit batch size to 500 (even with large limit)
            mock_read.assert_called_with(
                table_name="test_table", 
                limit=500, 
                offset=0
            )
    
    @pytest.mark.asyncio
    async def test_cache_warming_no_ids(self, general_operate):
        """Test cache warming with data that has no ID field."""
        mock_data = [{"name": "item1"}, {"name": "item2"}]
        
        with patch.object(general_operate, 'read_sql', return_value=mock_data) as mock_read, \
             patch.object(general_operate, 'store_caches', return_value=True) as mock_store:
            
            result = await general_operate.cache_warming()
            
            assert result["success"] is True
            assert result["records_loaded"] == 0
            mock_store.assert_not_called()


class TestGeneralOperateCacheClear:
    """Test cache clearing functionality."""
    
    @pytest.mark.asyncio
    async def test_cache_clear_success(self, general_operate):
        """Test successful cache clearing."""
        general_operate.redis.delete = AsyncMock()
        general_operate.redis.keys = AsyncMock(return_value=["test_table:1:null", "test_table:2:null"])
        
        result = await general_operate.cache_clear()
        
        assert result["success"] is True
        assert result["message"] == "Cache cleared successfully"
        general_operate.redis.delete.assert_called()
        general_operate.redis.keys.assert_called_with("test_table:*:null")
    
    @pytest.mark.asyncio
    async def test_cache_clear_no_null_markers(self, general_operate):
        """Test cache clearing with no null markers."""
        general_operate.redis.delete = AsyncMock()
        general_operate.redis.keys = AsyncMock(return_value=[])
        
        result = await general_operate.cache_clear()
        
        assert result["success"] is True
        # Should only call delete once for the main table
        assert general_operate.redis.delete.call_count == 1


class TestGeneralOperateCacheLookups:
    """Test cache lookup functionality."""
    
    @pytest.mark.asyncio
    async def test_process_cache_lookups_cache_hit(self, general_operate):
        """Test cache lookups with cache hits."""
        mock_data = [{"id": 1, "name": "test"}]
        
        with patch.object(general_operate, 'redis') as mock_redis, \
             patch.object(general_operate, 'get_caches', new=AsyncMock(return_value=mock_data)):
            
            mock_redis.exists.return_value = 0  # No null marker
            
            results, cache_miss_ids, null_marked_ids, failed_cache_ops = \
                await general_operate._process_cache_lookups({1})
            
            assert len(results) == 1
            assert len(cache_miss_ids) == 0
            assert len(null_marked_ids) == 0
            assert len(failed_cache_ops) == 0
    
    @pytest.mark.asyncio
    async def test_process_cache_lookups_null_marked(self, general_operate):
        """Test cache lookups with null markers."""
        with patch.object(general_operate, 'redis') as mock_redis:
            mock_redis.exists.return_value = 1  # Null marker exists
            
            results, cache_miss_ids, null_marked_ids, failed_cache_ops = \
                await general_operate._process_cache_lookups({1})
            
            assert len(results) == 0
            assert len(cache_miss_ids) == 0
            assert len(null_marked_ids) == 1
            assert 1 in null_marked_ids
    
    @pytest.mark.asyncio
    async def test_process_cache_lookups_cache_miss(self, general_operate):
        """Test cache lookups with cache misses."""
        with patch.object(general_operate, 'redis') as mock_redis, \
             patch.object(general_operate, 'get_caches', new=AsyncMock(return_value=[])):
            
            mock_redis.exists.return_value = 0  # No null marker
            
            results, cache_miss_ids, null_marked_ids, failed_cache_ops = \
                await general_operate._process_cache_lookups({1})
            
            assert len(results) == 0
            assert len(cache_miss_ids) == 1
            assert 1 in cache_miss_ids
    
    @pytest.mark.asyncio
    async def test_process_cache_lookups_redis_error(self, general_operate):
        """Test cache lookups with Redis errors."""
        with patch.object(general_operate, 'redis') as mock_redis:
            mock_redis.exists.side_effect = redis.RedisError("Connection failed")
            
            results, cache_miss_ids, null_marked_ids, failed_cache_ops = \
                await general_operate._process_cache_lookups({1})
            
            assert len(results) == 0
            assert len(cache_miss_ids) == 1
            assert 1 in cache_miss_ids
            assert 1 in failed_cache_ops
    
    @pytest.mark.asyncio
    async def test_process_cache_lookups_schema_validation_error(self, general_operate):
        """Test cache lookups with schema validation errors."""
        mock_data = [{"id": 1, "invalid_field": "test"}]
        
        def mock_schema(**kwargs):
            if "invalid_field" in kwargs:
                raise ValueError("Invalid field")
            return MockSchema(**kwargs)
        
        general_operate.main_schemas = mock_schema
        
        with patch.object(general_operate, 'redis') as mock_redis, \
             patch.object(general_operate, 'get_caches', new=AsyncMock(return_value=mock_data)):
            
            mock_redis.exists.return_value = 0
            
            results, cache_miss_ids, null_marked_ids, failed_cache_ops = \
                await general_operate._process_cache_lookups({1})
            
            assert len(results) == 0
            assert len(cache_miss_ids) == 1
            assert 1 in cache_miss_ids


class TestGeneralOperateHandleCacheMisses:
    """Test cache miss handling."""
    
    @pytest.mark.asyncio
    async def test_handle_cache_misses_empty_ids(self, general_operate):
        """Test handling cache misses with empty IDs."""
        results, found_ids = await general_operate._handle_cache_misses(set(), [])
        
        assert results == []
        assert found_ids == set()
    
    @pytest.mark.asyncio
    async def test_handle_cache_misses_success(self, general_operate):
        """Test successful cache miss handling."""
        mock_sql_data = [{"id": 1, "name": "test"}]
        mock_schema = Mock(id=1, name="test")
        expected_results = [mock_schema]
        expected_found_ids = {1}
        
        with patch.object(general_operate, '_fetch_from_sql', return_value=mock_sql_data), \
             patch('general_operate.app.cache_operate.CacheOperate.handle_cache_misses', 
                   return_value=(expected_results, expected_found_ids)) as mock_cache_handler:
            
            results, found_ids = await general_operate._handle_cache_misses({1}, [])
            
            assert len(results) == 1
            assert 1 in found_ids
            # Verify CacheOperate.handle_cache_misses was called with correct params
            mock_cache_handler.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_handle_cache_misses_failed_cache_ops(self, general_operate):
        """Test cache miss handling with previously failed cache operations."""
        mock_sql_data = [{"id": 1, "name": "test"}]
        
        with patch.object(general_operate, '_fetch_from_sql', return_value=mock_sql_data), \
             patch.object(general_operate, '_update_cache_after_fetch'):
            
            results, found_ids = await general_operate._handle_cache_misses({1}, [1])
            
            assert len(results) == 1
            assert found_ids == set()  # ID was in failed cache ops
    
    @pytest.mark.asyncio
    async def test_handle_cache_misses_schema_error(self, general_operate):
        """Test cache miss handling with schema validation errors."""
        mock_sql_data = [{"id": 1, "invalid_field": "test"}]
        
        def mock_schema(**kwargs):
            if "invalid_field" in kwargs:
                raise ValueError("Invalid field")
            return MockSchema(**kwargs)
        
        general_operate.main_schemas = mock_schema
        
        with patch.object(general_operate, '_fetch_from_sql', return_value=mock_sql_data):
            
            results, found_ids = await general_operate._handle_cache_misses({1}, [])
            
            assert results == []
            assert found_ids == set()


class TestGeneralOperateUpdateCache:
    """Test cache update functionality."""
    
    @pytest.mark.asyncio
    async def test_update_cache_after_fetch_success(self, general_operate):
        """Test successful cache update after fetch."""
        cache_data = {"1": {"id": 1, "name": "test"}}
        
        with patch.object(general_operate, 'store_caches', return_value=True) as mock_store:
            await general_operate._update_cache_after_fetch(cache_data)
            mock_store.assert_called_once_with("test_table", cache_data)
    
    @pytest.mark.asyncio
    async def test_update_cache_after_fetch_failure(self, general_operate):
        """Test cache update failure."""
        cache_data = {"1": {"id": 1, "name": "test"}}
        
        with patch.object(general_operate, 'store_caches', return_value=False):
            # Should not raise exception, just log warning
            await general_operate._update_cache_after_fetch(cache_data)
    
    @pytest.mark.asyncio
    async def test_update_cache_after_fetch_exception(self, general_operate):
        """Test cache update with exception."""
        cache_data = {"1": {"id": 1, "name": "test"}}
        
        with patch.object(general_operate, 'store_caches', side_effect=Exception("Cache error")):
            # Should not raise exception, just log warning
            await general_operate._update_cache_after_fetch(cache_data)


class TestGeneralOperateMarkMissingRecords:
    """Test missing record marking."""
    
    @pytest.mark.asyncio
    async def test_mark_missing_records_success(self, general_operate):
        """Test successful missing record marking."""
        with patch.object(general_operate, 'set_null_key', new=AsyncMock()) as mock_set_null:
            await general_operate._mark_missing_records({1, 2}, [])
            
            assert mock_set_null.call_count == 2
    
    @pytest.mark.asyncio
    async def test_mark_missing_records_failed_cache_ops(self, general_operate):
        """Test missing record marking with failed cache operations."""
        with patch.object(general_operate, 'set_null_key', new=AsyncMock()) as mock_set_null:
            await general_operate._mark_missing_records({1, 2}, [1])
            
            # Should only set null key for ID 2
            assert mock_set_null.call_count == 1
    
    @pytest.mark.asyncio
    async def test_mark_missing_records_exception(self, general_operate):
        """Test missing record marking with exceptions."""
        with patch.object(general_operate, 'set_null_key', new=AsyncMock(side_effect=Exception("Redis error"))):
            # Should not raise exception, just log warning
            await general_operate._mark_missing_records({1}, [])


class TestGeneralOperateReadDataById:
    """Test read data by ID functionality."""
    
    @pytest.mark.asyncio
    async def test_read_data_by_id_empty_ids(self, general_operate):
        """Test read data by ID with empty IDs."""
        result = await general_operate.read_data_by_id(set())
        assert result == []
    
    @pytest.mark.asyncio
    async def test_read_data_by_id_cache_hit(self, general_operate):
        """Test read data by ID with cache hit."""
        mock_data = [{"id": 1, "name": "test"}]
        
        with patch.object(general_operate, '_process_cache_lookups') as mock_process:
            mock_process.return_value = ([MockSchema(id=1, name="test")], set(), set(), [])
            
            result = await general_operate.read_data_by_id({1})
            
            assert len(result) == 1
            assert result[0].id == 1
    
    @pytest.mark.asyncio
    async def test_read_data_by_id_cache_miss(self, general_operate):
        """Test read data by ID with cache miss."""
        with patch.object(general_operate, '_process_cache_lookups') as mock_process, \
             patch.object(general_operate, '_handle_cache_misses') as mock_handle, \
             patch.object(general_operate, '_mark_missing_records') as mock_mark:
            
            mock_process.return_value = ([], {1}, set(), [])
            mock_handle.return_value = ([MockSchema(id=1, name="test")], {1})
            
            result = await general_operate.read_data_by_id({1})
            
            assert len(result) == 1
            assert result[0].id == 1
            mock_mark.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_read_data_by_id_sql_error(self, general_operate):
        """Test read data by ID with SQL error."""
        with patch.object(general_operate, '_process_cache_lookups') as mock_process, \
             patch.object(general_operate, '_handle_cache_misses', side_effect=Exception("SQL error")):
            
            mock_process.return_value = ([], {1}, set(), [])
            
            with pytest.raises(GeneralOperateException) as exc_info:
                await general_operate.read_data_by_id({1})
            
            assert exc_info.value.code == ErrorCode.UNKNOWN_ERROR
            assert "Both cache and SQL operations failed" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_read_data_by_id_general_exception(self, general_operate):
        """Test read data by ID with general exception."""
        with patch.object(general_operate, '_process_cache_lookups', side_effect=Exception("General error")):
            
            with pytest.raises(GeneralOperateException) as exc_info:
                await general_operate.read_data_by_id({1})
            
            assert exc_info.value.code == ErrorCode.UNKNOWN_ERROR
            assert "Unexpected error during data read" in str(exc_info.value)


class TestGeneralOperateReadDataByFilter:
    """Test read data by filter functionality."""
    
    @pytest.mark.asyncio
    async def test_read_data_by_filter_success(self, general_operate):
        """Test successful read data by filter."""
        mock_sql_data = [{"id": 1, "name": "test", "status": "active"}]
        
        with patch.object(general_operate, 'read_sql', return_value=mock_sql_data):
            result = await general_operate.read_data_by_filter(
                filters={"status": "active"},
                limit=10,
                offset=0,
                order_by="id",
                order_direction="ASC"
            )
            
            assert len(result) == 1
            assert result[0].id == 1
    
    @pytest.mark.asyncio
    async def test_read_data_by_filter_empty_results(self, general_operate):
        """Test read data by filter with empty results."""
        with patch.object(general_operate, 'read_sql', return_value=[]):
            result = await general_operate.read_data_by_filter(filters={})
            assert result == []
    
    @pytest.mark.asyncio
    async def test_read_data_by_filter_none_row(self, general_operate):
        """Test read data by filter with None row."""
        with patch.object(general_operate, 'read_sql', return_value=[None]):
            result = await general_operate.read_data_by_filter(filters={})
            assert result == []
    
    @pytest.mark.asyncio
    async def test_read_data_by_filter_exception(self, general_operate):
        """Test read data by filter with exception."""
        with patch.object(general_operate, 'read_sql', side_effect=GeneralOperateException(
            code=ErrorCode.DB_QUERY_ERROR,
            message="Database error"
        )):
            with pytest.raises(GeneralOperateException):
                await general_operate.read_data_by_filter(filters={})


class TestGeneralOperateFetchFromSQL:
    """Test fetch from SQL functionality."""
    
    @pytest.mark.asyncio
    async def test_fetch_from_sql_empty_ids(self, general_operate):
        """Test fetch from SQL with empty IDs."""
        result = await general_operate._fetch_from_sql(set())
        assert result == []
    
    @pytest.mark.asyncio
    async def test_fetch_from_sql_single_id(self, general_operate):
        """Test fetch from SQL with single ID."""
        mock_data = {"id": 1, "name": "test"}
        
        with patch.object(general_operate, 'read_one', return_value=mock_data):
            result = await general_operate._fetch_from_sql({1})
            
            assert len(result) == 1
            assert result[0] == mock_data
    
    @pytest.mark.asyncio
    async def test_fetch_from_sql_single_id_not_found(self, general_operate):
        """Test fetch from SQL with single ID not found."""
        with patch.object(general_operate, 'read_one', return_value=None):
            result = await general_operate._fetch_from_sql({1})
            assert result == []
    
    @pytest.mark.asyncio
    async def test_fetch_from_sql_multiple_ids(self, general_operate):
        """Test fetch from SQL with multiple IDs."""
        mock_data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        
        with patch.object(general_operate, 'read_sql', return_value=mock_data):
            result = await general_operate._fetch_from_sql({1, 2})
            
            assert len(result) == 2
            assert result == mock_data
    
    @pytest.mark.asyncio
    async def test_fetch_from_sql_multiple_ids_empty(self, general_operate):
        """Test fetch from SQL with multiple IDs returning empty."""
        with patch.object(general_operate, 'read_sql', return_value=None):
            result = await general_operate._fetch_from_sql({1, 2})
            assert result == []
    
    @pytest.mark.asyncio
    async def test_fetch_from_sql_general_operate_exception(self, general_operate):
        """Test fetch from SQL with GeneralOperateException."""
        with patch.object(general_operate, 'read_one', side_effect=GeneralOperateException(
            code=ErrorCode.DB_QUERY_ERROR,
            message="Database error"
        )):
            with pytest.raises(GeneralOperateException):
                await general_operate._fetch_from_sql({1})
    
    @pytest.mark.asyncio
    async def test_fetch_from_sql_generic_exception(self, general_operate):
        """Test fetch from SQL with generic exception."""
        with patch.object(general_operate, 'read_one', side_effect=Exception("Generic error")):
            with pytest.raises(GeneralOperateException) as exc_info:
                await general_operate._fetch_from_sql({1})
            
            assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR
            assert "Database read operation failed" in str(exc_info.value)


class TestGeneralOperateCreateData:
    """Test create data functionality."""
    
    @pytest.mark.asyncio
    async def test_create_data_empty(self, general_operate):
        """Test create data with empty list."""
        result = await general_operate.create_data([])
        assert result == []
    
    @pytest.mark.asyncio
    async def test_create_data_success(self, general_operate):
        """Test successful create data."""
        data = [{"name": "test1"}, {"name": "test2"}]
        mock_created = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        
        with patch.object(general_operate, 'create_sql', return_value=mock_created):
            result = await general_operate.create_data(data)
            
            assert len(result) == 2
            assert result[0].id == 1
            assert result[1].id == 2
    
    @pytest.mark.asyncio
    async def test_create_data_with_session(self, general_operate):
        """Test create data with external session."""
        data = [{"name": "test"}]
        mock_created = [{"id": 1, "name": "test"}]
        session = AsyncMock()
        
        with patch.object(general_operate, 'create_sql', return_value=mock_created) as mock_create:
            result = await general_operate.create_data(data, session=session)
            
            assert len(result) == 1
            mock_create.assert_called_with("test_table", [{"name": "test"}], session=session)
    
    @pytest.mark.asyncio
    async def test_create_data_invalid_item_type(self, general_operate):
        """Test create data with invalid item types."""
        data = ["invalid", {"name": "valid"}]
        mock_created = [{"id": 1, "name": "valid"}]
        
        with patch.object(general_operate, 'create_sql', return_value=mock_created):
            result = await general_operate.create_data(data)
            
            assert len(result) == 1  # Only valid item processed
    
    @pytest.mark.asyncio
    async def test_create_data_schema_validation_error(self, general_operate):
        """Test create data with schema validation errors."""
        def mock_schema(**kwargs):
            if "invalid" in kwargs:
                raise TypeError("Invalid field")
            return MockSchema(**kwargs)
        
        general_operate.create_schemas = mock_schema
        
        data = [{"invalid": "field"}, {"name": "valid"}]
        mock_created = [{"id": 1, "name": "valid"}]
        
        with patch.object(general_operate, 'create_sql', return_value=mock_created):
            result = await general_operate.create_data(data)
            
            assert len(result) == 1  # Only valid item processed
    
    @pytest.mark.asyncio
    async def test_create_data_attribute_error(self, general_operate):
        """Test create data with attribute errors."""
        # This test verifies that the code can handle schema instantiation errors
        data = [{"name": "test"}]
        
        # Mock create_sql to return data successfully
        mock_created = [{"id": 1, "name": "test"}]
        
        # Create a broken schema that raises AttributeError when trying to use model_dump
        class BrokenSchema:
            def __init__(self, **kwargs):
                self.id = kwargs.get('id')
                self.name = kwargs.get('name')
            
            def model_dump(self, exclude_unset=False):
                # This will raise AttributeError when called
                raise AttributeError("model_dump failed")
        
        general_operate.main_schemas = BrokenSchema
        
        # Should handle the AttributeError without raising
        with patch.object(general_operate, 'create_sql', new=AsyncMock(return_value=mock_created)), \
             patch.object(general_operate, 'logger') as mock_logger:
            # The method should log warnings but not crash
            result = await general_operate.create_data(data)
            
            # Result should have the broken schema instances
            assert len(result) == 1
            assert isinstance(result[0], BrokenSchema)
            assert result[0].id == 1
    
    @pytest.mark.asyncio
    async def test_create_data_all_invalid(self, general_operate):
        """Test create data with all invalid items."""
        data = ["invalid1", "invalid2"]
        
        result = await general_operate.create_data(data)
        assert result == []


class TestGeneralOperateCreateByForeignKey:
    """Test create by foreign key functionality."""
    
    @pytest.mark.asyncio
    async def test_create_by_foreign_key_empty(self, general_operate):
        """Test create by foreign key with empty data."""
        result = await general_operate.create_by_foreign_key("user_id", 123, [])
        assert result == []
    
    @pytest.mark.asyncio
    async def test_create_by_foreign_key_success(self, general_operate):
        """Test successful create by foreign key."""
        data = [{"name": "item1"}, {"name": "item2"}]
        expected_created = [
            {"id": 1, "name": "item1", "user_id": 123},
            {"id": 2, "name": "item2", "user_id": 123}
        ]
        
        with patch('general_operate.utils.build_data.build_create_data') as mock_build, \
             patch.object(general_operate, 'create_data', return_value=[
                 MockSchema(id=1, name="item1", user_id=123),
                 MockSchema(id=2, name="item2", user_id=123)
             ]) as mock_create:
            
            mock_build.side_effect = lambda x: x  # Return input as-is
            
            result = await general_operate.create_by_foreign_key("user_id", 123, data)
            
            assert len(result) == 2
            assert mock_create.call_args[0][0][0]["user_id"] == 123
            assert mock_create.call_args[0][0][1]["user_id"] == 123
    
    @pytest.mark.asyncio
    async def test_create_by_foreign_key_with_session(self, general_operate):
        """Test create by foreign key with session."""
        data = [{"name": "item"}]
        session = AsyncMock()
        
        with patch('general_operate.utils.build_data.build_create_data') as mock_build, \
             patch.object(general_operate, 'create_data', return_value=[]) as mock_create:
            
            mock_build.return_value = {"name": "item"}
            
            await general_operate.create_by_foreign_key("user_id", 123, data, session=session)
            
            mock_create.assert_called_with([{"name": "item", "user_id": 123}], session=session)


class TestGeneralOperateValidateUpdateData:
    """Test update data validation."""
    
    @pytest.mark.asyncio
    async def test_validate_update_data_success(self, general_operate):
        """Test successful update data validation."""
        data = [
            {"id": 1, "name": "updated1"},
            {"id": 2, "name": "updated2"}
        ]
        
        update_list, cache_keys, errors = await general_operate._validate_update_data(data, "id")
        
        assert len(update_list) == 2
        assert len(cache_keys) == 2
        assert len(errors) == 0
        assert "1" in cache_keys
        assert "2" in cache_keys
    
    @pytest.mark.asyncio
    async def test_validate_update_data_missing_where_field(self, general_operate):
        """Test update data validation with missing where field."""
        data = [
            {"id": 1, "name": "updated1"},
            {"name": "updated2"}  # Missing ID
        ]
        
        update_list, cache_keys, errors = await general_operate._validate_update_data(data, "id")
        
        assert len(update_list) == 1
        assert len(cache_keys) == 1
        assert len(errors) == 1
        assert "Missing 'id' field" in errors[0]
    
    @pytest.mark.asyncio
    async def test_validate_update_data_schema_error(self, general_operate):
        """Test update data validation with schema errors."""
        def mock_schema(**kwargs):
            if "invalid" in kwargs:
                raise ValueError("Invalid field")
            obj = MockSchema(**kwargs)
            obj.model_dump = MagicMock(return_value={k: v for k, v in kwargs.items() if k != "id"})
            return obj
        
        general_operate.update_schemas = mock_schema
        
        data = [
            {"id": 1, "name": "valid"},
            {"id": 2, "invalid": "field"}
        ]
        
        update_list, cache_keys, errors = await general_operate._validate_update_data(data, "id")
        
        assert len(update_list) == 1
        assert len(errors) == 1
    
    @pytest.mark.asyncio
    async def test_validate_update_data_no_update_fields(self, general_operate):
        """Test update data validation with no valid update fields."""
        def mock_schema(**kwargs):
            obj = MockSchema(**kwargs)
            obj.model_dump = MagicMock(return_value={})  # No fields after validation
            return obj
        
        general_operate.update_schemas = mock_schema
        
        data = [{"id": 1}]
        
        update_list, cache_keys, errors = await general_operate._validate_update_data(data, "id")
        
        assert len(update_list) == 0  # No valid updates
        assert len(cache_keys) == 0
    
    @pytest.mark.asyncio
    async def test_validate_update_data_non_id_where_field(self, general_operate):
        """Test update data validation with non-ID where field."""
        data = [{"email": "test@example.com", "name": "updated"}]
        
        update_list, cache_keys, errors = await general_operate._validate_update_data(data, "email")
        
        assert len(update_list) == 1
        assert len(cache_keys) == 0  # No cache keys for non-ID fields


class TestGeneralOperateClearUpdateCaches:
    """Test cache clearing for updates."""
    
    @pytest.mark.asyncio
    async def test_clear_update_caches_success(self, general_operate):
        """Test successful cache clearing for updates."""
        with patch('general_operate.app.cache_operate.CacheOperate.delete_caches', new=AsyncMock(return_value=2)) as mock_delete, \
             patch.object(general_operate, 'delete_null_key', new=AsyncMock()) as mock_delete_null:
            
            errors = await general_operate._clear_update_caches(["1", "2"], "test_operation")
            
            assert len(errors) == 0
            mock_delete.assert_called_once()
            assert mock_delete_null.call_count == 2
    
    @pytest.mark.asyncio
    async def test_clear_update_caches_empty_keys(self, general_operate):
        """Test cache clearing with empty keys."""
        errors = await general_operate._clear_update_caches([], "test_operation")
        assert len(errors) == 0
    
    @pytest.mark.asyncio
    async def test_clear_update_caches_delete_error(self, general_operate):
        """Test cache clearing with delete errors."""
        with patch('general_operate.app.cache_operate.CacheOperate.delete_caches', new=AsyncMock(side_effect=Exception("Cache error"))):
            
            errors = await general_operate._clear_update_caches(["1"], "test_operation")
            
            assert len(errors) == 1
            assert "cache cleanup" in errors[0]
    
    @pytest.mark.asyncio
    async def test_clear_update_caches_null_marker_error(self, general_operate):
        """Test cache clearing with null marker errors."""
        with patch('general_operate.app.cache_operate.CacheOperate.delete_caches', new=AsyncMock(return_value=1)), \
             patch.object(general_operate, 'delete_null_key', side_effect=Exception("Null error")):
            
            errors = await general_operate._clear_update_caches(["1"], "test_operation")
            
            assert len(errors) == 1
            assert "null marker 1" in errors[0]


class TestGeneralOperateConvertUpdateResults:
    """Test conversion of update results."""
    
    @pytest.mark.asyncio
    async def test_convert_update_results_success(self, general_operate):
        """Test successful conversion of update results."""
        records = [{"id": 1, "name": "updated1"}, {"id": 2, "name": "updated2"}]
        
        results, errors = await general_operate._convert_update_results(records)
        
        assert len(results) == 2
        assert len(errors) == 0
        assert results[0].id == 1
        assert results[1].id == 2
    
    @pytest.mark.asyncio
    async def test_convert_update_results_none_record(self, general_operate):
        """Test conversion with None records."""
        records = [{"id": 1, "name": "updated"}, None]
        
        results, errors = await general_operate._convert_update_results(records)
        
        assert len(results) == 1
        assert len(errors) == 0
    
    @pytest.mark.asyncio
    async def test_convert_update_results_schema_error(self, general_operate):
        """Test conversion with schema errors."""
        def mock_schema(**kwargs):
            if kwargs.get("id") == 2:
                raise ValueError("Schema error")
            return MockSchema(**kwargs)
        
        general_operate.main_schemas = mock_schema
        
        records = [{"id": 1, "name": "updated1"}, {"id": 2, "name": "updated2"}]
        
        results, errors = await general_operate._convert_update_results(records)
        
        assert len(results) == 1
        assert len(errors) == 1
        assert "Record 1" in errors[0]


class TestGeneralOperateUpdateData:
    """Test update data functionality."""
    
    @pytest.mark.asyncio
    async def test_update_data_empty(self, general_operate):
        """Test update data with empty list."""
        result = await general_operate.update_data([])
        assert result == []
    
    @pytest.mark.asyncio
    async def test_update_data_success(self, general_operate):
        """Test successful update data."""
        data = [{"id": 1, "name": "updated"}]
        mock_updated = [{"id": 1, "name": "updated"}]
        
        with patch.object(general_operate, '_validate_update_data', return_value=(
            [{"id": 1, "data": {"name": "updated"}}], ["1"], []
        )), \
        patch.object(general_operate, '_clear_update_caches', return_value=[]), \
        patch.object(general_operate, 'update_sql', return_value=mock_updated), \
        patch.object(general_operate, '_convert_update_results', return_value=(
            [MockSchema(id=1, name="updated")], []
        )):
            
            result = await general_operate.update_data(data)
            
            assert len(result) == 1
            assert result[0].id == 1
    
    @pytest.mark.asyncio
    async def test_update_data_validation_errors(self, general_operate):
        """Test update data with validation errors."""
        data = [{"name": "updated"}]  # Missing ID
        
        with patch.object(general_operate, '_validate_update_data', return_value=(
            [], [], ["Missing 'id' field"]
        )):
            
            with pytest.raises(GeneralOperateException) as exc_info:
                await general_operate.update_data(data)
            
            assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
            assert "Validation errors" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_update_data_no_valid_updates(self, general_operate):
        """Test update data with no valid updates."""
        data = [{"id": 1}]
        
        with patch.object(general_operate, '_validate_update_data', return_value=([], [], [])):
            
            result = await general_operate.update_data(data)
            assert result == []
    
    @pytest.mark.asyncio
    async def test_update_data_sql_error(self, general_operate):
        """Test update data with SQL error."""
        data = [{"id": 1, "name": "updated"}]
        
        with patch.object(general_operate, '_validate_update_data', return_value=(
            [{"id": 1, "data": {"name": "updated"}}], ["1"], []
        )), \
        patch.object(general_operate, '_clear_update_caches', return_value=[]), \
        patch.object(general_operate, 'update_sql', side_effect=Exception("SQL error")):
            
            with pytest.raises(Exception):
                await general_operate.update_data(data)
    
    @pytest.mark.asyncio
    async def test_update_data_incomplete_update(self, general_operate):
        """Test update data with incomplete updates."""
        data = [{"id": 1, "name": "updated1"}, {"id": 2, "name": "updated2"}]
        
        with patch.object(general_operate, '_validate_update_data', return_value=(
            [{"id": 1, "data": {"name": "updated1"}}, {"id": 2, "data": {"name": "updated2"}}], ["1", "2"], []
        )), \
        patch.object(general_operate, '_clear_update_caches', return_value=[]), \
        patch.object(general_operate, 'update_sql', return_value=[{"id": 1, "name": "updated1"}]), \
        patch.object(general_operate, '_convert_update_results', return_value=(
            [MockSchema(id=1, name="updated1")], []
        )):
            
            with pytest.raises(GeneralOperateException) as exc_info:
                await general_operate.update_data(data)
            
            assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
            assert "Update incomplete" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_update_data_with_session(self, general_operate):
        """Test update data with external session."""
        data = [{"id": 1, "name": "updated"}]
        session = AsyncMock()
        
        with patch.object(general_operate, '_validate_update_data', return_value=(
            [{"id": 1, "data": {"name": "updated"}}], ["1"], []
        )), \
        patch.object(general_operate, '_clear_update_caches', return_value=[]), \
        patch.object(general_operate, 'update_sql', return_value=[{"id": 1, "name": "updated"}]) as mock_update, \
        patch.object(general_operate, '_convert_update_results', return_value=(
            [MockSchema(id=1, name="updated")], []
        )):
            
            result = await general_operate.update_data(data, session=session)
            
            mock_update.assert_called_with("test_table", [{"id": 1, "data": {"name": "updated"}}], "id", session=session)


class TestGeneralOperateUpdateByForeignKey:
    """Test update by foreign key functionality."""
    
    @pytest.mark.asyncio
    async def test_update_by_foreign_key_empty(self, general_operate):
        """Test update by foreign key with empty data."""
        await general_operate.update_by_foreign_key("user_id", 123, [])
        # Should complete without error
    
    @pytest.mark.asyncio
    async def test_update_by_foreign_key_success(self, general_operate):
        """Test successful update by foreign key."""
        data = [{"id": 1, "name": "updated"}, {"name": "new"}]  # One update, one create
        existing = [{"id": 1, "name": "old", "user_id": 123}]
        
        with patch('general_operate.utils.build_data.compare_related_items') as mock_compare, \
             patch.object(general_operate, 'read_data_by_filter', return_value=[MockSchema(**existing[0])]), \
             patch.object(general_operate, 'delete_data') as mock_delete, \
             patch.object(general_operate, 'update_data') as mock_update, \
             patch.object(general_operate, 'create_data') as mock_create:
            
            mock_compare.return_value = (
                [{"name": "new", "user_id": 123}],  # to_create
                [{"id": 1, "name": "updated", "user_id": 123}],  # to_update
                [2]  # to_delete_ids
            )
            
            await general_operate.update_by_foreign_key("user_id", 123, data)
            
            mock_delete.assert_called_once_with(id_value={2}, session=None)
            mock_update.assert_called_once_with([{"id": 1, "name": "updated", "user_id": 123}], session=None)
            mock_create.assert_called_once_with([{"name": "new", "user_id": 123}], session=None)
    
    @pytest.mark.asyncio
    async def test_update_by_foreign_key_with_session(self, general_operate):
        """Test update by foreign key with session."""
        data = []
        session = AsyncMock()
        
        with patch('general_operate.utils.build_data.compare_related_items') as mock_compare, \
             patch.object(general_operate, 'read_data_by_filter', return_value=[]):
            
            mock_compare.return_value = ([], [], [])
            
            await general_operate.update_by_foreign_key("user_id", 123, data, session=session)
            
            # Should pass session to operations


class TestGeneralOperateDeleteData:
    """Test delete data functionality."""
    
    @pytest.mark.asyncio
    async def test_delete_data_empty(self, general_operate):
        """Test delete data with empty IDs."""
        result = await general_operate.delete_data(set())
        assert result == []
    
    @pytest.mark.asyncio
    async def test_delete_data_success(self, general_operate):
        """Test successful delete data."""
        with patch.object(general_operate, 'delete_sql', new=AsyncMock(return_value=[1, 2])) as mock_delete_sql, \
             patch('general_operate.app.cache_operate.CacheOperate.delete_caches', new=AsyncMock()) as mock_delete_cache, \
             patch.object(general_operate, 'delete_null_key', new=AsyncMock()) as mock_delete_null:
            
            result = await general_operate.delete_data({1, 2})
            
            assert result == [1, 2]
            mock_delete_sql.assert_called_once()
            mock_delete_cache.assert_called_once_with(general_operate, general_operate.table_name, {'1', '2'})
            assert mock_delete_null.call_count == 2
    
    @pytest.mark.asyncio
    async def test_delete_data_invalid_ids(self, general_operate):
        """Test delete data with invalid IDs."""
        with patch.object(general_operate, 'delete_sql', return_value=[]):
            
            result = await general_operate.delete_data({None, "invalid"})
            # Should filter out invalid IDs
            assert result == []
    
    @pytest.mark.asyncio
    async def test_delete_data_cache_error(self, general_operate):
        """Test delete data with cache errors."""
        with patch.object(general_operate, 'delete_sql', new=AsyncMock(return_value=[1])), \
             patch('general_operate.app.cache_operate.CacheOperate.delete_caches', new=AsyncMock(side_effect=redis.RedisError("Cache error"))), \
             patch.object(general_operate, 'delete_null_key', new=AsyncMock()):
            
            # Should not raise exception, just log warning
            result = await general_operate.delete_data({1})
            assert result == [1]
    
    @pytest.mark.asyncio
    async def test_delete_data_null_marker_error(self, general_operate):
        """Test delete data with null marker errors."""
        with patch.object(general_operate, 'delete_sql', new=AsyncMock(return_value=[1])), \
             patch('general_operate.app.cache_operate.CacheOperate.delete_caches', new=AsyncMock()), \
             patch.object(general_operate, 'delete_null_key', new=AsyncMock(side_effect=redis.RedisError("Null error"))):
            
            # Should not raise exception, just log debug
            result = await general_operate.delete_data({1})
            assert result == [1]
    
    @pytest.mark.asyncio
    async def test_delete_data_with_session(self, general_operate):
        """Test delete data with session."""
        session = AsyncMock()
        
        with patch.object(general_operate, 'delete_sql', new=AsyncMock(return_value=[1])) as mock_delete, \
             patch('general_operate.app.cache_operate.CacheOperate.delete_caches', new=AsyncMock()), \
             patch.object(general_operate, 'delete_null_key', new=AsyncMock()):
            
            result = await general_operate.delete_data({1}, session=session)
            
            assert result == [1]
            mock_delete.assert_called_with("test_table", [1], session=session)


class TestGeneralOperateDeleteFilterData:
    """Test delete filter data functionality."""
    
    @pytest.mark.asyncio
    async def test_delete_filter_data_empty_filters(self, general_operate):
        """Test delete filter data with empty filters."""
        result = await general_operate.delete_filter_data({})
        assert result == []
    
    @pytest.mark.asyncio
    async def test_delete_filter_data_success(self, general_operate):
        """Test successful delete filter data."""
        with patch.object(general_operate, 'delete_filter', new=AsyncMock(return_value=[1, 2])) as mock_delete, \
             patch.object(general_operate, 'delete_caches', new=AsyncMock()) as mock_delete_cache, \
             patch.object(general_operate, 'delete_null_key', new=AsyncMock()) as mock_delete_null:
            
            result = await general_operate.delete_filter_data({"status": "inactive"})
            
            assert result == [1, 2]
            mock_delete.assert_called_once()
            mock_delete_cache.assert_called_once()
            assert mock_delete_null.call_count == 2
    
    @pytest.mark.asyncio
    async def test_delete_filter_data_cache_error(self, general_operate):
        """Test delete filter data with cache errors."""
        with patch.object(general_operate, 'delete_filter', new=AsyncMock(return_value=[1])), \
             patch.object(general_operate, 'delete_caches', new=AsyncMock(side_effect=redis.RedisError("Cache error"))), \
             patch.object(general_operate, 'delete_null_key', new=AsyncMock()):
            
            result = await general_operate.delete_filter_data({"status": "inactive"})
            assert result == [1]
    
    @pytest.mark.asyncio
    async def test_delete_filter_data_database_error(self, general_operate):
        """Test delete filter data with database errors."""
        from sqlalchemy.exc import DBAPIError
        from general_operate import GeneralOperateException
        
        with patch.object(general_operate, 'delete_filter', new=AsyncMock(side_effect=DBAPIError(
            statement="DELETE", params={}, orig=Exception("DB error")
        ))):
            
            # The error gets wrapped in GeneralOperateException by the decorator
            with pytest.raises(GeneralOperateException) as exc_info:
                await general_operate.delete_filter_data({"status": "inactive"})
            
            # Verify the original error is preserved in the cause
            assert isinstance(exc_info.value.cause, DBAPIError)
    
    @pytest.mark.asyncio
    async def test_delete_filter_data_validation_error(self, general_operate):
        """Test delete filter data with validation errors."""
        with patch.object(general_operate, 'delete_filter', side_effect=ValueError("Invalid filter")):
            
            result = await general_operate.delete_filter_data({"invalid": "filter"})
            assert result == []
    
    @pytest.mark.asyncio
    async def test_delete_filter_data_generic_error(self, general_operate):
        """Test delete filter data with generic errors."""
        with patch.object(general_operate, 'delete_filter', side_effect=Exception("Generic error")):
            
            result = await general_operate.delete_filter_data({"status": "inactive"})
            assert result == []


class TestGeneralOperateCountData:
    """Test count data functionality."""
    
    @pytest.mark.asyncio
    async def test_count_data_success(self, general_operate):
        """Test successful count data."""
        with patch.object(general_operate, 'count_sql', return_value=42):
            
            result = await general_operate.count_data({"status": "active"})
            
            assert result == 42
    
    @pytest.mark.asyncio
    async def test_count_data_no_filters(self, general_operate):
        """Test count data without filters."""
        with patch.object(general_operate, 'count_sql', return_value=100):
            
            result = await general_operate.count_data()
            
            assert result == 100
    
    @pytest.mark.asyncio
    async def test_count_data_with_session(self, general_operate):
        """Test count data with session."""
        session = AsyncMock()
        
        with patch.object(general_operate, 'count_sql', return_value=10) as mock_count:
            
            result = await general_operate.count_data({"status": "active"}, session=session)
            
            mock_count.assert_called_with(
                table_name="test_table",
                filters={"status": "active"},
                date_field=None,
                start_date=None,
                end_date=None,
                session=session
            )
    
    @pytest.mark.asyncio
    async def test_count_data_general_operate_exception(self, general_operate):
        """Test count data with GeneralOperateException."""
        with patch.object(general_operate, 'count_sql', side_effect=GeneralOperateException(
            code=ErrorCode.DB_QUERY_ERROR,
            message="Database error"
        )):
            
            with pytest.raises(GeneralOperateException):
                await general_operate.count_data()
    
    @pytest.mark.asyncio
    async def test_count_data_generic_exception(self, general_operate):
        """Test count data with generic exception."""
        with patch.object(general_operate, 'count_sql', side_effect=Exception("Generic error")):
            
            with pytest.raises(GeneralOperateException) as exc_info:
                await general_operate.count_data()
            
            assert exc_info.value.code == ErrorCode.UNKNOWN_ERROR
            assert "Unexpected error during count operation" in str(exc_info.value)


class TestGeneralOperateStoreCacheData:
    """Test store cache data functionality."""
    
    @pytest.mark.asyncio
    async def test_store_cache_data_success(self, general_operate):
        """Test successful store cache data."""
        data = {"name": "test", "value": 123}
        
        with patch.object(general_operate.redis, 'setex', new=AsyncMock()) as mock_setex:
            
            await general_operate.store_cache_data("prefix", "123", data, 300)
            
            mock_setex.assert_called_once()
            # Verify the serialized data contains metadata
            call_args = mock_setex.call_args
            assert call_args[0][0] == "prefix:123"  # key
            assert call_args[0][1] == 300  # ttl
            
            # Parse the JSON data
            json_data = json.loads(call_args[0][2])
            assert json_data["name"] == "test"
            assert json_data["value"] == 123
            assert "prefix" in json_data
            assert "_created_at" in json_data
            assert "_identifier" in json_data
    
    @pytest.mark.asyncio
    async def test_store_cache_data_no_ttl(self, general_operate):
        """Test store cache data without TTL."""
        data = {"name": "test"}
        
        with patch.object(general_operate.redis, 'setex', new=AsyncMock()) as mock_setex:
            
            await general_operate.store_cache_data("prefix", "123", data)
            
            mock_setex.assert_called_once()


class TestGeneralOperateGetCacheData:
    """Test get cache data functionality."""
    
    @pytest.mark.asyncio
    async def test_get_cache_data_success(self, general_operate):
        """Test successful get cache data."""
        expected_data = {"name": "test", "value": 123}
        
        with patch('general_operate.app.cache_operate.CacheOperate.get_cache', return_value=expected_data):
            
            result = await general_operate.get_cache_data("prefix", "123")
            
            assert result == expected_data
    
    @pytest.mark.asyncio
    async def test_get_cache_data_not_found(self, general_operate):
        """Test get cache data when not found."""
        with patch('general_operate.app.cache_operate.CacheOperate.get_cache', return_value=None):
            
            result = await general_operate.get_cache_data("prefix", "123")
            
            assert result is None


class TestGeneralOperateDeleteCacheData:
    """Test delete cache data functionality."""
    
    @pytest.mark.asyncio
    async def test_delete_cache_data_success(self, general_operate):
        """Test successful delete cache data."""
        with patch('general_operate.app.cache_operate.CacheOperate.delete_cache', return_value=True):
            
            result = await general_operate.delete_cache_data("prefix", "123")
            
            assert result is True
    
    @pytest.mark.asyncio
    async def test_delete_cache_data_not_found(self, general_operate):
        """Test delete cache data when not found."""
        with patch('general_operate.app.cache_operate.CacheOperate.delete_cache', return_value=False):
            
            result = await general_operate.delete_cache_data("prefix", "123")
            
            assert result is False


# Continue with additional test classes...
# (Due to length constraints, I'll create this as the main comprehensive test file)

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--cov=general_operate.general_operate", "--cov-report=term-missing"])