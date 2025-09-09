"""Comprehensive test coverage for general_operate.py - achieving 100% coverage"""

import json
import pytest
import pytest_asyncio
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch, call, PropertyMock
from datetime import datetime, UTC
from contextlib import asynccontextmanager

import redis
from sqlalchemy.exc import DBAPIError

from general_operate.general_operate import GeneralOperate
from general_operate import GeneralOperateException, ErrorCode, ErrorContext
from general_operate.app.cache_operate import CacheOperate
from general_operate.app.sql_operate import SQLOperate
from general_operate.app.influxdb_operate import InfluxOperate
from general_operate.app.client.influxdb import InfluxDB


class MockModule:
    """Mock module for testing"""
    table_name = "test_table"
    
    class MainSchema:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)
        
        def model_dump(self):
            return self.__dict__.copy()
    
    class CreateSchema:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)
            
        def model_dump(self, exclude_unset=False):
            return self.__dict__.copy()
    
    class UpdateSchema:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)
            
        def model_dump(self, exclude_unset=False):
            if exclude_unset:
                return {k: v for k, v in self.__dict__.items() if v is not None}
            return self.__dict__.copy()
    
    main_schemas = MainSchema
    create_schemas = CreateSchema
    update_schemas = UpdateSchema


class TestGeneralOperator(GeneralOperate):
    """Test implementation of GeneralOperate"""
    
    def get_module(self):
        """Return mock module"""
        return MockModule()


class TestGeneralOperatorNoModule(GeneralOperate):
    """Test implementation without module"""
    
    def get_module(self):
        """Return None to test module-less initialization"""
        return None


@pytest_asyncio.fixture
async def operator():
    """Create test operator instance with all dependencies"""
    # Mock database client
    db_client = MagicMock()
    db_client.engine_type = "postgresql"
    db_client.get_engine = MagicMock()
    
    # Mock Redis client
    redis_client = AsyncMock(spec=redis.asyncio.Redis)
    
    # Mock InfluxDB with required attributes
    influx_client = MagicMock()
    influx_client.bucket = "test_bucket"
    influx_client.measurement = "test_measurement"
    influx_client.write_api = MagicMock()
    influx_client.query_api = MagicMock()
    
    # Mock Kafka config
    kafka_config = {"bootstrap_servers": "localhost:9092"}
    
    op = TestGeneralOperator(
        database_client=db_client,
        redis_client=redis_client,
        influxdb=influx_client,
        kafka_config=kafka_config
    )
    
    # Set up async mocks for Redis operations
    op.redis.exists = AsyncMock(return_value=False)
    op.redis.get = AsyncMock(return_value=None)
    op.redis.setex = AsyncMock(return_value=True)
    op.redis.delete = AsyncMock(return_value=1)
    op.redis.keys = AsyncMock(return_value=[])
    op.redis.pipeline = MagicMock()
    op.redis.ping = AsyncMock(return_value=True)
    
    return op


@pytest_asyncio.fixture
async def operator_no_module():
    """Create operator without module"""
    redis_client = AsyncMock(spec=redis.asyncio.Redis)
    return TestGeneralOperatorNoModule(redis_client=redis_client)


class TestInitialization:
    """Test __init__ method and initialization logic"""
    
    def test_init_with_all_dependencies(self):
        """Test initialization with all dependencies"""
        db_client = MagicMock()
        redis_client = MagicMock(spec=redis.asyncio.Redis)
        
        # Mock InfluxDB with required attributes
        influx_client = MagicMock()
        influx_client.bucket = "test_bucket"
        influx_client.measurement = "test_measurement"
        
        kafka_config = {"bootstrap_servers": "localhost:9092"}
        
        op = TestGeneralOperator(
            database_client=db_client,
            redis_client=redis_client,
            influxdb=influx_client,
            kafka_config=kafka_config
        )
        
        assert op.table_name == "test_table"
        assert op.main_schemas is not None
        assert op.create_schemas is not None
        assert op.update_schemas is not None
        assert hasattr(op, 'redis')
        assert hasattr(op, 'logger')
    
    def test_init_minimal(self):
        """Test initialization with minimal dependencies"""
        op = TestGeneralOperatorNoModule()
        
        assert op.table_name is None
        assert op.main_schemas is None
        assert op.create_schemas is None
        assert op.update_schemas is None
    
    def test_init_redis_only(self):
        """Test initialization with Redis only"""
        redis_client = MagicMock(spec=redis.asyncio.Redis)
        op = TestGeneralOperator(redis_client=redis_client)
        
        assert hasattr(op, 'redis')
        assert op.table_name == "test_table"
    
    def test_init_database_only(self):
        """Test initialization with database only"""
        db_client = MagicMock()
        op = TestGeneralOperator(database_client=db_client)
        
        assert hasattr(op, 'logger')
        assert op.table_name == "test_table"


class TestTransactionManager:
    """Test transaction context manager"""
    
    @pytest.mark.asyncio
    async def test_transaction_success(self, operator):
        """Test successful transaction"""
        mock_session = AsyncMock()
        mock_session.close = AsyncMock()
        
        # Mock the begin() context manager
        mock_begin = AsyncMock()
        mock_begin.__aenter__ = AsyncMock(return_value=None)
        mock_begin.__aexit__ = AsyncMock(return_value=None)
        mock_session.begin = MagicMock(return_value=mock_begin)
        
        with patch.object(operator, 'create_external_session', return_value=mock_session):
            async with operator.transaction() as session:
                assert session == mock_session
            
            mock_session.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_transaction_with_error(self, operator):
        """Test transaction with error handling"""
        mock_session = AsyncMock()
        mock_session.close = AsyncMock()
        
        # Mock the begin() context manager
        mock_begin = AsyncMock()
        mock_begin.__aenter__ = AsyncMock(return_value=None)
        mock_begin.__aexit__ = AsyncMock(return_value=None)
        mock_session.begin = MagicMock(return_value=mock_begin)
        
        with patch.object(operator, 'create_external_session', return_value=mock_session):
            try:
                async with operator.transaction() as session:
                    raise ValueError("Test error")
            except ValueError:
                pass
            
            # Session should still be closed
            mock_session.close.assert_called_once()


class TestHelperMethods:
    """Test helper methods"""
    
    def test_build_cache_key(self, operator):
        """Test _build_cache_key method"""
        key = operator._build_cache_key(123)
        assert key == "test_table:123"
        
        key = operator._build_cache_key("abc")
        assert key == "test_table:abc"
    
    def test_build_null_marker_key(self, operator):
        """Test _build_null_marker_key method"""
        key = operator._build_null_marker_key(123)
        assert key == "test_table:123:null"
        
        key = operator._build_null_marker_key("abc")
        assert key == "test_table:abc:null"
    
    def test_validate_with_schema_main(self, operator):
        """Test _validate_with_schema with main schema"""
        data = {"id": 1, "name": "test"}
        result = operator._validate_with_schema(data, "main")
        assert result.id == 1
        assert result.name == "test"
    
    def test_validate_with_schema_create(self, operator):
        """Test _validate_with_schema with create schema"""
        data = {"name": "test", "value": "data"}
        result = operator._validate_with_schema(data, "create")
        assert result.name == "test"
        assert result.value == "data"
    
    def test_validate_with_schema_update(self, operator):
        """Test _validate_with_schema with update schema"""
        data = {"id": 1, "value": "updated"}
        result = operator._validate_with_schema(data, "update")
        assert result.id == 1
        assert result.value == "updated"
    
    def test_validate_with_schema_invalid_type(self, operator):
        """Test _validate_with_schema with invalid schema type"""
        data = {"id": 1}
        # Should default to main_schemas
        result = operator._validate_with_schema(data, "invalid")
        assert result.id == 1
    
    def test_process_in_batches(self, operator):
        """Test _process_in_batches generator"""
        items = list(range(250))
        batches = list(operator._process_in_batches(items, batch_size=100))
        
        assert len(batches) == 3
        assert len(batches[0]) == 100
        assert len(batches[1]) == 100
        assert len(batches[2]) == 50
    
    def test_process_in_batches_empty(self, operator):
        """Test _process_in_batches with empty list"""
        items = []
        batches = list(operator._process_in_batches(items))
        assert len(batches) == 0
    
    def test_process_in_batches_small(self, operator):
        """Test _process_in_batches with small list"""
        items = [1, 2, 3]
        batches = list(operator._process_in_batches(items, batch_size=10))
        assert len(batches) == 1
        assert batches[0] == [1, 2, 3]


class TestHealthCheck:
    """Test health_check method"""
    
    @pytest.mark.asyncio
    async def test_health_check_all_healthy(self, operator):
        """Test health check when all services are healthy"""
        with patch.object(SQLOperate, 'health_check', return_value=True):
            with patch.object(CacheOperate, 'health_check', return_value=True):
                result = await operator.health_check()
                assert result is True
    
    @pytest.mark.asyncio
    async def test_health_check_sql_unhealthy(self, operator):
        """Test health check when SQL is unhealthy"""
        with patch.object(SQLOperate, 'health_check', return_value=False):
            with patch.object(CacheOperate, 'health_check', return_value=True):
                result = await operator.health_check()
                assert result is False
    
    @pytest.mark.asyncio
    async def test_health_check_cache_unhealthy(self, operator):
        """Test health check when cache is unhealthy"""
        with patch.object(SQLOperate, 'health_check', return_value=True):
            with patch.object(CacheOperate, 'health_check', return_value=False):
                result = await operator.health_check()
                assert result is False
    
    @pytest.mark.asyncio
    async def test_health_check_both_unhealthy(self, operator):
        """Test health check when both services are unhealthy"""
        with patch.object(SQLOperate, 'health_check', return_value=False):
            with patch.object(CacheOperate, 'health_check', return_value=False):
                result = await operator.health_check()
                assert result is False


class TestCacheWarming:
    """Test cache_warming method"""
    
    @pytest.mark.asyncio
    async def test_cache_warming_success(self, operator):
        """Test successful cache warming"""
        mock_data = [
            {"id": 1, "name": "test1"},
            {"id": 2, "name": "test2"}
        ]
        
        with patch.object(operator, 'read_sql', return_value=mock_data) as mock_read:
            with patch.object(operator, 'store_caches', return_value=True) as mock_store:
                result = await operator.cache_warming(limit=100)
                
                assert result["success"] is True
                assert result["records_loaded"] == 2
                mock_read.assert_called_once()
                mock_store.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_cache_warming_empty_data(self, operator):
        """Test cache warming with no data"""
        with patch.object(operator, 'read_sql', return_value=[]):
            result = await operator.cache_warming(limit=100)
            
            assert result["success"] is True
            assert result["records_loaded"] == 0
    
    @pytest.mark.asyncio
    async def test_cache_warming_with_pagination(self, operator):
        """Test cache warming with multiple batches"""
        # First batch
        batch1 = [{"id": i, "name": f"test{i}"} for i in range(1, 501)]
        # Second batch
        batch2 = [{"id": i, "name": f"test{i}"} for i in range(501, 601)]
        # Empty batch to stop iteration
        batch3 = []
        
        with patch.object(operator, 'read_sql', side_effect=[batch1, batch2, batch3]):
            with patch.object(operator, 'store_caches', return_value=True):
                result = await operator.cache_warming(limit=1000)
                
                assert result["success"] is True
                assert result["records_loaded"] == 600
    
    @pytest.mark.asyncio
    async def test_cache_warming_skip_invalid_records(self, operator):
        """Test cache warming skips records without ID"""
        mock_data = [
            {"id": 1, "name": "test1"},
            {"name": "no_id"},  # Missing ID
            {"id": 2, "name": "test2"},
            None,  # None record
        ]
        
        with patch.object(operator, 'read_sql', return_value=mock_data):
            with patch.object(operator, 'store_caches', return_value=True) as mock_store:
                result = await operator.cache_warming(limit=100)
                
                assert result["success"] is True
                assert result["records_loaded"] == 2
                
                # Check that only valid records were stored
                stored_data = mock_store.call_args[0][1]
                assert len(stored_data) == 2
                assert "1" in stored_data
                assert "2" in stored_data


class TestCacheClear:
    """Test cache_clear method"""
    
    @pytest.mark.asyncio
    async def test_cache_clear_success(self, operator):
        """Test successful cache clear"""
        operator.redis.keys = AsyncMock(return_value=[
            b"test_table:1:null",
            b"test_table:2:null"
        ])
        operator.redis.delete = AsyncMock(return_value=1)
        
        result = await operator.cache_clear()
        
        assert result["success"] is True
        assert result["message"] == "Cache cleared successfully"
        
        # Verify main cache and null markers were deleted
        assert operator.redis.delete.call_count == 2
    
    @pytest.mark.asyncio
    async def test_cache_clear_no_null_markers(self, operator):
        """Test cache clear with no null markers"""
        operator.redis.keys = AsyncMock(return_value=[])
        operator.redis.delete = AsyncMock(return_value=1)
        
        result = await operator.cache_clear()
        
        assert result["success"] is True
        # Only main cache delete should be called
        operator.redis.delete.assert_called_once_with("test_table")


class TestProcessCacheLookups:
    """Test _process_cache_lookups method"""
    
    @pytest.mark.asyncio
    async def test_process_cache_lookups_all_hits(self, operator):
        """Test when all IDs are found in cache"""
        operator.redis.exists = AsyncMock(return_value=False)
        
        async def mock_get_caches(table_name, ids):
            if '1' in ids:
                return [{"id": 1, "name": "test1"}]
            elif '2' in ids:
                return [{"id": 2, "name": "test2"}]
            return []
        
        with patch.object(operator, 'get_caches', side_effect=mock_get_caches):
            results, miss_ids, null_ids, failed_ops = await operator._process_cache_lookups({1, 2})
            
            assert len(results) == 2
            assert len(miss_ids) == 0
            assert len(null_ids) == 0
            assert len(failed_ops) == 0
    
    @pytest.mark.asyncio
    async def test_process_cache_lookups_with_null_markers(self, operator):
        """Test when some IDs have null markers"""
        operator.redis.exists = AsyncMock(side_effect=[True, False])  # First has null marker
        
        with patch.object(operator, 'get_caches', return_value=[]):
            results, miss_ids, null_ids, failed_ops = await operator._process_cache_lookups({1, 2})
            
            assert len(results) == 0
            assert 2 in miss_ids
            assert 1 in null_ids
            assert len(failed_ops) == 0
    
    @pytest.mark.asyncio
    async def test_process_cache_lookups_with_schema_error(self, operator):
        """Test when schema validation fails"""
        operator.redis.exists = AsyncMock(return_value=False)
        
        # Mock schema to raise error
        operator.main_schemas = MagicMock(side_effect=ValueError("Schema error"))
        
        cache_data = [{"id": 1, "invalid": "data"}]
        
        with patch.object(operator, 'get_caches', return_value=cache_data):
            results, miss_ids, null_ids, failed_ops = await operator._process_cache_lookups({1})
            
            assert len(results) == 0
            assert 1 in miss_ids
    
    @pytest.mark.asyncio
    async def test_process_cache_lookups_with_redis_error(self, operator):
        """Test when Redis operations fail"""
        operator.redis.exists = AsyncMock(side_effect=redis.RedisError("Connection error"))
        
        results, miss_ids, null_ids, failed_ops = await operator._process_cache_lookups({1})
        
        assert len(results) == 0
        assert 1 in miss_ids
        assert 1 in failed_ops


class TestHandleCacheMisses:
    """Test _handle_cache_misses method"""
    
    @pytest.mark.asyncio
    async def test_handle_cache_misses_success(self, operator):
        """Test successful handling of cache misses"""
        sql_data = [
            {"id": 1, "name": "test1"},
            {"id": 2, "name": "test2"}
        ]
        
        with patch.object(operator, '_fetch_from_sql', return_value=sql_data):
            with patch.object(operator, '_update_cache_after_fetch', return_value=None):
                results, found_ids = await operator._handle_cache_misses({1, 2}, [])
                
                assert len(results) == 2
                assert found_ids == {1, 2}
    
    @pytest.mark.asyncio
    async def test_handle_cache_misses_empty_set(self, operator):
        """Test with empty cache miss set"""
        results, found_ids = await operator._handle_cache_misses(set(), [])
        
        assert len(results) == 0
        assert len(found_ids) == 0
    
    @pytest.mark.asyncio
    async def test_handle_cache_misses_with_failed_ops(self, operator):
        """Test handling when some operations previously failed"""
        sql_data = [
            {"id": 1, "name": "test1"},
            {"id": 2, "name": "test2"}
        ]
        
        with patch.object(operator, '_fetch_from_sql', return_value=sql_data):
            with patch.object(operator, '_update_cache_after_fetch', return_value=None) as mock_update:
                results, found_ids = await operator._handle_cache_misses({1, 2}, [2])
                
                # ID 2 should not be in cache update since it failed before
                cache_data = mock_update.call_args[0][0]
                assert "1" in cache_data
                assert "2" not in cache_data
    
    @pytest.mark.asyncio
    async def test_handle_cache_misses_schema_error(self, operator):
        """Test when schema validation fails during cache miss handling"""
        sql_data = [{"id": 1, "name": "test1"}]
        
        # Mock schema to raise error
        operator.main_schemas = MagicMock(side_effect=ValueError("Schema error"))
        
        with patch.object(operator, '_fetch_from_sql', return_value=sql_data):
            with patch.object(operator, '_update_cache_after_fetch', return_value=None):
                results, found_ids = await operator._handle_cache_misses({1}, [])
                
                assert len(results) == 0
                assert len(found_ids) == 0


class TestUpdateCacheAfterFetch:
    """Test _update_cache_after_fetch method"""
    
    @pytest.mark.asyncio
    async def test_update_cache_after_fetch_success(self, operator):
        """Test successful cache update after fetch"""
        cache_data = {"1": {"id": 1, "name": "test"}}
        
        with patch.object(operator, 'store_caches', return_value=True):
            await operator._update_cache_after_fetch(cache_data)
            
            # Verify store_caches was called
            operator.store_caches.assert_called_once_with("test_table", cache_data)
    
    @pytest.mark.asyncio
    async def test_update_cache_after_fetch_failure(self, operator):
        """Test cache update failure handling"""
        cache_data = {"1": {"id": 1, "name": "test"}}
        
        with patch.object(operator, 'store_caches', return_value=False):
            # Should not raise, just log warning
            await operator._update_cache_after_fetch(cache_data)
    
    @pytest.mark.asyncio
    async def test_update_cache_after_fetch_exception(self, operator):
        """Test cache update exception handling"""
        cache_data = {"1": {"id": 1, "name": "test"}}
        
        with patch.object(operator, 'store_caches', side_effect=Exception("Cache error")):
            # Should not raise, just log warning
            await operator._update_cache_after_fetch(cache_data)


class TestMarkMissingRecords:
    """Test _mark_missing_records method"""
    
    @pytest.mark.asyncio
    async def test_mark_missing_records_success(self, operator):
        """Test successful marking of missing records"""
        with patch.object(operator, 'set_null_key', return_value=True) as mock_set:
            await operator._mark_missing_records({1, 2}, [])
            
            assert mock_set.call_count == 2
            mock_set.assert_any_call("test_table:1:null", 300)
            mock_set.assert_any_call("test_table:2:null", 300)
    
    @pytest.mark.asyncio
    async def test_mark_missing_records_skip_failed(self, operator):
        """Test skipping IDs that previously failed"""
        with patch.object(operator, 'set_null_key', return_value=True) as mock_set:
            await operator._mark_missing_records({1, 2}, [2])
            
            # Should only mark ID 1
            assert mock_set.call_count == 1
            mock_set.assert_called_with("test_table:1:null", 300)
    
    @pytest.mark.asyncio
    async def test_mark_missing_records_exception(self, operator):
        """Test exception handling when setting null markers"""
        with patch.object(operator, 'set_null_key', side_effect=Exception("Redis error")):
            # Should not raise, just log warning
            await operator._mark_missing_records({1}, [])


class TestReadDataById:
    """Test read_data_by_id method"""
    
    @pytest.mark.asyncio
    async def test_read_data_by_id_cache_hit(self, operator):
        """Test read with all cache hits"""
        operator.redis.exists = AsyncMock(return_value=False)
        
        cache_data = [{"id": 1, "name": "test1"}]
        with patch.object(operator, 'get_caches', return_value=cache_data):
            results = await operator.read_data_by_id({1})
            
            assert len(results) == 1
            assert results[0].id == 1
    
    @pytest.mark.asyncio
    async def test_read_data_by_id_empty_set(self, operator):
        """Test read with empty ID set"""
        results = await operator.read_data_by_id(set())
        assert len(results) == 0
    
    @pytest.mark.asyncio
    async def test_read_data_by_id_with_sql_fallback(self, operator):
        """Test read with SQL fallback for cache misses"""
        with patch.object(operator, '_process_cache_lookups', 
                         return_value=([], {1}, set(), [])):
            with patch.object(operator, '_handle_cache_misses',
                            return_value=([MagicMock(id=1)], {1})):
                with patch.object(operator, '_mark_missing_records'):
                    results = await operator.read_data_by_id({1})
                    
                    assert len(results) == 1
    
    @pytest.mark.asyncio
    async def test_read_data_by_id_sql_error(self, operator):
        """Test read when SQL fallback fails"""
        with patch.object(operator, '_process_cache_lookups',
                         return_value=([], {1}, set(), [])):
            with patch.object(operator, '_handle_cache_misses',
                            side_effect=Exception("SQL error")):
                with pytest.raises(GeneralOperateException) as exc_info:
                    await operator.read_data_by_id({1})
                
                assert exc_info.value.code == ErrorCode.UNKNOWN_ERROR
    
    @pytest.mark.asyncio
    async def test_read_data_by_id_unexpected_error(self, operator):
        """Test read with unexpected error"""
        with patch.object(operator, '_process_cache_lookups',
                         side_effect=RuntimeError("Unexpected")):
            with pytest.raises(GeneralOperateException) as exc_info:
                await operator.read_data_by_id({1})
            
            assert exc_info.value.code == ErrorCode.UNKNOWN_ERROR


class TestReadDataByFilter:
    """Test read_data_by_filter method"""
    
    @pytest.mark.asyncio
    async def test_read_data_by_filter_success(self, operator):
        """Test successful filter-based read"""
        sql_data = [
            {"id": 1, "name": "test1", "status": "active"},
            {"id": 2, "name": "test2", "status": "active"}
        ]
        
        with patch.object(operator, 'read_sql', return_value=sql_data):
            results = await operator.read_data_by_filter({"status": "active"})
            
            assert len(results) == 2
            assert results[0].id == 1
            assert results[1].id == 2
    
    @pytest.mark.asyncio
    async def test_read_data_by_filter_with_pagination(self, operator):
        """Test filter read with pagination"""
        sql_data = [{"id": 1, "name": "test1"}]
        
        with patch.object(operator, 'read_sql', return_value=sql_data) as mock_read:
            results = await operator.read_data_by_filter(
                {"status": "active"},
                limit=10,
                offset=20,
                order_by="created_at",
                order_direction="DESC"
            )
            
            mock_read.assert_called_once_with(
                table_name="test_table",
                filters={"status": "active"},
                limit=10,
                offset=20,
                order_by="created_at",
                order_direction="DESC",
                date_field=None,
                start_date=None,
                end_date=None,
                session=None
            )
    
    @pytest.mark.asyncio
    async def test_read_data_by_filter_empty_result(self, operator):
        """Test filter read with no results"""
        with patch.object(operator, 'read_sql', return_value=[]):
            results = await operator.read_data_by_filter({"status": "deleted"})
            assert len(results) == 0
    
    @pytest.mark.asyncio
    async def test_read_data_by_filter_skip_none(self, operator):
        """Test filter read skips None records"""
        sql_data = [
            {"id": 1, "name": "test1"},
            None,
            {"id": 2, "name": "test2"}
        ]
        
        with patch.object(operator, 'read_sql', return_value=sql_data):
            results = await operator.read_data_by_filter({})
            assert len(results) == 2


class TestFetchFromSQL:
    """Test _fetch_from_sql method"""
    
    @pytest.mark.asyncio
    async def test_fetch_from_sql_single_id(self, operator):
        """Test fetch with single ID"""
        with patch.object(operator, 'read_one', return_value={"id": 1, "name": "test"}):
            results = await operator._fetch_from_sql({1})
            
            assert len(results) == 1
            assert results[0]["id"] == 1
    
    @pytest.mark.asyncio
    async def test_fetch_from_sql_multiple_ids(self, operator):
        """Test fetch with multiple IDs"""
        sql_data = [
            {"id": 1, "name": "test1"},
            {"id": 2, "name": "test2"}
        ]
        
        with patch.object(operator, 'read_sql', return_value=sql_data):
            results = await operator._fetch_from_sql({1, 2})
            
            assert len(results) == 2
    
    @pytest.mark.asyncio
    async def test_fetch_from_sql_empty_set(self, operator):
        """Test fetch with empty ID set"""
        results = await operator._fetch_from_sql(set())
        assert len(results) == 0
    
    @pytest.mark.asyncio
    async def test_fetch_from_sql_not_found(self, operator):
        """Test fetch when record not found"""
        with patch.object(operator, 'read_one', return_value=None):
            results = await operator._fetch_from_sql({999})
            assert len(results) == 0
    
    @pytest.mark.asyncio
    async def test_fetch_from_sql_error(self, operator):
        """Test fetch with database error"""
        with patch.object(operator, 'read_one', side_effect=Exception("DB error")):
            with pytest.raises(GeneralOperateException) as exc_info:
                await operator._fetch_from_sql({1})
            
            assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR


class TestCreateData:
    """Test create_data method"""
    
    @pytest.mark.asyncio
    async def test_create_data_success(self, operator):
        """Test successful data creation"""
        create_data = [
            {"name": "test1", "value": "data1"},
            {"name": "test2", "value": "data2"}
        ]
        
        created_records = [
            {"id": 1, "name": "test1", "value": "data1"},
            {"id": 2, "name": "test2", "value": "data2"}
        ]
        
        with patch.object(operator, 'create_sql', return_value=created_records):
            results = await operator.create_data(create_data)
            
            assert len(results) == 2
            assert results[0].id == 1
            assert results[1].id == 2
    
    @pytest.mark.asyncio
    async def test_create_data_empty_list(self, operator):
        """Test create with empty list"""
        results = await operator.create_data([])
        assert len(results) == 0
    
    @pytest.mark.asyncio
    async def test_create_data_with_session(self, operator):
        """Test create with transaction session"""
        mock_session = AsyncMock()
        
        with patch.object(operator, 'create_sql', return_value=[]) as mock_create:
            await operator.create_data([{"name": "test"}], session=mock_session)
            
            mock_create.assert_called_once()
            assert mock_create.call_args[1]["session"] == mock_session
    
    @pytest.mark.asyncio
    async def test_create_data_skip_invalid(self, operator):
        """Test create skips invalid items"""
        create_data = [
            {"name": "valid"},
            "invalid_string",  # Not a dict
            {"name": "valid2"}
        ]
        
        with patch.object(operator, 'create_sql', return_value=[]):
            await operator.create_data(create_data)
            
            # Only valid items should be processed
            operator.create_sql.assert_called_once()
            call_data = operator.create_sql.call_args[0][1]
            assert len(call_data) == 2
    
    @pytest.mark.asyncio
    async def test_create_data_schema_validation_error(self, operator):
        """Test create with schema validation errors"""
        # Mock schema to raise different errors
        operator.create_schemas = MagicMock(side_effect=[
            TypeError("Type error"),
            ValueError("Value error"),
            AttributeError("Attribute error"),
            MockModule.CreateSchema(name="valid")
        ])
        
        create_data = [
            {"name": "error1"},
            {"name": "error2"},
            {"name": "error3"},
            {"name": "valid"}
        ]
        
        with patch.object(operator, 'create_sql', return_value=[]):
            await operator.create_data(create_data)
            
            # Only valid item should be processed
            operator.create_sql.assert_called_once()
            call_data = operator.create_sql.call_args[0][1]
            assert len(call_data) == 1


class TestCreateByForeignKey:
    """Test create_by_foreign_key method"""
    
    @pytest.mark.asyncio
    async def test_create_by_foreign_key_success(self, operator):
        """Test successful creation with foreign key"""
        data = [
            {"name": "item1", "value": "data1"},
            {"name": "item2", "value": "data2"}
        ]
        
        with patch('general_operate.utils.build_data.build_create_data', side_effect=lambda x: x):
            with patch.object(operator, 'create_data', return_value=[]) as mock_create:
                await operator.create_by_foreign_key("parent_id", 123, data)
                
                mock_create.assert_called_once()
                created_data = mock_create.call_args[0][0]
                
                # Check foreign key was added
                assert all(item["parent_id"] == 123 for item in created_data)
    
    @pytest.mark.asyncio
    async def test_create_by_foreign_key_empty(self, operator):
        """Test create by foreign key with empty data"""
        results = await operator.create_by_foreign_key("parent_id", 123, [])
        assert len(results) == 0
    
    @pytest.mark.asyncio
    async def test_create_by_foreign_key_with_session(self, operator):
        """Test create by foreign key with session"""
        mock_session = AsyncMock()
        
        with patch('general_operate.utils.build_data.build_create_data', side_effect=lambda x: x):
            with patch.object(operator, 'create_data', return_value=[]) as mock_create:
                await operator.create_by_foreign_key(
                    "parent_id", 123, [{"name": "test"}], session=mock_session
                )
                
                assert mock_create.call_args[1]["session"] == mock_session


class TestValidateUpdateData:
    """Test _validate_update_data method"""
    
    @pytest.mark.asyncio
    async def test_validate_update_data_success(self, operator):
        """Test successful update data validation"""
        data = [
            {"id": 1, "value": "updated1"},
            {"id": 2, "value": "updated2"}
        ]
        
        update_list, cache_keys, errors = await operator._validate_update_data(data, "id")
        
        assert len(update_list) == 2
        assert cache_keys == ["1", "2"]
        assert len(errors) == 0
    
    @pytest.mark.asyncio
    async def test_validate_update_data_missing_where_field(self, operator):
        """Test validation with missing where field"""
        data = [
            {"value": "updated"},  # Missing 'id'
            {"id": 2, "value": "updated2"}
        ]
        
        update_list, cache_keys, errors = await operator._validate_update_data(data, "id")
        
        assert len(update_list) == 1
        assert len(errors) == 1
        assert "Missing 'id' field" in errors[0]
    
    @pytest.mark.asyncio
    async def test_validate_update_data_no_valid_fields(self, operator):
        """Test validation when no valid update fields"""
        # Mock update schema to return only the ID (which gets excluded)
        operator.update_schemas = MagicMock(return_value=MagicMock(
            model_dump=lambda exclude_unset: {"id": 1}
        ))
        
        data = [{"id": 1}]
        
        update_list, cache_keys, errors = await operator._validate_update_data(data, "id")
        
        assert len(update_list) == 0
    
    @pytest.mark.asyncio
    async def test_validate_update_data_non_id_where_field(self, operator):
        """Test validation with non-ID where field"""
        data = [{"email": "test@example.com", "name": "updated"}]
        
        update_list, cache_keys, errors = await operator._validate_update_data(data, "email")
        
        assert len(update_list) == 1
        assert len(cache_keys) == 0  # No cache keys for non-ID updates
    
    @pytest.mark.asyncio
    async def test_validate_update_data_exception(self, operator):
        """Test validation with schema exception"""
        operator.update_schemas = MagicMock(side_effect=ValueError("Schema error"))
        
        data = [{"id": 1, "value": "updated"}]
        
        update_list, cache_keys, errors = await operator._validate_update_data(data, "id")
        
        assert len(update_list) == 0
        assert len(errors) == 1
        assert "Schema error" in errors[0]


class TestClearUpdateCaches:
    """Test _clear_update_caches method"""
    
    @pytest.mark.asyncio
    async def test_clear_update_caches_success(self, operator):
        """Test successful cache clearing for updates"""
        cache_keys = ["1", "2", "3"]
        
        with patch.object(CacheOperate, 'delete_caches', return_value=3):
            with patch.object(operator, 'delete_null_key', return_value=True):
                errors = await operator._clear_update_caches(cache_keys, "test_op")
                
                assert len(errors) == 0
                CacheOperate.delete_caches.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_clear_update_caches_empty(self, operator):
        """Test clear with empty cache keys"""
        errors = await operator._clear_update_caches([], "test_op")
        assert len(errors) == 0
    
    @pytest.mark.asyncio
    async def test_clear_update_caches_with_errors(self, operator):
        """Test clear with errors"""
        cache_keys = ["1", "2"]
        
        with patch.object(CacheOperate, 'delete_caches', side_effect=Exception("Cache error")):
            errors = await operator._clear_update_caches(cache_keys, "test_op")
            
            assert len(errors) == 1
            assert "cache cleanup" in errors[0]
    
    @pytest.mark.asyncio
    async def test_clear_update_caches_null_key_error(self, operator):
        """Test clear with null key deletion error"""
        cache_keys = ["1"]
        
        with patch.object(CacheOperate, 'delete_caches', return_value=1):
            with patch.object(operator, 'delete_null_key', side_effect=Exception("Null error")):
                errors = await operator._clear_update_caches(cache_keys, "test_op")
                
                assert len(errors) == 1
                assert "null marker" in errors[0]


class TestConvertUpdateResults:
    """Test _convert_update_results method"""
    
    @pytest.mark.asyncio
    async def test_convert_update_results_success(self, operator):
        """Test successful result conversion"""
        records = [
            {"id": 1, "name": "test1"},
            {"id": 2, "name": "test2"}
        ]
        
        results, errors = await operator._convert_update_results(records)
        
        assert len(results) == 2
        assert len(errors) == 0
    
    @pytest.mark.asyncio
    async def test_convert_update_results_with_none(self, operator):
        """Test conversion with None records"""
        records = [
            {"id": 1, "name": "test1"},
            None,
            {"id": 2, "name": "test2"}
        ]
        
        results, errors = await operator._convert_update_results(records)
        
        assert len(results) == 2
        assert len(errors) == 0
    
    @pytest.mark.asyncio
    async def test_convert_update_results_schema_error(self, operator):
        """Test conversion with schema errors"""
        records = [{"id": 1, "name": "test1"}]
        
        operator.main_schemas = MagicMock(side_effect=ValueError("Schema error"))
        
        results, errors = await operator._convert_update_results(records)
        
        assert len(results) == 0
        assert len(errors) == 1
        assert "Schema error" in errors[0]


class TestUpdateData:
    """Test update_data method"""
    
    @pytest.mark.asyncio
    async def test_update_data_success(self, operator):
        """Test successful update"""
        update_data = [
            {"id": 1, "value": "updated1"},
            {"id": 2, "value": "updated2"}
        ]
        
        updated_records = [
            {"id": 1, "value": "updated1"},
            {"id": 2, "value": "updated2"}
        ]
        
        with patch.object(operator, '_validate_update_data',
                         return_value=([{"id": 1, "data": {"value": "updated1"}},
                                       {"id": 2, "data": {"value": "updated2"}}],
                                      ["1", "2"], [])):
            with patch.object(operator, '_clear_update_caches', return_value=[]):
                with patch.object(operator, 'update_sql', return_value=updated_records):
                    with patch.object(operator, '_convert_update_results',
                                     return_value=([MagicMock(), MagicMock()], [])):
                        results = await operator.update_data(update_data)
                        
                        assert len(results) == 2
    
    @pytest.mark.asyncio
    async def test_update_data_empty(self, operator):
        """Test update with empty data"""
        results = await operator.update_data([])
        assert len(results) == 0
    
    @pytest.mark.asyncio
    async def test_update_data_validation_error(self, operator):
        """Test update with validation errors"""
        with patch.object(operator, '_validate_update_data',
                         return_value=([], [], ["Validation error"])):
            with pytest.raises(GeneralOperateException) as exc_info:
                await operator.update_data([{"value": "test"}])
            
            assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
    
    @pytest.mark.asyncio
    async def test_update_data_no_valid_data(self, operator):
        """Test update with no valid data after validation"""
        with patch.object(operator, '_validate_update_data',
                         return_value=([], [], [])):
            results = await operator.update_data([{"id": 1}])
            assert len(results) == 0
    
    @pytest.mark.asyncio
    async def test_update_data_incomplete(self, operator):
        """Test update when not all records are updated"""
        with patch.object(operator, '_validate_update_data',
                         return_value=([{"id": 1}, {"id": 2}], [], [])):
            with patch.object(operator, '_clear_update_caches', return_value=[]):
                with patch.object(operator, 'update_sql', return_value=[{"id": 1}]):
                    with patch.object(operator, '_convert_update_results',
                                     return_value=([MagicMock()], [])):
                        with pytest.raises(GeneralOperateException) as exc_info:
                            await operator.update_data([{"id": 1}, {"id": 2}])
                        
                        assert "Update incomplete" in str(exc_info.value.message)


class TestUpdateByForeignKey:
    """Test update_by_foreign_key method"""
    
    @pytest.mark.asyncio
    async def test_update_by_foreign_key_full_crud(self, operator):
        """Test update by foreign key with create, update, and delete"""
        existing_items = [
            {"id": 1, "parent_id": 123, "name": "existing1"},
            {"id": 2, "parent_id": 123, "name": "existing2"},
            {"id": 3, "parent_id": 123, "name": "to_delete"}
        ]
        
        new_items = [
            {"id": 0, "name": "new_item"},  # Create
            {"id": 1, "name": "updated1"},  # Update
            {"id": -3, "name": "delete"}    # Delete
        ]
        
        with patch.object(operator, 'read_data_by_filter', return_value=[
            MagicMock(model_dump=lambda: item) for item in existing_items
        ]):
            with patch('general_operate.utils.build_data.compare_related_items',
                      return_value=([{"name": "new_item"}],
                                   [{"id": 1, "name": "updated1"}],
                                   [3])):
                with patch.object(operator, 'delete_data', return_value=[3]):
                    with patch.object(operator, 'update_data', return_value=[]):
                        with patch.object(operator, 'create_data', return_value=[]):
                            await operator.update_by_foreign_key("parent_id", 123, new_items)
                            
                            operator.delete_data.assert_called_once()
                            operator.update_data.assert_called_once()
                            operator.create_data.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_by_foreign_key_empty_data(self, operator):
        """Test update by foreign key with empty data"""
        await operator.update_by_foreign_key("parent_id", 123, [])
        # Should return without doing anything
    
    @pytest.mark.asyncio
    async def test_update_by_foreign_key_only_create(self, operator):
        """Test update by foreign key with only create operations"""
        with patch.object(operator, 'read_data_by_filter', return_value=[]):
            with patch('general_operate.utils.build_data.compare_related_items',
                      return_value=([{"name": "new"}], [], [])):
                with patch.object(operator, 'create_data', return_value=[]):
                    await operator.update_by_foreign_key("parent_id", 123, [{"id": 0, "name": "new"}])
                    
                    operator.create_data.assert_called_once()


class TestDeleteData:
    """Test delete_data method"""
    
    @pytest.mark.asyncio
    async def test_delete_data_success(self, operator):
        """Test successful deletion"""
        with patch.object(operator, 'delete_sql', return_value=[1, 2]):
            with patch.object(CacheOperate, 'delete_caches', return_value=2):
                with patch.object(operator, 'delete_null_key', return_value=True):
                    results = await operator.delete_data({1, 2})
                    
                    assert results == [1, 2]
    
    @pytest.mark.asyncio
    async def test_delete_data_empty_set(self, operator):
        """Test delete with empty ID set"""
        results = await operator.delete_data(set())
        assert results == []
    
    @pytest.mark.asyncio
    async def test_delete_data_invalid_ids(self, operator):
        """Test delete with invalid IDs"""
        with patch.object(operator, 'delete_sql', return_value=[]):
            results = await operator.delete_data({"invalid", None, "123"})
            
            # Invalid IDs should be skipped
            operator.delete_sql.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_data_cache_error(self, operator):
        """Test delete with cache error (non-fatal)"""
        with patch.object(operator, 'delete_sql', return_value=[1]):
            with patch.object(CacheOperate, 'delete_caches', 
                            side_effect=redis.RedisError("Cache error")):
                results = await operator.delete_data({1})
                
                # Should still return successful SQL deletes
                assert results == [1]
    
    @pytest.mark.asyncio
    async def test_delete_data_null_key_error(self, operator):
        """Test delete with null key error (non-fatal)"""
        with patch.object(operator, 'delete_sql', return_value=[1]):
            with patch.object(CacheOperate, 'delete_caches', return_value=1):
                with patch.object(operator, 'delete_null_key',
                                side_effect=redis.RedisError("Null key error")):
                    results = await operator.delete_data({1})
                    
                    # Should still return successful SQL deletes
                    assert results == [1]


class TestDeleteFilterData:
    """Test delete_filter_data method"""
    
    @pytest.mark.asyncio
    async def test_delete_filter_data_success(self, operator):
        """Test successful filter-based deletion"""
        with patch.object(operator, 'delete_filter', return_value=[1, 2, 3]):
            with patch.object(operator, 'delete_caches', return_value=3):
                with patch.object(operator, 'delete_null_key', return_value=True):
                    results = await operator.delete_filter_data({"status": "deleted"})
                    
                    assert results == [1, 2, 3]
    
    @pytest.mark.asyncio
    async def test_delete_filter_data_empty_filters(self, operator):
        """Test delete with empty filters"""
        results = await operator.delete_filter_data({})
        assert results == []
    
    @pytest.mark.asyncio
    async def test_delete_filter_data_no_matches(self, operator):
        """Test delete when no records match filter"""
        with patch.object(operator, 'delete_filter', return_value=[]):
            results = await operator.delete_filter_data({"status": "nonexistent"})
            assert results == []
    
    @pytest.mark.asyncio
    async def test_delete_filter_data_db_error(self, operator):
        """Test delete with database error"""
        from sqlalchemy.exc import DBAPIError
        from general_operate.core.exceptions import GeneralOperateException
        
        # Create a proper DBAPIError
        error = DBAPIError("statement", {}, Exception("Database error"), False)
        
        with patch.object(operator, 'delete_filter', side_effect=error):
            with pytest.raises(GeneralOperateException):
                await operator.delete_filter_data({"status": "active"})
    
    @pytest.mark.asyncio
    async def test_delete_filter_data_validation_error(self, operator):
        """Test delete with validation error"""
        with patch.object(operator, 'delete_filter', side_effect=ValueError("Invalid filter")):
            results = await operator.delete_filter_data({"invalid": "filter"})
            assert results == []
    
    @pytest.mark.asyncio
    async def test_delete_filter_data_generic_error(self, operator):
        """Test delete with generic error"""
        with patch.object(operator, 'delete_filter', side_effect=RuntimeError("Unexpected")):
            results = await operator.delete_filter_data({"status": "active"})
            assert results == []


class TestCountData:
    """Test count_data method"""
    
    @pytest.mark.asyncio
    async def test_count_data_success(self, operator):
        """Test successful count"""
        with patch.object(operator, 'count_sql', return_value=42):
            count = await operator.count_data({"status": "active"})
            assert count == 42
    
    @pytest.mark.asyncio
    async def test_count_data_no_filters(self, operator):
        """Test count without filters"""
        with patch.object(operator, 'count_sql', return_value=100):
            count = await operator.count_data()
            assert count == 100
    
    @pytest.mark.asyncio
    async def test_count_data_with_session(self, operator):
        """Test count with session"""
        mock_session = AsyncMock()
        
        with patch.object(operator, 'count_sql', return_value=10) as mock_count:
            count = await operator.count_data(session=mock_session)
            
            assert count == 10
            assert mock_count.call_args[1]["session"] == mock_session
    
    @pytest.mark.asyncio
    async def test_count_data_error(self, operator):
        """Test count with error"""
        with patch.object(operator, 'count_sql', side_effect=Exception("Count error")):
            with pytest.raises(GeneralOperateException) as exc_info:
                await operator.count_data()
            
            assert exc_info.value.code == ErrorCode.UNKNOWN_ERROR


class TestCacheDataMethods:
    """Test cache data methods"""
    
    @pytest.mark.asyncio
    async def test_store_cache_data(self, operator):
        """Test store_cache_data method"""
        data = {"key": "value", "test": "data"}
        
        await operator.store_cache_data("prefix", "123", data, ttl_seconds=3600)
        
        operator.redis.setex.assert_called_once()
        call_args = operator.redis.setex.call_args[0]
        assert call_args[0] == "prefix:123"
        assert call_args[1] == 3600
    
    @pytest.mark.asyncio
    async def test_get_cache_data(self, operator):
        """Test get_cache_data method"""
        with patch.object(operator, 'get_cache', return_value={"data": "test"}):
            result = await operator.get_cache_data("prefix", "123")
            assert result == {"data": "test"}
    
    @pytest.mark.asyncio
    async def test_delete_cache_data(self, operator):
        """Test delete_cache_data method"""
        with patch.object(operator, 'delete_cache', return_value=True):
            result = await operator.delete_cache_data("prefix", "123")
            assert result is True


class TestUpsertData:
    """Test upsert_data method"""
    
    @pytest.mark.asyncio
    async def test_upsert_data_success(self, operator):
        """Test successful upsert"""
        data = [
            {"id": 1, "name": "test1", "value": "new1"},
            {"id": 2, "name": "test2", "value": "new2"}
        ]
        
        upserted_records = data.copy()
        
        with patch.object(operator, 'delete_caches', return_value=None):
            with patch.object(operator, 'delete_null_key', return_value=True):
                with patch.object(operator, 'upsert_sql', return_value=upserted_records):
                    results = await operator.upsert_data(
                        data,
                        conflict_fields=["id"],
                        update_fields=["value"]
                    )
                    
                    assert len(results) == 2
    
    @pytest.mark.asyncio
    async def test_upsert_data_empty(self, operator):
        """Test upsert with empty data"""
        results = await operator.upsert_data([], ["id"])
        assert len(results) == 0
    
    @pytest.mark.asyncio
    async def test_upsert_data_invalid_items(self, operator):
        """Test upsert with invalid items"""
        data = [
            {"id": 1, "name": "valid"},
            "invalid",  # Not a dict
            {"id": 2, "name": "valid2"}
        ]
        
        with patch.object(operator, 'upsert_sql', return_value=[]):
            results = await operator.upsert_data(data, ["id"])
            
            # Invalid item should be skipped
            operator.upsert_sql.assert_called_once()
            validated_data = operator.upsert_sql.call_args[0][1]
            assert len(validated_data) == 2
    
    @pytest.mark.asyncio
    async def test_upsert_data_schema_validation(self, operator):
        """Test upsert with schema validation fallback"""
        data = [{"id": 1, "name": "test"}]
        
        # Mock create schema to fail, update schema to succeed
        operator.create_schemas = MagicMock(side_effect=ValueError("Create failed"))
        operator.update_schemas = MagicMock(return_value=MagicMock(
            model_dump=lambda exclude_unset: {"id": 1, "name": "test"}
        ))
        
        with patch.object(operator, 'upsert_sql', return_value=data):
            results = await operator.upsert_data(data, ["id"])
            
            assert len(results) == 1


class TestExistsCheck:
    """Test exists_check method"""
    
    @pytest.mark.asyncio
    async def test_exists_check_cache_hit(self, operator):
        """Test exists check with cache hit"""
        operator.redis.exists = AsyncMock(return_value=False)
        
        with patch.object(operator, 'get_caches', return_value=[{"id": 1}]):
            result = await operator.exists_check(1)
            assert result is True
    
    @pytest.mark.asyncio
    async def test_exists_check_null_marker(self, operator):
        """Test exists check with null marker"""
        operator.redis.exists = AsyncMock(return_value=True)
        
        result = await operator.exists_check(999)
        assert result is False
    
    @pytest.mark.asyncio
    async def test_exists_check_database(self, operator):
        """Test exists check fallback to database"""
        operator.redis.exists = AsyncMock(return_value=False)
        
        with patch.object(operator, 'get_caches', return_value=None):
            with patch.object(operator, 'exists_sql', return_value={1: True}):
                result = await operator.exists_check(1)
                assert result is True
    
    @pytest.mark.asyncio
    async def test_exists_check_not_found(self, operator):
        """Test exists check when record doesn't exist"""
        operator.redis.exists = AsyncMock(return_value=False)
        
        with patch.object(operator, 'get_caches', return_value=None):
            with patch.object(operator, 'exists_sql', return_value={999: False}):
                with patch.object(operator, 'set_null_key', return_value=True):
                    result = await operator.exists_check(999)
                    assert result is False
                    
                    # Should set null marker
                    operator.set_null_key.assert_called_once()


class TestBatchExists:
    """Test batch_exists method"""
    
    @pytest.mark.asyncio
    async def test_batch_exists_all_cache(self, operator):
        """Test batch exists with all cache hits"""
        operator.redis = AsyncMock()
        
        mock_pipeline = AsyncMock()
        mock_pipeline.exists = MagicMock()
        mock_pipeline.execute = AsyncMock(return_value=[False, False, False])
        operator.redis.pipeline = MagicMock(return_value=mock_pipeline)
        
        cache_data = {"1": {"id": 1}, "2": {"id": 2}, "3": {"id": 3}}
        
        with patch.object(operator, 'get_caches', return_value=cache_data):
            result = await operator.batch_exists({1, 2, 3})
            
            assert all(result.values())
    
    @pytest.mark.asyncio
    async def test_batch_exists_mixed(self, operator):
        """Test batch exists with mixed cache and database"""
        operator.redis = AsyncMock()
        
        # Mock the pipeline context manager
        mock_pipeline = AsyncMock()
        mock_pipeline.exists = MagicMock()
        mock_pipeline.execute = AsyncMock(return_value=[False, True, False])  # ID 2 has null marker
        mock_pipeline.__aenter__ = AsyncMock(return_value=mock_pipeline)
        mock_pipeline.__aexit__ = AsyncMock(return_value=None)
        operator.redis.pipeline = MagicMock(return_value=mock_pipeline)
        
        with patch.object(operator, 'get_caches', return_value={"1": {"id": 1}}):
            with patch.object(operator, 'exists_sql', return_value={3: True}):
                result = await operator.batch_exists({1, 2, 3})
                
                assert result[1] is True  # Cache hit
                assert result[2] is False  # Null marker
                assert result[3] is True  # Database hit
    
    @pytest.mark.asyncio
    async def test_batch_exists_no_redis(self, operator):
        """Test batch exists without Redis"""
        operator.redis = None
        
        async def mock_exists_sql(table_name, ids, session=None):
            return {1: True, 2: False, 3: True}
        
        # With no Redis, should go straight to database check
        with patch.object(operator, 'exists_sql', side_effect=mock_exists_sql):
            result = await operator.batch_exists({1, 2, 3})
            
            assert result[1] is True
            assert result[2] is False
            assert result[3] is True
    
    @pytest.mark.asyncio
    async def test_batch_exists_empty_set(self, operator):
        """Test batch exists with empty set"""
        result = await operator.batch_exists(set())
        assert result == {}
    
    @pytest.mark.asyncio
    async def test_batch_exists_cache_error(self, operator):
        """Test batch exists with cache error fallback"""
        operator.redis = AsyncMock()
        operator.redis.pipeline = MagicMock(side_effect=Exception("Pipeline error"))
        
        with patch.object(operator, 'exists_sql', return_value={1: True, 2: False}):
            result = await operator.batch_exists({1, 2})
            
            # Should fallback to database
            assert result[1] is True
            assert result[2] is False


class TestRefreshCache:
    """Test refresh_cache method"""
    
    @pytest.mark.asyncio
    async def test_refresh_cache_success(self, operator):
        """Test successful cache refresh"""
        operator.redis = AsyncMock()
        
        mock_pipeline = AsyncMock()
        mock_pipeline.delete = MagicMock()
        mock_pipeline.setex = MagicMock()
        mock_pipeline.execute = AsyncMock(return_value=[])
        operator.redis.pipeline = MagicMock(return_value=mock_pipeline)
        
        sql_data = [
            {"id": 1, "name": "test1"},
            {"id": 2, "name": "test2"}
        ]
        
        with patch.object(operator, 'delete_caches', return_value=None):
            with patch.object(operator, 'read_sql', return_value=sql_data):
                with patch.object(operator, 'store_caches', return_value=None):
                    result = await operator.refresh_cache({1, 2, 3})
                    
                    assert result["refreshed"] == 2
                    assert result["not_found"] == 1
                    assert result["errors"] == 0
    
    @pytest.mark.asyncio
    async def test_refresh_cache_empty_set(self, operator):
        """Test refresh with empty ID set"""
        result = await operator.refresh_cache(set())
        
        assert result["refreshed"] == 0
        assert result["not_found"] == 0
        assert result["errors"] == 0
    
    @pytest.mark.asyncio
    async def test_refresh_cache_all_not_found(self, operator):
        """Test refresh when all IDs not found"""
        operator.redis = AsyncMock()
        
        mock_pipeline = AsyncMock()
        mock_pipeline.delete = MagicMock()
        mock_pipeline.setex = MagicMock()
        mock_pipeline.execute = AsyncMock(return_value=[])
        operator.redis.pipeline = MagicMock(return_value=mock_pipeline)
        
        with patch.object(operator, 'delete_caches', return_value=None):
            with patch.object(operator, 'read_sql', return_value=[]):
                result = await operator.refresh_cache({1, 2, 3})
                
                assert result["refreshed"] == 0
                assert result["not_found"] == 3
    
    @pytest.mark.asyncio
    async def test_refresh_cache_error(self, operator):
        """Test refresh with error"""
        with patch.object(operator, 'delete_caches', side_effect=Exception("Error")):
            result = await operator.refresh_cache({1})
            
            assert result["errors"] > 0


class TestGetDistinctValues:
    """Test get_distinct_values method"""
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_cache_hit(self, operator):
        """Test get distinct values with cache hit"""
        operator.redis.get = AsyncMock(return_value='["value1", "value2", "value3"]')
        
        result = await operator.get_distinct_values("category", cache_ttl=300)
        
        assert result == ["value1", "value2", "value3"]
        # Should not call database
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_cache_miss(self, operator):
        """Test get distinct values with cache miss"""
        operator.redis.get = AsyncMock(return_value=None)
        operator.redis.setex = AsyncMock()
        
        with patch.object(SQLOperate, 'get_distinct_values', return_value=["new1", "new2"]):
            result = await operator.get_distinct_values("category", cache_ttl=300)
            
            assert result == ["new1", "new2"]
            # Should cache the result
            operator.redis.setex.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_no_cache(self, operator):
        """Test get distinct values without caching"""
        with patch.object(SQLOperate, 'get_distinct_values', return_value=["value1"]):
            result = await operator.get_distinct_values("category", cache_ttl=0)
            
            assert result == ["value1"]
            # Should not attempt to read or write cache
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_with_filters(self, operator):
        """Test get distinct values with filters"""
        operator.redis.get = AsyncMock(return_value=None)
        operator.redis.setex = AsyncMock()
        
        filters = {"status": "active"}
        
        with patch.object(SQLOperate, 'get_distinct_values', return_value=["filtered"]) as mock_distinct:
            result = await operator.get_distinct_values("category", filters=filters, cache_ttl=300)
            
            assert result == ["filtered"]
            mock_distinct.assert_called_once()
            assert mock_distinct.call_args[0][3] == filters


class TestEdgeCasesAndErrorHandling:
    """Test edge cases and error conditions"""
    
    @pytest.mark.asyncio
    async def test_abstract_method_not_implemented(self):
        """Test that get_module must be implemented"""
        class InvalidOperator(GeneralOperate):
            pass
        
        with pytest.raises(TypeError):
            InvalidOperator()
    
    @pytest.mark.asyncio
    async def test_concurrent_operations(self, operator):
        """Test concurrent operations don't interfere"""
        async def read_task(id_val):
            return await operator.exists_check(id_val)
        
        operator.redis.exists = AsyncMock(return_value=False)
        
        async def mock_exists_sql(table_name, ids, session=None):
            return {i: i % 2 == 0 for i in ids}
        
        with patch.object(operator, 'get_caches', return_value=None):
            with patch.object(operator, 'exists_sql', side_effect=mock_exists_sql):
                # Run multiple concurrent checks
                tasks = [read_task(i) for i in range(10)]
                results = await asyncio.gather(*tasks)
                
                # Verify correct results
                for i, result in enumerate(results):
                    assert result == (i % 2 == 0)