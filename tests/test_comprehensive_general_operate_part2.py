"""Comprehensive unit tests for additional GeneralOperate methods to achieve 100% coverage."""

import pytest
import asyncio
import json
import hashlib
from unittest.mock import AsyncMock, MagicMock, patch, call, Mock
from datetime import datetime, UTC
import redis
import structlog

from general_operate.general_operate import GeneralOperate
from general_operate import GeneralOperateException, ErrorCode, ErrorContext
from general_operate.app.sql_operate import SQLOperate


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
def general_operate(mock_database_client, mock_redis_client, mock_module):
    """Create a GeneralOperate instance for testing."""
    return ConcreteGeneralOperate(
        database_client=mock_database_client,
        redis_client=mock_redis_client,
        module=mock_module
    )


class TestGeneralOperateUpsertData:
    """Test upsert data functionality."""
    
    @pytest.mark.asyncio
    async def test_upsert_data_empty(self, general_operate):
        """Test upsert data with empty list."""
        result = await general_operate.upsert_data([], ["id"])
        assert result == []
    
    @pytest.mark.asyncio
    async def test_upsert_data_success(self, general_operate):
        """Test successful upsert data."""
        data = [
            {"id": 1, "name": "updated"},
            {"name": "new_item"}
        ]
        mock_upserted = [
            {"id": 1, "name": "updated"},
            {"id": 2, "name": "new_item"}
        ]
        
        with patch.object(general_operate, 'delete_caches', new=AsyncMock()) as mock_delete_cache, \
             patch.object(general_operate, 'delete_null_key', new=AsyncMock()) as mock_delete_null, \
             patch.object(general_operate, 'upsert_sql', return_value=mock_upserted):
            
            result = await general_operate.upsert_data(data, ["id"])
            
            assert len(result) == 2
            assert result[0].id == 1
            assert result[1].id == 2
    
    @pytest.mark.asyncio
    async def test_upsert_data_invalid_item_type(self, general_operate):
        """Test upsert data with invalid item types."""
        data = ["invalid", {"name": "valid"}]
        
        with patch.object(general_operate, 'upsert_sql', return_value=[{"id": 1, "name": "valid"}]):
            result = await general_operate.upsert_data(data, ["id"])
            
            assert len(result) == 1  # Only valid item processed
    
    @pytest.mark.asyncio
    async def test_upsert_data_schema_validation_create(self, general_operate):
        """Test upsert data with create schema validation."""
        data = [{"name": "test"}]
        
        def mock_create_schema(**kwargs):
            return MockSchema(**kwargs)
        
        def mock_update_schema(**kwargs):
            raise ValueError("Create schema should be tried first")
        
        general_operate.create_schemas = mock_create_schema
        general_operate.update_schemas = mock_update_schema
        
        with patch.object(general_operate, 'upsert_sql', return_value=[{"id": 1, "name": "test"}]):
            result = await general_operate.upsert_data(data, ["id"])
            
            assert len(result) == 1
    
    @pytest.mark.asyncio
    async def test_upsert_data_schema_validation_fallback_update(self, general_operate):
        """Test upsert data fallback to update schema validation."""
        data = [{"id": 1, "name": "test"}]
        
        def mock_create_schema(**kwargs):
            raise ValueError("Create schema failed")
        
        def mock_update_schema(**kwargs):
            obj = MockSchema(**kwargs)
            obj.model_dump = MagicMock(return_value=kwargs)
            return obj
        
        general_operate.create_schemas = mock_create_schema
        general_operate.update_schemas = mock_update_schema
        
        with patch.object(general_operate, 'upsert_sql', return_value=[{"id": 1, "name": "test"}]):
            result = await general_operate.upsert_data(data, ["id"])
            
            assert len(result) == 1
    
    @pytest.mark.asyncio
    async def test_upsert_data_all_validation_fails(self, general_operate):
        """Test upsert data when all validation fails."""
        data = [{"invalid": "data"}]
        
        def mock_schema(**kwargs):
            raise ValueError("Schema validation failed")
        
        general_operate.create_schemas = mock_schema
        general_operate.update_schemas = mock_schema
        
        result = await general_operate.upsert_data(data, ["id"])
        assert result == []
    
    @pytest.mark.asyncio
    async def test_upsert_data_cache_operations(self, general_operate):
        """Test upsert data cache operations."""
        data = [{"id": 1, "name": "test"}]
        
        with patch.object(general_operate, 'delete_caches', new=AsyncMock()) as mock_delete_cache, \
             patch.object(general_operate, 'delete_null_key', new=AsyncMock()) as mock_delete_null, \
             patch.object(general_operate, 'upsert_sql', return_value=[{"id": 1, "name": "test"}]):
            
            result = await general_operate.upsert_data(data, ["id"])
            
            # Should delete cache twice (before and after)
            assert mock_delete_cache.call_count == 2
            # Should delete null marker during cleanup (best effort)
            mock_delete_null.assert_called()
    
    @pytest.mark.asyncio
    async def test_upsert_data_cache_error(self, general_operate):
        """Test upsert data with cache errors."""
        data = [{"id": 1, "name": "test"}]
        
        with patch.object(general_operate, 'delete_caches', side_effect=Exception("Cache error")), \
             patch.object(general_operate, 'upsert_sql', return_value=[{"id": 1, "name": "test"}]):
            
            # Should not raise exception, just log warning
            result = await general_operate.upsert_data(data, ["id"])
            assert len(result) == 1
    
    @pytest.mark.asyncio
    async def test_upsert_data_sql_error(self, general_operate):
        """Test upsert data with SQL error."""
        data = [{"name": "test"}]
        
        with patch.object(general_operate, 'upsert_sql', side_effect=Exception("SQL error")):
            
            with pytest.raises(Exception):
                await general_operate.upsert_data(data, ["id"])
    
    @pytest.mark.asyncio
    async def test_upsert_data_schema_conversion_error(self, general_operate):
        """Test upsert data with schema conversion errors."""
        data = [{"name": "test"}]
        mock_upserted = [{"id": 1, "invalid_field": "test"}]
        
        def mock_schema(**kwargs):
            if "invalid_field" in kwargs:
                raise ValueError("Schema conversion error")
            return MockSchema(**kwargs)
        
        general_operate.main_schemas = mock_schema
        
        with patch.object(general_operate, 'upsert_sql', return_value=mock_upserted):
            result = await general_operate.upsert_data(data, ["id"])
            
            assert result == []  # No results due to conversion error
    
    @pytest.mark.asyncio
    async def test_upsert_data_with_session(self, general_operate):
        """Test upsert data with session."""
        data = [{"name": "test"}]
        session = AsyncMock()
        
        with patch.object(general_operate, 'upsert_sql', return_value=[{"id": 1, "name": "test"}]) as mock_upsert:
            
            result = await general_operate.upsert_data(data, ["id"], session=session)
            
            mock_upsert.assert_called_with(
                "test_table",
                [{"name": "test"}],
                ["id"],
                None,
                session
            )
    
    @pytest.mark.asyncio
    async def test_upsert_data_with_update_fields(self, general_operate):
        """Test upsert data with specific update fields."""
        data = [{"name": "test", "status": "active", "priority": "high"}]
        
        with patch.object(general_operate, 'upsert_sql', return_value=[{"id": 1, "name": "test"}]) as mock_upsert:
            
            result = await general_operate.upsert_data(data, ["name"], ["status"])
            
            mock_upsert.assert_called_with(
                "test_table",
                [{"name": "test", "status": "active", "priority": "high"}],
                ["name"],
                ["status"],
                None
            )


class TestGeneralOperateExistsCheck:
    """Test exists check functionality."""
    
    @pytest.mark.asyncio
    async def test_exists_check_cache_null_marker(self, general_operate):
        """Test exists check with cache null marker."""
        # Mock CacheOperate.check_null_markers_batch to return that null marker exists
        with patch('general_operate.app.cache_operate.CacheOperate.check_null_markers_batch', 
                   new=AsyncMock(return_value=({123: True}, set()))):
            
            result = await general_operate.exists_check(123)
            
            assert result is False
    
    @pytest.mark.asyncio
    async def test_exists_check_cache_hit(self, general_operate):
        """Test exists check with cache hit."""
        with patch.object(general_operate.redis, 'exists', new=AsyncMock(return_value=0)), \
             patch.object(general_operate, 'get_caches', new=AsyncMock(return_value=[{"id": 123, "name": "test"}])):
            
            result = await general_operate.exists_check(123)
            
            assert result is True
    
    @pytest.mark.asyncio
    async def test_exists_check_cache_miss_exists_in_db(self, general_operate):
        """Test exists check with cache miss but exists in database."""
        with patch.object(general_operate.redis, 'exists', new=AsyncMock(return_value=0)), \
             patch.object(general_operate, 'get_caches', new=AsyncMock(return_value=[])), \
             patch.object(general_operate, 'exists_sql', new=AsyncMock(return_value={123: True})):
            
            result = await general_operate.exists_check(123)
            
            assert result is True
    
    @pytest.mark.asyncio
    async def test_exists_check_not_exists(self, general_operate):
        """Test exists check when record doesn't exist."""
        with patch('general_operate.app.cache_operate.CacheOperate.check_null_markers_batch', 
                   new=AsyncMock(return_value=({}, {123}))), \
             patch.object(general_operate, 'get_caches', new=AsyncMock(return_value=[])), \
             patch.object(general_operate, 'exists_sql', new=AsyncMock(return_value={123: False})), \
             patch('general_operate.app.cache_operate.CacheOperate.set_null_markers_batch', new=AsyncMock()) as mock_set_null:
            
            result = await general_operate.exists_check(123)
            
            assert result is False
            mock_set_null.assert_called_once_with(general_operate, "test_table", [123], expiry_seconds=300)
    
    @pytest.mark.asyncio
    async def test_exists_check_cache_error(self, general_operate):
        """Test exists check with cache errors."""
        with patch.object(general_operate.redis, 'exists', new=AsyncMock(side_effect=Exception("Cache error"))), \
             patch.object(general_operate, 'exists_sql', new=AsyncMock(return_value={123: True})):
            
            result = await general_operate.exists_check(123)
            
            assert result is True
    
    @pytest.mark.asyncio
    async def test_exists_check_set_null_marker_error(self, general_operate):
        """Test exists check with null marker setting error."""
        with patch.object(general_operate.redis, 'exists', new=AsyncMock(return_value=0)), \
             patch.object(general_operate, 'get_caches', new=AsyncMock(return_value=[])), \
             patch.object(general_operate, 'exists_sql', new=AsyncMock(return_value={123: False})), \
             patch.object(general_operate, 'set_null_key', new=AsyncMock(side_effect=Exception("Null key error"))):
            
            # Should not raise exception
            result = await general_operate.exists_check(123)
            assert result is False
    
    @pytest.mark.asyncio
    async def test_exists_check_database_error(self, general_operate):
        """Test exists check with database error."""
        with patch.object(general_operate.redis, 'exists', new=AsyncMock(return_value=0)), \
             patch.object(general_operate, 'get_caches', new=AsyncMock(return_value=[])), \
             patch.object(general_operate, 'exists_sql', new=AsyncMock(side_effect=Exception("Database error"))):
            
            with pytest.raises(Exception):
                await general_operate.exists_check(123)
    
    @pytest.mark.asyncio
    async def test_exists_check_with_session(self, general_operate):
        """Test exists check with session."""
        session = AsyncMock()
        
        with patch.object(general_operate.redis, 'exists', new=AsyncMock(return_value=0)), \
             patch.object(general_operate, 'get_caches', new=AsyncMock(return_value=[])), \
             patch.object(general_operate, 'exists_sql', new=AsyncMock(return_value={123: True})) as mock_exists:
            
            result = await general_operate.exists_check(123, session=session)
            
            mock_exists.assert_called_with("test_table", [123], session=session)


class TestGeneralOperateBatchExists:
    """Test batch exists functionality."""
    
    @pytest.mark.asyncio
    async def test_batch_exists_empty_ids(self, general_operate):
        """Test batch exists with empty IDs."""
        result = await general_operate.batch_exists(set())
        assert result == {}
    
    @pytest.mark.asyncio
    async def test_batch_exists_cache_pipeline_success(self, general_operate):
        """Test batch exists with successful cache pipeline."""
        mock_pipeline = AsyncMock()
        mock_pipeline.execute.return_value = [0, 1, 0]  # First and third not null-marked, second is null-marked
        general_operate.redis.pipeline.return_value.__aenter__ = AsyncMock(return_value=mock_pipeline)
        general_operate.redis.pipeline.return_value.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(general_operate, 'get_caches', new=AsyncMock(return_value={"1": {"id": 1}, "3": {"id": 3}})):
            
            result = await general_operate.batch_exists({1, 2, 3})
            
            assert result[1] is True   # Found in cache
            assert result[2] is False  # Null marked
            assert result[3] is True   # Found in cache
    
    @pytest.mark.asyncio
    async def test_batch_exists_cache_miss_database_check(self, general_operate):
        """Test batch exists with cache miss requiring database check."""
        # Mock check_null_markers_batch to return no null markers
        with patch('general_operate.app.cache_operate.CacheOperate.check_null_markers_batch', 
                   new=AsyncMock(return_value=({}, {1, 2}))), \
             patch.object(general_operate, 'get_caches', new=AsyncMock(return_value={})), \
             patch.object(general_operate, 'exists_sql', new=AsyncMock(return_value={1: True, 2: False})), \
             patch('general_operate.app.cache_operate.CacheOperate.set_null_markers_batch', new=AsyncMock()) as mock_set_null:
            
            result = await general_operate.batch_exists({1, 2})
            
            assert result[1] is True
            assert result[2] is False
            # set_null_markers_batch should be called only for non-existent IDs
            mock_set_null.assert_called_once_with(general_operate, "test_table", [2], expiry_seconds=300)
    
    @pytest.mark.asyncio
    async def test_batch_exists_cache_pipeline_error(self, general_operate):
        """Test batch exists with cache pipeline errors."""
        general_operate.redis.pipeline.side_effect = Exception("Pipeline error")
        
        with patch.object(general_operate, 'exists_sql', new=AsyncMock(return_value={1: True, 2: False})):
            
            result = await general_operate.batch_exists({1, 2})
            
            assert result[1] is True
            assert result[2] is False
    
    @pytest.mark.asyncio
    async def test_batch_exists_no_redis(self, general_operate):
        """Test batch exists without Redis."""
        general_operate.redis = None
        
        with patch.object(general_operate, 'exists_sql', new=AsyncMock(return_value={1: True, 2: False})):
            
            result = await general_operate.batch_exists({1, 2})
            
            assert result[1] is True
            assert result[2] is False
    
    @pytest.mark.asyncio
    async def test_batch_exists_database_error_fallback(self, general_operate):
        """Test batch exists with database error fallback."""
        mock_pipeline = AsyncMock()
        mock_pipeline.execute.return_value = [0]
        general_operate.redis.pipeline.return_value.__aenter__ = AsyncMock(return_value=mock_pipeline)
        general_operate.redis.pipeline.return_value.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(general_operate, 'get_caches', new=AsyncMock(return_value={})), \
             patch.object(general_operate, 'exists_sql', new=AsyncMock(side_effect=Exception("Database error"))):
            
            result = await general_operate.batch_exists({1})
            
            assert result[1] is False  # Defaults to False on database error
    
    @pytest.mark.asyncio
    async def test_batch_exists_set_null_marker_error(self, general_operate):
        """Test batch exists with null marker setting error."""
        mock_pipeline = AsyncMock()
        mock_pipeline.execute.return_value = [0]
        general_operate.redis.pipeline.return_value.__aenter__ = AsyncMock(return_value=mock_pipeline)
        general_operate.redis.pipeline.return_value.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(general_operate, 'get_caches', new=AsyncMock(return_value={})), \
             patch.object(general_operate, 'exists_sql', new=AsyncMock(return_value={1: False})), \
             patch.object(general_operate, 'set_null_key', new=AsyncMock(side_effect=redis.RedisError("Redis error"))):
            
            # Should not raise exception, just log debug
            result = await general_operate.batch_exists({1})
            assert result[1] is False
    
    @pytest.mark.asyncio
    async def test_batch_exists_with_session(self, general_operate):
        """Test batch exists with session."""
        session = AsyncMock()
        general_operate.redis = None  # Force database check
        
        with patch.object(general_operate, 'exists_sql', new=AsyncMock(return_value={1: True})) as mock_exists:
            
            result = await general_operate.batch_exists({1}, session=session)
            
            mock_exists.assert_called_with("test_table", [1], session=session)


class TestGeneralOperateRefreshCache:
    """Test refresh cache functionality."""
    
    @pytest.mark.asyncio
    async def test_refresh_cache_empty_ids(self, general_operate):
        """Test refresh cache with empty IDs."""
        result = await general_operate.refresh_cache(set())
        assert result == {"refreshed": 0, "not_found": 0, "errors": 0}
    
    @pytest.mark.asyncio
    async def test_refresh_cache_success(self, general_operate):
        """Test successful refresh cache."""
        mock_sql_data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        mock_pipeline = AsyncMock()
        general_operate.redis.pipeline.return_value.__aenter__ = AsyncMock(return_value=mock_pipeline)
        general_operate.redis.pipeline.return_value.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(general_operate, 'delete_caches', new=AsyncMock()) as mock_delete, \
             patch.object(general_operate, 'read_sql', new=AsyncMock(return_value=mock_sql_data)), \
             patch.object(general_operate, 'store_caches', new=AsyncMock()) as mock_store:
            
            result = await general_operate.refresh_cache({1, 2, 3})
            
            assert result["refreshed"] == 2
            assert result["not_found"] == 1  # ID 3 not found
            assert result["errors"] == 0
            mock_delete.assert_called_once()
            mock_store.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_refresh_cache_all_missing(self, general_operate):
        """Test refresh cache with all missing records."""
        mock_pipeline = AsyncMock()
        general_operate.redis.pipeline.return_value.__aenter__ = AsyncMock(return_value=mock_pipeline)
        general_operate.redis.pipeline.return_value.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(general_operate, 'delete_caches', new=AsyncMock()), \
             patch.object(general_operate, 'read_sql', new=AsyncMock(return_value=[])):
            
            result = await general_operate.refresh_cache({1, 2})
            
            assert result["refreshed"] == 0
            assert result["not_found"] == 2
            assert result["errors"] == 0
            # Should set null markers using pipeline
            assert mock_pipeline.setex.call_count == 2
    
    @pytest.mark.asyncio
    async def test_refresh_cache_no_redis(self, general_operate):
        """Test refresh cache without Redis."""
        general_operate.redis = None
        
        result = await general_operate.refresh_cache({1})
        
        # When no redis, should return errors for all IDs
        assert result["refreshed"] == 0
        assert result["not_found"] == 0
        assert result["errors"] == 1
    
    @pytest.mark.asyncio
    async def test_refresh_cache_pipeline_error(self, general_operate):
        """Test refresh cache with pipeline errors."""
        mock_pipeline = AsyncMock()
        mock_pipeline.execute.side_effect = redis.RedisError("Pipeline error")
        general_operate.redis.pipeline.return_value.__aenter__ = AsyncMock(return_value=mock_pipeline)
        general_operate.redis.pipeline.return_value.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(general_operate, 'delete_caches', new=AsyncMock()), \
             patch.object(general_operate, 'read_sql', new=AsyncMock(return_value=[])):
            
            # Should not raise exception, just log debug
            result = await general_operate.refresh_cache({1})
            assert result["not_found"] == 1
    
    @pytest.mark.asyncio
    async def test_refresh_cache_data_without_id(self, general_operate):
        """Test refresh cache with data that has no ID field."""
        mock_sql_data = [{"name": "test"}]  # No ID field
        
        with patch.object(general_operate, 'delete_caches', new=AsyncMock()), \
             patch.object(general_operate, 'read_sql', new=AsyncMock(return_value=mock_sql_data)):
            
            result = await general_operate.refresh_cache({1})
            
            assert result["refreshed"] == 0
            assert result["not_found"] == 1
    
    @pytest.mark.asyncio
    async def test_refresh_cache_general_error(self, general_operate):
        """Test refresh cache with general errors."""
        with patch.object(general_operate, 'delete_caches', side_effect=Exception("General error")):
            
            result = await general_operate.refresh_cache({1})
            
            assert result["errors"] == 1
            assert result["refreshed"] == 0
            assert result["not_found"] == 0


class TestGeneralOperateGetDistinctValues:
    """Test get distinct values functionality."""
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_cache_hit(self, general_operate):
        """Test get distinct values with cache hit."""
        cached_result = ["value1", "value2", "value3"]
        
        with patch.object(general_operate.redis, 'get', new=AsyncMock(return_value=json.dumps(cached_result))):
            
            result = await general_operate.get_distinct_values("category")
            
            assert result == cached_result
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_cache_miss(self, general_operate):
        """Test get distinct values with cache miss."""
        db_result = ["value1", "value2"]
        
        with patch.object(general_operate.redis, 'get', new=AsyncMock(return_value=None)), \
             patch('general_operate.app.sql_operate.SQLOperate.get_distinct_values', new=AsyncMock(return_value=db_result)) as mock_sql, \
             patch.object(general_operate.redis, 'setex', new=AsyncMock()) as mock_setex:
            
            result = await general_operate.get_distinct_values("category", {"status": "active"}, 600)
            
            assert result == db_result
            mock_sql.assert_called_once_with(general_operate, "test_table", "category", {"status": "active"}, session=None)
            mock_setex.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_no_caching(self, general_operate):
        """Test get distinct values with caching disabled."""
        db_result = ["value1", "value2"]
        
        with patch('general_operate.app.sql_operate.SQLOperate.get_distinct_values', new=AsyncMock(return_value=db_result)) as mock_sql:
            
            result = await general_operate.get_distinct_values("category", cache_ttl=0)
            
            assert result == db_result
            mock_sql.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_cache_read_error(self, general_operate):
        """Test get distinct values with cache read error."""
        db_result = ["value1", "value2"]
        
        with patch.object(general_operate.redis, 'get', new=AsyncMock(side_effect=Exception("Cache read error"))), \
             patch('general_operate.app.sql_operate.SQLOperate.get_distinct_values', new=AsyncMock(return_value=db_result)):
            
            result = await general_operate.get_distinct_values("category")
            
            assert result == db_result
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_cache_write_error(self, general_operate):
        """Test get distinct values with cache write error."""
        db_result = ["value1", "value2"]
        
        with patch.object(general_operate.redis, 'get', new=AsyncMock(return_value=None)), \
             patch('general_operate.app.sql_operate.SQLOperate.get_distinct_values', new=AsyncMock(return_value=db_result)), \
             patch.object(general_operate.redis, 'setex', new=AsyncMock(side_effect=Exception("Cache write error"))):
            
            # Should not raise exception, just log warning
            result = await general_operate.get_distinct_values("category")
            assert result == db_result
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_with_filters(self, general_operate):
        """Test get distinct values with filters."""
        filters = {"status": "active", "type": "premium"}
        expected_hash = hashlib.md5(json.dumps(filters, sort_keys=True).encode()).hexdigest()
        expected_key = f"test_table:distinct:category:{expected_hash}"
        
        with patch.object(general_operate.redis, 'get', new=AsyncMock(return_value=None)) as mock_get, \
             patch('general_operate.app.sql_operate.SQLOperate.get_distinct_values', new=AsyncMock(return_value=["value1"])):
            
            await general_operate.get_distinct_values("category", filters)
            
            mock_get.assert_called_with(expected_key)
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_no_filters(self, general_operate):
        """Test get distinct values without filters."""
        expected_hash = hashlib.md5("".encode()).hexdigest()
        expected_key = f"test_table:distinct:category:{expected_hash}"
        
        with patch.object(general_operate.redis, 'get', new=AsyncMock(return_value=None)) as mock_get, \
             patch('general_operate.app.sql_operate.SQLOperate.get_distinct_values', new=AsyncMock(return_value=["value1"])):
            
            await general_operate.get_distinct_values("category")
            
            mock_get.assert_called_with(expected_key)
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_database_error(self, general_operate):
        """Test get distinct values with database error."""
        with patch.object(general_operate.redis, 'get', new=AsyncMock(return_value=None)), \
             patch('general_operate.app.sql_operate.SQLOperate.get_distinct_values', new=AsyncMock(side_effect=Exception("Database error"))):
            
            with pytest.raises(Exception):
                await general_operate.get_distinct_values("category")
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_with_session(self, general_operate):
        """Test get distinct values with session."""
        session = AsyncMock()
        
        with patch.object(general_operate.redis, 'get', new=AsyncMock(return_value=None)), \
             patch('general_operate.app.sql_operate.SQLOperate.get_distinct_values', new=AsyncMock(return_value=["value1"])) as mock_sql:
            
            result = await general_operate.get_distinct_values("category", session=session)
            
            mock_sql.assert_called_with(general_operate, "test_table", "category", None, session=session)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--cov=general_operate.general_operate", "--cov-report=term-missing"])