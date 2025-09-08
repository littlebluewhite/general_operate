"""Additional tests to achieve 100% coverage for general_operate.py"""

import pytest
import pytest_asyncio
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, UTC

import redis
from sqlalchemy.exc import DBAPIError

from general_operate.general_operate import GeneralOperate
from general_operate import GeneralOperateException, ErrorCode
from general_operate.app.cache_operate import CacheOperate


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


@pytest_asyncio.fixture
async def operator():
    """Create test operator instance"""
    db_client = MagicMock()
    db_client.engine_type = "postgresql"
    
    redis_client = AsyncMock(spec=redis.asyncio.Redis)
    
    op = TestGeneralOperator(
        database_client=db_client,
        redis_client=redis_client
    )
    
    # Set up async mocks
    op.redis.exists = AsyncMock(return_value=False)
    op.redis.get = AsyncMock(return_value=None)
    op.redis.setex = AsyncMock(return_value=True)
    op.redis.delete = AsyncMock(return_value=1)
    op.redis.keys = AsyncMock(return_value=[])
    op.redis.pipeline = MagicMock()
    op.redis.ping = AsyncMock(return_value=True)
    
    return op


class TestTransactionManagerFixed:
    """Fixed tests for transaction context manager"""
    
    @pytest.mark.asyncio
    async def test_transaction_context(self, operator):
        """Test transaction context manager with proper mock"""
        mock_session = AsyncMock()
        
        # Create a proper async context manager for begin()
        mock_begin_ctx = AsyncMock()
        mock_begin_ctx.__aenter__ = AsyncMock(return_value=None)
        mock_begin_ctx.__aexit__ = AsyncMock(return_value=None)
        mock_session.begin = MagicMock(return_value=mock_begin_ctx)
        
        mock_session.close = AsyncMock()
        
        with patch.object(operator, 'create_external_session', return_value=mock_session):
            async with operator.transaction() as session:
                assert session == mock_session
            
            mock_session.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_transaction_with_exception(self, operator):
        """Test transaction closes session even with exception"""
        mock_session = AsyncMock()
        
        mock_begin_ctx = AsyncMock()
        mock_begin_ctx.__aenter__ = AsyncMock(return_value=None)
        mock_begin_ctx.__aexit__ = AsyncMock(return_value=None)
        mock_session.begin = MagicMock(return_value=mock_begin_ctx)
        
        mock_session.close = AsyncMock()
        
        with patch.object(operator, 'create_external_session', return_value=mock_session):
            try:
                async with operator.transaction() as session:
                    raise ValueError("Test error")
            except ValueError:
                pass
            
            mock_session.close.assert_called_once()


class TestReadDataByFilterMissingCoverage:
    """Test missing coverage in read_data_by_filter"""
    
    @pytest.mark.asyncio
    async def test_read_data_by_filter_raises_existing_exception(self, operator):
        """Test that existing GeneralOperateException is re-raised"""
        error = GeneralOperateException(
            code=ErrorCode.DB_QUERY_ERROR,
            message="Database error"
        )
        
        with patch.object(operator, 'read_sql', side_effect=error):
            with pytest.raises(GeneralOperateException) as exc_info:
                await operator.read_data_by_filter({"status": "active"})
            
            assert exc_info.value == error


class TestFetchFromSQLMissingCoverage:
    """Test missing coverage in _fetch_from_sql"""
    
    @pytest.mark.asyncio
    async def test_fetch_from_sql_preserves_exception(self, operator):
        """Test that existing GeneralOperateException is preserved"""
        error = GeneralOperateException(
            code=ErrorCode.DB_CONNECTION_ERROR,
            message="Connection lost"
        )
        
        with patch.object(operator, 'read_one', side_effect=error):
            with pytest.raises(GeneralOperateException) as exc_info:
                await operator._fetch_from_sql({1})
            
            assert exc_info.value == error


class TestCreateDataMissingCoverage:
    """Test missing coverage in create_data"""
    
    @pytest.mark.asyncio
    async def test_create_data_all_invalid(self, operator):
        """Test when all items fail validation"""
        # Mock schema to always raise errors
        operator.create_schemas = MagicMock(side_effect=ValueError("Invalid"))
        
        create_data = [
            {"name": "invalid1"},
            {"name": "invalid2"}
        ]
        
        result = await operator.create_data(create_data)
        assert result == []


class TestUpdateDataMissingCoverage:
    """Test missing coverage in update_data"""
    
    @pytest.mark.asyncio
    async def test_update_data_sql_error(self, operator):
        """Test update_data when SQL fails"""
        update_data = [{"id": 1, "value": "test"}]
        
        with patch.object(operator, '_validate_update_data',
                         return_value=([{"id": 1, "data": {"value": "test"}}], ["1"], [])):
            with patch.object(operator, '_clear_update_caches', return_value=[]):
                with patch.object(operator, 'update_sql', side_effect=Exception("SQL error")):
                    with pytest.raises(Exception) as exc_info:
                        await operator.update_data(update_data)
                    
                    assert "SQL error" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_update_data_with_cache_errors_and_schema_errors(self, operator):
        """Test update with both cache and schema conversion errors"""
        update_data = [{"id": 1, "value": "test"}]
        
        with patch.object(operator, '_validate_update_data',
                         return_value=([{"id": 1, "data": {"value": "test"}}], ["1"], [])):
            with patch.object(operator, '_clear_update_caches',
                            return_value=["cache error 1"]):
                with patch.object(operator, 'update_sql',
                                return_value=[{"id": 1, "value": "test"}]):
                    with patch.object(operator, '_convert_update_results',
                                    return_value=([], ["schema error"])):
                        with pytest.raises(GeneralOperateException) as exc_info:
                            await operator.update_data(update_data)
                        
                        assert "Schema errors" in exc_info.value.message


class TestUpdateByForeignKeyMissingCoverage:
    """Test missing coverage in update_by_foreign_key"""
    
    @pytest.mark.asyncio
    async def test_update_by_foreign_key_with_callbacks(self, operator):
        """Test update_by_foreign_key with missing item callbacks"""
        existing_items = [
            {"id": 1, "parent_id": 123, "name": "existing1"},
            {"id": 2, "parent_id": 123, "name": "existing2"}
        ]
        
        new_items = [
            {"id": 3, "name": "update_nonexistent"},  # Try to update non-existent
            {"id": -4, "name": "delete_nonexistent"}  # Try to delete non-existent
        ]
        
        # Track callback calls
        update_missing_calls = []
        delete_missing_calls = []
        
        def handle_missing_update(item_id):
            update_missing_calls.append(item_id)
        
        def handle_missing_delete(item_id):
            delete_missing_calls.append(item_id)
        
        with patch.object(operator, 'read_data_by_filter',
                         return_value=[MagicMock(model_dump=lambda: item) for item in existing_items]):
            with patch('general_operate.utils.build_data.compare_related_items') as mock_compare:
                # Set up the mock to call our callbacks
                def compare_side_effect(existing_items, new_items, foreign_key_field,
                                      foreign_key_value, handle_missing_update, handle_missing_delete):
                    # Call callbacks for missing items
                    handle_missing_update(3)
                    handle_missing_delete(4)
                    return ([], [], [])
                
                mock_compare.side_effect = compare_side_effect
                
                await operator.update_by_foreign_key("parent_id", 123, new_items)
                
                # Callbacks should have been provided
                mock_compare.assert_called_once()


class TestDeleteDataMissingCoverage:
    """Test missing coverage in delete_data"""
    
    @pytest.mark.asyncio
    async def test_delete_data_conversion_errors(self, operator):
        """Test delete with ID conversion errors"""
        # Mix of valid and invalid IDs
        id_set = {1, "valid", ValueError("object"), None, ""}
        
        with patch.object(operator, 'delete_sql', return_value=[1]) as mock_delete:
            results = await operator.delete_data(id_set)
            
            # Should only process valid IDs
            mock_delete.assert_called_once()
            processed_ids = mock_delete.call_args[0][1]
            assert 1 in processed_ids
            assert "valid" in processed_ids
            # Invalid IDs should be skipped
    
    @pytest.mark.asyncio
    async def test_delete_data_cache_cleanup_with_attribute_error(self, operator):
        """Test delete with AttributeError during cache cleanup"""
        with patch.object(operator, 'delete_sql', return_value=[1, 2]):
            with patch.object(CacheOperate, 'delete_caches',
                            side_effect=AttributeError("Missing attribute")):
                results = await operator.delete_data({1, 2})
                
                # Should still return successful SQL deletes
                assert results == [1, 2]
    
    @pytest.mark.asyncio
    async def test_delete_data_null_key_attribute_error(self, operator):
        """Test delete with AttributeError during null key deletion"""
        with patch.object(operator, 'delete_sql', return_value=[1]):
            with patch.object(CacheOperate, 'delete_caches', return_value=1):
                with patch.object(operator, 'delete_null_key',
                                side_effect=AttributeError("Missing method")):
                    results = await operator.delete_data({1})
                    
                    # Should still return successful SQL deletes
                    assert results == [1]


class TestDeleteFilterDataMissingCoverage:
    """Test missing coverage in delete_filter_data"""
    
    @pytest.mark.asyncio
    async def test_delete_filter_data_known_exception(self, operator):
        """Test delete_filter_data with known exception type"""
        error = GeneralOperateException(
            code=ErrorCode.DB_QUERY_ERROR,
            message="Query failed"
        )
        
        with patch.object(operator, 'delete_filter', side_effect=error):
            with pytest.raises(GeneralOperateException) as exc_info:
                await operator.delete_filter_data({"status": "active"})
            
            assert exc_info.value == error
    
    @pytest.mark.asyncio
    async def test_delete_filter_data_type_error(self, operator):
        """Test delete_filter_data with TypeError"""
        with patch.object(operator, 'delete_filter', side_effect=TypeError("Type error")):
            results = await operator.delete_filter_data({"invalid": "filter"})
            assert results == []
    
    @pytest.mark.asyncio
    async def test_delete_filter_data_key_error(self, operator):
        """Test delete_filter_data with KeyError"""
        with patch.object(operator, 'delete_filter', side_effect=KeyError("Missing key")):
            results = await operator.delete_filter_data({"missing": "key"})
            assert results == []
    
    @pytest.mark.asyncio
    async def test_delete_filter_data_cache_cleanup_attribute_error(self, operator):
        """Test cache cleanup with AttributeError during delete_filter_data"""
        with patch.object(operator, 'delete_filter', return_value=[1, 2]):
            with patch.object(operator, 'delete_caches',
                            side_effect=AttributeError("Missing method")):
                results = await operator.delete_filter_data({"status": "deleted"})
                
                # Should still return deleted IDs
                assert results == [1, 2]
    
    @pytest.mark.asyncio
    async def test_delete_filter_data_null_key_attribute_error(self, operator):
        """Test null key deletion with AttributeError"""
        with patch.object(operator, 'delete_filter', return_value=[1]):
            with patch.object(operator, 'delete_caches', return_value=None):
                with patch.object(operator, 'delete_null_key',
                                side_effect=AttributeError("Missing")):
                    results = await operator.delete_filter_data({"status": "deleted"})
                    
                    assert results == [1]


class TestCountDataMissingCoverage:
    """Test missing coverage in count_data"""
    
    @pytest.mark.asyncio
    async def test_count_data_preserves_exception(self, operator):
        """Test count_data preserves GeneralOperateException"""
        error = GeneralOperateException(
            code=ErrorCode.DB_QUERY_ERROR,
            message="Count failed"
        )
        
        with patch.object(operator, 'count_sql', side_effect=error):
            with pytest.raises(GeneralOperateException) as exc_info:
                await operator.count_data({"status": "active"})
            
            assert exc_info.value == error


class TestUpsertDataMissingCoverage:
    """Test missing coverage in upsert_data"""
    
    @pytest.mark.asyncio
    async def test_upsert_data_validation_fallback_error(self, operator):
        """Test upsert when both create and update schemas fail"""
        data = [{"id": 1, "name": "test"}]
        
        # Both schemas fail
        operator.create_schemas = MagicMock(side_effect=ValueError("Create failed"))
        operator.update_schemas = MagicMock(side_effect=ValueError("Update failed"))
        
        results = await operator.upsert_data(data, ["id"])
        
        # Should return empty when validation fails
        assert results == []
    
    @pytest.mark.asyncio
    async def test_upsert_data_sql_error(self, operator):
        """Test upsert when SQL operation fails"""
        data = [{"id": 1, "name": "test"}]
        
        with patch.object(operator, 'upsert_sql', side_effect=Exception("SQL error")):
            with pytest.raises(Exception) as exc_info:
                await operator.upsert_data(data, ["id"])
            
            assert "SQL error" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_upsert_data_schema_conversion_error(self, operator):
        """Test upsert when schema conversion fails after SQL"""
        data = [{"id": 1, "name": "test"}]
        
        with patch.object(operator, 'upsert_sql', return_value=[{"id": 1, "name": "test"}]):
            # Make main_schemas fail
            operator.main_schemas = MagicMock(side_effect=ValueError("Conversion error"))
            
            results = await operator.upsert_data(data, ["id"])
            
            # Should return empty results when conversion fails
            assert len(results) == 0
    
    @pytest.mark.asyncio
    async def test_upsert_data_cache_deletion_exception(self, operator):
        """Test upsert with cache deletion exception (non-fatal)"""
        data = [{"id": 1, "name": "test"}]
        
        with patch.object(operator, 'delete_caches', side_effect=Exception("Cache error")):
            with patch.object(operator, 'upsert_sql', return_value=[{"id": 1}]):
                results = await operator.upsert_data(data, ["id"])
                
                # Should still complete despite cache error
                assert len(results) == 1


class TestExistsCheckMissingCoverage:
    """Test missing coverage in exists_check"""
    
    @pytest.mark.asyncio
    async def test_exists_check_cache_error(self, operator):
        """Test exists_check when cache operations fail"""
        operator.redis.exists = AsyncMock(side_effect=Exception("Redis error"))
        
        # Should fall back to database
        with patch.object(operator, 'exists_sql', return_value={1: True}):
            result = await operator.exists_check(1)
            assert result is True
    
    @pytest.mark.asyncio
    async def test_exists_check_set_null_key_exception(self, operator):
        """Test exists_check when setting null key fails"""
        operator.redis.exists = AsyncMock(return_value=False)
        
        with patch.object(operator, 'get_caches', return_value=None):
            with patch.object(operator, 'exists_sql', return_value={999: False}):
                with patch.object(operator, 'set_null_key',
                                side_effect=Exception("Redis error")):
                    result = await operator.exists_check(999)
                    
                    # Should still return correct result
                    assert result is False


class TestBatchExistsMissingCoverage:
    """Test missing coverage in batch_exists"""
    
    @pytest.mark.asyncio
    async def test_batch_exists_no_redis_attribute(self, operator):
        """Test batch_exists when redis is None"""
        operator.redis = None
        
        with patch.object(operator, 'exists_sql', return_value={1: True, 2: False}):
            result = await operator.batch_exists({1, 2})
            
            assert result[1] is True
            assert result[2] is False
    
    @pytest.mark.asyncio
    async def test_batch_exists_null_key_connection_error(self, operator):
        """Test batch_exists with ConnectionError when setting null keys"""
        operator.redis = AsyncMock()
        
        mock_pipeline = AsyncMock()
        mock_pipeline.exists = MagicMock()
        mock_pipeline.execute = AsyncMock(return_value=[False, False])
        operator.redis.pipeline = MagicMock(return_value=mock_pipeline)
        
        with patch.object(operator, 'get_caches', return_value={}):
            with patch.object(operator, 'exists_sql', return_value={1: False, 2: False}):
                with patch.object(operator, 'set_null_key',
                                side_effect=redis.ConnectionError("Connection lost")):
                    result = await operator.batch_exists({1, 2})
                    
                    # Should still return results
                    assert result[1] is False
                    assert result[2] is False


class TestRefreshCacheMissingCoverage:
    """Test missing coverage in refresh_cache"""
    
    @pytest.mark.asyncio
    async def test_refresh_cache_pipeline_deletion_error(self, operator):
        """Test refresh_cache with pipeline deletion error"""
        operator.redis = AsyncMock()
        
        mock_pipeline = AsyncMock()
        mock_pipeline.delete = MagicMock()
        mock_pipeline.execute = AsyncMock(side_effect=redis.ConnectionError("Connection lost"))
        operator.redis.pipeline = MagicMock(return_value=mock_pipeline)
        
        with patch.object(operator, 'delete_caches', return_value=None):
            with patch.object(operator, 'read_sql', return_value=[]):
                result = await operator.refresh_cache({1})
                
                # Should handle error gracefully
                assert "errors" in result
    
    @pytest.mark.asyncio
    async def test_refresh_cache_pipeline_setex_error(self, operator):
        """Test refresh_cache with pipeline setex error"""
        operator.redis = AsyncMock()
        
        mock_pipeline = AsyncMock()
        mock_pipeline.delete = MagicMock()
        mock_pipeline.setex = MagicMock()
        mock_pipeline.execute = AsyncMock(side_effect=[
            None,  # First execute for delete succeeds
            redis.ConnectionError("Connection lost")  # Second for setex fails
        ])
        operator.redis.pipeline = MagicMock(return_value=mock_pipeline)
        
        with patch.object(operator, 'delete_caches', return_value=None):
            with patch.object(operator, 'read_sql', return_value=[]):
                result = await operator.refresh_cache({1, 2, 3})
                
                # Should still count not_found
                assert result["not_found"] == 3
    
    @pytest.mark.asyncio
    async def test_refresh_cache_no_redis_for_null_markers(self, operator):
        """Test refresh_cache when redis is None for null marker operations"""
        operator.redis = None
        
        with patch.object(operator, 'delete_caches', return_value=None):
            with patch.object(operator, 'read_sql', return_value=[]):
                result = await operator.refresh_cache({1, 2})
                
                # Should handle missing redis gracefully
                assert result["not_found"] == 2


class TestGetDistinctValuesMissingCoverage:
    """Test missing coverage in get_distinct_values"""
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_cache_read_error(self, operator):
        """Test get_distinct_values with cache read error"""
        operator.redis.get = AsyncMock(side_effect=Exception("Redis error"))
        
        with patch.object(operator, 'get_distinct_values',
                         wraps=operator.get_distinct_values):
            with patch('general_operate.general_operate.SQLOperate.get_distinct_values',
                      return_value=["value1", "value2"]):
                result = await operator.get_distinct_values("field", cache_ttl=300)
                
                # Should fall back to database
                assert result == ["value1", "value2"]
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_cache_write_error(self, operator):
        """Test get_distinct_values with cache write error"""
        operator.redis.get = AsyncMock(return_value=None)
        operator.redis.setex = AsyncMock(side_effect=Exception("Write error"))
        
        with patch('general_operate.general_operate.SQLOperate.get_distinct_values',
                  return_value=["value1"]):
            result = await operator.get_distinct_values("field", cache_ttl=300)
            
            # Should still return results despite cache write error
            assert result == ["value1"]
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_database_error(self, operator):
        """Test get_distinct_values with database error"""
        operator.redis.get = AsyncMock(return_value=None)
        
        with patch('general_operate.general_operate.SQLOperate.get_distinct_values',
                  side_effect=Exception("DB error")):
            with pytest.raises(Exception) as exc_info:
                await operator.get_distinct_values("field")
            
            assert "DB error" in str(exc_info.value)


class TestProcessCacheLookupsFixed:
    """Fixed test for _process_cache_lookups"""
    
    @pytest.mark.asyncio
    async def test_process_cache_lookups_returns_unique_results(self, operator):
        """Test that cache lookups return correct number of results"""
        operator.redis.exists = AsyncMock(return_value=False)
        
        # Return single cache item per ID
        async def mock_get_caches(table, ids):
            if "1" in ids:
                return [{"id": 1, "name": "test1"}]
            if "2" in ids:
                return [{"id": 2, "name": "test2"}]
            return []
        
        with patch.object(operator, 'get_caches', side_effect=mock_get_caches):
            results, miss_ids, null_ids, failed_ops = await operator._process_cache_lookups({1, 2})
            
            # Should return 2 unique results
            assert len(results) == 2
            assert len(miss_ids) == 0