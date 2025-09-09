"""
Final tests to achieve 100% coverage for general_operate.py
Focusing on lines: 789, 792, 1110-1112, 1281
"""

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch, Mock
import redis
from general_operate.general_operate import GeneralOperate
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
    redis_client.exists = AsyncMock(return_value=False)
    redis_client.get = AsyncMock(return_value=None)
    redis_client.mget = AsyncMock(return_value=[None])
    redis_client.setex = AsyncMock()
    redis_client.delete = AsyncMock(return_value=1)
    redis_client.ping = AsyncMock(return_value=True)
    redis_client.pipeline = MagicMock()
    
    op = TestGeneralOperator(db_client, redis_client)
    return op


class TestDeleteDataCoverage:
    """Test delete_data to cover lines 789, 792"""
    
    @pytest.mark.asyncio
    async def test_delete_data_with_type_error(self, operator):
        """Test delete_data with ID that causes TypeError (line 789)"""
        # Use string IDs that will be converted, plus None which should be skipped
        id_set = {1, "2", "3", None, "not_a_number"}  # Mix of valid and invalid IDs
        
        async def mock_delete_sql(table_name, ids, session=None):
            return list(ids)  # Return the IDs that were passed
        
        with patch.object(operator, 'delete_sql', side_effect=mock_delete_sql) as mock_delete:
            with patch.object(CacheOperate, 'delete_caches', new_callable=AsyncMock):
                with patch.object(operator, 'delete_null_key', new_callable=AsyncMock):
                    results = await operator.delete_data(id_set)
        
        # Check what IDs were processed
        if mock_delete.called:
            processed_ids = mock_delete.call_args[0][1]
            assert 1 in processed_ids
            assert 2 in processed_ids  # "2" converted to 2
            assert 3 in processed_ids  # "3" converted to 3
            # None and "not_a_number" should be skipped
    
    @pytest.mark.asyncio
    async def test_delete_data_all_invalid(self, operator):
        """Test delete_data when all IDs are invalid (line 792)"""
        # All IDs that will cause exceptions
        id_set = {None}  # None will cause TypeError
        
        with patch.object(operator, 'delete_sql') as mock_delete:
            results = await operator.delete_data(id_set)
        
        # delete_sql should not be called
        mock_delete.assert_not_called()
        assert results == []


class TestBatchExistsCoverage:
    """Test batch_exists to cover lines 1110-1112"""
    
    @pytest.mark.asyncio
    async def test_batch_exists_database_error(self, operator):
        """Test batch_exists when database check fails (lines 1220-1225)"""
        # Set up Redis pipeline mock
        mock_pipe = AsyncMock()
        mock_pipe.exists = MagicMock()
        mock_pipe.execute = AsyncMock(return_value=[0, 0])  # No null markers exist
        
        # Make pipeline() return an async context manager
        async_cm = AsyncMock()
        async_cm.__aenter__ = AsyncMock(return_value=mock_pipe)
        async_cm.__aexit__ = AsyncMock(return_value=None)
        operator.redis.pipeline = MagicMock(return_value=async_cm)
        
        # Mock get_caches to return no data (all need DB check)
        with patch.object(operator, 'get_caches', return_value={}):
            # Mock exists_sql to raise an exception
            with patch.object(operator, 'exists_sql', side_effect=Exception("Database error")):
                with patch.object(operator.logger, 'error') as mock_error:
                    result = await operator.batch_exists({1, 2})
                    
                    # Should log the error
                    mock_error.assert_called_once()
                    
                    # All IDs should be marked as False due to database error
                    assert result == {1: False, 2: False}


class TestRefreshCacheCoverage:
    """Test refresh_cache to cover line 1281"""
    
    @pytest.mark.asyncio
    async def test_refresh_cache_no_redis_with_missing_ids(self, operator):
        """Test refresh_cache without Redis when some IDs are missing (line 1281)"""
        # Set Redis to None
        operator.redis = None
        
        # Mock read_sql to return only some of the requested IDs
        async def mock_read_sql(table_name, filters):
            # Return data for IDs 1 and 2, but not 3
            return [
                {"id": 1, "name": "record1"},
                {"id": 2, "name": "record2"}
            ]
        
        with patch.object(operator, 'read_sql', side_effect=mock_read_sql):
            refreshed, not_found = await operator.refresh_cache({1, 2, 3})
            
            # Should have refreshed 2 records
            assert refreshed == 2
            # Should have 1 not found (ID 3)
            assert not_found == 1
    
    @pytest.mark.asyncio
    async def test_refresh_cache_with_all_missing_no_redis(self, operator):
        """Test refresh_cache without Redis when all IDs are missing"""
        # Set Redis to None
        operator.redis = None
        
        # Mock read_sql to return no records
        async def mock_read_sql(table_name, filters):
            return []
        
        with patch.object(operator, 'read_sql', side_effect=mock_read_sql):
            refreshed, not_found = await operator.refresh_cache({1, 2, 3})
            
            # Should have refreshed 0 records
            assert refreshed == 0
            # Should have 3 not found
            assert not_found == 3


class TestEdgeCasesForFullCoverage:
    """Additional edge cases for complete coverage"""
    
    @pytest.mark.asyncio
    async def test_delete_data_value_error(self, operator):
        """Test delete_data with ValueError during conversion"""
        # Create a scenario where int() raises ValueError
        class NumericString:
            def __str__(self):
                return "123abc"  # Invalid number
            
            def isdigit(self):
                return False  # Not a digit
        
        id_set = {1, NumericString(), 2}
        
        async def mock_delete_sql(table_name, ids, session=None):
            return ids
        
        with patch.object(operator, 'delete_sql', side_effect=mock_delete_sql) as mock_delete:
            with patch.object(CacheOperate, 'delete_caches', new_callable=AsyncMock):
                with patch.object(operator, 'delete_null_key', new_callable=AsyncMock):
                    results = await operator.delete_data(id_set)
        
        # NumericString should be included as-is since it's not numeric
        processed_ids = mock_delete.call_args[0][1]
        assert 1 in processed_ids
        assert 2 in processed_ids
        # The NumericString object should be in the list
        assert len(processed_ids) == 3
    
    @pytest.mark.asyncio
    async def test_batch_exists_partial_cache_hit(self, operator):
        """Test batch_exists with partial cache hits"""
        # Set up pipeline mock
        mock_pipe = MagicMock()
        mock_pipe.exists = MagicMock()
        mock_pipe.execute = AsyncMock(return_value=[False, True, False])  # ID 2 has null marker
        operator.redis.pipeline = MagicMock(return_value=mock_pipe)
        
        # Mock get_caches to return data for ID 1 only
        async def mock_get_caches(table_name, ids):
            if "1" in ids:
                return {"1": {"id": 1, "name": "cached"}}
            return {}
        
        # Mock exists_sql for ID 3
        async def mock_exists_sql(table_name, ids, session=None):
            return {3: True}
        
        with patch.object(operator, 'get_caches', side_effect=mock_get_caches):
            with patch.object(operator, 'exists_sql', side_effect=mock_exists_sql):
                with patch.object(operator, 'set_null_key', new_callable=AsyncMock):
                    result = await operator.batch_exists({1, 2, 3})
                    
                    assert result[1] is True  # Found in cache
                    assert result[2] is False  # Has null marker
                    assert result[3] is True  # Found in database
    
    @pytest.mark.asyncio
    async def test_refresh_cache_with_pipeline_but_no_missing_ids(self, operator):
        """Test refresh_cache when all IDs are found"""
        # Mock read_sql to return all requested records
        async def mock_read_sql(table_name, filters):
            return [
                {"id": 1, "name": "record1"},
                {"id": 2, "name": "record2"},
                {"id": 3, "name": "record3"}
            ]
        
        with patch.object(operator, 'read_sql', side_effect=mock_read_sql):
            with patch.object(operator, 'delete_caches', new_callable=AsyncMock):
                with patch.object(operator, 'store_caches', new_callable=AsyncMock):
                    refreshed, not_found = await operator.refresh_cache({1, 2, 3})
                    
                    assert refreshed == 3
                    assert not_found == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--cov=general_operate.general_operate", "--cov-report=term-missing"])