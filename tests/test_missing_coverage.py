"""Tests to cover the remaining missing lines in general_operate.py"""

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import redis
from general_operate.general_operate import GeneralOperate


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
    
    op = TestGeneralOperator(db_client, redis_client)
    return op


class TestRefreshCacheMissingCoverage:
    """Test refresh_cache to cover line 1281"""
    
    @pytest.mark.asyncio
    async def test_refresh_cache_missing_ids_no_redis(self, operator):
        """Test refresh_cache with missing IDs but no Redis (line 1281)"""
        # Set Redis to None
        operator.redis = None
        
        result = await operator.refresh_cache({1, 2, 3})
        
        # When no redis, should return errors for all IDs
        assert result["refreshed"] == 0
        assert result["not_found"] == 0
        assert result["errors"] == 3
    
    @pytest.mark.asyncio
    async def test_refresh_cache_with_redis_pipeline_failure_on_missing(self, operator):
        """Test refresh_cache when Redis pipeline fails while setting null markers"""
        # Mock read_sql to return some records but not all
        async def mock_read_sql(table_name, filters):
            return [{"id": 1, "name": "record1"}]  # Only ID 1 found, 2 and 3 missing
        
        # Mock Redis pipeline to fail
        mock_pipe = AsyncMock()
        mock_pipe.setex = MagicMock()
        mock_pipe.delete = MagicMock()
        mock_pipe.execute = AsyncMock(side_effect=redis.RedisError("Pipeline failed"))
        mock_pipe.__aenter__ = AsyncMock(return_value=mock_pipe)
        mock_pipe.__aexit__ = AsyncMock(return_value=None)
        operator.redis.pipeline = MagicMock(return_value=mock_pipe)
        
        with patch.object(operator, 'read_sql', side_effect=mock_read_sql):
            with patch.object(operator, 'store_caches', new_callable=AsyncMock):
                with patch.object(operator, 'delete_caches', return_value=None):
                    result = await operator.refresh_cache({1, 2, 3})
                    
                    # Should still report correct counts even if pipeline failed
                    assert result["refreshed"] == 1
                    assert result["not_found"] == 2  # IDs 2 and 3 not found


class TestDeleteDataEdgeCases:
    """Test delete_data edge cases for lines 786-792"""
    
    @pytest.mark.asyncio
    async def test_delete_data_with_none_id(self, operator):
        """Test delete_data with None ID to trigger TypeError (lines 786-789)"""
        id_set = {1, None, 2}
        
        async def mock_delete_sql(table_name, ids, session=None):
            return ids
        
        with patch.object(operator, 'delete_sql', side_effect=mock_delete_sql) as mock_delete:
            with patch('general_operate.general_operate.CacheOperate.delete_caches', new_callable=AsyncMock):
                with patch.object(operator, 'delete_null_key', new_callable=AsyncMock):
                    results = await operator.delete_data(id_set)
        
        # None should be skipped
        processed_ids = mock_delete.call_args[0][1]
        assert 1 in processed_ids
        assert 2 in processed_ids
        assert None not in processed_ids
    
    @pytest.mark.asyncio
    async def test_delete_data_all_invalid_ids(self, operator):
        """Test delete_data when all IDs are invalid (line 792)"""
        # Create an object that will cause TypeError when converted
        class UnconvertibleObject:
            def __int__(self):
                raise TypeError("Cannot convert to int")
            def __str__(self):
                return "unconvertible"
            def isdigit(self):
                return False
        
        id_set = {None, UnconvertibleObject()}
        
        with patch.object(operator, 'delete_sql') as mock_delete:
            results = await operator.delete_data(id_set)
        
        # delete_sql should not be called
        mock_delete.assert_not_called()
        assert results == []
    
    @pytest.mark.asyncio
    async def test_delete_data_value_error_on_conversion(self, operator):
        """Test delete_data with ID that causes ValueError (lines 786-789)"""
        # Create an object that looks numeric but fails conversion
        class BadNumericString:
            def __str__(self):
                return "123"
            def __int__(self):
                raise ValueError("Conversion failed")
        
        bad_obj = BadNumericString()
        id_set = {1, bad_obj, 2}
        
        async def mock_delete_sql(table_name, ids, session=None):
            return ids
        
        with patch.object(operator, 'delete_sql', side_effect=mock_delete_sql) as mock_delete:
            with patch('general_operate.general_operate.CacheOperate.delete_caches', new_callable=AsyncMock):
                with patch.object(operator, 'delete_null_key', new_callable=AsyncMock):
                    results = await operator.delete_data(id_set)
        
        # BadNumericString should be skipped due to ValueError when converting to int
        processed_ids = mock_delete.call_args[0][1]
        assert 1 in processed_ids
        assert 2 in processed_ids
        # The bad object should have been skipped due to ValueError
        assert len(processed_ids) == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--cov=general_operate.general_operate", "--cov-report=term-missing"])