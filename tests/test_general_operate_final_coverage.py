"""Final tests to achieve 100% coverage for general_operate.py"""

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch

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


class TestDeleteDataFinalCoverage:
    """Test remaining coverage in delete_data method (lines 786-789, 792)"""
    
    @pytest.mark.asyncio
    async def test_delete_data_mixed_valid_invalid_ids(self, operator):
        """Test delete_data with mix of valid numeric, string, and invalid IDs"""
        # Mix of different ID types
        id_set = {
            123,  # Valid int
            "456",  # Valid numeric string
            "abc",  # Non-numeric string (valid)
            "",  # Empty string (should be skipped)
            None,  # None (should be skipped)
        }
        
        with patch.object(operator, 'delete_sql', return_value=[123, 456, "abc"]) as mock_delete:
            results = await operator.delete_data(id_set)
            
            # Check that delete_sql was called with processed IDs
            mock_delete.assert_called_once()
            processed_ids = mock_delete.call_args[0][1]
            
            # Should have 123 (int), 456 (converted from "456"), "abc" (string), and "" (empty string)
            assert 123 in processed_ids
            assert 456 in processed_ids
            assert "abc" in processed_ids
            assert "" in processed_ids  # Empty string is processed as it's a valid str
            # Only None should be skipped
            assert None not in processed_ids
    
    @pytest.mark.asyncio
    async def test_delete_data_conversion_exceptions(self, operator):
        """Test delete_data with IDs that cause TypeError during conversion"""
        # Create objects that will cause TypeError when processed
        class BadObject:
            def __str__(self):
                # Return a string so the str() call succeeds
                return "badobj"
            def __int__(self):
                raise TypeError("Cannot convert to int")
        
        id_set = {
            1,  # Valid
            BadObject(),  # Will cause TypeError when trying to convert to int
            2,  # Valid
        }
        
        async def mock_delete_sql(table_name, ids, session=None):
            return ids
        
        with patch.object(operator, 'delete_sql', side_effect=mock_delete_sql) as mock_delete:
            with patch.object(operator, 'delete_caches', new_callable=AsyncMock):
                with patch.object(operator, 'delete_null_key', new_callable=AsyncMock):
                    results = await operator.delete_data(id_set)
            
            # Should process only valid basic type IDs (BadObject gets filtered out)
            processed_ids = mock_delete.call_args[0][1]
            assert 1 in processed_ids
            assert 2 in processed_ids
            # BadObject should be filtered out as it's not a basic type
            assert len(processed_ids) == 2


class TestUpsertDataFinalCoverage:
    """Test remaining coverage in upsert_data method (lines 1019-1020)"""
    
    @pytest.mark.asyncio
    async def test_upsert_data_null_key_deletion_exception(self, operator):
        """Test upsert_data when null key deletion fails"""
        data = [{"id": 1, "name": "test"}]
        
        # Setup successful upsert but failed null key deletion
        with patch.object(operator, 'delete_caches', return_value=None):
            with patch.object(operator, 'delete_null_key',
                            side_effect=Exception("Null key error")):
                with patch.object(operator, 'upsert_sql',
                                return_value=[{"id": 1, "name": "test"}]):
                    results = await operator.upsert_data(
                        data,
                        conflict_fields=["id"],
                        update_fields=["name"]
                    )
                    
                    # Should complete successfully despite null key error
                    assert len(results) == 1
                    assert results[0].id == 1


class TestRefreshCacheFinalCoverage:
    """Test remaining coverage in refresh_cache method (lines 1277-1281)"""
    
    @pytest.mark.asyncio
    async def test_refresh_cache_missing_ids_pipeline_error(self, operator):
        """Test refresh_cache when setting null markers for missing IDs fails"""
        operator.redis = AsyncMock()
        
        # Setup pipeline that fails on the second execute (for null markers)
        mock_pipeline = AsyncMock()
        mock_pipeline.delete = MagicMock()
        mock_pipeline.setex = MagicMock()
        
        # First execute for delete works, second for setex of null markers fails
        execute_calls = [
            None,  # Delete operation succeeds
            redis.ConnectionError("Connection lost")  # Null marker operation fails
        ]
        mock_pipeline.execute = AsyncMock(side_effect=execute_calls)
        operator.redis.pipeline = MagicMock(return_value=mock_pipeline)
        
        with patch.object(operator, 'delete_caches', return_value=None):
            # Return partial results - ID 1 found, IDs 2 and 3 not found
            mock_sql_results = [{"id": 1, "name": "test1"}]
            with patch.object(operator, 'read_sql', return_value=mock_sql_results):
                with patch.object(operator, 'store_caches', return_value=None):
                    result = await operator.refresh_cache({1, 2, 3})
                    
                    # Should handle the error and still report correct counts
                    assert result["refreshed"] == 1
                    assert result["not_found"] == 2  # IDs 2 and 3 not found
                    assert result["errors"] == 0
    
    @pytest.mark.asyncio
    async def test_refresh_cache_all_not_found_pipeline_error(self, operator):
        """Test refresh_cache when all IDs not found and pipeline fails"""
        operator.redis = AsyncMock()
        
        mock_pipeline = AsyncMock()
        mock_pipeline.delete = MagicMock()
        mock_pipeline.setex = MagicMock()
        
        # Pipeline fails when trying to set null markers
        mock_pipeline.execute = AsyncMock(side_effect=redis.ConnectionError("Connection lost"))
        operator.redis.pipeline = MagicMock(return_value=mock_pipeline)
        
        with patch.object(operator, 'delete_caches', return_value=None):
            # No results from SQL - all IDs not found
            with patch.object(operator, 'read_sql', return_value=[]):
                result = await operator.refresh_cache({1, 2, 3})
                
                # Should handle error and count all as not found
                assert result["refreshed"] == 0
                assert result["not_found"] == 3
                assert result["errors"] == 0