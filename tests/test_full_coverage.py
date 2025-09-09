"""
Comprehensive tests to achieve 100% coverage for general_operate.py and sql_operate.py
Targets all missing lines to reach 100% coverage from current 90% and 92% respectively.
"""

import asyncio
import json
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch, Mock
import redis
from sqlalchemy.exc import DBAPIError
from sqlalchemy.dialects.postgresql.asyncpg import AsyncAdapt_asyncpg_dbapi
from sqlalchemy.orm.exc import UnmappedInstanceError
import pymysql
from contextlib import asynccontextmanager

from general_operate.general_operate import GeneralOperate
from general_operate.app.sql_operate import SQLOperate
from general_operate import GeneralOperateException, ErrorCode, ErrorContext
from general_operate.core.exceptions import DatabaseException


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
        return MockModule()


class MockSQLClient:
    """Mock SQL client"""
    def __init__(self, engine_type="postgresql"):
        self.engine_type = engine_type
    
    def get_engine(self):
        mock_engine = MagicMock()
        return mock_engine


@pytest_asyncio.fixture
async def operator():
    """Create test operator instance"""
    db_client = MockSQLClient()
    redis_client = AsyncMock(spec=redis.asyncio.Redis)
    op = TestGeneralOperator(db_client, redis_client)
    return op


@pytest_asyncio.fixture
async def mysql_operator():
    """Create test operator instance with MySQL"""
    db_client = MockSQLClient("mysql")
    redis_client = AsyncMock(spec=redis.asyncio.Redis)
    op = TestGeneralOperator(db_client, redis_client)
    return op


@pytest_asyncio.fixture
async def sql_operate():
    """Create SQLOperate instance for testing"""
    client = MockSQLClient()
    return SQLOperate(client)


@pytest_asyncio.fixture
async def mysql_sql_operate():
    """Create SQLOperate instance with MySQL for testing"""
    client = MockSQLClient("mysql")
    return SQLOperate(client)


class TestTransactionContextManager:
    """Test transaction context manager - covers line 86"""
    
    @pytest.mark.asyncio
    async def test_transaction_successful_yield(self, operator):
        """Test successful transaction yield (line 86)"""
        mock_session = AsyncMock()
        
        # Create a proper async context manager for begin()
        @asynccontextmanager
        async def mock_begin():
            yield mock_session
        
        mock_session.begin = mock_begin
        mock_session.close = AsyncMock()
        
        with patch.object(operator, 'create_external_session', return_value=mock_session):
            async with operator.transaction() as session:
                # This should hit line 86: yield session
                assert session is mock_session
            
            # Verify session was closed
            mock_session.close.assert_called_once()


class TestReadDataByFilterExceptions:
    """Test read_data_by_filter exception handling - covers lines 421-422"""
    
    @pytest.mark.asyncio
    async def test_read_data_by_filter_general_operate_exception_reraise(self, operator):
        """Test GeneralOperateException re-raise (lines 421-422)"""
        filters = {"id": 1}
        
        # Mock read_sql to raise GeneralOperateException
        exc = GeneralOperateException(
            code=ErrorCode.DB_QUERY_ERROR,
            message="Test error",
            context=ErrorContext(operation="test")
        )
        
        with patch.object(operator, 'read_sql', side_effect=exc):
            with pytest.raises(GeneralOperateException) as exc_info:
                await operator.read_data_by_filter(filters)
            
            # Should re-raise the same exception (line 422)
            assert exc_info.value is exc


class TestFetchFromSQLExceptions:
    """Test _fetch_from_sql exception handling - covers line 444"""
    
    @pytest.mark.asyncio 
    async def test_fetch_from_sql_general_operate_exception_reraise(self, operator):
        """Test GeneralOperateException re-raise in _fetch_from_sql (line 444)"""
        id_set = {1, 2}
        
        # Mock read_sql to raise GeneralOperateException
        exc = GeneralOperateException(
            code=ErrorCode.DB_QUERY_ERROR,
            message="Test error",
            context=ErrorContext(operation="test")
        )
        
        with patch.object(operator, 'read_sql', side_effect=exc):
            with pytest.raises(GeneralOperateException) as exc_info:
                await operator._fetch_from_sql(id_set)
            
            # Should re-raise the same exception (line 444)
            assert exc_info.value is exc


class TestCreateDataEdgeCases:
    """Test create_data edge cases - covers line 485"""
    
    @pytest.mark.asyncio
    async def test_create_data_empty_validated_data(self, operator):
        """Test create_data with empty validated data (line 485)"""
        # Provide data that will fail validation
        invalid_data = [{"invalid": "data"}]
        
        # Mock create_schemas to raise validation errors
        def mock_create_schema(**kwargs):
            raise ValueError("Validation failed")
        
        operator.create_schemas = mock_create_schema
        
        # Should return empty list at line 485
        result = await operator.create_data(invalid_data)
        assert result == []


class TestUpdateDataExceptions:
    """Test update_data exception handling - covers lines 672-674, 685, 692"""
    
    @pytest.mark.asyncio
    async def test_update_data_sql_exception(self, operator):
        """Test SQL exception in update_data (lines 672-674)"""
        data = [{"id": 1, "name": "test"}]
        
        # Mock validation to pass
        with patch.object(operator, '_validate_update_data', return_value=(data, [], [])):
            with patch.object(operator, '_clear_update_caches', return_value=[]):
                # Mock update_sql to raise exception
                with patch.object(operator, 'update_sql', side_effect=Exception("SQL Error")):
                    with pytest.raises(Exception) as exc_info:
                        await operator.update_data(data)
                    
                    assert "SQL Error" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_update_data_cache_delete_errors_warning(self, operator):
        """Test cache delete errors warning (line 685)"""
        data = [{"id": 1, "name": "test"}]
        updated_records = [{"id": 1, "name": "test"}]
        
        # Mock validation and update to succeed
        with patch.object(operator, '_validate_update_data', return_value=(data, ["1"], [])):
            with patch.object(operator, 'update_sql', return_value=updated_records):
                with patch.object(operator, '_convert_update_results', return_value=(data, [])):
                    # Mock cache cleanup to return errors
                    with patch.object(operator, '_clear_update_caches', return_value=["cache error"]):
                        with patch.object(operator.logger, 'warning') as mock_warning:
                            result = await operator.update_data(data)
                            
                            # Should log warning at line 685
                            mock_warning.assert_called()
                            assert "Cache delete errors" in str(mock_warning.call_args[0][0])
    
    @pytest.mark.asyncio
    async def test_update_data_incomplete_update_with_schema_errors(self, operator):
        """Test incomplete update with schema errors (line 692)"""
        data = [{"id": 1, "name": "test"}, {"id": 2, "name": "test2"}]
        updated_records = [{"id": 1, "name": "test"}]  # Only one record updated
        
        with patch.object(operator, '_validate_update_data', return_value=(data, [], [])):
            with patch.object(operator, '_clear_update_caches', return_value=[]):
                with patch.object(operator, 'update_sql', return_value=updated_records):
                    # Mock conversion to return schema errors
                    with patch.object(operator, '_convert_update_results', return_value=([{"id": 1}], ["Schema error for record 2"])):
                        with pytest.raises(GeneralOperateException) as exc_info:
                            await operator.update_data(data)
                        
                        # Should include schema errors in message (line 692)
                        assert "Schema errors" in str(exc_info.value)


class TestUpdateByForeignKeyPrintStatements:
    """Test update_by_foreign_key print statements - covers lines 732, 736"""
    
    @pytest.mark.asyncio
    async def test_update_by_foreign_key_print_warnings(self, operator):
        """Test print statements in update_by_foreign_key (lines 732, 736)"""
        foreign_key_field = "parent_id"
        foreign_key_value = 1
        data = [{"id": 999}]  # Non-existent ID for update
        
        # Mock existing items to be empty
        with patch.object(operator, 'read_data_by_filter', return_value=[]):
            # Create the handlers that will trigger print statements
            def handle_missing_update(item_id):
                print(f"Warning: Attempted to update non-existent record with ID {item_id}")
            
            def handle_missing_delete(item_id):
                print(f"Warning: Attempted to delete non-existent record with ID {item_id}")
            
            # Mock compare_related_items to call the handlers
            def mock_compare(existing_items, new_items, foreign_key_field, foreign_key_value, handle_missing_update, handle_missing_delete):
                # Simulate scenarios that trigger the handlers
                handle_missing_update(999)  # This should trigger line 732
                handle_missing_delete(999)  # This should trigger line 736
                return ([], [], [])  # Return empty operations
            
            with patch('general_operate.utils.build_data.compare_related_items', side_effect=mock_compare):
                with patch('builtins.print') as mock_print:
                    await operator.update_by_foreign_key(foreign_key_field, foreign_key_value, data)
                    
                    # Should have printed warnings (lines 732, 736)
                    assert mock_print.call_count >= 2
                    
                    # Check that warning messages were printed
                    print_calls = [str(call[0][0]) for call in mock_print.call_args_list]
                    update_warning = any("update non-existent record" in call for call in print_calls)
                    delete_warning = any("delete non-existent record" in call for call in print_calls)
                    assert update_warning and delete_warning


class TestDeleteDataExceptions:
    """Test delete_data exception handling - covers lines 786-789, 792"""
    
    @pytest.mark.asyncio
    async def test_delete_data_type_error_in_validation(self, operator):
        """Test TypeError during ID validation (lines 786-789)"""
        # Test the TypeError handling in the delete_data ID validation
        # The validation tries int(id_key) if str(id_key).isdigit()
        # We'll trigger the TypeError in the except block
        
        # Create a minimal test that doesn't break isinstance
        class BadID:
            """An ID that will cause issues during conversion"""
            pass
        
        bad_id = BadID()
        
        # Mock at the function level to avoid breaking isinstance
        original_str = str
        original_int = int
        
        def patched_str(obj):
            if obj is bad_id:
                # Return a mock that has isdigit method
                mock_str_result = Mock()
                mock_str_result.isdigit = Mock(return_value=True)
                return mock_str_result
            return original_str(obj)
        
        def patched_int(obj):
            # If it's our mock string result, raise TypeError
            if hasattr(obj, 'isdigit') and callable(getattr(obj, 'isdigit')):
                raise TypeError("Cannot convert mock to int")
            if obj is bad_id:
                raise TypeError("Cannot convert BadID to int")
            return original_int(obj)
        
        id_set = {1, bad_id, 2}
        
        # Patch where the function is used, not globally
        with patch.object(operator, 'delete_sql', new_callable=AsyncMock, return_value=[1, 2]) as mock_delete:
            # Need to patch CacheOperate.delete_caches as it's called directly
            with patch('general_operate.general_operate.CacheOperate.delete_caches', new_callable=AsyncMock):
                with patch.object(operator, 'delete_null_key', new_callable=AsyncMock):
                    # Only patch during the actual call
                    import general_operate.general_operate as go_module
                    with patch.object(go_module, 'str', side_effect=patched_str):
                        with patch.object(go_module, 'int', side_effect=patched_int):
                            result = await operator.delete_data(id_set)
        
        # BadID should be skipped due to TypeError in int() conversion
        called_ids = mock_delete.call_args[0][1]
        assert 1 in called_ids
        assert 2 in called_ids
        assert len(called_ids) == 2  # BadID was skipped
    
    @pytest.mark.asyncio
    async def test_delete_data_all_invalid_ids_empty_return(self, operator):
        """Test delete_data with all invalid IDs (line 792)"""
        # Create objects that will cause exceptions during validation
        class BadObject:
            """Object that raises exception when converted to string"""
            pass
        
        bad1 = BadObject()
        bad2 = BadObject()
        
        # Patch str() to raise ValueError for our bad objects
        original_str = str
        def patched_str(obj):
            if obj is bad1 or obj is bad2:
                raise ValueError("Cannot convert to string")
            return original_str(obj)
        
        id_set = {bad1, bad2}
        
        # Patch at module level where delete_data is defined
        import general_operate.general_operate as go_module
        with patch.object(go_module, 'str', side_effect=patched_str):
            with patch.object(operator, 'delete_sql', new_callable=AsyncMock) as mock_delete:
                with patch('general_operate.general_operate.CacheOperate.delete_caches', new_callable=AsyncMock):
                    with patch.object(operator, 'delete_null_key', new_callable=AsyncMock):
                        result = await operator.delete_data(id_set)
        
        # Should return empty list without calling delete_sql (line 792)
        mock_delete.assert_not_called()
        assert result == []


class TestDeleteFilterDataExceptions:
    """Test delete_filter_data exception handling - covers lines 851-859"""
    
    @pytest.mark.asyncio
    async def test_delete_filter_data_redis_error_during_null_marker_cleanup(self, operator):
        """Test RedisError during null marker cleanup (lines 851-859)"""
        filters = {"status": "inactive"}
        deleted_ids = [1, 2, 3]
        
        with patch.object(operator, 'delete_filter', return_value=deleted_ids):
            with patch.object(operator, 'delete_caches', new_callable=AsyncMock):
                # Mock delete_null_key to raise RedisError on first call
                mock_delete_null = AsyncMock(side_effect=[redis.RedisError("Redis down"), None, None])
                with patch.object(operator, 'delete_null_key', mock_delete_null):
                    result = await operator.delete_filter_data(filters)
        
        assert result == deleted_ids
        # Should have attempted to delete all null markers despite the error
        assert mock_delete_null.call_count == 3


class TestCountDataExceptions:
    """Test count_data exception handling - covers line 905"""
    
    @pytest.mark.asyncio
    async def test_count_data_general_operate_exception_reraise(self, operator):
        """Test GeneralOperateException re-raise in count_data (line 905)"""
        filters = {"status": "active"}
        
        exc = GeneralOperateException(
            code=ErrorCode.DB_QUERY_ERROR,
            message="Count failed",
            context=ErrorContext(operation="count")
        )
        
        with patch.object(operator, 'count_sql', side_effect=exc):
            with pytest.raises(GeneralOperateException) as exc_info:
                await operator.count_data(filters)
            
            # Should re-raise the same exception (line 905)
            assert exc_info.value is exc


class TestUpsertDataExceptions:
    """Test upsert_data exception handling - covers lines 1001-1003, 1006-1007, 1019-1022, 1033-1035, 1044-1045, 1051-1052"""
    
    @pytest.mark.asyncio
    async def test_upsert_data_schema_validation_failures(self, operator):
        """Test schema validation failures (lines 1001-1003)"""
        data = [{"id": 1, "name": "test"}, {"invalid": "data"}]
        conflict_fields = ["id"]
        
        def mock_create_schema(**kwargs):
            if "invalid" in kwargs:
                raise ValueError("Invalid schema")
            return MockModule.CreateSchema(**kwargs)
        
        operator.create_schemas = mock_create_schema
        
        with patch.object(operator, 'delete_caches', new_callable=AsyncMock):
            with patch.object(operator, 'delete_null_key', new_callable=AsyncMock):
                with patch.object(operator, 'upsert_sql', return_value=[{"id": 1, "name": "test"}]):
                    result = await operator.upsert_data(data, conflict_fields)
        
        # Should only process valid data, invalid item logged and skipped
        assert len(result) == 1
    
    @pytest.mark.asyncio
    async def test_upsert_data_no_valid_data_after_validation(self, operator):
        """Test no valid data after validation (lines 1041-1043)"""
        data = [{"invalid": "data1"}, {"invalid": "data2"}]
        conflict_fields = ["id"]
        
        # Mock both create_schemas and update_schemas to raise exceptions (invalid data)
        def mock_schema_error(**kwargs):
            raise ValueError("Schema validation failed")
        
        operator.create_schemas = mock_schema_error
        operator.update_schemas = mock_schema_error
        
        # Since there's no valid data, should return empty list before SQL execution
        result = await operator.upsert_data(data, conflict_fields)
        
        # Should return empty list (lines 1041-1043)
        assert result == []
    
    @pytest.mark.asyncio
    async def test_upsert_data_cache_cleanup_exception(self, operator):
        """Test cache cleanup exception (lines 1019-1022)"""
        data = [{"id": 1, "name": "test"}]
        conflict_fields = ["id"]
        
        with patch.object(operator, 'delete_caches', side_effect=Exception("Cache error")):
            with patch.object(operator, 'upsert_sql', return_value=[{"id": 1, "name": "test"}]):
                with patch.object(operator.logger, 'warning') as mock_warning:
                    result = await operator.upsert_data(data, conflict_fields)
                
                # Should log cache cleanup failure (lines 1019-1022)
                mock_warning.assert_called()
                assert "Pre-upsert cache cleanup failed" in str(mock_warning.call_args[0][0])
    
    @pytest.mark.asyncio
    async def test_upsert_data_sql_operation_failure(self, operator):
        """Test SQL operation failure (lines 1033-1035)"""
        data = [{"id": 1, "name": "test"}]
        conflict_fields = ["id"]
        
        with patch.object(operator, 'delete_caches', new_callable=AsyncMock):
            with patch.object(operator, 'upsert_sql', side_effect=Exception("SQL Error")):
                with pytest.raises(Exception) as exc_info:
                    await operator.upsert_data(data, conflict_fields)
                
                assert "SQL Error" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_upsert_data_schema_conversion_error(self, operator):
        """Test schema conversion error (lines 1044-1045)"""
        data = [{"id": 1, "name": "test"}]
        conflict_fields = ["id"]
        
        def mock_main_schema(**kwargs):
            raise ValueError("Schema conversion failed")
        
        operator.main_schemas = mock_main_schema
        
        with patch.object(operator, 'delete_caches', new_callable=AsyncMock):
            with patch.object(operator, 'upsert_sql', return_value=[{"id": 1, "name": "test"}]):
                with patch.object(operator.logger, 'error') as mock_error:
                    result = await operator.upsert_data(data, conflict_fields)
                
                # Should log schema conversion error (lines 1044-1045)
                mock_error.assert_called()
                assert "Schema conversion failed" in str(mock_error.call_args[0][0])
    
    @pytest.mark.asyncio
    async def test_upsert_data_post_cache_cleanup_failure(self, operator):
        """Test post-upsert cache cleanup failure (lines 1051-1052)"""
        data = [{"id": 1, "name": "test"}]
        conflict_fields = ["id"]
        
        with patch.object(operator, 'delete_caches', side_effect=[None, Exception("Post cleanup error")]):
            with patch.object(operator, 'upsert_sql', return_value=[{"id": 1, "name": "test"}]):
                # Should not raise exception, just continue
                result = await operator.upsert_data(data, conflict_fields)
                assert len(result) == 1


class TestExistsCheckExceptions:
    """Test exists_check exception handling - covers lines 1086-1087, 1104-1105, 1110-1112"""
    
    @pytest.mark.asyncio
    async def test_exists_check_cache_operation_failure(self, operator):
        """Test cache operation failure (lines 1086-1087)"""
        id_value = 1
        
        # Mock CacheOperate.check_null_markers_batch to raise error
        with patch('general_operate.app.cache_operate.CacheOperate.check_null_markers_batch', 
                   side_effect=redis.RedisError("Redis down")), \
             patch.object(operator, 'exists_sql', return_value={1: True}):
            with patch.object(operator.logger, 'warning') as mock_warning:
                result = await operator.exists_check(id_value)
            
            # Should log cache failure warning
            mock_warning.assert_called()
            assert "Cache check failed" in str(mock_warning.call_args[0][0])
            assert result is True
    
    @pytest.mark.asyncio
    async def test_exists_check_null_marker_set_failure(self, operator):
        """Test null marker set failure (lines 1104-1105)"""
        id_value = 1
        
        with patch.object(operator, 'exists_sql', return_value={1: False}):
            with patch.object(operator, 'set_null_key', side_effect=Exception("Set failed")):
                # Should not raise exception, just continue
                result = await operator.exists_check(id_value)
                assert result is False
    
    @pytest.mark.asyncio
    async def test_exists_check_database_failure(self, operator):
        """Test database check failure (lines 1110-1112)"""
        id_value = 1
        
        with patch.object(operator, 'exists_sql', side_effect=Exception("DB Error")):
            with pytest.raises(Exception) as exc_info:
                await operator.exists_check(id_value)
            
            assert "DB Error" in str(exc_info.value)


class TestBatchExistsExceptions:
    """Test batch_exists exception handling - covers line 1197"""
    
    @pytest.mark.asyncio
    async def test_batch_exists_redis_error_null_marker_handling(self, operator):
        """Test RedisError during null marker operations (line 1219)"""
        id_values = {1, 2, 3}
        
        # Mock check_null_markers_batch to return no null markers
        with patch('general_operate.app.cache_operate.CacheOperate.check_null_markers_batch', 
                   new=AsyncMock(return_value=({}, {1, 2, 3}))), \
             patch.object(operator, 'get_caches', return_value={}), \
             patch.object(operator, 'exists_sql', return_value={1: True, 2: False, 3: False}), \
             patch('general_operate.app.cache_operate.CacheOperate.set_null_markers_batch', 
                   side_effect=redis.RedisError("Redis error")) as mock_set_null:
            with patch.object(operator.logger, 'debug') as mock_debug:
                result = await operator.batch_exists(id_values)
            
            # Should try to set null markers for non-existent items (2 and 3)
            # set_null_markers_batch should be called once with the non-existent IDs
            mock_set_null.assert_called_once()
            call_args = mock_set_null.call_args[0]
            assert set(call_args[2]) == {2, 3}  # The non-existent IDs
            
            # Debug log should be called for the failed null marker operation
            assert mock_debug.call_count >= 1


class TestRefreshCacheExceptions:
    """Test refresh_cache exception handling - covers lines 1242-1244, 1277-1281, 1292-1296"""
    
    @pytest.mark.asyncio
    async def test_refresh_cache_null_marker_pipeline_failure(self, operator):
        """Test null marker pipeline failure (lines 1242-1244)"""
        id_values = {1, 2, 3}
        
        # Mock pipeline to fail during execution
        mock_pipe = AsyncMock()
        mock_pipe.delete = MagicMock()
        mock_pipe.execute = AsyncMock(side_effect=redis.RedisError("Pipeline failed"))
        mock_pipe.__aenter__ = AsyncMock(return_value=mock_pipe)
        mock_pipe.__aexit__ = AsyncMock(return_value=None)
        operator.redis.pipeline = MagicMock(return_value=mock_pipe)
        
        async def mock_read_sql(*args, **kwargs):
            return []
        
        with patch.object(operator, 'delete_caches', new_callable=AsyncMock):
            with patch.object(operator, 'read_sql', side_effect=mock_read_sql):
                with patch.object(operator.logger, 'debug') as mock_debug:
                    result = await operator.refresh_cache(id_values)
                
                # Should log pipeline failure (lines 1242-1244)
                mock_debug.assert_called()
                debug_calls = [str(call[0][0]) for call in mock_debug.call_args_list]
                assert any("Failed to clear null markers in bulk" in call for call in debug_calls)
    
    @pytest.mark.asyncio
    async def test_refresh_cache_missing_ids_pipeline_failure(self, operator):
        """Test missing IDs pipeline failure (lines 1292-1301)"""
        id_values = {1, 2, 3}
        
        # Mock read_sql to return partial results
        async def mock_read_sql(*args, **kwargs):
            return [{"id": 1, "name": "test"}]
        
        with patch.object(operator, 'read_sql', side_effect=mock_read_sql):
            with patch.object(operator, 'store_caches', new_callable=AsyncMock):
                with patch.object(operator, 'delete_caches', new_callable=AsyncMock):
                    # Mock pipeline failure for missing IDs - pipeline should be async context manager
                    # First pipeline call (delete null markers) should succeed
                    # Second pipeline call (set null markers) should fail
                    call_count = 0
                    def pipeline_side_effect(transaction=False):
                        nonlocal call_count
                        call_count += 1
                        
                        mock_pipe = AsyncMock()
                        mock_pipe.setex = MagicMock()
                        mock_pipe.delete = MagicMock()
                        
                        if call_count == 1:
                            # First call succeeds (delete null markers)
                            mock_pipe.execute = AsyncMock(return_value=[])
                        else:
                            # Second call fails (set null markers for missing IDs)
                            mock_pipe.execute = AsyncMock(side_effect=redis.RedisError("Pipeline failed"))
                        
                        mock_pipe.__aenter__ = AsyncMock(return_value=mock_pipe)
                        mock_pipe.__aexit__ = AsyncMock(return_value=None)
                        return mock_pipe
                    
                    operator.redis.pipeline = MagicMock(side_effect=pipeline_side_effect)
                    
                    with patch.object(operator.logger, 'debug') as mock_debug:
                        result = await operator.refresh_cache(id_values)
                    
                    # Should handle pipeline failure gracefully (lines 1292-1301)
                    assert result["refreshed"] == 1
                    assert result["not_found"] == 2  # IDs 2,3 not found
    
    @pytest.mark.asyncio
    async def test_refresh_cache_all_missing_pipeline_failure(self, operator):
        """Test all missing IDs pipeline failure (lines 1307-1316)"""
        id_values = {1, 2, 3}
        
        async def mock_read_sql(*args, **kwargs):
            return []  # No results found
        
        with patch.object(operator, 'read_sql', side_effect=mock_read_sql):
            with patch.object(operator, 'store_caches', new_callable=AsyncMock):
                with patch.object(operator, 'delete_caches', new_callable=AsyncMock):
                    # Mock pipeline failure for all missing IDs
                    # First pipeline call (delete null markers) should succeed
                    # Second pipeline call (set null markers for all missing IDs) should fail
                    call_count = 0
                    def pipeline_side_effect(transaction=False):
                        nonlocal call_count
                        call_count += 1
                        
                        mock_pipe = AsyncMock()
                        mock_pipe.setex = MagicMock()
                        mock_pipe.delete = MagicMock()
                        
                        if call_count == 1:
                            # First call succeeds (delete null markers)
                            mock_pipe.execute = AsyncMock(return_value=[])
                        else:
                            # Second call fails (set null markers for all missing IDs)
                            mock_pipe.execute = AsyncMock(side_effect=redis.RedisError("Pipeline failed"))
                        
                        mock_pipe.__aenter__ = AsyncMock(return_value=mock_pipe)
                        mock_pipe.__aexit__ = AsyncMock(return_value=None)
                        return mock_pipe
                    
                    operator.redis.pipeline = MagicMock(side_effect=pipeline_side_effect)
                    
                    with patch.object(operator.logger, 'debug') as mock_debug:
                        result = await operator.refresh_cache(id_values)
                    
                    # Should handle pipeline failure gracefully (lines 1307-1316)
                    assert result["refreshed"] == 0
                    assert result["not_found"] == 3


class TestGetDistinctValuesExceptions:
    """Test get_distinct_values exception handling - covers lines 1347-1348, 1368-1369, 1374-1376"""
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_cache_read_failure(self, operator):
        """Test cache read failure (lines 1382-1383)"""
        field = "status"
        filters = {"active": True}
        cache_ttl = 300
        
        # Mock Redis get to fail but setex to succeed
        operator.redis.get = AsyncMock(side_effect=redis.RedisError("Cache read failed"))
        operator.redis.setex = AsyncMock(return_value=True)  # Allow cache write to succeed
        
        with patch('general_operate.app.sql_operate.SQLOperate.get_distinct_values', return_value=["active", "inactive"]):
            with patch.object(operator.logger, 'warning') as mock_warning:
                result = await operator.get_distinct_values(field, filters, cache_ttl)
            
            # Should log cache read failure (lines 1382-1383)
            mock_warning.assert_called()
            # Check if any of the warning calls contains the cache read failure message
            warning_messages = [str(call.args[0]) for call in mock_warning.call_args_list]
            assert any("Cache read failed" in msg for msg in warning_messages), f"Expected 'Cache read failed' in warnings: {warning_messages}"
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_cache_write_failure(self, operator):
        """Test cache write failure (lines 1368-1369)"""
        field = "status"
        cache_ttl = 300
        distinct_values = ["active", "inactive"]
        
        # Mock Redis setex to fail
        operator.redis.setex = AsyncMock(side_effect=redis.RedisError("Cache write failed"))
        
        with patch('general_operate.app.sql_operate.SQLOperate.get_distinct_values', return_value=distinct_values):
            with patch.object(operator.logger, 'warning') as mock_warning:
                result = await operator.get_distinct_values(field, cache_ttl=cache_ttl)
            
            # Should log cache write failure (lines 1368-1369)
            mock_warning.assert_called()
            assert "Cache write failed" in str(mock_warning.call_args[0][0])
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_database_failure(self, operator):
        """Test database query failure (lines 1374-1376)"""
        field = "status"
        
        with patch('general_operate.app.sql_operate.SQLOperate.get_distinct_values', side_effect=Exception("DB query failed")):
            with pytest.raises(Exception) as exc_info:
                await operator.get_distinct_values(field)
            
            assert "DB query failed" in str(exc_info.value)


# SQL Operate Tests for missing coverage
class TestSQLOperateExceptionHandling:
    """Test SQLOperate exception handling to cover missing lines"""
    
    @pytest.mark.asyncio
    async def test_json_decode_error_handling(self, sql_operate):
        """Test JSON decode error in exception handler (lines 44-50)"""
        
        @sql_operate.exception_handler
        async def test_func(self):
            raise json.JSONDecodeError("Invalid JSON", "test", 0)
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func(sql_operate)
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "JSON decode error" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_postgresql_dbapi_error_handling(self, sql_operate):
        """Test PostgreSQL DBAPI error handling (lines 52-61)"""
        
        # Create mock PostgreSQL error
        pg_error = MagicMock(spec=AsyncAdapt_asyncpg_dbapi.Error)
        pg_error.sqlstate = "23505"
        pg_error.__str__ = lambda self: "duplicate key: some detailed message"
        
        db_error = DBAPIError("statement", {}, pg_error)
        
        @sql_operate.exception_handler
        async def test_func(self):
            raise db_error
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func(sql_operate)
        
        assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR
        assert "some detailed message" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_mysql_error_handling(self, mysql_sql_operate):
        """Test MySQL error handling (lines 62-69)"""
        
        mysql_error = pymysql.Error(1062, "Duplicate entry 'value' for key 'PRIMARY'")
        db_error = DBAPIError("statement", {}, mysql_error)
        
        @mysql_sql_operate.exception_handler
        async def test_func(self):
            raise db_error
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func(mysql_sql_operate)
        
        assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR
        assert "Duplicate entry" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_generic_dbapi_error_handling(self, sql_operate):
        """Test generic DBAPI error handling (lines 70-78)"""
        
        generic_error = Exception("Generic database error")
        db_error = DBAPIError("statement", {}, generic_error)
        
        @sql_operate.exception_handler
        async def test_func(self):
            raise db_error
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func(sql_operate)
        
        assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR
        assert "Generic database error" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_unmapped_instance_error_handling(self, sql_operate):
        """Test UnmappedInstanceError handling (lines 79-85)"""
        
        @sql_operate.exception_handler
        async def test_func(self):
            raise UnmappedInstanceError("Instance not mapped")
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func(sql_operate)
        
        assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR
        assert "one or more of ids is not exist" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_sql_validation_error_handling(self, sql_operate):
        """Test SQL validation error handling (lines 86-93)"""
        
        @sql_operate.exception_handler
        async def test_func(self):
            raise ValueError("SQL validation failed")
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func(sql_operate)
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "SQL operation validation error" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_database_exception_reraise(self, sql_operate):
        """Test DatabaseException re-raise (lines 94-97)"""
        
        original_exc = DatabaseException(
            code=ErrorCode.DB_QUERY_ERROR,
            message="Original error",
            context=ErrorContext(operation="test")
        )
        
        @sql_operate.exception_handler
        async def test_func(self):
            raise original_exc
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func(sql_operate)
        
        assert exc_info.value is original_exc
    
    @pytest.mark.asyncio
    async def test_unknown_error_handling(self, sql_operate):
        """Test unknown error handling (lines 98-104)"""
        
        @sql_operate.exception_handler
        async def test_func(self):
            raise RuntimeError("Unexpected error")
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func(sql_operate)
        
        assert exc_info.value.code == ErrorCode.UNKNOWN_ERROR
        assert "Operation error" in str(exc_info.value)


class TestSQLOperateValidation:
    """Test SQLOperate validation methods for missing coverage"""
    
    def test_validate_identifier_invalid_type(self, sql_operate):
        """Test identifier validation with invalid types (lines 133-138)"""
        
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_identifier(None, "table name")
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "must be a non-empty string" in str(exc_info.value)
        
        with pytest.raises(DatabaseException):
            sql_operate._validate_identifier(123, "column name")
    
    def test_validate_identifier_too_long(self, sql_operate):
        """Test identifier validation with length > 64 (lines 140-145)"""
        
        long_name = "a" * 65
        
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_identifier(long_name, "table name")
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "too long" in str(exc_info.value)
    
    def test_validate_identifier_invalid_characters(self, sql_operate):
        """Test identifier validation with invalid characters (lines 147-152)"""
        
        invalid_names = ["table-name", "table name", "123table", "table$name"]
        
        for invalid_name in invalid_names:
            with pytest.raises(DatabaseException) as exc_info:
                sql_operate._validate_identifier(invalid_name, "identifier")
            
            assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
            assert "invalid characters" in str(exc_info.value)
    
    def test_validate_data_value_non_serializable_list(self, sql_operate):
        """Test data value validation with non-serializable data (lines 167-179)"""
        
        class NonSerializable:
            pass
        
        non_serializable_data = [NonSerializable()]
        
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_data_value(non_serializable_data, "test_column")
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "non-serializable data" in str(exc_info.value)
    
    def test_validate_data_value_string_too_long(self, sql_operate):
        """Test data value validation with string too long (lines 182-187)"""
        
        long_string = "a" * 65536
        
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_data_value(long_string, "test_column")
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "too long" in str(exc_info.value)
    
    def test_validate_data_value_dangerous_sql_patterns(self, sql_operate):
        """Test data value validation with dangerous SQL patterns (lines 210-224)"""
        
        dangerous_values = [
            "test -- comment",
            "test /* comment */",
            "EXEC sp_something",
            "DROP TABLE users",
            "SELECT FROM users"
        ]
        
        for dangerous_value in dangerous_values:
            with pytest.raises(DatabaseException) as exc_info:
                sql_operate._validate_data_value(dangerous_value, "regular_column")
            
            assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
            assert ("dangerous SQL" in str(exc_info.value) or 
                    "dangerous SQL keyword" in str(exc_info.value) or
                    "dangerous SQL characters" in str(exc_info.value))
    
    def test_validate_data_value_exempt_columns(self, sql_operate):
        """Test data value validation with exempt columns (lines 207-208)"""
        
        # These columns should allow SQL keywords
        exempt_columns = ["content_template", "subject_template", "description", "content"]
        sql_content = "SELECT * FROM users WHERE active = 1"
        
        for column in exempt_columns:
            # Should not raise exception for exempt columns
            try:
                sql_operate._validate_data_value(sql_content, column)
            except DatabaseException:
                pytest.fail(f"Should not raise exception for exempt column: {column}")
    
    def test_validate_data_dict_not_dict(self, sql_operate):
        """Test data dict validation with non-dict input (lines 233-234)"""
        
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_data_dict("not a dict")
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "must be a dictionary" in str(exc_info.value)
    
    def test_validate_data_dict_empty_not_allowed(self, sql_operate):
        """Test data dict validation with empty dict (lines 236-237)"""
        
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_data_dict({}, allow_empty=False)
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "cannot be empty" in str(exc_info.value)
    
    def test_validate_data_dict_unhashable_value_handling(self, sql_operate):
        """Test data dict validation with unhashable values (lines 256-262)"""
        
        data = {
            "list_field": [1, 2, 3],
            "dict_field": {"nested": "value"}
        }
        
        validated = sql_operate._validate_data_dict(data, allow_empty=True)
        
        # Should convert to JSON strings
        assert isinstance(validated["list_field"], str)
        assert isinstance(validated["dict_field"], str)
        assert json.loads(validated["list_field"]) == [1, 2, 3]
        assert json.loads(validated["dict_field"]) == {"nested": "value"}
    
    def test_validate_data_dict_no_valid_data(self, sql_operate):
        """Test data dict validation with no valid data after filtering (lines 264-265)"""
        
        # Data with only null values
        data = {
            "field1": None,
            "field2": "null",  # This is in null_set
            "field3": -999999  # This is in null_set
        }
        
        # All values are converted to None but still included
        result = sql_operate._validate_data_dict(data, allow_empty=False)
        
        # Should have all fields with None values
        assert "field1" in result
        assert result["field1"] is None
        assert "field2" in result
        assert result["field2"] is None
        assert "field3" in result
        assert result["field3"] is None


class TestSQLOperateWhereClause:
    """Test SQLOperate WHERE clause building for missing coverage"""
    
    def test_build_where_clause_postgresql_list_handling(self, sql_operate):
        """Test PostgreSQL list handling in WHERE clause (lines 296-299)"""
        
        # sql_operate is already PostgreSQL by default
        filters = {
            "status": ["active", "pending"],
            "category_id": [1, 2, 3]
        }
        
        where_clause, params = sql_operate._build_where_clause(filters)
        
        assert "status = ANY(:status)" in where_clause
        assert "category_id = ANY(:category_id)" in where_clause
        assert params["status"] == ["active", "pending"]
        assert params["category_id"] == [1, 2, 3]


class TestSQLOperateNullHandling:
    """Test null value handling in various scenarios"""
    
    def test_null_set_membership_unhashable(self, sql_operate):
        """Test null_set membership with unhashable types (lines 160-165 in _validate_data_value)"""
        
        # Test with list that's not in null_set (should not raise TypeError)
        test_list = [1, 2, 3]
        
        # Should not raise exception
        try:
            sql_operate._validate_data_value(test_list, "test_column")
        except DatabaseException:
            pass  # Validation might fail for other reasons, but not TypeError
        except TypeError:
            pytest.fail("Should handle unhashable types gracefully")


# Additional edge cases and missing lines coverage
class TestEdgeCasesAndMissingLines:
    """Test remaining edge cases and missing line coverage"""
    
    @pytest.mark.asyncio
    async def test_general_operate_module_none_handling(self):
        """Test GeneralOperate when get_module returns None (lines 46-51)"""
        
        class NoModuleOperator(GeneralOperate):
            def get_module(self):
                return None
        
        db_client = MockSQLClient()
        redis_client = AsyncMock(spec=redis.asyncio.Redis)
        
        operator = NoModuleOperator(db_client, redis_client)
        
        # Should handle None module gracefully
        assert operator.table_name is None
        assert operator.main_schemas is None
        assert operator.create_schemas is None
        assert operator.update_schemas is None
    
    @pytest.mark.asyncio
    async def test_cache_operations_without_redis(self, operator):
        """Test cache operations when Redis is None"""
        
        operator.redis = None
        
        # Test batch_exists without Redis (lines 1172-1174)
        id_values = {1, 2, 3}
        with patch.object(operator, 'exists_sql', return_value={1: True, 2: False, 3: True}):
            result = await operator.batch_exists(id_values)
        
        assert result == {1: True, 2: False, 3: True}
    
    def test_sql_operate_sync_wrapper_coverage(self, sql_operate):
        """Test sync wrapper in exception_handler decorator (lines 118-123)"""
        
        @sql_operate.exception_handler
        def sync_test_func(self):
            raise ValueError("SQL operation validation error")
        
        with pytest.raises(DatabaseException) as exc_info:
            sync_test_func(sql_operate)
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR


# Run specific missing line tests
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--cov=general_operate.general_operate", "--cov=general_operate.app.sql_operate", "--cov-report=term-missing"])