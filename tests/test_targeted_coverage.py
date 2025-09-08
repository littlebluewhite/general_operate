"""
Targeted tests for specific missing lines to achieve 100% coverage.
This file focuses on the exact missing line numbers provided.
"""

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch, Mock
import redis
from contextlib import asynccontextmanager
import json
from sqlalchemy.exc import DBAPIError
from sqlalchemy.dialects.postgresql.asyncpg import AsyncAdapt_asyncpg_dbapi
import pymysql

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
        return MagicMock()


@pytest_asyncio.fixture
async def operator():
    """Create test operator instance"""
    db_client = MockSQLClient()
    redis_client = AsyncMock(spec=redis.asyncio.Redis)
    op = TestGeneralOperator(db_client, redis_client)
    return op


class TestSpecificMissingLines:
    """Test specific missing lines for 100% coverage"""
    
    @pytest.mark.asyncio
    async def test_transaction_yield_line_86(self, operator):
        """Test transaction yield (line 86)"""
        mock_session = AsyncMock()
        
        @asynccontextmanager
        async def mock_begin():
            yield mock_session
        
        mock_session.begin = mock_begin
        mock_session.close = AsyncMock()
        
        with patch.object(operator, 'create_external_session', return_value=mock_session):
            async with operator.transaction() as session:
                # This covers line 86: yield session
                assert session is mock_session
    
    @pytest.mark.asyncio
    async def test_read_filter_exception_lines_421_422(self, operator):
        """Test exception re-raise in read_data_by_filter (lines 421-422)"""
        exc = GeneralOperateException(
            code=ErrorCode.DB_QUERY_ERROR,
            message="Test error",
            context=ErrorContext(operation="test")
        )
        
        with patch.object(operator, 'read_sql', side_effect=exc):
            with pytest.raises(GeneralOperateException) as exc_info:
                await operator.read_data_by_filter({"id": 1})
            assert exc_info.value is exc  # Line 422: raise e
    
    @pytest.mark.asyncio
    async def test_fetch_sql_exception_line_444(self, operator):
        """Test exception re-raise in _fetch_from_sql (line 444)"""
        exc = GeneralOperateException(
            code=ErrorCode.DB_QUERY_ERROR,
            message="Test error",
            context=ErrorContext(operation="test")
        )
        
        with patch.object(operator, 'read_sql', side_effect=exc):
            with pytest.raises(GeneralOperateException) as exc_info:
                await operator._fetch_from_sql({1, 2})
            assert exc_info.value is exc  # Line 444: raise
    
    @pytest.mark.asyncio
    async def test_create_data_empty_return_line_485(self, operator):
        """Test empty return in create_data (line 485)"""
        # Mock schema validation to always fail
        def failing_schema(**kwargs):
            raise ValueError("Always fails")
        
        operator.create_schemas = failing_schema
        
        result = await operator.create_data([{"test": "data"}])
        assert result == []  # Line 485: return []
    
    @pytest.mark.asyncio 
    async def test_update_data_sql_exception_lines_672_674(self, operator):
        """Test SQL exception in update_data (lines 672-674)"""
        data = [{"id": 1, "name": "test"}]
        
        with patch.object(operator, '_validate_update_data', return_value=(data, [], [])):
            with patch.object(operator, '_clear_update_caches', return_value=[]):
                with patch.object(operator, 'update_sql', side_effect=Exception("SQL Error")):
                    # Line 672: try, 673: updated_records = await..., 674: except
                    with pytest.raises(Exception):
                        await operator.update_data(data)
    
    @pytest.mark.asyncio
    async def test_update_data_cache_warning_line_685(self, operator):
        """Test cache delete errors warning (line 685)"""
        data = [{"id": 1, "name": "test"}]
        
        with patch.object(operator, '_validate_update_data', return_value=(data, ["1"], [])):
            with patch.object(operator, 'update_sql', return_value=data):
                with patch.object(operator, '_convert_update_results', return_value=(data, [])):
                    with patch.object(operator, '_clear_update_caches', return_value=["cache error"]):
                        with patch.object(operator.logger, 'warning') as mock_warning:
                            await operator.update_data(data)
                            # Line 685: self.logger.warning(...)
                            mock_warning.assert_called()
    
    @pytest.mark.asyncio
    async def test_update_data_schema_error_line_692(self, operator):
        """Test schema errors in update message (line 692)"""
        data = [{"id": 1}, {"id": 2}]
        
        with patch.object(operator, '_validate_update_data', return_value=(data, [], [])):
            with patch.object(operator, '_clear_update_caches', return_value=[]):
                with patch.object(operator, 'update_sql', return_value=[{"id": 1}]): # Only 1 record
                    with patch.object(operator, '_convert_update_results', return_value=([{"id": 1}], ["Schema error"])):
                        with pytest.raises(GeneralOperateException) as exc_info:
                            await operator.update_data(data)
                        # Line 692: error_msg += f". Schema errors: ..."
                        assert "Schema errors" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_update_foreign_key_print_lines_732_736(self, operator):
        """Test print statements (lines 732, 736)"""
        
        def mock_compare_items(existing_items, new_items, fk_field, fk_value, handle_update, handle_delete):
            # Trigger the print statements
            handle_update(999)  # Line 732: print warning
            handle_delete(888)  # Line 736: print warning
            return [], [], []
        
        with patch.object(operator, 'read_data_by_filter', return_value=[]):
            with patch('general_operate.utils.build_data.compare_related_items', side_effect=mock_compare_items):
                with patch('builtins.print') as mock_print:
                    await operator.update_by_foreign_key("fk", 1, [])
                    assert mock_print.call_count == 2
    
    @pytest.mark.asyncio
    async def test_delete_data_type_error_lines_786_789(self, operator):
        """Test TypeError handling in delete_data (lines 786-789)"""
        
        class BadInt:
            def __str__(self):
                return "123"
            
            def isdigit(self):
                return True
            
            def __int__(self):
                raise TypeError("Cannot convert")
        
        bad_obj = BadInt()
        
        with patch.object(operator, 'delete_sql', return_value=[]) as mock_delete:
            with patch.object(operator, 'delete_caches', new_callable=AsyncMock):
                result = await operator.delete_data({bad_obj, 1, 2})
        
        # BadInt should be skipped due to TypeError (line 786-789)
        called_ids = mock_delete.call_args[0][1]
        assert bad_obj not in called_ids
        assert 1 in called_ids
        assert 2 in called_ids
    
    @pytest.mark.asyncio
    async def test_delete_data_empty_ids_line_792(self, operator):
        """Test empty validated_ids (line 792)"""
        
        class AlwaysBadObject:
            def __str__(self):
                raise ValueError("Bad string")
        
        with patch.object(operator, 'delete_sql') as mock_delete:
            result = await operator.delete_data({AlwaysBadObject()})
        
        mock_delete.assert_not_called()
        assert result == []  # Line 792: return []
    
    @pytest.mark.asyncio
    async def test_delete_filter_redis_error_lines_851_859(self, operator):
        """Test RedisError in delete_filter_data (lines 851-859)"""
        
        with patch.object(operator, 'delete_filter', return_value=[1, 2, 3]):
            with patch.object(operator, 'delete_caches', new_callable=AsyncMock):
                # Mock delete_null_key to raise RedisError
                with patch.object(operator, 'delete_null_key', side_effect=redis.RedisError("Redis down")):
                    result = await operator.delete_filter_data({"status": "inactive"})
                    # Should handle error gracefully (lines 851-859)
                    assert result == [1, 2, 3]
    
    @pytest.mark.asyncio
    async def test_count_data_exception_line_905(self, operator):
        """Test exception re-raise in count_data (line 905)"""
        exc = GeneralOperateException(
            code=ErrorCode.DB_QUERY_ERROR,
            message="Count failed",
            context=ErrorContext(operation="count")
        )
        
        with patch.object(operator, 'count_sql', side_effect=exc):
            with pytest.raises(GeneralOperateException) as exc_info:
                await operator.count_data()
            assert exc_info.value is exc  # Line 905: raise
    
    @pytest.mark.asyncio
    async def test_upsert_validation_error_lines_1001_1003(self, operator):
        """Test upsert validation error (lines 1001-1003)"""
        
        def bad_schema(**kwargs):
            raise ValueError("Schema validation failed")
        
        operator.create_schemas = bad_schema
        operator.update_schemas = bad_schema
        
        data = [{"id": 1, "name": "test"}]
        
        with patch.object(operator, 'delete_caches', new_callable=AsyncMock):
            with patch.object(operator, 'upsert_sql', return_value=[]):
                result = await operator.upsert_data(data, ["id"])
                # Lines 1001-1003: validation fails, logged and skipped
                assert result == []
    
    @pytest.mark.asyncio
    async def test_upsert_no_valid_data_lines_1006_1007(self, operator):
        """Test no valid data after validation (lines 1006-1007)"""
        
        def always_fail(**kwargs):
            raise Exception("Always fails")
        
        operator.create_schemas = always_fail
        operator.update_schemas = always_fail
        
        result = await operator.upsert_data([{"bad": "data"}], ["id"])
        assert result == []  # Lines 1006-1007: return []
    
    @pytest.mark.asyncio
    async def test_upsert_cache_error_lines_1019_1022(self, operator):
        """Test cache cleanup error (lines 1019-1022)"""
        
        with patch.object(operator, 'delete_caches', side_effect=Exception("Cache error")):
            with patch.object(operator, 'upsert_sql', return_value=[{"id": 1}]):
                with patch.object(operator.logger, 'warning') as mock_warning:
                    result = await operator.upsert_data([{"id": 1}], ["id"])
                    # Lines 1019-1022: cache error logged
                    mock_warning.assert_called()
    
    @pytest.mark.asyncio
    async def test_upsert_sql_error_lines_1033_1035(self, operator):
        """Test SQL error in upsert (lines 1033-1035)"""
        
        with patch.object(operator, 'delete_caches', new_callable=AsyncMock):
            with patch.object(operator, 'upsert_sql', side_effect=Exception("SQL failed")):
                # Lines 1033-1035: SQL error handling
                with pytest.raises(Exception):
                    await operator.upsert_data([{"id": 1}], ["id"])
    
    @pytest.mark.asyncio
    async def test_upsert_schema_error_lines_1044_1045(self, operator):
        """Test schema conversion error (lines 1044-1045)"""
        
        def bad_main_schema(**kwargs):
            raise ValueError("Schema conversion failed")
        
        operator.main_schemas = bad_main_schema
        
        with patch.object(operator, 'delete_caches', new_callable=AsyncMock):
            with patch.object(operator, 'upsert_sql', return_value=[{"id": 1}]):
                with patch.object(operator.logger, 'error') as mock_error:
                    result = await operator.upsert_data([{"id": 1}], ["id"])
                    # Lines 1044-1045: schema error logged
                    mock_error.assert_called()
    
    @pytest.mark.asyncio
    async def test_upsert_cache_cleanup_error_lines_1051_1052(self, operator):
        """Test post-upsert cache cleanup error (lines 1051-1052)"""
        
        with patch.object(operator, 'delete_caches', side_effect=[None, Exception("Post cleanup error")]):
            with patch.object(operator, 'upsert_sql', return_value=[{"id": 1}]):
                # Should not raise exception, just continue (lines 1051-1052)
                result = await operator.upsert_data([{"id": 1}], ["id"])
                assert len(result) == 1
    
    @pytest.mark.asyncio
    async def test_exists_check_cache_error_lines_1086_1087(self, operator):
        """Test cache error in exists_check (lines 1086-1087)"""
        
        operator.redis.exists = AsyncMock(side_effect=redis.RedisError("Cache error"))
        
        with patch.object(operator, 'exists_sql', return_value={1: True}):
            with patch.object(operator.logger, 'warning') as mock_warning:
                result = await operator.exists_check(1)
                # Lines 1086-1087: cache error logged
                mock_warning.assert_called()
                assert result is True
    
    @pytest.mark.asyncio
    async def test_exists_check_null_marker_error_lines_1104_1105(self, operator):
        """Test null marker error (lines 1104-1105)"""
        
        with patch.object(operator, 'exists_sql', return_value={1: False}):
            with patch.object(operator, 'set_null_key', side_effect=Exception("Null key error")):
                # Should not raise exception (lines 1104-1105)
                result = await operator.exists_check(1)
                assert result is False
    
    @pytest.mark.asyncio
    async def test_exists_check_db_error_lines_1110_1112(self, operator):
        """Test database error (lines 1110-1112)"""
        
        with patch.object(operator, 'exists_sql', side_effect=Exception("DB error")):
            # Lines 1110-1112: exception handling
            with pytest.raises(Exception):
                await operator.exists_check(1)
    
    @pytest.mark.asyncio
    async def test_batch_exists_null_marker_error_line_1197(self, operator):
        """Test null marker error in batch_exists (line 1197)"""
        
        with patch.object(operator, 'exists_sql', return_value={1: False, 2: False}):
            with patch.object(operator, 'set_null_key', side_effect=redis.RedisError("Redis error")):
                with patch.object(operator.logger, 'debug') as mock_debug:
                    result = await operator.batch_exists({1, 2})
                    # Line 1197: debug log for null marker failure
                    mock_debug.assert_called()
    
    @pytest.mark.asyncio
    async def test_refresh_cache_pipeline_error_lines_1242_1244(self, operator):
        """Test pipeline error in refresh_cache (lines 1242-1244)"""
        
        mock_pipe = AsyncMock()
        mock_pipe.execute = AsyncMock(side_effect=redis.RedisError("Pipeline failed"))
        operator.redis.pipeline = MagicMock(return_value=mock_pipe)
        
        with patch.object(operator, 'delete_caches', new_callable=AsyncMock):
            with patch.object(operator, 'read_sql', return_value=[]):
                with patch.object(operator.logger, 'debug') as mock_debug:
                    result = await operator.refresh_cache({1, 2})
                    # Lines 1242-1244: pipeline failure logged
                    mock_debug.assert_called()
    
    @pytest.mark.asyncio
    async def test_refresh_cache_no_redis_lines_1277_1281(self, operator):
        """Test refresh_cache without Redis (lines 1277-1281)"""
        
        operator.redis = None
        
        with patch.object(operator, 'read_sql', return_value=[{"id": 1}]):
            with patch.object(operator, 'store_caches', new_callable=AsyncMock):
                result = await operator.refresh_cache({1, 2, 3})
                # Lines 1277-1281: handle missing IDs without Redis
                assert result["refreshed"] == 1
                assert result["not_found"] == 2
    
    @pytest.mark.asyncio
    async def test_refresh_cache_all_missing_error_lines_1292_1296(self, operator):
        """Test all missing IDs pipeline error (lines 1292-1296)"""
        
        with patch.object(operator, 'read_sql', return_value=[]):  # All missing
            mock_pipe = AsyncMock()
            mock_pipe.execute = AsyncMock(side_effect=redis.RedisError("Pipeline failed"))
            operator.redis.pipeline = MagicMock(return_value=mock_pipe)
            
            with patch.object(operator.logger, 'debug') as mock_debug:
                result = await operator.refresh_cache({1, 2, 3})
                # Lines 1292-1296: pipeline failure handled
                assert result["not_found"] == 3
    
    @pytest.mark.asyncio
    async def test_distinct_values_cache_read_error_lines_1347_1348(self, operator):
        """Test cache read error in get_distinct_values (lines 1347-1348)"""
        
        operator.redis.get = AsyncMock(side_effect=redis.RedisError("Cache read failed"))
        
        with patch('general_operate.app.sql_operate.SQLOperate.get_distinct_values', return_value=["val1", "val2"]):
            with patch.object(operator.logger, 'warning') as mock_warning:
                result = await operator.get_distinct_values("field")
                # Lines 1347-1348: cache read error logged
                mock_warning.assert_called()
    
    @pytest.mark.asyncio
    async def test_distinct_values_cache_write_error_lines_1368_1369(self, operator):
        """Test cache write error (lines 1368-1369)"""
        
        operator.redis.setex = AsyncMock(side_effect=redis.RedisError("Cache write failed"))
        
        with patch('general_operate.app.sql_operate.SQLOperate.get_distinct_values', return_value=["val1"]):
            with patch.object(operator.logger, 'warning') as mock_warning:
                result = await operator.get_distinct_values("field", cache_ttl=300)
                # Lines 1368-1369: cache write error logged
                mock_warning.assert_called()
    
    @pytest.mark.asyncio
    async def test_distinct_values_db_error_lines_1374_1376(self, operator):
        """Test database error (lines 1374-1376)"""
        
        with patch('general_operate.app.sql_operate.SQLOperate.get_distinct_values', side_effect=Exception("DB error")):
            # Lines 1374-1376: database error handling
            with pytest.raises(Exception):
                await operator.get_distinct_values("field")


# SQL Operate tests for specific missing lines
class TestSQLOperateSpecificLines:
    """Test SQLOperate specific missing lines"""
    
    def test_json_decode_error_lines_44_50(self):
        """Test JSON decode error (lines 44-50)"""
        client = MockSQLClient()
        sql_op = SQLOperate(client)
        
        @sql_op.exception_handler
        async def test_func(self):
            raise json.JSONDecodeError("Invalid JSON", "test", 0)
        
        with pytest.raises(DatabaseException) as exc_info:
            # This will trigger the exception handler (lines 44-50)
            asyncio.run(test_func(sql_op))
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
    
    def test_postgresql_error_lines_52_61(self):
        """Test PostgreSQL error handling (lines 52-61)"""
        client = MockSQLClient()
        sql_op = SQLOperate(client)
        
        pg_error = MagicMock(spec=AsyncAdapt_asyncpg_dbapi.Error)
        pg_error.sqlstate = "23505"
        pg_error.__str__ = lambda: "duplicate key error: detailed message"
        
        db_error = DBAPIError("statement", {}, pg_error)
        
        @sql_op.exception_handler
        async def test_func(self):
            raise db_error
        
        with pytest.raises(DatabaseException) as exc_info:
            asyncio.run(test_func(sql_op))
        
        # Lines 52-61: PostgreSQL error processing
        assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR
        assert "detailed message" in str(exc_info.value)
    
    def test_mysql_error_lines_62_69(self):
        """Test MySQL error handling (lines 62-69)"""
        client = MockSQLClient("mysql")
        sql_op = SQLOperate(client)
        
        mysql_error = pymysql.Error(1062, "Duplicate entry")
        db_error = DBAPIError("statement", {}, mysql_error)
        
        @sql_op.exception_handler
        async def test_func(self):
            raise db_error
        
        with pytest.raises(DatabaseException) as exc_info:
            asyncio.run(test_func(sql_op))
        
        # Lines 62-69: MySQL error processing
        assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR
    
    def test_generic_db_error_lines_70_78(self):
        """Test generic database error (lines 70-78)"""
        client = MockSQLClient()
        sql_op = SQLOperate(client)
        
        generic_error = Exception("Generic error")
        db_error = DBAPIError("statement", {}, generic_error)
        
        @sql_op.exception_handler
        async def test_func(self):
            raise db_error
        
        with pytest.raises(DatabaseException) as exc_info:
            asyncio.run(test_func(sql_op))
        
        # Lines 70-78: generic database error handling
        assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR
    
    def test_sync_wrapper_lines_114_126(self):
        """Test sync wrapper (lines 114-126)"""
        client = MockSQLClient()
        sql_op = SQLOperate(client)
        
        @sql_op.exception_handler
        def sync_func(self):
            raise ValueError("Sync error")
        
        with pytest.raises(DatabaseException):
            # Lines 114-126: sync wrapper handling
            sync_func(sql_op)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--cov=general_operate.general_operate", "--cov=general_operate.app.sql_operate", "--cov-report=term-missing"])