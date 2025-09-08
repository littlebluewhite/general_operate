"""
Comprehensive test file to achieve 100% coverage for sql_operate.py and general_operate.py.
Specifically targets missing lines identified in coverage analysis.
"""

import pytest
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch, Mock
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

import redis
from redis.exceptions import RedisError, ConnectionError as RedisConnectionError, TimeoutError as RedisTimeoutError, ResponseError as RedisResponseError
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm.exc import UnmappedInstanceError
import asyncpg
import pymysql

from general_operate.app.sql_operate import SQLOperate
from general_operate.general_operate import GeneralOperate
from general_operate.app.client.database import SQLClient
from general_operate.core.exceptions import DatabaseException, CacheException, ErrorCode, ErrorContext


class MockModule:
    """Mock module for testing."""
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
    """Test implementation of GeneralOperate."""
    
    def get_module(self):
        """Return mock module."""
        return MockModule()


@pytest.fixture
def mock_sql_client():
    """Mock SQLClient for testing."""
    mock_client = MagicMock(spec=SQLClient)
    mock_client.engine_type = "postgresql"
    return mock_client


@pytest.fixture 
def sql_operate(mock_sql_client):
    """SQLOperate instance with mocked client."""
    return SQLOperate(mock_sql_client)


@pytest.fixture
def mock_redis():
    """Mock Redis client."""
    mock = AsyncMock()
    mock_pipe = AsyncMock()
    
    class MockPipeline:
        def __init__(self, pipe_mock):
            self.pipe_mock = pipe_mock
            
        async def __aenter__(self):
            return self.pipe_mock
            
        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return None
            
    mock.pipeline = MagicMock(return_value=MockPipeline(mock_pipe))
    return mock


@pytest.fixture
def general_operate(mock_redis):
    """GeneralOperate instance with mocked dependencies."""
    # Mock database client
    db_client = MagicMock(spec=SQLClient)
    db_client.engine_type = "postgresql"
    db_client.get_engine = MagicMock()
    
    # Create instance of concrete GeneralOperate implementation
    go = TestGeneralOperator(
        database_client=db_client,
        redis_client=mock_redis
    )
    
    # Set up mocks
    go.sql_operate = MagicMock()
    go.cache_operate = MagicMock()
    
    return go


class TestSQLOperateExceptionHandling:
    """Test exception handling in SQLOperate."""
    
    @pytest.mark.asyncio
    async def test_json_decode_error_in_exception_handler(self, sql_operate):
        """Test JSONDecodeError handling in exception handler - Line 45."""
        # Create a decorated function to test the exception handler
        @sql_operate.exception_handler
        async def test_func(self):
            raise json.JSONDecodeError("Invalid JSON", "doc", 0)
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func(sql_operate)
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "JSON decode error" in str(exc_info.value)
        assert exc_info.value.context.details["error_type"] == "json_decode"
    
    @pytest.mark.asyncio
    async def test_sql_validation_errors_in_exception_handler(self, sql_operate):
        """Test SQL validation errors in exception handler - Lines 86-99."""
        # Test ValueError with SQL content
        @sql_operate.exception_handler
        async def test_func_value_error(self):
            raise ValueError("SQL syntax error in query")
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func_value_error(sql_operate)
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "SQL operation validation error" in str(exc_info.value)
        assert exc_info.value.context.details["error_type"] == "sql_validation"
        
        # Test TypeError with SQL content
        @sql_operate.exception_handler
        async def test_func_type_error(self):
            raise TypeError("SQL parameter type mismatch")
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func_type_error(sql_operate)
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        
        # Test AttributeError with SQL content
        @sql_operate.exception_handler
        async def test_func_attr_error(self):
            raise AttributeError("SQL connection attribute missing")
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func_attr_error(sql_operate)
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
    
    @pytest.mark.asyncio
    async def test_already_database_exception_passthrough(self, sql_operate):
        """Test that existing DatabaseException is re-raised - Lines 94-96."""
        original_exception = DatabaseException(
            code=ErrorCode.DB_CONNECTION_ERROR,
            message="Original error",
            context=ErrorContext(operation="test")
        )
        
        @sql_operate.exception_handler
        async def test_func(self):
            raise original_exception
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func(sql_operate)
        
        assert exc_info.value is original_exception
    
    @pytest.mark.asyncio
    async def test_unexpected_exception_handling(self, sql_operate):
        """Test unexpected exception handling - Lines 97-104."""
        @sql_operate.exception_handler
        async def test_func(self):
            raise RuntimeError("Unexpected runtime error")
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func(sql_operate)
        
        assert exc_info.value.code == ErrorCode.UNKNOWN_ERROR
        assert "Operation error" in str(exc_info.value)
        assert exc_info.value.context.details["error_type"] == "unexpected"


class TestSQLOperateDataValidation:
    """Test data validation methods in SQLOperate."""
    
    @pytest.mark.asyncio
    async def test_validate_data_dict_json_serialization(self, sql_operate):
        """Test JSON serialization in _validate_data_dict - Lines 252-253."""
        # Test with list that needs JSON serialization
        data = {"items": [1, 2, 3], "metadata": {"key": "value"}}
        
        result = sql_operate._validate_data_dict(data, "test_operation")
        
        assert "items" in result
        assert "metadata" in result
        assert isinstance(result["items"], str)  # Should be JSON serialized
        assert isinstance(result["metadata"], str)  # Should be JSON serialized
        
        # Verify the JSON content
        assert json.loads(result["items"]) == [1, 2, 3]
        assert json.loads(result["metadata"]) == {"key": "value"}
    
    @pytest.mark.asyncio  
    async def test_validate_data_dict_unhashable_fallback(self, sql_operate):
        """Test unhashable value handling - Line 262."""
        # Create a custom unhashable object that fails membership test
        class UnhashableType:
            def __init__(self, value):
                self.value = value
            
            def __hash__(self):
                raise TypeError("unhashable type")
            
            def __eq__(self, other):
                return isinstance(other, UnhashableType) and self.value == other.value
        
        unhashable_value = UnhashableType("test")
        data = {"unhashable_key": unhashable_value}
        
        # This should trigger the TypeError exception block and fall through to line 262
        result = sql_operate._validate_data_dict(data, "test_operation")
        
        # Should include the unhashable value in the result
        assert "unhashable_key" in result
        assert result["unhashable_key"] == unhashable_value
    
    @pytest.mark.asyncio
    async def test_validate_data_dict_empty_data_validation(self, sql_operate):
        """Test empty data validation - Line 265."""
        # Test with allow_empty=False (default)
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_data_dict({}, "test_operation", allow_empty=False)
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "cannot be empty" in str(exc_info.value)
        
        # Test with allow_empty=True should not raise
        result = sql_operate._validate_data_dict({}, "test_operation", allow_empty=True)
        assert result == {}


class TestSQLOperatePostgreSQLMySQL:
    """Test PostgreSQL vs MySQL specific branches."""
    
    def test_postgresql_detection(self):
        """Test PostgreSQL database type detection."""
        mock_client = MagicMock()
        mock_client.engine_type = "postgresql"
        sql_op = SQLOperate(mock_client)
        assert sql_op._is_postgresql is True
    
    def test_mysql_detection(self):
        """Test MySQL database type detection."""
        mock_client = MagicMock()
        mock_client.engine_type = "mysql"
        sql_op = SQLOperate(mock_client)
        assert sql_op._is_postgresql is False
    
    def test_case_insensitive_detection(self):
        """Test case insensitive database type detection."""
        mock_client = MagicMock()
        mock_client.engine_type = "POSTGRESQL"
        sql_op = SQLOperate(mock_client)
        assert sql_op._is_postgresql is True


class TestGeneralOperateCacheLookupErrors:
    """Test cache lookup error handling in GeneralOperate - Lines 256-258, 262-267."""
    
    @pytest.mark.asyncio
    async def test_schema_validation_failure_in_cache_lookup(self, general_operate, mock_redis):
        """Test schema validation failure during cache lookup - Lines 256-258."""
        general_operate.redis = mock_redis
        
        # Mock the dependencies
        mock_redis.exists.return_value = False  # No null marker
        general_operate.get_caches = AsyncMock(return_value=[{"id": 1, "invalid_field": "bad_data"}])
        
        # Mock schema to raise exception
        general_operate.main_schemas.side_effect = Exception("Schema validation failed")
        
        results, cache_miss_ids, null_marked_ids, failed_cache_ops = await general_operate._process_cache_lookups({1})
        
        # Should add to cache_miss_ids due to schema failure
        assert 1 in cache_miss_ids
        assert len(results) == 0
    
    @pytest.mark.asyncio
    async def test_redis_error_during_cache_lookup(self, general_operate, mock_redis):
        """Test Redis error during cache lookup - Lines 262-267."""
        general_operate.redis = mock_redis
        
        # Mock Redis to raise error on exists check
        mock_redis.exists.side_effect = RedisConnectionError("Connection failed")
        
        results, cache_miss_ids, null_marked_ids, failed_cache_ops = await general_operate._process_cache_lookups({1})
        
        # Should handle error gracefully
        assert 1 in cache_miss_ids
        assert 1 in failed_cache_ops
        assert len(results) == 0
    
    @pytest.mark.asyncio
    async def test_generic_exception_during_cache_lookup(self, general_operate, mock_redis):
        """Test generic exception during cache lookup - Lines 262-267."""
        general_operate.redis = mock_redis
        
        # Mock unexpected exception on exists check
        mock_redis.exists.side_effect = Exception("Unexpected error")
        
        results, cache_miss_ids, null_marked_ids, failed_cache_ops = await general_operate._process_cache_lookups({1})
        
        # Should handle error gracefully
        assert 1 in cache_miss_ids
        assert 1 in failed_cache_ops
        assert len(results) == 0


class TestGeneralOperateFetchFromSQLErrors:
    """Test _fetch_from_sql error cases - Lines 426-447."""
    
    @pytest.mark.asyncio
    async def test_fetch_from_sql_empty_id_set(self, general_operate):
        """Test _fetch_from_sql with empty ID set."""
        result = await general_operate._fetch_from_sql(set())
        assert result == []
    
    @pytest.mark.asyncio
    async def test_fetch_from_sql_single_id_error(self, general_operate):
        """Test _fetch_from_sql single ID error case - Lines 445-447."""
        general_operate.read_one = AsyncMock(side_effect=Exception("Database error"))
        
        with pytest.raises(DatabaseException) as exc_info:
            await general_operate._fetch_from_sql({1})
        
        assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR
        assert "Database read operation failed" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_fetch_from_sql_multiple_ids_error(self, general_operate):
        """Test _fetch_from_sql multiple IDs error case - Lines 445-447."""
        general_operate.read_sql = AsyncMock(side_effect=Exception("Bulk read error"))
        
        with pytest.raises(DatabaseException) as exc_info:
            await general_operate._fetch_from_sql({1, 2, 3})
        
        assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR
        assert "Database read operation failed" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_fetch_from_sql_database_exception_passthrough(self, general_operate):
        """Test that DatabaseException is passed through."""
        db_exc = DatabaseException(
            code=ErrorCode.DB_CONNECTION_ERROR,
            message="Connection failed",
            context=ErrorContext(operation="test")
        )
        general_operate.read_one = AsyncMock(side_effect=db_exc)
        
        with pytest.raises(DatabaseException) as exc_info:
            await general_operate._fetch_from_sql({1})
        
        assert exc_info.value is db_exc


class TestGeneralOperateDeleteDataErrors:
    """Test delete_data error paths - Lines 786-819."""
    
    @pytest.mark.asyncio
    async def test_delete_data_invalid_id_handling(self, general_operate):
        """Test invalid ID handling in delete_data - Lines 786-789."""
        # Mock delete_sql to return empty list (no valid IDs)
        general_operate.delete_sql = AsyncMock(return_value=[])
        
        # Pass invalid IDs that can't be converted
        result = await general_operate.delete_data([None, "invalid", object()])
        
        assert result == []
        general_operate.delete_sql.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_data_cache_delete_error(self, general_operate, mock_redis):
        """Test cache delete error handling - Lines 806-809."""
        general_operate.redis = mock_redis
        general_operate.delete_sql = AsyncMock(return_value=[1, 2])
        
        # Mock CacheOperate.delete_caches to raise RedisError
        with patch('general_operate.general_operate.CacheOperate.delete_caches') as mock_delete_caches:
            mock_delete_caches.side_effect = RedisConnectionError("Cache delete failed")
            
            result = await general_operate.delete_data([1, 2])
            
            # Should still return successfully deleted IDs despite cache error
            assert result == [1, 2]
    
    @pytest.mark.asyncio
    async def test_delete_data_null_marker_delete_error(self, general_operate):
        """Test null marker delete error handling - Lines 816-819."""
        general_operate.delete_sql = AsyncMock(return_value=[1])
        general_operate.delete_null_key = AsyncMock(side_effect=RedisError("Null key delete failed"))
        
        # Mock CacheOperate.delete_caches to succeed
        with patch('general_operate.general_operate.CacheOperate.delete_caches'):
            result = await general_operate.delete_data([1])
            
            # Should still succeed despite null marker delete failure
            assert result == [1]


class TestGeneralOperateDeleteFilterDataErrors:
    """Test delete_filter_data error handling - Lines 851-873."""
    
    @pytest.mark.asyncio
    async def test_delete_filter_data_null_marker_error(self, general_operate):
        """Test null marker delete error in delete_filter_data - Lines 851-854."""
        # Mock successful filter delete but failing null marker delete
        general_operate.delete_filter_sql = AsyncMock(return_value=[1, 2])
        general_operate.delete_null_key = AsyncMock(side_effect=RedisError("Null key failed"))
        
        with patch('general_operate.general_operate.CacheOperate.delete_caches'):
            result = await general_operate.delete_filter_data({"status": "inactive"})
            
            # Should succeed despite null marker errors
            assert result == [1, 2]
    
    @pytest.mark.asyncio
    async def test_delete_filter_data_cache_cleanup_error(self, general_operate):
        """Test cache cleanup error in delete_filter_data - Lines 856-859."""
        general_operate.delete_filter_sql = AsyncMock(return_value=[1, 2])
        general_operate.delete_null_key = AsyncMock()
        
        # Mock cache delete to fail
        with patch('general_operate.general_operate.CacheOperate.delete_caches') as mock_delete:
            mock_delete.side_effect = RedisError("Cache cleanup failed")
            
            result = await general_operate.delete_filter_data({"status": "inactive"})
            
            # Should succeed despite cache cleanup failure
            assert result == [1, 2]
    
    @pytest.mark.asyncio
    async def test_delete_filter_data_database_error(self, general_operate):
        """Test database error handling - Lines 863-865."""
        db_error = DBAPIError("Database error", None, None)
        general_operate.delete_filter_sql = AsyncMock(side_effect=db_error)
        
        with pytest.raises(DBAPIError):
            await general_operate.delete_filter_data({"status": "inactive"})
    
    @pytest.mark.asyncio
    async def test_delete_filter_data_validation_error(self, general_operate):
        """Test validation error handling - Lines 866-869."""
        general_operate.delete_filter_sql = AsyncMock(side_effect=ValueError("Invalid filter"))
        
        result = await general_operate.delete_filter_data({"status": "invalid"})
        
        # Should return empty list for validation errors
        assert result == []
    
    @pytest.mark.asyncio
    async def test_delete_filter_data_generic_error(self, general_operate):
        """Test generic error handling - Lines 870-873."""
        general_operate.delete_filter_sql = AsyncMock(side_effect=RuntimeError("Unexpected error"))
        
        result = await general_operate.delete_filter_data({"status": "test"})
        
        # Should return empty list for generic errors
        assert result == []


class TestGeneralOperateRefreshCacheErrors:
    """Test refresh_cache error cases - Lines 1269-1299."""
    
    @pytest.mark.asyncio
    async def test_refresh_cache_null_marker_pipeline_error(self, general_operate, mock_redis):
        """Test null marker pipeline error in refresh_cache - Lines 1276-1278."""
        general_operate.redis = mock_redis
        
        # Mock pipeline to raise error during execution
        mock_pipe = AsyncMock()
        mock_pipe.execute.side_effect = RedisConnectionError("Pipeline failed")
        
        class MockPipeline:
            async def __aenter__(self):
                return mock_pipe
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                return None
        
        mock_redis.pipeline.return_value = MockPipeline()
        
        # Mock to have missing IDs
        with patch.object(general_operate, '_fetch_from_sql') as mock_fetch:
            mock_fetch.return_value = []  # No results found
            
            result = await general_operate.refresh_cache([1, 2, 3])
            
            # Should handle pipeline error gracefully
            assert result["not_found"] == 3
            assert result["refreshed"] == 0
            assert result["errors"] == 0
    
    @pytest.mark.asyncio
    async def test_refresh_cache_all_ids_not_found_pipeline_error(self, general_operate, mock_redis):
        """Test pipeline error when all IDs not found - Lines 1291-1293."""
        general_operate.redis = mock_redis
        
        # Mock pipeline to fail
        mock_pipe = AsyncMock()
        mock_pipe.execute.side_effect = ConnectionError("Pipeline connection failed")
        
        class MockPipeline:
            async def __aenter__(self):
                return mock_pipe
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                return None
        
        mock_redis.pipeline.return_value = MockPipeline()
        
        # Mock _fetch_from_sql to return empty (all not found)
        with patch.object(general_operate, '_fetch_from_sql') as mock_fetch:
            mock_fetch.return_value = []
            
            result = await general_operate.refresh_cache([1, 2])
            
            # Should handle pipeline error gracefully
            assert result["not_found"] == 2
            assert result["refreshed"] == 0
            assert result["errors"] == 0
    
    @pytest.mark.asyncio
    async def test_refresh_cache_generic_exception(self, general_operate):
        """Test generic exception handling in refresh_cache - Lines 1297-1299."""
        # Mock _fetch_from_sql to raise unexpected exception
        with patch.object(general_operate, '_fetch_from_sql') as mock_fetch:
            mock_fetch.side_effect = Exception("Unexpected database error")
            
            result = await general_operate.refresh_cache([1, 2])
            
            # Should handle error and set errors count
            assert result["errors"] == 2
            assert result["refreshed"] == 0
            assert result["not_found"] == 0


class TestEdgeCasesAndTransactions:
    """Test various edge cases and transaction scenarios."""
    
    @pytest.mark.asyncio
    async def test_unmapped_instance_error_handling(self, sql_operate):
        """Test UnmappedInstanceError handling in exception handler."""
        @sql_operate.exception_handler
        async def test_func(self):
            raise UnmappedInstanceError("Instance not mapped")
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func(sql_operate)
        
        assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR
        assert "one or more of ids is not exist" in str(exc_info.value)
    
    def test_valid_identifier_pattern_validation(self, sql_operate):
        """Test SQL injection protection with identifier validation."""
        # Valid identifiers should pass
        sql_operate._validate_identifier("valid_column", "column")
        sql_operate._validate_identifier("table123", "table")
        sql_operate._validate_identifier("_private", "field")
        
        # Invalid identifiers should raise exceptions
        with pytest.raises(DatabaseException):
            sql_operate._validate_identifier("'; DROP TABLE users; --", "column")
        
        with pytest.raises(DatabaseException):
            sql_operate._validate_identifier("123invalid", "column")
    
    @pytest.mark.asyncio
    async def test_null_set_handling(self, sql_operate):
        """Test null_set values are properly handled."""
        data = {
            "normal_value": "test",
            "null_marker": -999999,
            "null_string": "null",
            "actual_none": None
        }
        
        result = sql_operate._validate_data_dict(data, "test_op")
        
        # Should exclude null_set values and None
        assert "normal_value" in result
        assert "null_marker" not in result
        assert "null_string" not in result  
        assert "actual_none" not in result


class TestDatabaseConnectivityErrors:
    """Test database connectivity and transaction errors."""
    
    @pytest.mark.asyncio
    async def test_connection_pool_exhaustion(self, sql_operate):
        """Test handling of connection pool exhaustion."""
        @sql_operate.exception_handler
        async def test_func(self):
            # Simulate asyncpg pool exhaustion
            raise asyncpg.exceptions.TooManyConnectionsError("Connection pool exhausted")
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func(sql_operate)
        
        assert exc_info.value.code == ErrorCode.UNKNOWN_ERROR
    
    @pytest.mark.asyncio  
    async def test_mysql_connection_error(self, sql_operate):
        """Test MySQL specific connection errors."""
        @sql_operate.exception_handler
        async def test_func(self):
            raise pymysql.err.OperationalError(2003, "Can't connect to MySQL server")
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func(sql_operate)
        
        assert exc_info.value.code == ErrorCode.UNKNOWN_ERROR


if __name__ == "__main__":
    pytest.main([__file__, "-v"])