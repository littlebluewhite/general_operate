"""
Improved coverage tests for general_operate.py
Target the remaining uncovered lines to boost overall coverage from 61% to 80%+
Focus on exception handling, edge cases, and foreign key operations
"""

import asyncio
import json
import os
import sys
import re
from unittest.mock import AsyncMock, MagicMock, patch, Mock
from typing import Any, Dict, List

import pytest
import redis
import pymysql
from pydantic import BaseModel, ValidationError
from sqlalchemy.exc import DBAPIError, SQLAlchemyError
from sqlalchemy.orm.exc import UnmappedInstanceError
from sqlalchemy.dialects.postgresql.asyncpg import AsyncAdapt_asyncpg_dbapi

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from general_operate.general_operate import GeneralOperate, GeneralOperateException
from general_operate.app.cache_operate import CacheOperate
from general_operate.app.sql_operate import SQLOperate


# Test module for comprehensive coverage
class CoverageTestModule:
    def __init__(self, table_name: str, topic_name: str = None):
        self.table_name = table_name
        self.topic_name = topic_name
        
        class MainSchema(BaseModel):
            id: int
            name: str
            foreign_id: int | None = None
            value: Any = None
            status: str = "active"
            
            model_config = {"from_attributes": True}
        
        class CreateSchema(BaseModel):
            name: str
            foreign_id: int | None = None
            value: Any = None
            status: str = "active"
        
        class UpdateSchema(BaseModel):
            id: int | None = None
            name: str | None = None
            foreign_id: int | None = None
            value: Any = None
            status: str | None = None
        
        self.sql_model = None
        self.main_schemas = MainSchema
        self.create_schemas = CreateSchema
        self.update_schemas = UpdateSchema


class CoverageGeneralOperate(GeneralOperate):
    def __init__(self, module, database_client, redis_client=None, influxdb=None, kafka_config=None, exc=GeneralOperateException):
        self.module = module
        super().__init__(database_client, redis_client, influxdb, kafka_config, exc)
    
    def get_module(self):
        return self.module


@pytest.fixture
def coverage_mock_database_client():
    client = AsyncMock()
    client.engine_type = "postgresql"
    client.db_name = "coverage_test_db"
    client.get_engine.return_value = MagicMock()
    return client


@pytest.fixture
def coverage_mock_redis_client():
    client = AsyncMock(spec=redis.asyncio.Redis)
    # Set up all async methods
    for method in ['setex', 'get', 'delete', 'hdel', 'hset', 'hmget', 'hexists', 
                   'hgetall', 'hlen', 'exists', 'ttl', 'expire', 'ping', 'keys']:
        setattr(client, method, AsyncMock())
    client.ping.return_value = True
    return client


@pytest.fixture
def coverage_general_operate(coverage_mock_database_client, coverage_mock_redis_client):
    module = CoverageTestModule("coverage_test_table", "coverage_topic")
    return CoverageGeneralOperate(
        module=module,
        database_client=coverage_mock_database_client,
        redis_client=coverage_mock_redis_client
    )


class TestExceptionHandlerBranches:
    """Test all exception handler decorator branches for maximum coverage"""
    
    @pytest.mark.asyncio
    async def test_redis_connection_error(self, coverage_general_operate):
        """Test redis connection error handling"""
        with patch.object(SQLOperate, "health_check", new_callable=AsyncMock) as mock_health:
            mock_health.side_effect = redis.ConnectionError("Connection failed")
            
            with pytest.raises(GeneralOperateException) as exc_info:
                await coverage_general_operate.health_check()
            
            assert exc_info.value.status_code == 487
            assert exc_info.value.message_code == 3001
            assert "Redis connection error" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_redis_timeout_error(self, coverage_general_operate):
        """Test redis timeout error handling"""
        with patch.object(SQLOperate, "health_check", new_callable=AsyncMock) as mock_health:
            mock_health.side_effect = redis.TimeoutError("Timeout occurred")
            
            with pytest.raises(GeneralOperateException) as exc_info:
                await coverage_general_operate.health_check()
            
            assert exc_info.value.status_code == 487
            assert exc_info.value.message_code == 3002
            assert "Redis timeout error" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_redis_response_error(self, coverage_general_operate):
        """Test redis response error handling"""
        with patch.object(SQLOperate, "health_check", new_callable=AsyncMock) as mock_health:
            mock_health.side_effect = redis.ResponseError("Invalid response")
            
            with pytest.raises(GeneralOperateException) as exc_info:
                await coverage_general_operate.health_check()
            
            assert exc_info.value.status_code == 487
            assert exc_info.value.message_code == 3003
            assert "Redis response error" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_redis_error_with_code_pattern(self, coverage_general_operate):
        """Test redis error with error code pattern"""
        with patch.object(SQLOperate, "health_check", new_callable=AsyncMock) as mock_health:
            mock_health.side_effect = redis.RedisError("Error 1234 occurred")
            
            with pytest.raises(GeneralOperateException) as exc_info:
                await coverage_general_operate.health_check()
            
            assert exc_info.value.status_code == 492
            assert exc_info.value.message_code == 1234
            assert "Redis error" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_redis_error_without_code_pattern(self, coverage_general_operate):
        """Test redis error without error code pattern"""
        with patch.object(SQLOperate, "health_check", new_callable=AsyncMock) as mock_health:
            mock_health.side_effect = redis.RedisError("Generic redis error")
            
            with pytest.raises(GeneralOperateException) as exc_info:
                await coverage_general_operate.health_check()
            
            assert exc_info.value.status_code == 487
            assert exc_info.value.message_code == 3004
            assert "Generic redis error" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_json_decode_error(self, coverage_general_operate):
        """Test JSON decode error handling"""
        with patch.object(SQLOperate, "health_check", new_callable=AsyncMock) as mock_health:
            mock_health.side_effect = json.JSONDecodeError("Invalid JSON", "doc", 0)
            
            with pytest.raises(GeneralOperateException) as exc_info:
                await coverage_general_operate.health_check()
            
            assert exc_info.value.status_code == 491
            assert exc_info.value.message_code == 3005
            assert "JSON decode error" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_dbapi_error_postgresql(self, coverage_general_operate):
        """Test DBAPIError with PostgreSQL error"""
        # Create a proper AsyncAdapt_asyncpg_dbapi.Error mock
        pg_error = Mock(spec=AsyncAdapt_asyncpg_dbapi.Error)
        pg_error.sqlstate = "23505"
        pg_error.__str__ = Mock(return_value="prefix: duplicate key value: detail message")
        
        db_error = DBAPIError("statement", {}, pg_error)
        db_error.orig = pg_error
        
        with patch.object(SQLOperate, "health_check", new_callable=AsyncMock) as mock_health:
            mock_health.side_effect = db_error
            
            with pytest.raises(GeneralOperateException) as exc_info:
                await coverage_general_operate.health_check()
            
            assert exc_info.value.status_code == 489
            assert exc_info.value.message_code == "23505"

    @pytest.mark.asyncio
    async def test_dbapi_error_mysql(self, coverage_general_operate):
        """Test DBAPIError with MySQL error"""
        mysql_error = pymysql.Error(1062, "Duplicate entry")
        db_error = DBAPIError("statement", {}, mysql_error)
        db_error.orig = mysql_error
        
        with patch.object(SQLOperate, "health_check", new_callable=AsyncMock) as mock_health:
            mock_health.side_effect = db_error
            
            with pytest.raises(GeneralOperateException) as exc_info:
                await coverage_general_operate.health_check()
            
            assert exc_info.value.status_code == 486
            assert exc_info.value.message_code == 1062
            assert "Duplicate entry" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_dbapi_error_generic(self, coverage_general_operate):
        """Test DBAPIError with generic database error"""
        generic_error = Exception("Generic database error")
        db_error = DBAPIError("statement", {}, generic_error)
        db_error.orig = generic_error
        
        with patch.object(SQLOperate, "health_check", new_callable=AsyncMock) as mock_health:
            mock_health.side_effect = db_error
            
            with pytest.raises(GeneralOperateException) as exc_info:
                await coverage_general_operate.health_check()
            
            assert exc_info.value.status_code == 487
            assert exc_info.value.message_code == "UNKNOWN"

    @pytest.mark.asyncio
    async def test_unmapped_instance_error(self, coverage_general_operate):
        """Test UnmappedInstanceError handling"""
        with patch.object(SQLOperate, "health_check", new_callable=AsyncMock) as mock_health:
            mock_health.side_effect = UnmappedInstanceError("Instance not mapped")
            
            with pytest.raises(GeneralOperateException) as exc_info:
                await coverage_general_operate.health_check()
            
            assert exc_info.value.status_code == 486
            assert exc_info.value.message_code == 2
            assert "one or more of ids is not exist" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_sql_validation_error(self, coverage_general_operate):
        """Test SQL validation error handling"""
        with patch.object(SQLOperate, "health_check", new_callable=AsyncMock) as mock_health:
            mock_health.side_effect = ValueError("SQL validation failed")
            
            with pytest.raises(GeneralOperateException) as exc_info:
                await coverage_general_operate.health_check()
            
            assert exc_info.value.status_code == 400
            assert exc_info.value.message_code == 299
            assert "SQL operation validation error" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_general_operate_exception_passthrough(self, coverage_general_operate):
        """Test that GeneralOperateException is passed through unchanged"""
        original_exception = GeneralOperateException(
            status_code=404, message_code=1001, message="Original error"
        )
        
        with patch.object(SQLOperate, "health_check", new_callable=AsyncMock) as mock_health:
            mock_health.side_effect = original_exception
            
            with pytest.raises(GeneralOperateException) as exc_info:
                await coverage_general_operate.health_check()
            
            assert exc_info.value is original_exception

    def test_sync_wrapper_exception_handling(self, coverage_general_operate):
        """Test sync wrapper exception handling"""
        # Create a sync method that raises an exception
        @GeneralOperate.exception_handler
        def sync_method(self):
            raise ValueError("Sync method error")
        
        with pytest.raises(GeneralOperateException) as exc_info:
            sync_method(coverage_general_operate)
        
        assert exc_info.value.status_code == 500
        assert exc_info.value.message_code == 9999


class TestTransactionContextManager:
    """Test transaction context manager functionality"""
    
    @pytest.mark.asyncio
    async def test_transaction_success(self, coverage_general_operate):
        """Test successful transaction context manager"""
        mock_session = AsyncMock()
        mock_session.close = AsyncMock()
        
        # Mock begin() to return an async context manager
        from contextlib import asynccontextmanager
        
        @asynccontextmanager
        async def mock_begin():
            yield
        
        mock_session.begin = mock_begin
        
        with patch.object(coverage_general_operate, 'create_external_session', return_value=mock_session):
            async with coverage_general_operate.transaction() as session:
                assert session == mock_session
                
            mock_session.close.assert_called_once()

    @pytest.mark.asyncio 
    async def test_transaction_exception(self, coverage_general_operate):
        """Test transaction context manager with exception"""
        mock_session = AsyncMock()
        mock_session.close = AsyncMock()
        
        # Mock begin() to return an async context manager
        from contextlib import asynccontextmanager
        
        @asynccontextmanager
        async def mock_begin():
            yield
        
        mock_session.begin = mock_begin
        
        with patch.object(coverage_general_operate, 'create_external_session', return_value=mock_session):
            with pytest.raises(Exception, match="Transaction failed"):
                async with coverage_general_operate.transaction() as session:
                    assert session == mock_session
                    raise Exception("Transaction failed")
                    
            mock_session.close.assert_called_once()


class TestForeignKeyOperations:
    """Test foreign key related operations for better coverage"""
    
    @pytest.mark.asyncio
    async def test_create_by_foreign_key_success(self, coverage_general_operate):
        """Test successful create_by_foreign_key operation"""
        foreign_key_field = "tutorial_id"
        foreign_key_value = 123
        data = [
            {"name": "Item 1", "value": "test1"},
            {"name": "Item 2", "value": "test2"}
        ]
        
        expected_data = [
            {"name": "Item 1", "value": "test1", "tutorial_id": 123},
            {"name": "Item 2", "value": "test2", "tutorial_id": 123}
        ]
        
        with patch.object(coverage_general_operate, 'create_data', new_callable=AsyncMock) as mock_create:
            mock_create.return_value = [
                coverage_general_operate.main_schemas(id=1, name="Item 1", value="test1", foreign_id=123),
                coverage_general_operate.main_schemas(id=2, name="Item 2", value="test2", foreign_id=123)
            ]
            
            # Mock the build_create_data function
            with patch('general_operate.utils.build_data.build_create_data', side_effect=lambda x: x):
                result = await coverage_general_operate.create_by_foreign_key(
                    foreign_key_field, foreign_key_value, data
                )
            
            assert len(result) == 2
            mock_create.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_by_foreign_key_empty_data(self, coverage_general_operate):
        """Test create_by_foreign_key with empty data"""
        result = await coverage_general_operate.create_by_foreign_key("tutorial_id", 123, [])
        assert result == []

    @pytest.mark.asyncio
    async def test_update_by_foreign_key_success(self, coverage_general_operate):
        """Test successful update_by_foreign_key operation"""
        foreign_key_field = "tutorial_id"
        foreign_key_value = 123
        data = [
            {"id": 1, "name": "Updated Item 1"},
            {"id": 0, "name": "New Item"},  # Create
            {"id": -2, "name": "Delete Item"}  # Delete
        ]
        
        # Mock existing data
        existing_data = [
            coverage_general_operate.main_schemas(id=1, name="Item 1", foreign_id=123),
            coverage_general_operate.main_schemas(id=2, name="Item 2", foreign_id=123)
        ]
        
        with patch.object(coverage_general_operate, 'read_data_by_filter', new_callable=AsyncMock) as mock_read, \
             patch.object(coverage_general_operate, 'delete_data', new_callable=AsyncMock) as mock_delete, \
             patch.object(coverage_general_operate, 'update_data', new_callable=AsyncMock) as mock_update, \
             patch.object(coverage_general_operate, 'create_data', new_callable=AsyncMock) as mock_create, \
             patch('general_operate.utils.build_data.compare_related_items') as mock_compare:
            
            mock_read.return_value = existing_data
            mock_compare.return_value = (
                [{"name": "New Item", "tutorial_id": 123}],  # to_create
                [{"id": 1, "name": "Updated Item 1"}],       # to_update  
                [2]                                          # to_delete_ids
            )
            
            await coverage_general_operate.update_by_foreign_key(
                foreign_key_field, foreign_key_value, data
            )
            
            mock_delete.assert_called_once_with(id_value={2}, session=None)
            mock_update.assert_called_once_with([{"id": 1, "name": "Updated Item 1"}], session=None)
            mock_create.assert_called_once_with([{"name": "New Item", "tutorial_id": 123}], session=None)

    @pytest.mark.asyncio
    async def test_update_by_foreign_key_empty_data(self, coverage_general_operate):
        """Test update_by_foreign_key with empty data"""
        result = await coverage_general_operate.update_by_foreign_key("tutorial_id", 123, [])
        assert result is None


class TestReadDataByFilterMethod:
    """Test read_data_by_filter method for better coverage"""
    
    @pytest.mark.asyncio
    async def test_read_data_by_filter_success(self, coverage_general_operate):
        """Test successful read_data_by_filter operation"""
        filters = {"status": "active", "name": "test"}
        sql_results = [
            {"id": 1, "name": "test", "status": "active", "foreign_id": None, "value": None},
            {"id": 2, "name": "test2", "status": "active", "foreign_id": None, "value": None}
        ]
        
        with patch.object(coverage_general_operate, 'read_sql', new_callable=AsyncMock) as mock_read_sql:
            mock_read_sql.return_value = sql_results
            
            result = await coverage_general_operate.read_data_by_filter(filters, limit=10, offset=0)
            
            assert len(result) == 2
            assert result[0].name == "test"
            assert result[1].name == "test2"
            
            mock_read_sql.assert_called_once_with(
                table_name=coverage_general_operate.table_name,
                filters=filters,
                limit=10,
                offset=0
            )

    @pytest.mark.asyncio
    async def test_read_data_by_filter_exception_passthrough(self, coverage_general_operate):
        """Test read_data_by_filter exception passthrough"""
        filters = {"status": "active"}
        
        with patch.object(coverage_general_operate, 'read_sql', new_callable=AsyncMock) as mock_read_sql:
            mock_read_sql.side_effect = GeneralOperateException(
                status_code=500, message_code=1001, message="SQL error"
            )
            
            with pytest.raises(GeneralOperateException) as exc_info:
                await coverage_general_operate.read_data_by_filter(filters)
            
            assert exc_info.value.status_code == 500
            assert exc_info.value.message_code == 1001

    @pytest.mark.asyncio
    async def test_read_data_by_filter_with_none_results(self, coverage_general_operate):
        """Test read_data_by_filter with None results from SQL"""
        filters = {"status": "nonexistent"}
        
        with patch.object(coverage_general_operate, 'read_sql', new_callable=AsyncMock) as mock_read_sql:
            mock_read_sql.return_value = [None, None]  # SQL returns None items
            
            result = await coverage_general_operate.read_data_by_filter(filters)
            
            assert len(result) == 0  # Should skip None items


class TestCreateDataEdgeCases:
    """Test create_data edge cases for better coverage"""
    
    @pytest.mark.asyncio
    async def test_create_data_with_invalid_dict_items(self, coverage_general_operate):
        """Test create_data with invalid dictionary items"""
        invalid_data = [
            "string_item",  # Not a dict
            123,            # Not a dict
            {"name": "valid", "status": "active"},  # Valid
            None            # Not a dict
        ]
        
        with patch.object(coverage_general_operate, 'create_sql', new_callable=AsyncMock) as mock_create_sql:
            mock_create_sql.return_value = [
                {"id": 1, "name": "valid", "status": "active", "foreign_id": None, "value": None}
            ]
            
            result = await coverage_general_operate.create_data(invalid_data)
            
            assert len(result) == 1  # Only valid dict should be processed
            assert result[0].name == "valid"

    @pytest.mark.asyncio 
    async def test_create_data_schema_validation_errors(self, coverage_general_operate):
        """Test create_data with various schema validation errors"""
        invalid_data = [
            {"invalid_field": "test"},  # TypeError - missing required field
            {"name": 123, "status": None},  # ValueError - wrong type
        ]
        
        result = await coverage_general_operate.create_data(invalid_data)
        assert len(result) == 0  # All items should be skipped due to validation errors

    @pytest.mark.asyncio
    async def test_create_data_attribute_error(self, coverage_general_operate):
        """Test create_data with AttributeError"""
        # Create mock that raises AttributeError on model_dump
        mock_schema = Mock()
        mock_schema.side_effect = AttributeError("'NoneType' object has no attribute 'model_dump'")
        
        with patch.object(coverage_general_operate, 'create_schemas', mock_schema):
            data = [{"name": "test"}]
            result = await coverage_general_operate.create_data(data)
            assert len(result) == 0


class TestDeleteFilterDataCompleteScenarios:
    """Test delete_filter_data complete scenarios for coverage"""
    
    @pytest.mark.asyncio
    async def test_delete_filter_data_cache_cleanup_success(self, coverage_general_operate):
        """Test delete_filter_data with successful cache cleanup"""
        filters = {"status": "inactive"}
        deleted_ids = [1, 2, 3]
        
        with patch.object(coverage_general_operate, 'delete_filter', new_callable=AsyncMock) as mock_delete_filter, \
             patch.object(coverage_general_operate, 'delete_caches', new_callable=AsyncMock) as mock_delete_caches, \
             patch.object(coverage_general_operate, 'delete_null_key', new_callable=AsyncMock) as mock_delete_null_key:
            
            mock_delete_filter.return_value = deleted_ids
            mock_delete_caches.return_value = 3
            mock_delete_null_key.return_value = True
            
            result = await coverage_general_operate.delete_filter_data(filters)
            
            assert result == deleted_ids
            mock_delete_caches.assert_called_once_with(coverage_general_operate.table_name, {"1", "2", "3"})
            assert mock_delete_null_key.call_count == 3

    @pytest.mark.asyncio
    async def test_delete_filter_data_cache_cleanup_redis_error(self, coverage_general_operate):
        """Test delete_filter_data with Redis error during cache cleanup"""
        filters = {"status": "inactive"}
        deleted_ids = [1, 2]
        
        with patch.object(coverage_general_operate, 'delete_filter', new_callable=AsyncMock) as mock_delete_filter, \
             patch.object(coverage_general_operate, 'delete_caches', new_callable=AsyncMock) as mock_delete_caches, \
             patch.object(coverage_general_operate, 'delete_null_key', new_callable=AsyncMock) as mock_delete_null_key:
            
            mock_delete_filter.return_value = deleted_ids
            mock_delete_caches.side_effect = redis.RedisError("Cache delete failed")
            mock_delete_null_key.side_effect = redis.RedisError("Null key delete failed")
            
            result = await coverage_general_operate.delete_filter_data(filters)
            
            # Should still return deleted IDs despite cache errors
            assert result == deleted_ids

    @pytest.mark.asyncio
    async def test_delete_filter_data_sql_exception(self, coverage_general_operate):
        """Test delete_filter_data with SQL exception"""
        filters = {"status": "inactive"}
        
        with patch.object(coverage_general_operate, 'delete_filter', new_callable=AsyncMock) as mock_delete_filter:
            mock_delete_filter.side_effect = DBAPIError("SQL error", {}, Exception("DB error"))
            
            with pytest.raises(GeneralOperateException) as exc_info:
                await coverage_general_operate.delete_filter_data(filters)
            
            assert exc_info.value.status_code == 487
            assert exc_info.value.message_code == "UNKNOWN"

    @pytest.mark.asyncio
    async def test_delete_filter_data_validation_errors(self, coverage_general_operate):
        """Test delete_filter_data with validation errors"""
        filters = {"invalid": "data"}
        
        with patch.object(coverage_general_operate, 'delete_filter', new_callable=AsyncMock) as mock_delete_filter:
            mock_delete_filter.side_effect = ValueError("Invalid filter data")
            
            result = await coverage_general_operate.delete_filter_data(filters)
            assert result == []

    @pytest.mark.asyncio
    async def test_delete_filter_data_generic_exception(self, coverage_general_operate):
        """Test delete_filter_data with generic exception"""
        filters = {"status": "inactive"}
        
        with patch.object(coverage_general_operate, 'delete_filter', new_callable=AsyncMock) as mock_delete_filter:
            mock_delete_filter.side_effect = Exception("Generic error")
            
            result = await coverage_general_operate.delete_filter_data(filters)
            assert result == []


class TestDeleteDataEdgeCases:
    """Test delete_data edge cases for better coverage"""
    
    @pytest.mark.asyncio
    async def test_delete_data_id_validation_errors(self, coverage_general_operate):
        """Test delete_data with ID validation errors"""
        invalid_ids = {"invalid_id", None, "", "abc"}
        
        with patch.object(coverage_general_operate, 'delete_sql', new_callable=AsyncMock) as mock_delete_sql:
            mock_delete_sql.return_value = []
            
            result = await coverage_general_operate.delete_data(invalid_ids)
            
            # Should return empty list as no valid IDs were found
            assert result == []

    @pytest.mark.asyncio
    async def test_delete_data_cache_redis_errors(self, coverage_general_operate):
        """Test delete_data with various Redis errors"""
        ids_to_delete = {1, 2}
        
        with patch.object(coverage_general_operate, 'delete_sql', new_callable=AsyncMock) as mock_delete_sql, \
             patch.object(CacheOperate, 'delete_caches', new_callable=AsyncMock) as mock_delete_caches, \
             patch.object(coverage_general_operate, 'delete_null_key', new_callable=AsyncMock) as mock_delete_null_key:
            
            mock_delete_sql.return_value = [1, 2]
            mock_delete_caches.side_effect = redis.RedisError("Cache delete failed")
            mock_delete_null_key.side_effect = AttributeError("Redis client not available")
            
            result = await coverage_general_operate.delete_data(ids_to_delete)
            
            # Should still return deleted IDs despite cache errors
            assert result == [1, 2]


class TestUpdateDataEdgeCases:
    """Test update_data edge cases for better coverage"""
    
    @pytest.mark.asyncio
    async def test_update_data_where_field_validation(self, coverage_general_operate):
        """Test update_data with missing where_field"""
        update_data = [
            {"name": "Updated"},  # Missing 'id' field
            {"id": 1, "name": "Valid Update"}
        ]
        
        with pytest.raises(GeneralOperateException) as exc_info:
            await coverage_general_operate.update_data(update_data)
        
        assert exc_info.value.status_code == 400
        assert "Missing 'id' field" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_update_data_no_valid_fields_after_validation(self, coverage_general_operate):
        """Test update_data with no valid fields after validation"""
        update_data = [
            {"id": 1}  # Only contains where_field, no update fields
        ]
        
        result = await coverage_general_operate.update_data(update_data)
        
        # When no valid update fields, should return empty list
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_update_data_schema_conversion_failure(self, coverage_general_operate):
        """Test update_data with schema conversion failure"""
        update_data = [{"id": 1, "name": "Test"}]
        
        with patch.object(coverage_general_operate, 'update_sql', new_callable=AsyncMock) as mock_update_sql, \
             patch.object(CacheOperate, 'delete_caches', new_callable=AsyncMock):
            
            # Return record that fails schema conversion
            mock_update_sql.return_value = [{"id": 1, "invalid_field": "bad_data"}]
            
            with pytest.raises(GeneralOperateException) as exc_info:
                await coverage_general_operate.update_data(update_data)
            
            assert exc_info.value.status_code == 400
            assert "Update incomplete" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_update_data_incomplete_updates(self, coverage_general_operate):
        """Test update_data with incomplete updates"""
        update_data = [
            {"id": 1, "name": "Test1"},
            {"id": 2, "name": "Test2"}
        ]
        
        with patch.object(coverage_general_operate, 'update_sql', new_callable=AsyncMock) as mock_update_sql, \
             patch.object(CacheOperate, 'delete_caches', new_callable=AsyncMock):
            
            # Only return one record instead of two
            mock_update_sql.return_value = [
                {"id": 1, "name": "Test1", "status": "active", "foreign_id": None, "value": None}
            ]
            
            with pytest.raises(GeneralOperateException) as exc_info:
                await coverage_general_operate.update_data(update_data)
            
            assert exc_info.value.status_code == 400
            assert "Update incomplete" in exc_info.value.message
            assert "1 of 2 records failed to update" in exc_info.value.message


class TestFetchFromSQLEdgeCases:
    """Test _fetch_from_sql edge cases"""
    
    @pytest.mark.asyncio
    async def test_fetch_from_sql_empty_set(self, coverage_general_operate):
        """Test _fetch_from_sql with empty set"""
        result = await coverage_general_operate._fetch_from_sql(set())
        assert result == []

    @pytest.mark.asyncio
    async def test_fetch_from_sql_single_id_none_result(self, coverage_general_operate):
        """Test _fetch_from_sql with single ID returning None"""
        with patch.object(coverage_general_operate, 'read_one', new_callable=AsyncMock) as mock_read_one:
            mock_read_one.return_value = None
            
            result = await coverage_general_operate._fetch_from_sql({999})
            assert result == []

    @pytest.mark.asyncio
    async def test_fetch_from_sql_exception_handling(self, coverage_general_operate):
        """Test _fetch_from_sql exception handling"""
        with patch.object(coverage_general_operate, 'read_one', new_callable=AsyncMock) as mock_read_one:
            mock_read_one.side_effect = Exception("Database connection failed")
            
            with pytest.raises(GeneralOperateException) as exc_info:
                await coverage_general_operate._fetch_from_sql({1})
            
            assert exc_info.value.status_code == 500
            assert exc_info.value.message_code == 9994
            assert "Database read operation failed" in exc_info.value.message


class TestKafkaRelatedMethods:
    """Test Kafka-related methods for better coverage"""
    
    @pytest.mark.asyncio
    async def test_kafka_lifespan_no_components(self, coverage_general_operate):
        """Test kafka_lifespan with no Kafka components"""
        async with coverage_general_operate.kafka_lifespan() as go:
            assert go == coverage_general_operate

    @pytest.mark.asyncio
    async def test_send_event_no_producer(self, coverage_general_operate):
        """Test send_event with no producer configured"""
        with pytest.raises(GeneralOperateException) as exc_info:
            await coverage_general_operate.send_event("test_event", {"data": "test"})
        
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 4200
        assert "Kafka producer not configured" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_start_consuming_no_consumer(self, coverage_general_operate):
        """Test start_consuming with no consumer configured"""
        async def dummy_handler(data):
            pass
            
        with pytest.raises(GeneralOperateException) as exc_info:
            await coverage_general_operate.start_consuming(dummy_handler)
        
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 4202
        assert "Kafka consumer not configured" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_publish_event_no_event_bus(self, coverage_general_operate):
        """Test publish_event with no event bus configured"""
        with pytest.raises(GeneralOperateException) as exc_info:
            await coverage_general_operate.publish_event("test_event", {"data": "test"})
        
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 4203
        assert "Kafka event bus not configured" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_subscribe_event_no_event_bus(self, coverage_general_operate):
        """Test subscribe_event with no event bus configured"""
        async def dummy_handler(data):
            pass
            
        with pytest.raises(GeneralOperateException) as exc_info:
            await coverage_general_operate.subscribe_event("test_event", dummy_handler)
        
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 4203
        assert "Kafka event bus not configured" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_kafka_health_check_no_components(self, coverage_general_operate):
        """Test kafka_health_check with no components"""
        result = await coverage_general_operate.kafka_health_check()
        
        expected = {
            "producer": None,
            "consumer": None,
            "event_bus": None
        }
        assert result == expected