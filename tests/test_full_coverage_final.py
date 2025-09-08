"""
Comprehensive test suite for achieving 100% coverage of:
- general_operate/general_operate.py
- general_operate/app/cache_operate.py  
- general_operate/app/sql_operate.py
"""

import json
from unittest.mock import Mock, AsyncMock, patch, MagicMock, PropertyMock
from datetime import datetime, UTC
from typing import Any

import pytest
import redis
from redis import RedisError, ConnectionError as RedisConnectionError, TimeoutError as RedisTimeoutError, ResponseError
from sqlalchemy.exc import DBAPIError
from sqlalchemy.dialects.postgresql.asyncpg import AsyncAdapt_asyncpg_dbapi
from sqlalchemy.orm.exc import UnmappedInstanceError
import asyncpg
import pymysql

from general_operate import GeneralOperateException, ErrorCode, ErrorContext
from general_operate.general_operate import GeneralOperate
from general_operate.app.cache_operate import CacheOperate, CacheException
from general_operate.app.sql_operate import SQLOperate, DatabaseException
from general_operate.app.client.database import SQLClient


# ============================================================================
# Test Fixtures
# ============================================================================

class TestModule:
    """Mock module for testing GeneralOperate"""
    table_name = "test_table"
    main_schemas = Mock()
    create_schemas = Mock()
    update_schemas = Mock()


class TestGeneralOperateImpl(GeneralOperate):
    """Concrete implementation of GeneralOperate for testing"""
    
    def get_module(self):
        return TestModule()


@pytest.fixture
def mock_redis():
    """Create a mock Redis client"""
    mock = AsyncMock(spec=redis.asyncio.Redis)
    
    # Create a proper async context manager for pipeline
    pipeline_mock = AsyncMock()
    pipeline_mock.get = Mock()
    pipeline_mock.setex = Mock()
    pipeline_mock.execute = AsyncMock(return_value=[])
    pipeline_mock.__aenter__ = AsyncMock(return_value=pipeline_mock)
    pipeline_mock.__aexit__ = AsyncMock(return_value=None)
    
    mock.pipeline = Mock(return_value=pipeline_mock)
    mock.exists = AsyncMock(return_value=0)
    mock.get = AsyncMock(return_value=None)
    mock.setex = AsyncMock(return_value=True)
    mock.delete = AsyncMock(return_value=1)
    mock.keys = AsyncMock(return_value=[])
    mock.ping = AsyncMock(return_value=True)
    mock.ttl = AsyncMock(return_value=100)
    mock.expire = AsyncMock(return_value=True)
    return mock


@pytest.fixture
def mock_sql_client():
    """Create a mock SQL client"""
    mock = Mock(spec=SQLClient)
    mock.engine_type = "postgresql"
    mock.get_engine = Mock()
    return mock


@pytest.fixture
def cache_operate(mock_redis):
    """Create CacheOperate instance with mock Redis"""
    return CacheOperate(mock_redis)


@pytest.fixture
def sql_operate(mock_sql_client):
    """Create SQLOperate instance with mock SQL client"""
    return SQLOperate(mock_sql_client)


@pytest.fixture
def general_operate(mock_sql_client, mock_redis):
    """Create GeneralOperate instance with mocks"""
    return TestGeneralOperateImpl(mock_sql_client, mock_redis)


# ============================================================================
# CacheOperate Tests
# ============================================================================

class TestCacheOperate:
    """Tests for CacheOperate class"""
    
    @pytest.mark.asyncio
    async def test_get_caches_success(self, cache_operate, mock_redis):
        """Test successful cache retrieval"""
        mock_data = {"id": 1, "name": "test"}
        mock_redis.pipeline.return_value.execute = AsyncMock(
            return_value=[json.dumps(mock_data)]
        )
        
        result = await cache_operate.get_caches("prefix", {"key1"})
        
        assert result == [mock_data]
    
    @pytest.mark.asyncio
    async def test_get_caches_invalid_json(self, cache_operate, mock_redis):
        """Test cache retrieval with invalid JSON"""
        mock_redis.pipeline.return_value.execute = AsyncMock(
            return_value=["invalid json"]
        )
        
        result = await cache_operate.get_caches("prefix", {"key1"})
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_get_cache_success(self, cache_operate, mock_redis):
        """Test single cache retrieval"""
        mock_data = {"id": 1, "name": "test"}
        mock_redis.get.return_value = json.dumps(mock_data)
        
        result = await cache_operate.get_cache("prefix", "key1")
        
        assert result == mock_data
    
    @pytest.mark.asyncio
    async def test_get_cache_not_found(self, cache_operate, mock_redis):
        """Test cache retrieval when key doesn't exist"""
        mock_redis.get.return_value = None
        
        result = await cache_operate.get_cache("prefix", "key1")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_cache_json_decode_error(self, cache_operate, mock_redis):
        """Test cache retrieval with JSON decode error"""
        mock_redis.get.return_value = "invalid json"
        
        result = await cache_operate.get_cache("prefix", "key1")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_store_caches_success(self, cache_operate, mock_redis):
        """Test batch cache storage"""
        data = {"key1": {"id": 1}, "key2": {"id": 2}}
        mock_redis.pipeline.return_value.execute = AsyncMock()
        
        result = await cache_operate.store_caches("prefix", data, 3600)
        
        assert result is True
    
    @pytest.mark.asyncio
    async def test_store_caches_empty_data(self, cache_operate):
        """Test batch cache storage with empty data"""
        result = await cache_operate.store_caches("prefix", {})
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_store_cache_success(self, cache_operate, mock_redis):
        """Test single cache storage"""
        data = {"id": 1, "name": "test"}
        mock_redis.setex = AsyncMock(return_value=True)
        
        await cache_operate.store_cache("prefix", "key1", data, 3600)
        
        mock_redis.setex.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_caches_success(self, cache_operate, mock_redis):
        """Test batch cache deletion"""
        mock_redis.delete.return_value = 2
        
        result = await cache_operate.delete_caches("prefix", {"key1", "key2"})
        
        assert result == 2
    
    @pytest.mark.asyncio
    async def test_delete_caches_empty_set(self, cache_operate, mock_redis):
        """Test batch cache deletion with empty set"""
        result = await cache_operate.delete_caches("prefix", set())
        
        assert result == 0
    
    @pytest.mark.asyncio
    async def test_delete_cache_success(self, cache_operate, mock_redis):
        """Test single cache deletion"""
        mock_redis.delete.return_value = 1
        
        result = await cache_operate.delete_cache("prefix", "key1")
        
        assert result is True
    
    @pytest.mark.asyncio
    async def test_delete_cache_not_found(self, cache_operate, mock_redis):
        """Test cache deletion when key doesn't exist"""
        mock_redis.delete.return_value = 0
        
        result = await cache_operate.delete_cache("prefix", "key1")
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_cache_exists_true(self, cache_operate, mock_redis):
        """Test cache existence check - key exists"""
        mock_redis.exists.return_value = 1
        
        result = await cache_operate.cache_exists("prefix", "key1")
        
        assert result is True
    
    @pytest.mark.asyncio
    async def test_cache_exists_false(self, cache_operate, mock_redis):
        """Test cache existence check - key doesn't exist"""
        mock_redis.exists.return_value = 0
        
        result = await cache_operate.cache_exists("prefix", "key1")
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_cache_extend_ttl_success(self, cache_operate, mock_redis):
        """Test extending cache TTL"""
        mock_redis.ttl.return_value = 100
        mock_redis.expire.return_value = True
        
        result = await cache_operate.cache_extend_ttl("prefix", "key1", 60)
        
        assert result is True
        mock_redis.expire.assert_called_once_with("prefix:key1", 160)
    
    @pytest.mark.asyncio
    async def test_cache_extend_ttl_key_not_exists(self, cache_operate, mock_redis):
        """Test extending TTL for non-existent key"""
        mock_redis.ttl.return_value = -1
        
        result = await cache_operate.cache_extend_ttl("prefix", "key1", 60)
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_set_null_key(self, cache_operate, mock_redis):
        """Test setting null marker key"""
        mock_redis.setex.return_value = True
        
        result = await cache_operate.set_null_key("null_key", 300)
        
        assert result is True
    
    @pytest.mark.asyncio
    async def test_delete_null_key(self, cache_operate, mock_redis):
        """Test deleting null marker key"""
        mock_redis.delete.return_value = 1
        
        result = await cache_operate.delete_null_key("null_key")
        
        assert result is True
    
    @pytest.mark.asyncio
    async def test_health_check_success(self, cache_operate, mock_redis):
        """Test Redis health check - healthy"""
        mock_redis.ping.return_value = True
        
        result = await cache_operate.health_check()
        
        assert result is True
    
    @pytest.mark.asyncio
    async def test_health_check_connection_error(self, cache_operate, mock_redis):
        """Test Redis health check - connection error"""
        mock_redis.ping.side_effect = RedisConnectionError("Connection failed")
        
        result = await cache_operate.health_check()
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_health_check_timeout_error(self, cache_operate, mock_redis):
        """Test Redis health check - timeout error"""
        mock_redis.ping.side_effect = RedisTimeoutError("Timeout")
        
        result = await cache_operate.health_check()
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_health_check_response_error(self, cache_operate, mock_redis):
        """Test Redis health check - response error"""
        mock_redis.ping.side_effect = ResponseError("Response error")
        
        result = await cache_operate.health_check()
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_health_check_generic_redis_error(self, cache_operate, mock_redis):
        """Test Redis health check - generic Redis error"""
        mock_redis.ping.side_effect = RedisError("Error 500: Generic error")
        
        result = await cache_operate.health_check()
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_health_check_unexpected_error(self, cache_operate, mock_redis):
        """Test Redis health check - unexpected error"""
        mock_redis.ping.side_effect = Exception("Unexpected")
        
        result = await cache_operate.health_check()
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_exception_handler_redis_connection_error(self, cache_operate):
        """Test exception handler with Redis connection error"""
        @CacheOperate.exception_handler
        async def test_func(self):
            raise redis.ConnectionError("Connection failed")
        
        with pytest.raises(CacheException) as exc_info:
            await test_func(cache_operate)
        
        assert exc_info.value.code == ErrorCode.CACHE_CONNECTION_ERROR
    
    @pytest.mark.asyncio
    async def test_exception_handler_redis_timeout_error(self, cache_operate):
        """Test exception handler with Redis timeout error"""
        @CacheOperate.exception_handler
        async def test_func(self):
            raise redis.TimeoutError("Timeout")
        
        with pytest.raises(CacheException) as exc_info:
            await test_func(cache_operate)
        
        assert exc_info.value.code == ErrorCode.CACHE_CONNECTION_ERROR
    
    @pytest.mark.asyncio
    async def test_exception_handler_redis_response_error(self, cache_operate):
        """Test exception handler with Redis response error"""
        @CacheOperate.exception_handler
        async def test_func(self):
            raise redis.ResponseError("Response error")
        
        with pytest.raises(CacheException) as exc_info:
            await test_func(cache_operate)
        
        assert exc_info.value.code == ErrorCode.CACHE_KEY_ERROR
    
    @pytest.mark.asyncio
    async def test_exception_handler_json_decode_error(self, cache_operate):
        """Test exception handler with JSON decode error"""
        @CacheOperate.exception_handler
        async def test_func(self):
            raise json.JSONDecodeError("msg", "doc", 0)
        
        with pytest.raises(CacheException) as exc_info:
            await test_func(cache_operate)
        
        assert exc_info.value.code == ErrorCode.CACHE_SERIALIZATION_ERROR
    
    @pytest.mark.asyncio
    async def test_exception_handler_sql_validation_error(self, cache_operate):
        """Test exception handler with SQL validation error"""
        @CacheOperate.exception_handler
        async def test_func(self):
            raise ValueError("SQL validation failed")
        
        with pytest.raises(CacheException) as exc_info:
            await test_func(cache_operate)
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
    
    @pytest.mark.asyncio
    async def test_exception_handler_reraise_cache_exception(self, cache_operate):
        """Test exception handler re-raising CacheException"""
        original_exc = CacheException(
            code=ErrorCode.CACHE_KEY_ERROR,
            message="Test error",
            context=ErrorContext(operation="test")
        )
        
        @CacheOperate.exception_handler
        async def test_func(self):
            raise original_exc
        
        with pytest.raises(CacheException) as exc_info:
            await test_func(cache_operate)
        
        assert exc_info.value is original_exc
    
    @pytest.mark.asyncio
    async def test_exception_handler_unknown_error(self, cache_operate):
        """Test exception handler with unknown error"""
        @CacheOperate.exception_handler
        async def test_func(self):
            raise RuntimeError("Unknown error")
        
        with pytest.raises(CacheException) as exc_info:
            await test_func(cache_operate)
        
        assert exc_info.value.code == ErrorCode.UNKNOWN_ERROR
    
    def test_exception_handler_sync_function(self, cache_operate):
        """Test exception handler with synchronous function"""
        @CacheOperate.exception_handler
        def test_func(self):
            raise RuntimeError("Sync error")
        
        with pytest.raises(CacheException) as exc_info:
            test_func(cache_operate)
        
        assert exc_info.value.code == ErrorCode.UNKNOWN_ERROR


# ============================================================================
# SQLOperate Tests
# ============================================================================

class TestSQLOperate:
    """Tests for SQLOperate class"""
    
    def create_mock_session(self):
        """Helper to create a properly configured mock session"""
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        mock_session.begin = AsyncMock()
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()
        mock_session.close = AsyncMock()
        return mock_session
    
    @pytest.mark.asyncio
    async def test_create_sql_postgresql_success(self, sql_operate, mock_sql_client):
        """Test PostgreSQL create with RETURNING clause"""
        mock_sql_client.engine_type = "postgresql"
        sql_operate._is_postgresql = True
        
        mock_session = self.create_mock_session()
        mock_result = Mock()
        mock_result.fetchall.return_value = [Mock(_mapping={"id": 1, "name": "test"})]
        mock_session.execute.return_value = mock_result
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_session):
            result = await sql_operate.create_sql("test_table", {"name": "test"})
        
        assert result == [{"id": 1, "name": "test"}]
        mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_sql_mysql_success(self, sql_operate, mock_sql_client):
        """Test MySQL create without RETURNING clause"""
        mock_sql_client.engine_type = "mysql"
        sql_operate._is_postgresql = False
        
        mock_session = self.create_mock_session()
        mock_insert_result = Mock()
        mock_insert_result.lastrowid = 1
        mock_insert_result.rowcount = 1
        
        mock_select_result = Mock()
        mock_select_result.fetchall.return_value = [Mock(_mapping={"id": 1, "name": "test"})]
        
        mock_session.execute.side_effect = [mock_insert_result, mock_select_result]
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_session):
            result = await sql_operate.create_sql("test_table", {"name": "test"})
        
        assert result == [{"id": 1, "name": "test"}]
        mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_sql_with_session(self, sql_operate):
        """Test create with external session"""
        mock_session = AsyncMock()
        mock_result = Mock()
        mock_result.fetchall.return_value = [Mock(_mapping={"id": 1})]
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.create_sql("test_table", {"name": "test"}, session=mock_session)
        
        assert result == [{"id": 1}]
        mock_session.commit.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_create_sql_rollback_on_error(self, sql_operate):
        """Test rollback on create error"""
        mock_session = self.create_mock_session()
        mock_session.execute.side_effect = Exception("DB error")
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_session):
            with pytest.raises(Exception):
                await sql_operate.create_sql("test_table", {"name": "test"})
        
        mock_session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_read_sql_with_filters(self, sql_operate):
        """Test read with filters"""
        mock_session = self.create_mock_session()
        mock_result = Mock()
        mock_result.fetchall.return_value = [Mock(_mapping={"id": 1, "name": "test"})]
        mock_session.execute.return_value = mock_result
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_session):
            result = await sql_operate.read_sql(
                "test_table",
                filters={"status": "active"},
                order_by="created_at",
                order_direction="DESC",
                limit=10,
                offset=0
            )
        
        assert result == [{"id": 1, "name": "test"}]
    
    @pytest.mark.asyncio
    async def test_read_sql_with_list_filters_postgresql(self, sql_operate):
        """Test read with list filters on PostgreSQL"""
        sql_operate._is_postgresql = True
        mock_session = self.create_mock_session()
        mock_result = Mock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_session):
            result = await sql_operate.read_sql(
                "test_table",
                filters={"id": [1, 2, 3]}
            )
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_read_sql_with_list_filters_mysql(self, sql_operate):
        """Test read with list filters on MySQL"""
        sql_operate._is_postgresql = False
        mock_session = self.create_mock_session()
        mock_result = Mock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_session):
            result = await sql_operate.read_sql(
                "test_table",
                filters={"id": [1, 2, 3]}
            )
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_count_sql_success(self, sql_operate):
        """Test count operation"""
        mock_session = self.create_mock_session()
        mock_result = Mock()
        mock_result.fetchone.return_value = (42,)
        mock_session.execute.return_value = mock_result
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_session):
            result = await sql_operate.count_sql("test_table", filters={"active": True})
        
        assert result == 42
    
    @pytest.mark.asyncio
    async def test_read_one_success(self, sql_operate):
        """Test read single record"""
        mock_session = self.create_mock_session()
        mock_result = Mock()
        mock_result.fetchone.return_value = Mock(_mapping={"id": 1, "name": "test"})
        mock_session.execute.return_value = mock_result
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_session):
            result = await sql_operate.read_one("test_table", 1)
        
        assert result == {"id": 1, "name": "test"}
    
    @pytest.mark.asyncio
    async def test_read_one_not_found(self, sql_operate):
        """Test read single record not found"""
        mock_session = self.create_mock_session()
        mock_result = Mock()
        mock_result.fetchone.return_value = None
        mock_session.execute.return_value = mock_result
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_session):
            result = await sql_operate.read_one("test_table", 999)
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_update_sql_postgresql(self, sql_operate):
        """Test update on PostgreSQL with RETURNING"""
        sql_operate._is_postgresql = True
        mock_session = self.create_mock_session()
        mock_result = Mock()
        mock_result.fetchone.return_value = Mock(_mapping={"id": 1, "name": "updated"})
        mock_session.execute.return_value = mock_result
        
        update_data = [{"id": 1, "data": {"name": "updated"}}]
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_session):
            result = await sql_operate.update_sql("test_table", update_data)
        
        assert result == [{"id": 1, "name": "updated"}]
        mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_sql_mysql(self, sql_operate):
        """Test update on MySQL without RETURNING"""
        sql_operate._is_postgresql = False
        mock_session = self.create_mock_session()
        
        # First call: UPDATE
        mock_update_result = Mock()
        mock_update_result.rowcount = 1
        
        # Second call: SELECT
        mock_select_result = Mock()
        mock_select_result.fetchone.return_value = Mock(_mapping={"id": 1, "name": "updated"})
        
        mock_session.execute.side_effect = [mock_update_result, mock_select_result]
        
        update_data = [{"id": 1, "data": {"name": "updated"}}]
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_session):
            result = await sql_operate.update_sql("test_table", update_data)
        
        assert result == [{"id": 1, "name": "updated"}]
    
    @pytest.mark.asyncio
    async def test_delete_sql_single_id(self, sql_operate):
        """Test delete single record"""
        mock_session = self.create_mock_session()
        mock_result = Mock()
        mock_result.rowcount = 1
        mock_session.execute.return_value = mock_result
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_session):
            result = await sql_operate.delete_sql("test_table", 1)
        
        assert result == [1]
        mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_sql_multiple_ids_postgresql(self, sql_operate):
        """Test delete multiple records on PostgreSQL"""
        sql_operate._is_postgresql = True
        mock_session = self.create_mock_session()
        mock_result = Mock()
        mock_result.rowcount = 3
        mock_session.execute.return_value = mock_result
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_session):
            result = await sql_operate.delete_sql("test_table", [1, 2, 3])
        
        assert result == [1, 2, 3]
    
    @pytest.mark.asyncio
    async def test_delete_sql_multiple_ids_mysql(self, sql_operate):
        """Test delete multiple records on MySQL"""
        sql_operate._is_postgresql = False
        mock_session = self.create_mock_session()
        mock_result = Mock()
        mock_result.rowcount = 3
        mock_session.execute.return_value = mock_result
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_session):
            result = await sql_operate.delete_sql("test_table", [1, 2, 3])
        
        assert result == [1, 2, 3]
    
    @pytest.mark.asyncio
    async def test_delete_filter_success(self, sql_operate):
        """Test delete with filters"""
        mock_session = self.create_mock_session()
        
        # First call: SELECT to get IDs
        mock_select_result = Mock()
        mock_select_result.fetchall.return_value = [(1,), (2,), (3,)]
        
        # Second call: DELETE
        mock_delete_result = Mock()
        
        mock_session.execute.side_effect = [mock_select_result, mock_delete_result]
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_session):
            result = await sql_operate.delete_filter("test_table", {"status": "inactive"})
        
        assert result == [1, 2, 3]
    
    @pytest.mark.asyncio
    async def test_upsert_sql_postgresql(self, sql_operate):
        """Test upsert on PostgreSQL"""
        sql_operate._is_postgresql = True
        mock_session = self.create_mock_session()
        mock_result = Mock()
        mock_result.fetchall.return_value = [Mock(_mapping={"id": 1, "name": "upserted"})]
        mock_session.execute.return_value = mock_result
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_session):
            result = await sql_operate.upsert_sql(
                "test_table",
                [{"id": 1, "name": "upserted"}],
                conflict_fields=["id"]
            )
        
        assert result == [{"id": 1, "name": "upserted"}]
    
    @pytest.mark.asyncio
    async def test_upsert_sql_mysql(self, sql_operate):
        """Test upsert on MySQL"""
        sql_operate._is_postgresql = False
        mock_session = self.create_mock_session()
        
        # First call: INSERT ... ON DUPLICATE KEY UPDATE
        mock_insert_result = Mock()
        
        # Second call: SELECT to fetch results
        mock_select_result = Mock()
        mock_select_result.fetchall.return_value = [Mock(_mapping={"id": 1, "name": "upserted"})]
        
        mock_session.execute.side_effect = [mock_insert_result, mock_select_result]
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_session):
            result = await sql_operate.upsert_sql(
                "test_table",
                [{"id": 1, "name": "upserted"}],
                conflict_fields=["id"]
            )
        
        assert result == [{"id": 1, "name": "upserted"}]
    
    @pytest.mark.asyncio
    async def test_exists_sql_single_id(self, sql_operate):
        """Test existence check for single ID"""
        mock_session = self.create_mock_session()
        mock_result = Mock()
        mock_result.fetchone.return_value = (1,)
        mock_session.execute.return_value = mock_result
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_session):
            result = await sql_operate.exists_sql("test_table", [1])
        
        assert result == {1: True}
    
    @pytest.mark.asyncio
    async def test_exists_sql_multiple_ids_postgresql(self, sql_operate):
        """Test existence check for multiple IDs on PostgreSQL"""
        sql_operate._is_postgresql = True
        mock_session = self.create_mock_session()
        mock_result = Mock()
        mock_result.fetchall.return_value = [(1,), (3,)]
        mock_session.execute.return_value = mock_result
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_session):
            result = await sql_operate.exists_sql("test_table", [1, 2, 3])
        
        assert result == {1: True, 2: False, 3: True}
    
    @pytest.mark.asyncio
    async def test_get_distinct_values(self, sql_operate):
        """Test getting distinct values"""
        mock_session = self.create_mock_session()
        mock_result = Mock()
        mock_result.fetchall.return_value = [("value1",), ("value2",), (None,)]
        mock_session.execute.return_value = mock_result
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_session):
            result = await sql_operate.get_distinct_values(
                "test_table",
                "status",
                filters={"active": True},
                limit=10
            )
        
        assert result == ["value1", "value2"]
    
    @pytest.mark.asyncio
    async def test_execute_query_select(self, sql_operate):
        """Test execute raw SELECT query"""
        mock_session = self.create_mock_session()
        mock_result = Mock()
        mock_result.fetchall.return_value = [Mock(_mapping={"id": 1})]
        mock_session.execute.return_value = mock_result
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_session):
            result = await sql_operate.execute_query("SELECT * FROM test_table")
        
        assert result == [{"id": 1}]
    
    @pytest.mark.asyncio
    async def test_execute_query_update(self, sql_operate):
        """Test execute raw UPDATE query"""
        mock_session = self.create_mock_session()
        mock_result = Mock()
        mock_result.rowcount = 5
        mock_session.execute.return_value = mock_result
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_session):
            result = await sql_operate.execute_query("UPDATE test_table SET status = 'active'")
        
        assert result == {"affected_rows": 5}
        mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_health_check_success(self, sql_operate):
        """Test database health check - healthy"""
        mock_session = self.create_mock_session()
        mock_result = Mock()
        mock_result.fetchone.return_value = (1,)
        mock_session.execute.return_value = mock_result
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_session):
            result = await sql_operate.health_check()
        
        assert result is True
    
    @pytest.mark.asyncio
    async def test_health_check_dbapi_error(self, sql_operate):
        """Test database health check - DBAPI error"""
        mock_session = self.create_mock_session()
        mock_session.execute.side_effect = DBAPIError("statement", {}, Exception())
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_session):
            result = await sql_operate.health_check()
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_health_check_connection_error(self, sql_operate):
        """Test database health check - connection error"""
        mock_session = self.create_mock_session()
        mock_session.execute.side_effect = ConnectionError("Connection failed")
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_session):
            result = await sql_operate.health_check()
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_validate_identifier_valid(self, sql_operate):
        """Test identifier validation - valid"""
        sql_operate._validate_identifier("valid_table_name")
        sql_operate._validate_identifier("column_123")
        sql_operate._validate_identifier("_private_field")
    
    @pytest.mark.asyncio
    async def test_validate_identifier_invalid(self, sql_operate):
        """Test identifier validation - invalid"""
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_identifier("")
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_identifier("table-name")  # Contains hyphen
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_identifier("a" * 65)  # Too long
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
    
    @pytest.mark.asyncio
    async def test_validate_data_value_dangerous_patterns(self, sql_operate):
        """Test data validation - dangerous patterns"""
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_data_value("--comment", "field")
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_data_value("DROP TABLE users", "field")
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
    
    @pytest.mark.asyncio
    async def test_validate_data_value_exempt_columns(self, sql_operate):
        """Test data validation - exempt columns"""
        # These should not raise exceptions
        sql_operate._validate_data_value("DROP TABLE users", "description")
        sql_operate._validate_data_value("--comment", "content_template")
    
    @pytest.mark.asyncio
    async def test_exception_handler_postgresql_error(self, sql_operate):
        """Test exception handler with PostgreSQL error"""
        pg_error = Mock(spec=AsyncAdapt_asyncpg_dbapi.Error)
        pg_error.sqlstate = "23505"
        pg_error.__str__ = Mock(return_value="ERROR: duplicate key value")
        
        dbapi_error = DBAPIError("statement", {}, pg_error)
        dbapi_error.orig = pg_error
        
        @SQLOperate.exception_handler
        async def test_func(self):
            raise dbapi_error
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func(sql_operate)
        
        assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR
    
    @pytest.mark.asyncio
    async def test_exception_handler_mysql_error(self, sql_operate):
        """Test exception handler with MySQL error"""
        mysql_error = pymysql.Error(1062, "Duplicate entry")
        dbapi_error = DBAPIError("statement", {}, mysql_error)
        dbapi_error.orig = mysql_error
        
        @SQLOperate.exception_handler
        async def test_func(self):
            raise dbapi_error
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func(sql_operate)
        
        assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR
    
    @pytest.mark.asyncio
    async def test_exception_handler_unmapped_instance_error(self, sql_operate):
        """Test exception handler with UnmappedInstanceError"""
        @SQLOperate.exception_handler
        async def test_func(self):
            raise UnmappedInstanceError("Instance not mapped")
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func(sql_operate)
        
        assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR
    
    @pytest.mark.asyncio
    async def test_read_sql_with_date_range(self, sql_operate):
        """Test read with date range filtering"""
        mock_session = self.create_mock_session()
        mock_result = Mock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_session):
            result = await sql_operate.read_sql_with_date_range(
                "test_table",
                filters={"status": "active"},
                date_field="created_at",
                start_date="2024-01-01",
                end_date="2024-12-31"
            )
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_read_sql_with_conditions(self, sql_operate):
        """Test read with custom conditions"""
        mock_session = self.create_mock_session()
        mock_result = Mock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_session):
            result = await sql_operate.read_sql_with_conditions(
                "test_table",
                conditions=["status = :status", "created_at > :date"],
                params={"status": "active", "date": "2024-01-01"}
            )
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_get_aggregated_data(self, sql_operate):
        """Test aggregated data retrieval"""
        mock_session = self.create_mock_session()
        mock_result = Mock()
        mock_result.fetchall.return_value = [Mock(_mapping={"status": "active", "count": 10})]
        mock_session.execute.return_value = mock_result
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_session):
            result = await sql_operate.get_aggregated_data(
                "test_table",
                group_by=["status"],
                aggregations={"count": "*"}
            )
        
        assert result == [{"status": "active", "count": 10}]
    
    @pytest.mark.asyncio
    async def test_execute_raw_query_all(self, sql_operate):
        """Test execute raw query - fetch all"""
        mock_session = self.create_mock_session()
        mock_result = Mock()
        mock_result.fetchall.return_value = [Mock(_mapping={"id": 1})]
        mock_session.execute.return_value = mock_result
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_session):
            result = await sql_operate.execute_raw_query(
                "SELECT * FROM test_table",
                fetch_mode="all"
            )
        
        assert result == [{"id": 1}]
    
    @pytest.mark.asyncio
    async def test_execute_raw_query_one(self, sql_operate):
        """Test execute raw query - fetch one"""
        mock_session = self.create_mock_session()
        mock_result = Mock()
        mock_result.fetchone.return_value = Mock(_mapping={"id": 1})
        mock_session.execute.return_value = mock_result
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_session):
            result = await sql_operate.execute_raw_query(
                "SELECT * FROM test_table",
                fetch_mode="one"
            )
        
        assert result == {"id": 1}
    
    @pytest.mark.asyncio
    async def test_execute_raw_query_none(self, sql_operate):
        """Test execute raw query - fetch none"""
        mock_session = self.create_mock_session()
        mock_session.execute.return_value = Mock()
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_session):
            result = await sql_operate.execute_raw_query(
                "UPDATE test_table SET status = 'active'",
                fetch_mode="none"
            )
        
        assert result is None


# ============================================================================
# GeneralOperate Tests
# ============================================================================

class TestGeneralOperateClass:
    """Tests for GeneralOperate class"""
    
    @pytest.mark.asyncio
    async def test_init_with_no_module(self, mock_sql_client, mock_redis):
        """Test initialization when get_module returns None"""
        class NoModuleOperator(GeneralOperate):
            def get_module(self):
                return None
        
        operator = NoModuleOperator(mock_sql_client, mock_redis)
        assert operator.table_name is None
        assert operator.main_schemas is None
    
    @pytest.mark.asyncio
    async def test_transaction_context_manager(self, general_operate):
        """Test transaction context manager"""
        mock_session = AsyncMock()
        mock_session.begin = Mock(return_value=AsyncMock(__aenter__=AsyncMock(), __aexit__=AsyncMock()))
        mock_session.close = AsyncMock()
        
        with patch.object(general_operate, 'create_external_session', return_value=mock_session):
            async with general_operate.transaction() as session:
                assert session is mock_session
        
        mock_session.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_health_check_all_healthy(self, general_operate):
        """Test health check when all services are healthy"""
        with patch.object(SQLOperate, 'health_check', return_value=True):
            with patch.object(CacheOperate, 'health_check', return_value=True):
                result = await general_operate.health_check()
        
        assert result is True
    
    @pytest.mark.asyncio
    async def test_health_check_sql_unhealthy(self, general_operate):
        """Test health check when SQL is unhealthy"""
        with patch.object(SQLOperate, 'health_check', return_value=False):
            with patch.object(CacheOperate, 'health_check', return_value=True):
                result = await general_operate.health_check()
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_cache_warming_success(self, general_operate, mock_redis):
        """Test cache warming operation"""
        mock_data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        
        with patch.object(general_operate, 'read_sql', side_effect=[mock_data, []]):
            with patch.object(general_operate, 'store_caches', return_value=True):
                result = await general_operate.cache_warming(limit=100)
        
        assert result["success"] is True
        assert result["records_loaded"] == 2
    
    @pytest.mark.asyncio
    async def test_cache_clear_success(self, general_operate, mock_redis):
        """Test cache clear operation"""
        mock_redis.keys.return_value = ["test_table:1:null", "test_table:2:null"]
        
        result = await general_operate.cache_clear()
        
        assert result["success"] is True
        mock_redis.delete.assert_called()
    
    @pytest.mark.asyncio
    async def test_read_data_by_id_cache_hit(self, general_operate, mock_redis):
        """Test read by ID with cache hit"""
        mock_data = {"id": 1, "name": "test"}
        TestModule.main_schemas.return_value = mock_data
        
        with patch.object(general_operate, 'get_caches', return_value=[mock_data]):
            result = await general_operate.read_data_by_id({1})
        
        assert len(result) == 1
        assert result[0] == mock_data
    
    @pytest.mark.asyncio
    async def test_read_data_by_id_cache_miss(self, general_operate, mock_redis):
        """Test read by ID with cache miss"""
        mock_data = {"id": 1, "name": "test"}
        TestModule.main_schemas.return_value = mock_data
        
        with patch.object(general_operate, 'get_caches', return_value=[]):
            with patch.object(general_operate, '_fetch_from_sql', return_value=[mock_data]):
                with patch.object(general_operate, 'store_caches', return_value=True):
                    result = await general_operate.read_data_by_id({1})
        
        assert len(result) == 1
        assert result[0] == mock_data
    
    @pytest.mark.asyncio
    async def test_read_data_by_id_null_marked(self, general_operate, mock_redis):
        """Test read by ID with null marker"""
        mock_redis.exists.return_value = 1  # Null marker exists
        
        result = await general_operate.read_data_by_id({1})
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_read_data_by_filter(self, general_operate):
        """Test read data by filter"""
        mock_data = [{"id": 1, "status": "active"}]
        TestModule.main_schemas.return_value = mock_data[0]
        
        with patch.object(general_operate, 'read_sql', return_value=mock_data):
            result = await general_operate.read_data_by_filter(
                {"status": "active"},
                limit=10,
                order_by="created_at"
            )
        
        assert len(result) == 1
    
    @pytest.mark.asyncio
    async def test_create_data_success(self, general_operate):
        """Test create data"""
        mock_input = [{"name": "test"}]
        mock_created = [{"id": 1, "name": "test"}]
        TestModule.create_schemas.return_value = Mock(model_dump=lambda: mock_input[0])
        TestModule.main_schemas.return_value = mock_created[0]
        
        with patch.object(general_operate, 'create_sql', return_value=mock_created):
            result = await general_operate.create_data(mock_input)
        
        assert result == mock_created
    
    @pytest.mark.asyncio
    async def test_create_data_validation_error(self, general_operate):
        """Test create data with validation error"""
        TestModule.create_schemas.side_effect = ValueError("Invalid data")
        
        result = await general_operate.create_data([{"invalid": "data"}])
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_create_by_foreign_key(self, general_operate):
        """Test create by foreign key"""
        mock_data = [{"name": "test"}]
        mock_created = [{"id": 1, "tutorial_id": 123, "name": "test"}]
        
        with patch('general_operate.utils.build_data.build_create_data', return_value=mock_data[0]):
            with patch.object(general_operate, 'create_data', return_value=mock_created):
                result = await general_operate.create_by_foreign_key(
                    "tutorial_id", 123, mock_data
                )
        
        assert result == mock_created
    
    @pytest.mark.asyncio
    async def test_update_data_success(self, general_operate):
        """Test update data"""
        mock_input = [{"id": 1, "name": "updated"}]
        mock_updated = [{"id": 1, "name": "updated"}]
        TestModule.update_schemas.return_value = Mock(model_dump=lambda exclude_unset: mock_input[0])
        TestModule.main_schemas.return_value = mock_updated[0]
        
        with patch.object(general_operate, 'update_sql', return_value=mock_updated):
            with patch.object(general_operate, 'delete_caches', return_value=1):
                result = await general_operate.update_data(mock_input)
        
        assert result == mock_updated
    
    @pytest.mark.asyncio
    async def test_update_data_validation_error(self, general_operate):
        """Test update data with validation error"""
        mock_input = [{"name": "updated"}]  # Missing ID field
        
        with pytest.raises(GeneralOperateException) as exc_info:
            await general_operate.update_data(mock_input)
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
    
    @pytest.mark.asyncio
    async def test_update_by_foreign_key(self, general_operate):
        """Test update by foreign key"""
        existing = [{"id": 1, "tutorial_id": 123, "name": "old"}]
        new_data = [{"id": 1, "name": "updated"}]
        
        with patch.object(general_operate, 'read_data_by_filter', return_value=[Mock(model_dump=lambda: existing[0])]):
            with patch('general_operate.utils.build_data.compare_related_items', return_value=([], new_data, [])):
                with patch.object(general_operate, 'update_data', return_value=[]):
                    await general_operate.update_by_foreign_key(
                        "tutorial_id", 123, new_data
                    )
    
    @pytest.mark.asyncio
    async def test_delete_data_success(self, general_operate):
        """Test delete data"""
        with patch.object(general_operate, 'delete_sql', return_value=[1, 2]):
            with patch.object(general_operate, 'delete_caches', return_value=2):
                result = await general_operate.delete_data({1, 2})
        
        assert result == [1, 2]
    
    @pytest.mark.asyncio
    async def test_delete_filter_data_success(self, general_operate):
        """Test delete by filter"""
        with patch.object(general_operate, 'delete_filter', return_value=[1, 2, 3]):
            with patch.object(general_operate, 'delete_caches', return_value=3):
                result = await general_operate.delete_filter_data({"status": "inactive"})
        
        assert result == [1, 2, 3]
    
    @pytest.mark.asyncio
    async def test_count_data(self, general_operate):
        """Test count data"""
        with patch.object(general_operate, 'count_sql', return_value=42):
            result = await general_operate.count_data({"active": True})
        
        assert result == 42
    
    @pytest.mark.asyncio
    async def test_upsert_data_success(self, general_operate):
        """Test upsert data"""
        mock_input = [{"id": 1, "name": "upserted"}]
        mock_result = [{"id": 1, "name": "upserted"}]
        TestModule.create_schemas.return_value = Mock(model_dump=lambda: mock_input[0])
        TestModule.main_schemas.return_value = mock_result[0]
        
        with patch.object(general_operate, 'upsert_sql', return_value=mock_result):
            with patch.object(general_operate, 'delete_caches', return_value=1):
                result = await general_operate.upsert_data(
                    mock_input,
                    conflict_fields=["id"]
                )
        
        assert result == mock_result
    
    @pytest.mark.asyncio
    async def test_exists_check_cache_hit(self, general_operate, mock_redis):
        """Test existence check with cache hit"""
        with patch.object(general_operate, 'get_caches', return_value=[{"id": 1}]):
            result = await general_operate.exists_check(1)
        
        assert result is True
    
    @pytest.mark.asyncio
    async def test_exists_check_database(self, general_operate):
        """Test existence check from database"""
        with patch.object(general_operate, 'get_caches', return_value=[]):
            with patch.object(general_operate, 'exists_sql', return_value={1: True}):
                result = await general_operate.exists_check(1)
        
        assert result is True
    
    @pytest.mark.asyncio
    async def test_batch_exists_with_cache(self, general_operate, mock_redis):
        """Test batch existence check with cache"""
        mock_redis.pipeline.return_value.exists = Mock()
        mock_redis.pipeline.return_value.execute = AsyncMock(
            return_value=[False, False, True]  # null marker results
        )
        
        with patch.object(general_operate, 'get_caches', return_value=[]):
            with patch.object(general_operate, 'exists_sql', return_value={1: True, 2: False}):
                result = await general_operate.batch_exists({1, 2, 3})
        
        assert 3 in result  # ID 3 had null marker
        assert result[3] is False
    
    @pytest.mark.asyncio
    async def test_refresh_cache_success(self, general_operate):
        """Test refresh cache operation"""
        mock_data = [{"id": 1, "name": "refreshed"}]
        
        with patch.object(general_operate, 'delete_caches', return_value=1):
            with patch.object(general_operate, 'read_sql', return_value=mock_data):
                with patch.object(general_operate, 'store_caches', return_value=True):
                    result = await general_operate.refresh_cache({1})
        
        assert result["refreshed"] == 1
        assert result["not_found"] == 0
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_cached(self, general_operate, mock_redis):
        """Test get distinct values from cache"""
        cached_values = ["value1", "value2"]
        mock_redis.get.return_value = json.dumps(cached_values)
        
        result = await general_operate.get_distinct_values("status", cache_ttl=300)
        
        assert result == cached_values
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_database(self, general_operate, mock_redis):
        """Test get distinct values from database"""
        mock_redis.get.return_value = None
        db_values = ["value1", "value2"]
        
        with patch.object(SQLOperate, 'get_distinct_values', return_value=db_values):
            result = await general_operate.get_distinct_values("status", cache_ttl=300)
        
        assert result == db_values
    
    @pytest.mark.asyncio
    async def test_store_cache_data(self, general_operate, mock_redis):
        """Test store cache data utility"""
        data = {"key": "value"}
        
        await general_operate.store_cache_data("prefix", "id", data, 3600)
        
        mock_redis.setex.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_cache_data(self, general_operate):
        """Test get cache data utility"""
        with patch.object(general_operate, 'get_cache', return_value={"data": "value"}):
            result = await general_operate.get_cache_data("prefix", "id")
        
        assert result == {"data": "value"}
    
    @pytest.mark.asyncio
    async def test_delete_cache_data(self, general_operate):
        """Test delete cache data utility"""
        with patch.object(general_operate, 'delete_cache', return_value=True):
            result = await general_operate.delete_cache_data("prefix", "id")
        
        assert result is True
    
    def test_build_cache_key(self, general_operate):
        """Test cache key building"""
        key = general_operate._build_cache_key(123)
        assert key == "test_table:123"
    
    def test_build_null_marker_key(self, general_operate):
        """Test null marker key building"""
        key = general_operate._build_null_marker_key(123)
        assert key == "test_table:123:null"
    
    def test_validate_with_schema(self, general_operate):
        """Test schema validation"""
        mock_data = {"name": "test"}
        TestModule.main_schemas.return_value = mock_data
        
        result = general_operate._validate_with_schema(mock_data, "main")
        assert result == mock_data
    
    def test_process_in_batches(self, general_operate):
        """Test batch processing"""
        items = list(range(250))
        batches = list(general_operate._process_in_batches(items, 100))
        
        assert len(batches) == 3
        assert len(batches[0]) == 100
        assert len(batches[1]) == 100
        assert len(batches[2]) == 50
    
    @pytest.mark.asyncio
    async def test_exception_propagation(self, general_operate):
        """Test that GeneralOperateException is properly propagated"""
        with patch.object(general_operate, 'read_sql', side_effect=GeneralOperateException(
            code=ErrorCode.DB_QUERY_ERROR,
            message="Test error",
            context=ErrorContext(operation="test")
        )):
            with pytest.raises(GeneralOperateException) as exc_info:
                await general_operate.read_data_by_filter({})
            
            assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR


# ============================================================================
# Edge Cases and Error Conditions
# ============================================================================

class TestEdgeCases:
    """Test edge cases and error conditions"""
    
    @pytest.mark.asyncio
    async def test_sql_validate_data_with_unhashable_types(self, sql_operate):
        """Test SQL data validation with unhashable types"""
        data = {
            "list_field": [1, 2, 3],
            "dict_field": {"nested": "value"},
            "normal_field": "value"
        }
        
        result = sql_operate._validate_data_dict(data)
        
        # Lists and dicts should be JSON serialized
        assert isinstance(result["list_field"], str)
        assert isinstance(result["dict_field"], str)
        assert result["normal_field"] == "value"
    
    @pytest.mark.asyncio
    async def test_sql_validate_data_with_null_values(self, sql_operate):
        """Test SQL data validation with null values"""
        data = {
            "field1": None,
            "field2": -999999,  # In null_set
            "field3": "null",    # In null_set
            "field4": "value"
        }
        
        result = sql_operate._validate_data_dict(data)
        
        assert "field1" not in result
        assert "field2" not in result
        assert "field3" not in result
        assert result["field4"] == "value"
    
    @pytest.mark.asyncio
    async def test_sql_build_where_clause_empty_list(self, sql_operate):
        """Test WHERE clause building with empty list filter"""
        filters = {"ids": [], "status": "active"}
        
        where_clause, params = sql_operate._build_where_clause(filters)
        
        # Empty list should be skipped
        assert "ids" not in where_clause
        assert "status = :status" in where_clause
    
    @pytest.mark.asyncio
    async def test_general_operate_cache_miss_with_failed_ops(self, general_operate):
        """Test cache miss handling with failed cache operations"""
        mock_redis = general_operate.redis
        mock_redis.exists.return_value = 0
        mock_redis.get.side_effect = RedisError("Cache error")
        
        mock_data = [{"id": 1, "name": "test"}]
        TestModule.main_schemas.return_value = mock_data[0]
        
        with patch.object(general_operate, '_fetch_from_sql', return_value=mock_data):
            with patch.object(general_operate, 'store_caches', side_effect=RedisError("Store failed")):
                result = await general_operate.read_data_by_id({1})
        
        # Should still return data even if cache operations fail
        assert len(result) == 1
    
    @pytest.mark.asyncio
    async def test_general_operate_update_incomplete(self, general_operate):
        """Test update when not all records are updated"""
        TestModule.update_schemas.return_value = Mock(model_dump=lambda exclude_unset: {"name": "updated"})
        TestModule.main_schemas.return_value = {"id": 1, "name": "updated"}
        
        # Return fewer records than requested
        with patch.object(general_operate, 'update_sql', return_value=[{"id": 1}]):
            with patch.object(general_operate, 'delete_caches', return_value=1):
                with pytest.raises(GeneralOperateException) as exc_info:
                    await general_operate.update_data([
                        {"id": 1, "name": "updated"},
                        {"id": 2, "name": "updated"}
                    ])
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
    
    @pytest.mark.asyncio
    async def test_sql_validation_error_propagation(self, sql_operate):
        """Test that validation errors are properly propagated"""
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate.create_sql("test_table", "not_a_dict")
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
    
    @pytest.mark.asyncio
    async def test_sql_invalid_order_direction(self, sql_operate):
        """Test invalid order direction"""
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate.read_sql(
                "test_table",
                order_by="created_at",
                order_direction="INVALID"
            )
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
    
    @pytest.mark.asyncio
    async def test_sql_invalid_limit(self, sql_operate):
        """Test invalid limit value"""
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate.read_sql("test_table", limit=-1)
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
    
    @pytest.mark.asyncio
    async def test_sql_invalid_offset(self, sql_operate):
        """Test invalid offset value"""
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate.read_sql("test_table", offset=-1)
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
    
    @pytest.mark.asyncio
    async def test_sql_invalid_fetch_mode(self, sql_operate):
        """Test invalid fetch mode for raw query"""
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate.execute_raw_query(
                "SELECT * FROM test",
                fetch_mode="invalid"
            )
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
    
    @pytest.mark.asyncio
    async def test_general_operate_empty_operations(self, general_operate):
        """Test operations with empty data"""
        assert await general_operate.read_data_by_id(set()) == []
        assert await general_operate.create_data([]) == []
        assert await general_operate.update_data([]) == []
        assert await general_operate.delete_data(set()) == []
        assert await general_operate.create_by_foreign_key("fk", 1, []) == []
    
    @pytest.mark.asyncio
    async def test_cache_operate_random_ttl(self, cache_operate):
        """Test that random TTL is applied when not specified"""
        with patch('general_operate.app.cache_operate.random.randint', return_value=3000):
            key, ttl, data = await CacheOperate._CacheOperate__store_cache_inner(
                "prefix", "id", {"data": "value"}, None
            )
        
        assert ttl == 3000
    
    @pytest.mark.asyncio
    async def test_sql_health_check_unexpected_result(self, sql_operate):
        """Test health check with unexpected result"""
        mock_session = AsyncMock()
        mock_result = Mock()
        mock_result.fetchone.return_value = (999,)  # Unexpected value
        mock_session.execute.return_value = mock_result
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_session):
            result = await sql_operate.health_check()
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_general_operate_batch_exists_no_redis(self, general_operate):
        """Test batch exists when Redis is not available"""
        general_operate.redis = None
        
        with patch.object(general_operate, 'exists_sql', return_value={1: True, 2: False}):
            result = await general_operate.batch_exists({1, 2})
        
        assert result == {1: True, 2: False}
    
    @pytest.mark.asyncio
    async def test_general_operate_refresh_cache_pipeline_error(self, general_operate, mock_redis):
        """Test refresh cache with pipeline error"""
        mock_redis.pipeline.side_effect = RedisError("Pipeline error")
        
        with patch.object(general_operate, 'delete_caches', return_value=0):
            with patch.object(general_operate, 'read_sql', return_value=[]):
                result = await general_operate.refresh_cache({1})
        
        # Should still work without pipeline
        assert "not_found" in result
# Additional tests for better coverage
import asyncio
from unittest.mock import Mock, AsyncMock, patch
import pytest
import redis
from general_operate.app.cache_operate import CacheOperate
from general_operate.app.sql_operate import SQLOperate


class TestAdditionalCoverage:
    '''Additional tests to increase coverage'''
    
    @pytest.mark.asyncio
    async def test_cache_exception_handler_redis_error_with_code(self):
        '''Test exception handler with Redis error containing error code'''
        cache_operate = CacheOperate(AsyncMock())
        
        @CacheOperate.exception_handler
        async def test_func(self):
            raise redis.RedisError('Error 1234: Test error')
        
        with pytest.raises(Exception) as exc_info:
            await test_func(cache_operate)
        
        assert 'CACHE_CONNECTION_ERROR' in str(exc_info.value.code)
    
    @pytest.mark.asyncio  
    async def test_sql_exception_handler_generic_dbapi_error(self):
        '''Test SQL exception handler with generic DBAPI error'''
        from sqlalchemy.exc import DBAPIError
        
        mock_client = Mock()
        mock_client.engine_type = 'postgresql'
        sql_operate = SQLOperate(mock_client)
        
        @SQLOperate.exception_handler
        async def test_func(self):
            # Generic DBAPIError without specific database error
            error = DBAPIError('statement', {}, Exception('Generic error'))
            error.orig = Exception('Generic database error')
            raise error
        
        with pytest.raises(Exception) as exc_info:
            await test_func(sql_operate)
        
        assert 'DB_QUERY_ERROR' in str(exc_info.value.code)
    
    @pytest.mark.asyncio
    async def test_sql_validate_long_string(self):
        '''Test SQL validation with very long string'''
        mock_client = Mock()
        mock_client.engine_type = 'postgresql'
        sql_operate = SQLOperate(mock_client)
        
        # Test string that's too long
        long_string = 'a' * 65536
        
        with pytest.raises(Exception) as exc_info:
            sql_operate._validate_data_value(long_string, 'test_field')
        
        assert 'too long' in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_sql_validate_data_with_non_serializable(self):
        '''Test SQL validation with non-serializable data'''
        mock_client = Mock()
        mock_client.engine_type = 'postgresql'
        sql_operate = SQLOperate(mock_client)
        
        # Create a non-serializable object
        import datetime
        non_serializable = {'field': datetime.datetime}  # Class object, not instance
        
        with pytest.raises(Exception):
            sql_operate._validate_data_value(non_serializable, 'test_field')

