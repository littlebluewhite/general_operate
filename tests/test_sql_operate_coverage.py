"""
Focused test suite for SQLOperate to achieve better coverage.
Tests public API methods and exception paths.
"""

import asyncio
import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import asyncpg
import pymysql
import pytest
from sqlalchemy import text
from sqlalchemy.dialects.postgresql.asyncpg import AsyncAdapt_asyncpg_dbapi
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.exc import UnmappedInstanceError

from general_operate.app.client.database import SQLClient
from general_operate.app.sql_operate import SQLOperate
from general_operate.core.exceptions import DatabaseException, ErrorCode


@pytest.fixture
def mock_sql_client():
    """Create a mock SQL client"""
    client = Mock(spec=SQLClient)
    client.engine_type = "postgresql"
    
    # Create mock session
    mock_session = AsyncMock(spec=AsyncSession)
    mock_session.execute = AsyncMock()
    mock_session.commit = AsyncMock()
    mock_session.rollback = AsyncMock()
    mock_session.close = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock()
    
    client.create_session = AsyncMock(return_value=mock_session)
    client.create_external_session = AsyncMock(return_value=mock_session)
    
    return client


@pytest.fixture
def sql_operate(mock_sql_client):
    """Create SQLOperate instance with mock client"""
    return SQLOperate(mock_sql_client)


@pytest.fixture
def mock_session():
    """Create a standalone mock session"""
    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.close = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock()
    return session


class TestSQLOperatePublicAPI:
    """Test public API methods of SQLOperate"""
    
    @pytest.mark.asyncio
    async def test_create_sql_success(self, sql_operate, mock_sql_client):
        """Test successful create operation"""
        mock_session = mock_sql_client.create_session.return_value
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1, "name": "test"}])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.create_sql("users", {"name": "test"})
        
        assert result == [{"id": 1, "name": "test"}]
        mock_session.commit.assert_called_once()
        mock_session.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_sql_batch(self, sql_operate, mock_sql_client):
        """Test batch create operation"""
        mock_session = mock_sql_client.create_session.return_value
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[
            {"id": 1, "name": "test1"},
            {"id": 2, "name": "test2"}
        ])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.create_sql("users", [
            {"name": "test1"},
            {"name": "test2"}
        ])
        
        assert len(result) == 2
        mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_read_sql_by_id(self, sql_operate, mock_sql_client):
        """Test read by ID"""
        mock_session = mock_sql_client.create_session.return_value
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1, "name": "test"}])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.read_sql("users", 1)
        
        assert result == [{"id": 1, "name": "test"}]
        mock_session.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_read_sql_with_filters(self, sql_operate, mock_sql_client):
        """Test read with filters"""
        mock_session = mock_sql_client.create_session.return_value
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[
            {"id": 1, "status": "active"},
            {"id": 2, "status": "active"}
        ])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.read_sql("users", filters={"status": "active"})
        
        assert len(result) == 2
        assert all(r["status"] == "active" for r in result)
    
    @pytest.mark.asyncio
    async def test_update_sql_single(self, sql_operate, mock_sql_client):
        """Test single update operation"""
        sql_operate._is_postgresql = True
        mock_session = mock_sql_client.create_session.return_value
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1, "name": "updated"}])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.update_sql("users", {"id": 1, "name": "updated"})
        
        assert result == [{"id": 1, "name": "updated"}]
        mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_sql_batch(self, sql_operate, mock_sql_client):
        """Test batch update operation"""
        sql_operate._is_postgresql = True
        mock_session = mock_sql_client.create_session.return_value
        
        # Mock multiple update results
        results = []
        for i in range(1, 4):
            mock_result = Mock()
            mock_result.mappings = Mock(return_value=[{"id": i, "name": f"updated{i}"}])
            results.append(mock_result)
        
        mock_session.execute.side_effect = results
        
        result = await sql_operate.update_sql("users", [
            {"id": 1, "name": "updated1"},
            {"id": 2, "name": "updated2"},
            {"id": 3, "name": "updated3"}
        ])
        
        assert len(result) == 3
        assert mock_session.commit.call_count == 3
    
    @pytest.mark.asyncio
    async def test_delete_sql_single_id(self, sql_operate, mock_sql_client):
        """Test delete single record"""
        sql_operate._is_postgresql = True
        mock_session = mock_sql_client.create_session.return_value
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1}])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.delete_sql("users", 1)
        
        assert result == [1]
        mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_sql_multiple_ids(self, sql_operate, mock_sql_client):
        """Test delete multiple records"""
        sql_operate._is_postgresql = True
        mock_session = mock_sql_client.create_session.return_value
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1}, {"id": 2}])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.delete_sql("users", [1, 2])
        
        assert result == [1, 2]
        mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_exists_sql_single(self, sql_operate, mock_sql_client):
        """Test existence check for single ID"""
        mock_session = mock_sql_client.create_session.return_value
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1}])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.exists_sql("users", [1])
        
        assert result == {1: True}
    
    @pytest.mark.asyncio
    async def test_exists_sql_multiple(self, sql_operate, mock_sql_client):
        """Test existence check for multiple IDs"""
        mock_session = mock_sql_client.create_session.return_value
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1}, {"id": 3}])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.exists_sql("users", [1, 2, 3])
        
        assert result == {1: True, 2: False, 3: True}
    
    @pytest.mark.asyncio
    async def test_count_sql(self, sql_operate, mock_sql_client):
        """Test count operation"""
        mock_session = mock_sql_client.create_session.return_value
        mock_result = Mock()
        mock_result.scalar = Mock(return_value=42)
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.count_sql("users")
        
        assert result == 42
    
    @pytest.mark.asyncio
    async def test_get_distinct_values(self, sql_operate, mock_sql_client):
        """Test getting distinct values"""
        mock_session = mock_sql_client.create_session.return_value
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[
            {"status": "active"},
            {"status": "inactive"},
            {"status": "pending"}
        ])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.get_distinct_values("users", "status")
        
        assert result == ["active", "inactive", "pending"]
    
    @pytest.mark.asyncio
    async def test_upsert_sql_insert(self, sql_operate, mock_sql_client):
        """Test upsert when record doesn't exist (insert)"""
        mock_session = mock_sql_client.create_session.return_value
        
        # First check - record doesn't exist
        mock_check_result = Mock()
        mock_check_result.scalar = Mock(return_value=None)
        
        # Insert result
        mock_insert_result = Mock()
        mock_insert_result.mappings = Mock(return_value=[{"id": 1, "email": "new@example.com"}])
        
        mock_session.execute.side_effect = [mock_check_result, mock_insert_result]
        
        result = await sql_operate.upsert_sql(
            "users",
            {"email": "new@example.com", "name": "New User"},
            update_fields=["name"]
        )
        
        assert result == [{"id": 1, "email": "new@example.com"}]
    
    @pytest.mark.asyncio
    async def test_upsert_sql_update(self, sql_operate, mock_sql_client):
        """Test upsert when record exists (update)"""
        sql_operate._is_postgresql = True
        mock_session = mock_sql_client.create_session.return_value
        
        # First check - record exists
        mock_check_result = Mock()
        mock_check_result.scalar = Mock(return_value=1)
        
        # Select existing record
        mock_select_result = Mock()
        mock_select_result.mappings = Mock(return_value=Mock(
            first=Mock(return_value={"id": 1, "email": "existing@example.com"})
        ))
        
        # Update result
        mock_update_result = Mock()
        mock_update_result.mappings = Mock(return_value=[{"id": 1, "email": "existing@example.com", "name": "Updated"}])
        
        mock_session.execute.side_effect = [mock_check_result, mock_select_result, mock_update_result]
        
        result = await sql_operate.upsert_sql(
            "users",
            {"email": "existing@example.com", "name": "Updated"},
            update_fields=["name"]
        )
        
        assert result == [{"id": 1, "email": "existing@example.com", "name": "Updated"}]
    
    @pytest.mark.asyncio
    async def test_delete_filter(self, sql_operate, mock_sql_client):
        """Test delete by filter"""
        sql_operate._is_postgresql = True
        mock_session = mock_sql_client.create_session.return_value
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1}, {"id": 2}])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.delete_filter("users", {"status": "inactive"})
        
        assert result == [1, 2]
        mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_read_one(self, sql_operate, mock_sql_client):
        """Test read_one method"""
        mock_session = mock_sql_client.create_session.return_value
        mock_result = Mock()
        mock_mappings = Mock()
        mock_mappings.first = Mock(return_value={"id": 1, "name": "test"})
        mock_result.mappings = Mock(return_value=mock_mappings)
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.read_one("users", 1)
        
        assert result == {"id": 1, "name": "test"}
    
    @pytest.mark.asyncio
    async def test_execute_query(self, sql_operate, mock_sql_client):
        """Test execute_query method"""
        mock_session = mock_sql_client.create_session.return_value
        mock_result = Mock()
        mock_result.returns_rows = True
        mock_result.mappings = Mock(return_value=[{"count": 10}])
        mock_result.rowcount = 0
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.execute_query("SELECT COUNT(*) as count FROM users")
        
        assert result == {"rows": [{"count": 10}], "affected": 0}
    
    @pytest.mark.asyncio
    async def test_execute_raw_query(self, sql_operate, mock_sql_client):
        """Test execute_raw_query method"""
        mock_session = mock_sql_client.create_session.return_value
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1, "name": "test"}])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.execute_raw_query(
            "SELECT * FROM users WHERE id = :id",
            {"id": 1}
        )
        
        assert result == [{"id": 1, "name": "test"}]
    
    @pytest.mark.asyncio
    async def test_health_check_success(self, sql_operate, mock_sql_client):
        """Test successful health check"""
        mock_session = mock_sql_client.create_session.return_value
        mock_result = Mock()
        mock_result.scalar = Mock(return_value=1)
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.health_check()
        
        assert result is True
    
    @pytest.mark.asyncio
    async def test_health_check_failure(self, sql_operate, mock_sql_client):
        """Test failed health check"""
        mock_sql_client.create_session.side_effect = Exception("Connection failed")
        
        result = await sql_operate.health_check()
        
        assert result is False


class TestMySQLBehavior:
    """Test MySQL-specific behavior"""
    
    @pytest.mark.asyncio
    async def test_mysql_create(self, mock_sql_client):
        """Test MySQL create with LAST_INSERT_ID"""
        mock_sql_client.engine_type = "mysql"
        sql_operate = SQLOperate(mock_sql_client)
        
        mock_session = mock_sql_client.create_session.return_value
        mock_result = Mock()
        mock_result.lastrowid = 100
        mock_result.rowcount = 1
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.create_sql("users", {"name": "test"})
        
        assert len(result) == 1
        assert result[0]["id"] == 100
    
    @pytest.mark.asyncio
    async def test_mysql_update(self, mock_sql_client):
        """Test MySQL update operation"""
        mock_sql_client.engine_type = "mysql"
        sql_operate = SQLOperate(mock_sql_client)
        
        mock_session = mock_sql_client.create_session.return_value
        
        # Update result
        mock_update_result = Mock()
        mock_update_result.rowcount = 1
        
        # Select result
        mock_select_result = Mock()
        mock_select_result.mappings = Mock(return_value=[{"id": 1, "name": "updated"}])
        
        mock_session.execute.side_effect = [mock_update_result, mock_select_result]
        
        result = await sql_operate.update_sql("users", {"id": 1, "name": "updated"})
        
        assert result == [{"id": 1, "name": "updated"}]
    
    @pytest.mark.asyncio
    async def test_mysql_delete(self, mock_sql_client):
        """Test MySQL delete operation"""
        mock_sql_client.engine_type = "mysql"
        sql_operate = SQLOperate(mock_sql_client)
        
        mock_session = mock_sql_client.create_session.return_value
        mock_result = Mock()
        mock_result.rowcount = 2
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.delete_sql("users", [1, 2])
        
        assert result == [1, 2]
    
    @pytest.mark.asyncio
    async def test_mysql_delete_filter(self, mock_sql_client):
        """Test MySQL delete by filter"""
        mock_sql_client.engine_type = "mysql"
        sql_operate = SQLOperate(mock_sql_client)
        
        mock_session = mock_sql_client.create_session.return_value
        
        # First SELECT to get IDs
        mock_select_result = Mock()
        mock_select_result.mappings = Mock(return_value=[{"id": 1}, {"id": 2}])
        
        # Then DELETE
        mock_delete_result = Mock()
        mock_delete_result.rowcount = 2
        
        mock_session.execute.side_effect = [mock_select_result, mock_delete_result]
        
        result = await sql_operate.delete_filter("users", {"status": "inactive"})
        
        assert result == [1, 2]


class TestExceptionHandling:
    """Test exception handling"""
    
    @pytest.mark.asyncio
    async def test_json_decode_error(self, sql_operate):
        """Test JSON decode error handling"""
        @sql_operate.exception_handler
        async def test_func(self):
            raise json.JSONDecodeError("test", "doc", 1)
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func(sql_operate)
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "JSON decode error" in exc_info.value.message
    
    @pytest.mark.asyncio
    async def test_postgresql_error(self, sql_operate):
        """Test PostgreSQL error handling"""
        @sql_operate.exception_handler
        async def test_func(self):
            pg_error = Mock(spec=AsyncAdapt_asyncpg_dbapi.Error)
            pg_error.sqlstate = "23505"
            pg_error.__str__ = Mock(return_value="ERROR: duplicate key violates unique constraint")
            raise DBAPIError("statement", {}, pg_error)
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func(sql_operate)
        
        assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR
        assert exc_info.value.context.details["sqlstate"] == "23505"
    
    @pytest.mark.asyncio
    async def test_mysql_error(self, sql_operate):
        """Test MySQL error handling"""
        @sql_operate.exception_handler
        async def test_func(self):
            mysql_error = pymysql.Error(1062, "Duplicate entry")
            raise DBAPIError("statement", {}, mysql_error)
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func(sql_operate)
        
        assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR
        assert "Duplicate entry" in exc_info.value.message
    
    @pytest.mark.asyncio
    async def test_unmapped_instance_error(self, sql_operate):
        """Test UnmappedInstanceError handling"""
        @sql_operate.exception_handler
        async def test_func(self):
            raise UnmappedInstanceError("test")
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func(sql_operate)
        
        assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR
        assert "id: one or more of ids is not exist" in exc_info.value.message
    
    @pytest.mark.asyncio
    async def test_generic_db_error(self, sql_operate):
        """Test generic database error"""
        @sql_operate.exception_handler
        async def test_func(self):
            raise DBAPIError("statement", {}, Exception("Generic error"))
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func(sql_operate)
        
        assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR
    
    @pytest.mark.asyncio
    async def test_sql_validation_error(self, sql_operate):
        """Test SQL validation error"""
        @sql_operate.exception_handler
        async def test_func(self):
            raise ValueError("Invalid SQL query")
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func(sql_operate)
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "SQL operation validation error" in exc_info.value.message
    
    @pytest.mark.asyncio
    async def test_preserve_database_exception(self, sql_operate):
        """Test that DatabaseException is preserved"""
        original_exc = DatabaseException(
            code=ErrorCode.NOT_FOUND,
            message="Custom error",
            context=None
        )
        
        @sql_operate.exception_handler
        async def test_func(self):
            raise original_exc
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func(sql_operate)
        
        assert exc_info.value is original_exc
    
    @pytest.mark.asyncio
    async def test_unknown_error(self, sql_operate):
        """Test unknown error"""
        @sql_operate.exception_handler
        async def test_func(self):
            raise RuntimeError("Unknown error")
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func(sql_operate)
        
        assert exc_info.value.code == ErrorCode.UNKNOWN_ERROR


class TestDateRangeQueries:
    """Test date range query functionality"""
    
    @pytest.mark.asyncio
    async def test_read_sql_with_date_range(self, sql_operate, mock_sql_client):
        """Test date range query"""
        mock_session = mock_sql_client.create_session.return_value
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[
            {"id": 1, "created_at": "2024-01-15"},
            {"id": 2, "created_at": "2024-01-20"}
        ])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.read_sql_with_date_range(
            "orders", "created_at", "2024-01-01", "2024-01-31"
        )
        
        assert len(result) == 2
    
    @pytest.mark.asyncio
    async def test_read_sql_with_date_range_and_filters(self, sql_operate, mock_sql_client):
        """Test date range query with additional filters"""
        mock_session = mock_sql_client.create_session.return_value
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[
            {"id": 1, "status": "completed", "created_at": "2024-01-15"}
        ])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.read_sql_with_date_range(
            "orders", "created_at", "2024-01-01", "2024-01-31",
            filters={"status": "completed"}
        )
        
        assert len(result) == 1
        assert result[0]["status"] == "completed"


class TestConditionalQueries:
    """Test conditional query functionality"""
    
    @pytest.mark.asyncio
    async def test_read_sql_with_conditions(self, sql_operate, mock_sql_client):
        """Test conditional query"""
        mock_session = mock_sql_client.create_session.return_value
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[
            {"id": 1, "age": 25},
            {"id": 2, "age": 28}
        ])
        mock_session.execute.return_value = mock_result
        
        conditions = [
            {"column": "age", "operator": ">", "value": 18},
            {"column": "age", "operator": "<", "value": 30}
        ]
        
        result = await sql_operate.read_sql_with_conditions("users", conditions)
        
        assert len(result) == 2
        assert all(18 < r["age"] < 30 for r in result)


class TestAggregation:
    """Test aggregation functionality"""
    
    @pytest.mark.asyncio
    async def test_get_aggregated_data(self, sql_operate, mock_sql_client):
        """Test aggregation query"""
        mock_session = mock_sql_client.create_session.return_value
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[
            {"department": "Sales", "total": 10, "avg_salary": 75000}
        ])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.get_aggregated_data(
            "employees",
            aggregations={"total": "COUNT(*)", "avg_salary": "AVG(salary)"},
            group_by=["department"]
        )
        
        assert len(result) == 1
        assert result[0]["department"] == "Sales"
        assert result[0]["total"] == 10


class TestSessionManagement:
    """Test session management"""
    
    @pytest.mark.asyncio
    async def test_external_session_not_closed(self, sql_operate, mock_session):
        """Test that external sessions are not closed"""
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[])
        mock_session.execute.return_value = mock_result
        
        await sql_operate.read_sql("users", 1, session=mock_session)
        
        # External session should not be closed
        mock_session.close.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_session_rollback_on_error(self, sql_operate, mock_sql_client):
        """Test session rollback on error"""
        mock_session = mock_sql_client.create_session.return_value
        mock_session.execute.side_effect = Exception("Query failed")
        
        with pytest.raises(Exception):
            await sql_operate.create_sql("users", {"name": "test"})
        
        mock_session.rollback.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--cov=general_operate.app.sql_operate", "--cov-report=term-missing"])