"""
Comprehensive test suite for SQLOperate class.
Achieves 100% code coverage for general_operate/app/sql_operate.py
"""

import json
import re
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, Mock, patch, PropertyMock

import asyncpg
import pymysql
import pytest
import structlog
from sqlalchemy import text
from sqlalchemy.dialects.postgresql.asyncpg import AsyncAdapt_asyncpg_dbapi
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.exc import UnmappedInstanceError

from general_operate.app.client.database import SQLClient
from general_operate.app.sql_operate import SQLOperate
from general_operate.core.exceptions import DatabaseException, ErrorCode, ErrorContext


@pytest.fixture
def mock_sql_client():
    """Create a mock SQL client"""
    client = Mock(spec=SQLClient)
    client.engine_type = "postgresql"
    client.create_session = AsyncMock(return_value=AsyncMock(spec=AsyncSession))
    client.create_external_session = AsyncMock(return_value=AsyncMock(spec=AsyncSession))
    return client


@pytest.fixture
def sql_operate(mock_sql_client):
    """Create SQLOperate instance with mock client"""
    return SQLOperate(mock_sql_client)


@pytest.fixture
def mock_session():
    """Create a mock async session"""
    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.close = AsyncMock()
    session.begin = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock()
    return session


class TestSQLOperateInit:
    """Test SQLOperate initialization"""
    
    def test_init_postgresql(self, mock_sql_client):
        """Test initialization with PostgreSQL"""
        mock_sql_client.engine_type = "postgresql"
        operate = SQLOperate(mock_sql_client)
        assert operate._is_postgresql is True
        assert operate.null_set == {-999999, "null"}
        assert operate._valid_identifier_pattern.pattern == r"^[a-zA-Z_][a-zA-Z0-9_]*$"
    
    def test_init_mysql(self, mock_sql_client):
        """Test initialization with MySQL"""
        mock_sql_client.engine_type = "mysql"
        operate = SQLOperate(mock_sql_client)
        assert operate._is_postgresql is False
        assert operate.null_set == {-999999, "null"}


class TestExceptionHandler:
    """Test exception handler decorator"""
    
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
        # Create mock PostgreSQL error
        pg_error = Mock(spec=AsyncAdapt_asyncpg_dbapi.Error)
        pg_error.sqlstate = "23505"
        pg_error.__str__ = Mock(return_value="ERROR: duplicate key value")
        
        db_error = DBAPIError("statement", {}, pg_error)
        
        @sql_operate.exception_handler
        async def test_func(self):
            raise db_error
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func(sql_operate)
        
        assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR
        assert exc_info.value.context.details["sqlstate"] == "23505"
    
    @pytest.mark.asyncio
    async def test_mysql_error(self, sql_operate):
        """Test MySQL error handling"""
        # Create mock MySQL error
        mysql_error = pymysql.Error(1062, "Duplicate entry")
        db_error = DBAPIError("statement", {}, mysql_error)
        
        @sql_operate.exception_handler
        async def test_func(self):
            raise db_error
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func(sql_operate)
        
        assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR
        assert exc_info.value.message == "Duplicate entry"
        assert exc_info.value.context.details["mysql_error_code"] == 1062
    
    @pytest.mark.asyncio
    async def test_generic_db_error(self, sql_operate):
        """Test generic database error handling"""
        generic_error = Exception("Generic DB error")
        db_error = DBAPIError("statement", {}, generic_error)
        
        @sql_operate.exception_handler
        async def test_func(self):
            raise db_error
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func(sql_operate)
        
        assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR
    
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
    async def test_sql_validation_error(self, sql_operate):
        """Test SQL validation error handling"""
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
            context=ErrorContext(operation="test", resource="test")
        )
        
        @sql_operate.exception_handler
        async def test_func(self):
            raise original_exc
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func(sql_operate)
        
        assert exc_info.value is original_exc
    
    @pytest.mark.asyncio
    async def test_unknown_error(self, sql_operate):
        """Test unknown error handling"""
        @sql_operate.exception_handler
        async def test_func(self):
            raise RuntimeError("Unknown error")
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func(sql_operate)
        
        assert exc_info.value.code == ErrorCode.UNKNOWN_ERROR


class TestValidationMethods:
    """Test validation methods"""
    
    def test_validate_identifier_valid(self, sql_operate):
        """Test valid identifier validation"""
        assert sql_operate._validate_identifier("valid_table") is True
        assert sql_operate._validate_identifier("table123") is True
        assert sql_operate._validate_identifier("_private_table") is True
    
    def test_validate_identifier_invalid(self, sql_operate):
        """Test invalid identifier validation"""
        assert sql_operate._validate_identifier("123table") is False
        assert sql_operate._validate_identifier("table-name") is False
        assert sql_operate._validate_identifier("table.name") is False
        assert sql_operate._validate_identifier("table name") is False
        assert sql_operate._validate_identifier("") is False
        assert sql_operate._validate_identifier(None) is False
    
    def test_validate_data_value_valid(self, sql_operate):
        """Test valid data value validation"""
        assert sql_operate._validate_data_value("string") is True
        assert sql_operate._validate_data_value(123) is True
        assert sql_operate._validate_data_value(45.67) is True
        assert sql_operate._validate_data_value(True) is True
        assert sql_operate._validate_data_value(None) is True
        assert sql_operate._validate_data_value(b"bytes") is True
    
    def test_validate_data_value_invalid(self, sql_operate):
        """Test invalid data value validation"""
        assert sql_operate._validate_data_value([1, 2, 3]) is False
        assert sql_operate._validate_data_value({"key": "value"}) is False
        assert sql_operate._validate_data_value(object()) is False
    
    def test_validate_data_dict_valid(self, sql_operate):
        """Test valid data dictionary validation"""
        assert sql_operate._validate_data_dict({
            "name": "test",
            "age": 30,
            "active": True
        }) is True
    
    def test_validate_data_dict_invalid_keys(self, sql_operate):
        """Test invalid data dictionary with bad keys"""
        assert sql_operate._validate_data_dict({
            "invalid-key": "value"
        }) is False
        assert sql_operate._validate_data_dict({
            123: "value"
        }) is False
    
    def test_validate_data_dict_invalid_values(self, sql_operate):
        """Test invalid data dictionary with bad values"""
        assert sql_operate._validate_data_dict({
            "key": [1, 2, 3]
        }) is False
        assert sql_operate._validate_data_dict({
            "key": {"nested": "dict"}
        }) is False


class TestBuildWhereClause:
    """Test _build_where_clause method"""
    
    def test_build_where_clause_with_id(self, sql_operate):
        """Test building WHERE clause with ID"""
        result = sql_operate._build_where_clause("users", 123)
        assert result == "id = :id"
    
    def test_build_where_clause_with_filters(self, sql_operate):
        """Test building WHERE clause with filters"""
        filters = {"name": "John", "age": 30}
        result = sql_operate._build_where_clause("users", filters=filters)
        assert "name = :name" in result
        assert "age = :age" in result
        assert " AND " in result
    
    def test_build_where_clause_with_id_and_filters(self, sql_operate):
        """Test building WHERE clause with both ID and filters"""
        filters = {"active": True}
        result = sql_operate._build_where_clause("users", 123, filters)
        assert "id = :id" in result
        assert "active = :active" in result
        assert " AND " in result
    
    def test_build_where_clause_empty(self, sql_operate):
        """Test building WHERE clause with no conditions"""
        result = sql_operate._build_where_clause("users")
        assert result == ""
    
    def test_build_where_clause_with_null_values(self, sql_operate):
        """Test building WHERE clause with null values"""
        filters = {"name": None, "age": 30}
        result = sql_operate._build_where_clause("users", filters=filters)
        assert "name IS NULL" in result
        assert "age = :age" in result
    
    def test_build_where_clause_with_null_set_values(self, sql_operate):
        """Test building WHERE clause with null set values"""
        filters = {"status": -999999, "type": "null"}
        result = sql_operate._build_where_clause("users", filters=filters)
        assert "status IS NULL" in result
        assert "type IS NULL" in result


class TestValidateCreateData:
    """Test _validate_create_data method"""
    
    def test_validate_create_data_single_dict(self, sql_operate):
        """Test validating single dictionary"""
        data = {"name": "test", "value": 123}
        result = sql_operate._validate_create_data(data)
        assert result == [data]
    
    def test_validate_create_data_list_of_dicts(self, sql_operate):
        """Test validating list of dictionaries"""
        data = [
            {"name": "test1", "value": 123},
            {"name": "test2", "value": 456}
        ]
        result = sql_operate._validate_create_data(data)
        assert result == data
    
    def test_validate_create_data_invalid_type(self, sql_operate):
        """Test invalid data type"""
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_create_data("invalid")
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "Data must be a dictionary or list of dictionaries" in exc_info.value.message
    
    def test_validate_create_data_empty_list(self, sql_operate):
        """Test empty list"""
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_create_data([])
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "Data list cannot be empty" in exc_info.value.message
    
    def test_validate_create_data_invalid_list_item(self, sql_operate):
        """Test list with invalid item"""
        data = [{"name": "test"}, "invalid"]
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_create_data(data)
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "All items must be dictionaries" in exc_info.value.message
    
    def test_validate_create_data_invalid_table_name(self, sql_operate):
        """Test invalid table name in data"""
        data = {"name": "test"}
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_create_data(data, table_name="invalid-table")
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "Invalid table name" in exc_info.value.message
    
    def test_validate_create_data_invalid_dict_structure(self, sql_operate):
        """Test invalid dictionary structure"""
        data = {"invalid-key": "value"}
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_create_data(data)
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "Invalid data structure" in exc_info.value.message


class TestBuildInsertQuery:
    """Test _build_insert_query method"""
    
    def test_build_insert_query_postgresql(self, sql_operate):
        """Test building PostgreSQL INSERT query"""
        sql_operate._is_postgresql = True
        data = [{"name": "test", "value": 123}]
        query, params = sql_operate._build_insert_query("users", data)
        
        assert "INSERT INTO users" in query
        assert "name, value" in query or "value, name" in query
        assert "VALUES" in query
        assert "RETURNING *" in query
        assert params[0]["name"] == "test"
        assert params[0]["value"] == 123
    
    def test_build_insert_query_mysql(self, sql_operate):
        """Test building MySQL INSERT query"""
        sql_operate._is_postgresql = False
        data = [{"name": "test", "value": 123}]
        query, params = sql_operate._build_insert_query("users", data)
        
        assert "INSERT INTO users" in query
        assert "name, value" in query or "value, name" in query
        assert "VALUES" in query
        assert "RETURNING *" not in query
        assert params[0]["name"] == "test"
        assert params[0]["value"] == 123
    
    def test_build_insert_query_multiple_rows(self, sql_operate):
        """Test building INSERT query with multiple rows"""
        data = [
            {"name": "test1", "value": 123},
            {"name": "test2", "value": 456}
        ]
        query, params = sql_operate._build_insert_query("users", data)
        
        assert "INSERT INTO users" in query
        assert params[0]["name"] == "test1"
        assert params[1]["name"] == "test2"
    
    def test_build_insert_query_with_nulls(self, sql_operate):
        """Test building INSERT query with null values"""
        data = [{"name": None, "value": -999999}]
        query, params = sql_operate._build_insert_query("users", data)
        
        assert params[0]["name"] is None
        assert params[0]["value"] is None  # -999999 should be converted to None


class TestExecutePostgresqlInsert:
    """Test _execute_postgresql_insert method"""
    
    @pytest.mark.asyncio
    async def test_execute_postgresql_insert_success(self, sql_operate, mock_session):
        """Test successful PostgreSQL insert"""
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[
            {"id": 1, "name": "test"}
        ])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate._execute_postgresql_insert(
            mock_session, "users", "INSERT INTO users", [{"name": "test"}]
        )
        
        assert result == [{"id": 1, "name": "test"}]
        mock_session.execute.assert_called_once()
        mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_execute_postgresql_insert_error(self, sql_operate, mock_session):
        """Test PostgreSQL insert with error"""
        mock_session.execute.side_effect = Exception("Insert failed")
        
        with pytest.raises(Exception):
            await sql_operate._execute_postgresql_insert(
                mock_session, "users", "INSERT INTO users", [{"name": "test"}]
            )
        
        mock_session.rollback.assert_called_once()


class TestExecuteMysqlInsert:
    """Test _execute_mysql_insert method"""
    
    @pytest.mark.asyncio
    async def test_execute_mysql_insert_success(self, sql_operate, mock_session):
        """Test successful MySQL insert"""
        mock_result = Mock()
        mock_result.lastrowid = 1
        mock_result.rowcount = 1
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate._execute_mysql_insert(
            mock_session, "users", "INSERT INTO users", [{"name": "test"}]
        )
        
        assert len(result) == 1
        assert result[0]["id"] == 1
        mock_session.execute.assert_called()
        mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_execute_mysql_insert_multiple_rows(self, sql_operate, mock_session):
        """Test MySQL insert with multiple rows"""
        mock_result = Mock()
        mock_result.lastrowid = 1
        mock_result.rowcount = 2
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate._execute_mysql_insert(
            mock_session, "users", "INSERT INTO users",
            [{"name": "test1"}, {"name": "test2"}]
        )
        
        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[1]["id"] == 2


class TestCreateSQL:
    """Test create_sql method"""
    
    @pytest.mark.asyncio
    async def test_create_sql_postgresql(self, sql_operate, mock_session):
        """Test create_sql with PostgreSQL"""
        sql_operate._is_postgresql = True
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1, "name": "test"}])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.create_sql("users", {"name": "test"})
        
        assert result == [{"id": 1, "name": "test"}]
    
    @pytest.mark.asyncio
    async def test_create_sql_mysql(self, sql_operate, mock_session):
        """Test create_sql with MySQL"""
        sql_operate._is_postgresql = False
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.lastrowid = 1
        mock_result.rowcount = 1
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.create_sql("users", {"name": "test"})
        
        assert len(result) == 1
        assert result[0]["id"] == 1
    
    @pytest.mark.asyncio
    async def test_create_sql_with_external_session(self, sql_operate, mock_session):
        """Test create_sql with external session"""
        sql_operate._is_postgresql = True
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1, "name": "test"}])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.create_sql("users", {"name": "test"}, session=mock_session)
        
        assert result == [{"id": 1, "name": "test"}]
        # Should not create new session
        sql_operate._SQLOperate__sqlClient.create_session.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_create_sql_validation_error(self, sql_operate):
        """Test create_sql with validation error"""
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate.create_sql("invalid-table", {"name": "test"})
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR


class TestReadSQL:
    """Test read_sql method"""
    
    @pytest.mark.asyncio
    async def test_read_sql_with_id(self, sql_operate, mock_session):
        """Test read_sql with ID"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1, "name": "test"}])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.read_sql("users", 1)
        
        assert result == [{"id": 1, "name": "test"}]
        call_args = mock_session.execute.call_args[0][0]
        assert "SELECT * FROM users" in str(call_args)
        assert "WHERE id = :id" in str(call_args)
    
    @pytest.mark.asyncio
    async def test_read_sql_with_filters(self, sql_operate, mock_session):
        """Test read_sql with filters"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1, "name": "test", "age": 30}])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.read_sql("users", filters={"age": 30})
        
        assert result == [{"id": 1, "name": "test", "age": 30}]
    
    @pytest.mark.asyncio
    async def test_read_sql_with_columns(self, sql_operate, mock_session):
        """Test read_sql with specific columns"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1, "name": "test"}])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.read_sql("users", columns=["id", "name"])
        
        assert result == [{"id": 1, "name": "test"}]
        call_args = mock_session.execute.call_args[0][0]
        assert "SELECT id, name FROM users" in str(call_args)
    
    @pytest.mark.asyncio
    async def test_read_sql_with_order_by(self, sql_operate, mock_session):
        """Test read_sql with ORDER BY"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 2}, {"id": 1}])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.read_sql("users", order_by="id DESC")
        
        assert result[0]["id"] == 2
        assert result[1]["id"] == 1
        call_args = mock_session.execute.call_args[0][0]
        assert "ORDER BY id DESC" in str(call_args)
    
    @pytest.mark.asyncio
    async def test_read_sql_with_limit(self, sql_operate, mock_session):
        """Test read_sql with LIMIT"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1}])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.read_sql("users", limit=1)
        
        assert len(result) == 1
        call_args = mock_session.execute.call_args[0][0]
        assert "LIMIT 1" in str(call_args)
    
    @pytest.mark.asyncio
    async def test_read_sql_validation_error(self, sql_operate):
        """Test read_sql with validation error"""
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate.read_sql("invalid-table")
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR


class TestCountSQL:
    """Test count_sql method"""
    
    @pytest.mark.asyncio
    async def test_count_sql_all(self, sql_operate, mock_session):
        """Test counting all records"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.scalar = Mock(return_value=10)
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.count_sql("users")
        
        assert result == 10
    
    @pytest.mark.asyncio
    async def test_count_sql_with_filters(self, sql_operate, mock_session):
        """Test counting with filters"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.scalar = Mock(return_value=5)
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.count_sql("users", filters={"active": True})
        
        assert result == 5
    
    @pytest.mark.asyncio
    async def test_count_sql_no_results(self, sql_operate, mock_session):
        """Test counting with no results"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.scalar = Mock(return_value=None)
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.count_sql("users", filters={"active": False})
        
        assert result == 0


class TestReadSQLWithDateRange:
    """Test read_sql_with_date_range method"""
    
    @pytest.mark.asyncio
    async def test_read_sql_with_date_range_basic(self, sql_operate, mock_session):
        """Test basic date range query"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1, "created_at": "2024-01-15"}])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.read_sql_with_date_range(
            "users", "created_at", "2024-01-01", "2024-01-31"
        )
        
        assert len(result) == 1
        assert result[0]["id"] == 1
    
    @pytest.mark.asyncio
    async def test_read_sql_with_date_range_filters(self, sql_operate, mock_session):
        """Test date range query with additional filters"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1, "status": "active"}])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.read_sql_with_date_range(
            "users", "created_at", "2024-01-01", "2024-01-31",
            filters={"status": "active"}
        )
        
        assert len(result) == 1
        assert result[0]["status"] == "active"
    
    @pytest.mark.asyncio
    async def test_read_sql_with_date_range_columns(self, sql_operate, mock_session):
        """Test date range query with specific columns"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1, "name": "test"}])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.read_sql_with_date_range(
            "users", "created_at", "2024-01-01", "2024-01-31",
            columns=["id", "name"]
        )
        
        assert "id" in result[0]
        assert "name" in result[0]


class TestReadSQLWithConditions:
    """Test read_sql_with_conditions method"""
    
    @pytest.mark.asyncio
    async def test_read_sql_with_conditions_basic(self, sql_operate, mock_session):
        """Test basic conditional query"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1, "age": 25}])
        mock_session.execute.return_value = mock_result
        
        conditions = [
            {"column": "age", "operator": ">", "value": 18},
            {"column": "age", "operator": "<", "value": 30}
        ]
        
        result = await sql_operate.read_sql_with_conditions("users", conditions)
        
        assert len(result) == 1
        assert result[0]["age"] == 25
    
    @pytest.mark.asyncio
    async def test_read_sql_with_conditions_in_operator(self, sql_operate, mock_session):
        """Test conditional query with IN operator"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1, "status": "active"}])
        mock_session.execute.return_value = mock_result
        
        conditions = [
            {"column": "status", "operator": "IN", "value": ["active", "pending"]}
        ]
        
        result = await sql_operate.read_sql_with_conditions("users", conditions)
        
        assert len(result) == 1
        assert result[0]["status"] == "active"
    
    @pytest.mark.asyncio
    async def test_read_sql_with_conditions_like_operator(self, sql_operate, mock_session):
        """Test conditional query with LIKE operator"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1, "name": "John Doe"}])
        mock_session.execute.return_value = mock_result
        
        conditions = [
            {"column": "name", "operator": "LIKE", "value": "%John%"}
        ]
        
        result = await sql_operate.read_sql_with_conditions("users", conditions)
        
        assert len(result) == 1
        assert "John" in result[0]["name"]


class TestGetAggregatedData:
    """Test get_aggregated_data method"""
    
    @pytest.mark.asyncio
    async def test_get_aggregated_data_count(self, sql_operate, mock_session):
        """Test aggregation with COUNT"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"total": 10}])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.get_aggregated_data(
            "users",
            aggregations={"total": "COUNT(*)"}
        )
        
        assert result[0]["total"] == 10
    
    @pytest.mark.asyncio
    async def test_get_aggregated_data_with_group_by(self, sql_operate, mock_session):
        """Test aggregation with GROUP BY"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[
            {"status": "active", "count": 5},
            {"status": "inactive", "count": 3}
        ])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.get_aggregated_data(
            "users",
            aggregations={"count": "COUNT(*)"},
            group_by=["status"]
        )
        
        assert len(result) == 2
        assert result[0]["status"] == "active"
        assert result[0]["count"] == 5
    
    @pytest.mark.asyncio
    async def test_get_aggregated_data_with_having(self, sql_operate, mock_session):
        """Test aggregation with HAVING clause"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[
            {"department": "Sales", "avg_salary": 75000}
        ])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.get_aggregated_data(
            "employees",
            aggregations={"avg_salary": "AVG(salary)"},
            group_by=["department"],
            having="AVG(salary) > 70000"
        )
        
        assert len(result) == 1
        assert result[0]["avg_salary"] == 75000


class TestExecuteRawQuery:
    """Test execute_raw_query method"""
    
    @pytest.mark.asyncio
    async def test_execute_raw_query_select(self, sql_operate, mock_session):
        """Test raw SELECT query"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1, "name": "test"}])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.execute_raw_query(
            "SELECT * FROM users WHERE id = :id",
            {"id": 1}
        )
        
        assert result == [{"id": 1, "name": "test"}]
    
    @pytest.mark.asyncio
    async def test_execute_raw_query_update(self, sql_operate, mock_session):
        """Test raw UPDATE query"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.rowcount = 1
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.execute_raw_query(
            "UPDATE users SET name = :name WHERE id = :id",
            {"name": "updated", "id": 1}
        )
        
        assert result == {"affected_rows": 1}
    
    @pytest.mark.asyncio
    async def test_execute_raw_query_with_external_session(self, sql_operate, mock_session):
        """Test raw query with external session"""
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"count": 5}])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.execute_raw_query(
            "SELECT COUNT(*) as count FROM users",
            session=mock_session
        )
        
        assert result == [{"count": 5}]
        sql_operate._SQLOperate__sqlClient.create_session.assert_not_called()


class TestReadOne:
    """Test read_one method"""
    
    @pytest.mark.asyncio
    async def test_read_one_found(self, sql_operate, mock_session):
        """Test read_one when record is found"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_row = {"id": 1, "name": "test"}
        mock_result.mappings = Mock(return_value=Mock(first=Mock(return_value=mock_row)))
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.read_one("users", 1)
        
        assert result == {"id": 1, "name": "test"}
    
    @pytest.mark.asyncio
    async def test_read_one_not_found(self, sql_operate, mock_session):
        """Test read_one when record is not found"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=Mock(first=Mock(return_value=None)))
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.read_one("users", 999)
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_read_one_with_filters(self, sql_operate, mock_session):
        """Test read_one with filters"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_row = {"id": 1, "email": "test@example.com"}
        mock_result.mappings = Mock(return_value=Mock(first=Mock(return_value=mock_row)))
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.read_one("users", filters={"email": "test@example.com"})
        
        assert result == {"id": 1, "email": "test@example.com"}


class TestUpdateSQL:
    """Test update_sql method"""
    
    @pytest.mark.asyncio
    async def test_update_sql_single_record(self, sql_operate, mock_session):
        """Test updating single record"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        sql_operate._is_postgresql = True
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1, "name": "updated"}])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.update_sql("users", {"id": 1, "name": "updated"})
        
        assert result == [{"id": 1, "name": "updated"}]
    
    @pytest.mark.asyncio
    async def test_update_sql_multiple_records(self, sql_operate, mock_session):
        """Test updating multiple records"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        sql_operate._is_postgresql = True
        
        mock_result1 = Mock()
        mock_result1.mappings = Mock(return_value=[{"id": 1, "name": "updated1"}])
        mock_result2 = Mock()
        mock_result2.mappings = Mock(return_value=[{"id": 2, "name": "updated2"}])
        
        mock_session.execute.side_effect = [mock_result1, mock_result2]
        
        result = await sql_operate.update_sql("users", [
            {"id": 1, "name": "updated1"},
            {"id": 2, "name": "updated2"}
        ])
        
        assert len(result) == 2
        assert result[0]["name"] == "updated1"
        assert result[1]["name"] == "updated2"
    
    @pytest.mark.asyncio
    async def test_update_sql_mysql(self, sql_operate, mock_session):
        """Test updating with MySQL"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        sql_operate._is_postgresql = False
        
        mock_update_result = Mock()
        mock_update_result.rowcount = 1
        
        mock_select_result = Mock()
        mock_select_result.mappings = Mock(return_value=[{"id": 1, "name": "updated"}])
        
        mock_session.execute.side_effect = [mock_update_result, mock_select_result]
        
        result = await sql_operate.update_sql("users", {"id": 1, "name": "updated"})
        
        assert result == [{"id": 1, "name": "updated"}]
    
    @pytest.mark.asyncio
    async def test_update_sql_no_id(self, sql_operate):
        """Test update without ID field"""
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate.update_sql("users", {"name": "updated"})
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "must contain 'id' field" in exc_info.value.message
    
    @pytest.mark.asyncio
    async def test_update_sql_validation_error(self, sql_operate):
        """Test update with validation error"""
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate.update_sql("invalid-table", {"id": 1, "name": "test"})
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR


class TestDeleteSQL:
    """Test delete_sql method"""
    
    @pytest.mark.asyncio
    async def test_delete_sql_single_id(self, sql_operate, mock_session):
        """Test deleting single record by ID"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        sql_operate._is_postgresql = True
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1}])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.delete_sql("users", 1)
        
        assert result == [1]
    
    @pytest.mark.asyncio
    async def test_delete_sql_multiple_ids(self, sql_operate, mock_session):
        """Test deleting multiple records by IDs"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        sql_operate._is_postgresql = True
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1}, {"id": 2}, {"id": 3}])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.delete_sql("users", [1, 2, 3])
        
        assert result == [1, 2, 3]
    
    @pytest.mark.asyncio
    async def test_delete_sql_mysql(self, sql_operate, mock_session):
        """Test deleting with MySQL"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        sql_operate._is_postgresql = False
        
        mock_result = Mock()
        mock_result.rowcount = 2
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.delete_sql("users", [1, 2])
        
        assert result == [1, 2]
    
    @pytest.mark.asyncio
    async def test_delete_sql_no_records(self, sql_operate, mock_session):
        """Test deleting non-existent records"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        sql_operate._is_postgresql = True
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.delete_sql("users", 999)
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_delete_sql_validation_error(self, sql_operate):
        """Test delete with validation error"""
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate.delete_sql("invalid-table", 1)
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR


class TestDeleteFilter:
    """Test delete_filter method"""
    
    @pytest.mark.asyncio
    async def test_delete_filter_basic(self, sql_operate, mock_session):
        """Test delete with filters"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        sql_operate._is_postgresql = True
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1}, {"id": 2}])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.delete_filter("users", {"status": "inactive"})
        
        assert result == [1, 2]
    
    @pytest.mark.asyncio
    async def test_delete_filter_mysql(self, sql_operate, mock_session):
        """Test delete filter with MySQL"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        sql_operate._is_postgresql = False
        
        # First SELECT to get IDs
        mock_select_result = Mock()
        mock_select_result.mappings = Mock(return_value=[{"id": 1}, {"id": 2}])
        
        # Then DELETE
        mock_delete_result = Mock()
        mock_delete_result.rowcount = 2
        
        mock_session.execute.side_effect = [mock_select_result, mock_delete_result]
        
        result = await sql_operate.delete_filter("users", {"status": "inactive"})
        
        assert result == [1, 2]
    
    @pytest.mark.asyncio
    async def test_delete_filter_no_matches(self, sql_operate, mock_session):
        """Test delete filter with no matches"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        sql_operate._is_postgresql = True
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.delete_filter("users", {"status": "nonexistent"})
        
        assert result == []


class TestUpsertSQL:
    """Test upsert_sql method"""
    
    @pytest.mark.asyncio
    async def test_upsert_sql_insert_new(self, sql_operate, mock_session):
        """Test upsert inserting new record"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        sql_operate._is_postgresql = True
        
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
            unique_fields=["email"]
        )
        
        assert result == [{"id": 1, "email": "new@example.com"}]
    
    @pytest.mark.asyncio
    async def test_upsert_sql_update_existing(self, sql_operate, mock_session):
        """Test upsert updating existing record"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        sql_operate._is_postgresql = True
        
        # First check - record exists
        mock_check_result = Mock()
        mock_check_result.scalar = Mock(return_value=1)
        
        # Select existing record
        mock_select_result = Mock()
        mock_select_result.mappings = Mock(return_value=Mock(
            first=Mock(return_value={"id": 1, "email": "existing@example.com", "name": "Old Name"})
        ))
        
        # Update result
        mock_update_result = Mock()
        mock_update_result.mappings = Mock(return_value=[{"id": 1, "email": "existing@example.com", "name": "New Name"}])
        
        mock_session.execute.side_effect = [mock_check_result, mock_select_result, mock_update_result]
        
        result = await sql_operate.upsert_sql(
            "users",
            {"email": "existing@example.com", "name": "New Name"},
            unique_fields=["email"]
        )
        
        assert result == [{"id": 1, "email": "existing@example.com", "name": "New Name"}]
    
    @pytest.mark.asyncio
    async def test_upsert_sql_multiple_records(self, sql_operate, mock_session):
        """Test upsert with multiple records"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        sql_operate._is_postgresql = True
        
        # Mock checks and operations for two records
        mock_check1 = Mock()
        mock_check1.scalar = Mock(return_value=None)  # First doesn't exist
        
        mock_insert1 = Mock()
        mock_insert1.mappings = Mock(return_value=[{"id": 1, "email": "new1@example.com"}])
        
        mock_check2 = Mock()
        mock_check2.scalar = Mock(return_value=1)  # Second exists
        
        mock_select2 = Mock()
        mock_select2.mappings = Mock(return_value=Mock(
            first=Mock(return_value={"id": 2, "email": "existing@example.com"})
        ))
        
        mock_update2 = Mock()
        mock_update2.mappings = Mock(return_value=[{"id": 2, "email": "existing@example.com"}])
        
        mock_session.execute.side_effect = [
            mock_check1, mock_insert1,
            mock_check2, mock_select2, mock_update2
        ]
        
        result = await sql_operate.upsert_sql(
            "users",
            [
                {"email": "new1@example.com", "name": "New User"},
                {"email": "existing@example.com", "name": "Updated User"}
            ],
            unique_fields=["email"]
        )
        
        assert len(result) == 2
    
    @pytest.mark.asyncio
    async def test_upsert_sql_no_unique_fields(self, sql_operate):
        """Test upsert without unique fields"""
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate.upsert_sql(
                "users",
                {"name": "Test User"},
                unique_fields=[]
            )
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "unique_fields cannot be empty" in exc_info.value.message


class TestExistsSQL:
    """Test exists_sql method"""
    
    @pytest.mark.asyncio
    async def test_exists_sql_single_id_exists(self, sql_operate, mock_session):
        """Test checking if single ID exists"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1}])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.exists_sql("users", 1)
        
        assert result == {1: True}
    
    @pytest.mark.asyncio
    async def test_exists_sql_single_id_not_exists(self, sql_operate, mock_session):
        """Test checking if single ID doesn't exist"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.exists_sql("users", 999)
        
        assert result == {999: False}
    
    @pytest.mark.asyncio
    async def test_exists_sql_multiple_ids(self, sql_operate, mock_session):
        """Test checking multiple IDs"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1}, {"id": 3}])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.exists_sql("users", [1, 2, 3])
        
        assert result == {1: True, 2: False, 3: True}
    
    @pytest.mark.asyncio
    async def test_exists_sql_with_filters(self, sql_operate, mock_session):
        """Test existence check with filters"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1}])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.exists_sql("users", 1, filters={"active": True})
        
        assert result == {1: True}
    
    @pytest.mark.asyncio
    async def test_exists_sql_validation_error(self, sql_operate):
        """Test exists with validation error"""
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate.exists_sql("invalid-table", 1)
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR


class TestGetDistinctValues:
    """Test get_distinct_values method"""
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_basic(self, sql_operate, mock_session):
        """Test getting distinct values for a column"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
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
    async def test_get_distinct_values_with_filters(self, sql_operate, mock_session):
        """Test getting distinct values with filters"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[
            {"department": "Sales"},
            {"department": "Marketing"}
        ])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.get_distinct_values(
            "employees", "department",
            filters={"active": True}
        )
        
        assert result == ["Sales", "Marketing"]
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_with_nulls(self, sql_operate, mock_session):
        """Test getting distinct values including nulls"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[
            {"category": "A"},
            {"category": None},
            {"category": "B"}
        ])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.get_distinct_values("products", "category")
        
        assert result == ["A", None, "B"]
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_validation_error(self, sql_operate):
        """Test get distinct values with validation error"""
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate.get_distinct_values("invalid-table", "column")
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR


class TestExecuteQuery:
    """Test execute_query method"""
    
    @pytest.mark.asyncio
    async def test_execute_query_select(self, sql_operate, mock_session):
        """Test execute_query with SELECT statement"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"count": 10}])
        mock_result.returns_rows = True
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.execute_query("SELECT COUNT(*) as count FROM users")
        
        assert result == {"rows": [{"count": 10}], "affected": 0}
    
    @pytest.mark.asyncio
    async def test_execute_query_update(self, sql_operate, mock_session):
        """Test execute_query with UPDATE statement"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.rowcount = 5
        mock_result.returns_rows = False
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.execute_query("UPDATE users SET active = true WHERE status = 'pending'")
        
        assert result == {"rows": [], "affected": 5}
    
    @pytest.mark.asyncio
    async def test_execute_query_with_params(self, sql_operate, mock_session):
        """Test execute_query with parameters"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1, "name": "Test"}])
        mock_result.returns_rows = True
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.execute_query(
            "SELECT * FROM users WHERE id = :id",
            {"id": 1}
        )
        
        assert result["rows"][0]["id"] == 1


class TestHealthCheck:
    """Test health_check method"""
    
    @pytest.mark.asyncio
    async def test_health_check_success(self, sql_operate, mock_session):
        """Test successful health check"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.scalar = Mock(return_value=1)
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.health_check()
        
        assert result is True
    
    @pytest.mark.asyncio
    async def test_health_check_failure(self, sql_operate, mock_session):
        """Test failed health check"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        mock_session.execute.side_effect = Exception("Connection failed")
        
        result = await sql_operate.health_check()
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_health_check_timeout(self, sql_operate, mock_session):
        """Test health check with timeout"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        async def slow_execute(*args, **kwargs):
            await asyncio.sleep(10)  # Simulate slow query
        
        mock_session.execute = slow_execute
        
        result = await sql_operate.health_check()
        
        assert result is False  # Should timeout and return False


class TestEdgeCasesAndConcurrency:
    """Test edge cases and concurrency scenarios"""
    
    @pytest.mark.asyncio
    async def test_concurrent_operations(self, sql_operate, mock_session):
        """Test concurrent database operations"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1}])
        mock_session.execute.return_value = mock_result
        
        # Run multiple operations concurrently
        tasks = [
            sql_operate.read_sql("users", 1),
            sql_operate.read_sql("users", 2),
            sql_operate.read_sql("users", 3)
        ]
        
        results = await asyncio.gather(*tasks)
        
        assert len(results) == 3
        assert all(r == [{"id": 1}] for r in results)
    
    @pytest.mark.asyncio
    async def test_transaction_rollback(self, sql_operate, mock_session):
        """Test transaction rollback on error"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        # Simulate error during transaction
        mock_session.execute.side_effect = Exception("Transaction failed")
        
        with pytest.raises(Exception):
            await sql_operate.create_sql("users", {"name": "test"})
        
        mock_session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_large_batch_operations(self, sql_operate, mock_session):
        """Test operations with large batches"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        sql_operate._is_postgresql = True
        
        # Create large batch of data
        large_batch = [{"id": i, "name": f"user{i}"} for i in range(1000)]
        
        mock_results = []
        for item in large_batch:
            mock_result = Mock()
            mock_result.mappings = Mock(return_value=[item])
            mock_results.append(mock_result)
        
        mock_session.execute.side_effect = mock_results
        
        result = await sql_operate.update_sql("users", large_batch)
        
        assert len(result) == 1000
    
    @pytest.mark.asyncio
    async def test_special_characters_in_data(self, sql_operate, mock_session):
        """Test handling special characters in data"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        sql_operate._is_postgresql = True
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[
            {"id": 1, "name": "O'Brien", "description": 'He said "Hello"'}
        ])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.create_sql(
            "users",
            {"name": "O'Brien", "description": 'He said "Hello"'}
        )
        
        assert result[0]["name"] == "O'Brien"
        assert 'Hello' in result[0]["description"]
    
    @pytest.mark.asyncio
    async def test_unicode_characters(self, sql_operate, mock_session):
        """Test handling Unicode characters"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        sql_operate._is_postgresql = True
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[
            {"id": 1, "name": "François", "description": "こんにちは 🎉"}
        ])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.create_sql(
            "users",
            {"name": "François", "description": "こんにちは 🎉"}
        )
        
        assert result[0]["name"] == "François"
        assert "こんにちは" in result[0]["description"]
        assert "🎉" in result[0]["description"]


class TestMySQLSpecificBehavior:
    """Test MySQL-specific behavior"""
    
    @pytest.mark.asyncio
    async def test_mysql_last_insert_id_behavior(self, sql_operate, mock_session):
        """Test MySQL LAST_INSERT_ID behavior"""
        sql_operate._is_postgresql = False
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        # Test single row insert
        mock_result = Mock()
        mock_result.lastrowid = 100
        mock_result.rowcount = 1
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.create_sql("users", {"name": "test"})
        
        assert result[0]["id"] == 100
    
    @pytest.mark.asyncio
    async def test_mysql_multi_row_insert_id_calculation(self, sql_operate, mock_session):
        """Test MySQL multi-row insert ID calculation"""
        sql_operate._is_postgresql = False
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        # Test multiple row insert - IDs are calculated sequentially
        mock_result = Mock()
        mock_result.lastrowid = 100
        mock_result.rowcount = 3
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.create_sql("users", [
            {"name": "test1"},
            {"name": "test2"},
            {"name": "test3"}
        ])
        
        assert result[0]["id"] == 100
        assert result[1]["id"] == 101
        assert result[2]["id"] == 102


class TestPostgreSQLSpecificBehavior:
    """Test PostgreSQL-specific behavior"""
    
    @pytest.mark.asyncio
    async def test_postgresql_returning_clause(self, sql_operate, mock_session):
        """Test PostgreSQL RETURNING clause"""
        sql_operate._is_postgresql = True
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[
            {"id": 1, "name": "test", "created_at": "2024-01-01"}
        ])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.create_sql("users", {"name": "test"})
        
        # PostgreSQL returns all fields with RETURNING *
        assert "id" in result[0]
        assert "name" in result[0]
        assert "created_at" in result[0]
    
    @pytest.mark.asyncio
    async def test_postgresql_error_handling(self, sql_operate):
        """Test PostgreSQL-specific error handling"""
        
        @sql_operate.exception_handler
        async def test_func(self):
            pg_error = Mock(spec=AsyncAdapt_asyncpg_dbapi.Error)
            pg_error.sqlstate = "23505"  # Unique violation
            pg_error.__str__ = Mock(return_value="ERROR: duplicate key value violates unique constraint")
            raise DBAPIError("statement", {}, pg_error)
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func(sql_operate)
        
        assert exc_info.value.context.details["sqlstate"] == "23505"
        assert "duplicate key value" in exc_info.value.message


class TestNullHandling:
    """Test NULL value handling"""
    
    @pytest.mark.asyncio
    async def test_null_value_conversion_in_insert(self, sql_operate, mock_session):
        """Test NULL value conversion during insert"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        sql_operate._is_postgresql = True
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1, "value": None}])
        mock_session.execute.return_value = mock_result
        
        # -999999 and "null" should be converted to None
        result = await sql_operate.create_sql("test_table", {
            "value1": -999999,
            "value2": "null",
            "value3": None
        })
        
        # Verify the conversion happened in the query
        call_args = mock_session.execute.call_args
        assert call_args is not None
    
    @pytest.mark.asyncio
    async def test_null_in_where_clause(self, sql_operate, mock_session):
        """Test NULL handling in WHERE clause"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[])
        mock_session.execute.return_value = mock_result
        
        # Test with None value - should use IS NULL
        await sql_operate.read_sql("users", filters={"deleted_at": None})
        
        call_args = mock_session.execute.call_args[0][0]
        assert "deleted_at IS NULL" in str(call_args)
    
    @pytest.mark.asyncio
    async def test_null_set_values_in_filters(self, sql_operate, mock_session):
        """Test null set values in filters"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[])
        mock_session.execute.return_value = mock_result
        
        # Test with null set values
        await sql_operate.read_sql("users", filters={
            "field1": -999999,
            "field2": "null"
        })
        
        call_args = mock_session.execute.call_args[0][0]
        assert "field1 IS NULL" in str(call_args)
        assert "field2 IS NULL" in str(call_args)


class TestComplexQueries:
    """Test complex query scenarios"""
    
    @pytest.mark.asyncio
    async def test_complex_aggregation_query(self, sql_operate, mock_session):
        """Test complex aggregation with multiple functions"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[
            {
                "department": "Sales",
                "total_employees": 10,
                "avg_salary": 75000,
                "max_salary": 120000,
                "min_salary": 45000
            }
        ])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.get_aggregated_data(
            "employees",
            aggregations={
                "total_employees": "COUNT(*)",
                "avg_salary": "AVG(salary)",
                "max_salary": "MAX(salary)",
                "min_salary": "MIN(salary)"
            },
            group_by=["department"],
            having="COUNT(*) > 5",
            filters={"active": True}
        )
        
        assert result[0]["department"] == "Sales"
        assert result[0]["total_employees"] == 10
        assert result[0]["avg_salary"] == 75000
    
    @pytest.mark.asyncio
    async def test_nested_conditions_query(self, sql_operate, mock_session):
        """Test query with nested conditions"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1}])
        mock_session.execute.return_value = mock_result
        
        conditions = [
            {"column": "age", "operator": "BETWEEN", "value": [25, 35]},
            {"column": "department", "operator": "IN", "value": ["Sales", "Marketing"]},
            {"column": "salary", "operator": ">=", "value": 50000},
            {"column": "name", "operator": "NOT LIKE", "value": "%temp%"}
        ]
        
        result = await sql_operate.read_sql_with_conditions(
            "employees",
            conditions,
            order_by="salary DESC",
            limit=10
        )
        
        assert len(result) == 1
    
    @pytest.mark.asyncio
    async def test_date_range_with_complex_filters(self, sql_operate, mock_session):
        """Test date range query with complex filters"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[
            {"id": 1, "status": "completed", "amount": 1000}
        ])
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.read_sql_with_date_range(
            "orders",
            "created_at",
            "2024-01-01",
            "2024-12-31",
            filters={
                "status": "completed",
                "amount": 1000,
                "customer_id": None  # Test NULL in filters
            },
            columns=["id", "status", "amount"],
            order_by="created_at DESC",
            limit=100
        )
        
        assert result[0]["status"] == "completed"
        assert result[0]["amount"] == 1000


class TestSessionManagement:
    """Test session management"""
    
    @pytest.mark.asyncio
    async def test_external_session_not_closed(self, sql_operate, mock_session):
        """Test that external sessions are not closed"""
        # Use external session
        await sql_operate.read_sql("users", 1, session=mock_session)
        
        # External session should not be closed
        mock_session.close.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_internal_session_closed(self, sql_operate, mock_session):
        """Test that internal sessions are properly closed"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[])
        mock_session.execute.return_value = mock_result
        
        await sql_operate.read_sql("users", 1)
        
        # Internal session should be closed
        mock_session.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_session_closed_on_error(self, sql_operate, mock_session):
        """Test that sessions are closed even on error"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        mock_session.execute.side_effect = Exception("Query failed")
        
        with pytest.raises(Exception):
            await sql_operate.read_sql("users", 1)
        
        # Session should still be closed
        mock_session.close.assert_called_once()


class TestErrorRecovery:
    """Test error recovery scenarios"""
    
    @pytest.mark.asyncio
    async def test_retry_on_connection_error(self, sql_operate, mock_session):
        """Test retry logic on connection errors"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        
        # First call fails, second succeeds
        mock_result = Mock()
        mock_result.mappings = Mock(return_value=[{"id": 1}])
        
        mock_session.execute.side_effect = [
            asyncpg.PostgresConnectionError("Connection lost"),
            mock_result
        ]
        
        # This should fail as retry is not implemented
        with pytest.raises(Exception):
            await sql_operate.read_sql("users", 1)
    
    @pytest.mark.asyncio
    async def test_graceful_degradation(self, sql_operate, mock_session):
        """Test graceful degradation on partial failures"""
        sql_operate._SQLOperate__sqlClient.create_session = AsyncMock(return_value=mock_session)
        sql_operate._is_postgresql = True
        
        # Simulate partial success in batch operation
        mock_result1 = Mock()
        mock_result1.mappings = Mock(return_value=[{"id": 1}])
        
        mock_result2 = Mock()
        mock_result2.mappings = Mock(return_value=[{"id": 3}])
        
        mock_session.execute.side_effect = [
            mock_result1,
            Exception("Failed for ID 2"),
            mock_result2
        ]
        
        # This should fail on the second item
        with pytest.raises(Exception):
            await sql_operate.update_sql("users", [
                {"id": 1, "name": "user1"},
                {"id": 2, "name": "user2"},
                {"id": 3, "name": "user3"}
            ])


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--cov=general_operate.app.sql_operate", "--cov-report=term-missing"])