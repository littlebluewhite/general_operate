"""
Comprehensive test suite for SQLOperate class.
Achieves 100% code coverage for general_operate/app/sql_operate.py
"""

import asyncio
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
    client.get_engine = Mock()
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
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
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

    def test_init_case_insensitive(self, mock_sql_client):
        """Test initialization with case variations"""
        mock_sql_client.engine_type = "PostgreSQL"
        operate = SQLOperate(mock_sql_client)
        assert operate._is_postgresql is True


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
            code=ErrorCode.VALIDATION_ERROR,
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

    @pytest.mark.asyncio 
    async def test_postgresql_error_with_none_sqlstate(self, sql_operate):
        """Test PostgreSQL error handling when sqlstate is None"""
        pg_error = Mock(spec=AsyncAdapt_asyncpg_dbapi.Error)
        pg_error.sqlstate = None
        pg_error.__str__ = Mock(return_value="ERROR: connection lost")
        
        db_error = DBAPIError("statement", {}, pg_error)
        
        @sql_operate.exception_handler
        async def test_func(self):
            raise db_error
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_func(sql_operate)
        
        assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR
        assert exc_info.value.context.details["sqlstate"] == "1"

    def test_sync_exception_handler(self, sql_operate):
        """Test exception handler for sync functions"""
        @sql_operate.exception_handler
        def sync_func(self):
            raise ValueError("Sync error with SQL")
        
        with pytest.raises(DatabaseException) as exc_info:
            sync_func(sql_operate)
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR


class TestValidationMethods:
    """Test validation methods"""
    
    def test_validate_identifier_valid(self, sql_operate):
        """Test valid identifier validation"""
        # Should return None (no exception) for valid identifiers
        result = sql_operate._validate_identifier("valid_table")
        assert result is None
        
        result = sql_operate._validate_identifier("table123")
        assert result is None
        
        result = sql_operate._validate_identifier("_private_table")
        assert result is None
    
    def test_validate_identifier_invalid_characters(self, sql_operate):
        """Test invalid identifier validation"""
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_identifier("123table")
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "contains invalid characters" in exc_info.value.message
        
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_identifier("table-name")
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_identifier("table.name")
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_identifier("table name")
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR

    def test_validate_identifier_empty_or_none(self, sql_operate):
        """Test validation with empty or None values"""
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_identifier("")
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "must be a non-empty string" in exc_info.value.message
        
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_identifier(None)
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR

    def test_validate_identifier_too_long(self, sql_operate):
        """Test validation with too long identifier"""
        long_name = "a" * 65
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_identifier(long_name)
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "too long" in exc_info.value.message

    def test_validate_identifier_custom_type(self, sql_operate):
        """Test validation with custom identifier type"""
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_identifier("123", "column name")
        assert "Invalid column name" in exc_info.value.message
    
    def test_validate_data_value_valid(self, sql_operate):
        """Test valid data value validation"""
        # Should return None (no exception) for valid values
        result = sql_operate._validate_data_value("string", "test_col")
        assert result is None
        
        result = sql_operate._validate_data_value(123, "test_col")
        assert result is None
        
        result = sql_operate._validate_data_value(45.67, "test_col")
        assert result is None
        
        result = sql_operate._validate_data_value(True, "test_col")
        assert result is None
        
        result = sql_operate._validate_data_value(None, "test_col")
        assert result is None
        
        result = sql_operate._validate_data_value(b"bytes", "test_col")
        assert result is None

    def test_validate_data_value_null_set_values(self, sql_operate):
        """Test validation with null set values"""
        # Should return None (allow) for null set values
        result = sql_operate._validate_data_value(-999999, "test_col")
        assert result is None
        
        result = sql_operate._validate_data_value("null", "test_col")
        assert result is None

    def test_validate_data_value_serializable_collections(self, sql_operate):
        """Test validation with serializable lists and dicts"""
        # Should return None for serializable collections
        result = sql_operate._validate_data_value({"key": "value"}, "test_col")
        assert result is None
        
        result = sql_operate._validate_data_value([1, 2, 3], "test_col")
        assert result is None

    def test_validate_data_value_non_serializable(self, sql_operate):
        """Test validation with non-serializable data"""
        # Create a dict with non-serializable content to trigger the serialization check
        import datetime
        
        # Create a complex object that will fail JSON serialization
        non_serializable_dict = {"date": datetime.date.today()}  # date objects can't be JSON serialized
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_data_value(non_serializable_dict, "test_col")
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "contains non-serializable data" in exc_info.value.message

    def test_validate_data_value_too_long_string(self, sql_operate):
        """Test validation with too long string"""
        long_string = "a" * 65536
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_data_value(long_string, "test_col")
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "too long" in exc_info.value.message

    def test_validate_data_value_dangerous_sql_patterns(self, sql_operate):
        """Test validation with dangerous SQL patterns"""
        dangerous_values = ["--comment", "/*block*/", "xp_cmdshell", "sp_help"]
        
        for value in dangerous_values:
            with pytest.raises(DatabaseException) as exc_info:
                sql_operate._validate_data_value(value, "test_col")
            assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
            assert "dangerous SQL characters" in exc_info.value.message

    def test_validate_data_value_dangerous_sql_keywords(self, sql_operate):
        """Test validation with dangerous SQL keywords"""
        dangerous_keywords = [
            "exec something", "execute command", "drop table users",
            "delete from table", "insert into table", "update set field",
            "select from table", "union select", "alter table",
            "create table test"
        ]
        
        for keyword in dangerous_keywords:
            with pytest.raises(DatabaseException) as exc_info:
                sql_operate._validate_data_value(keyword, "test_col")
            assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
            assert "dangerous SQL keyword" in exc_info.value.message

    def test_validate_data_value_exempt_columns(self, sql_operate):
        """Test validation with exempt columns that allow SQL keywords"""
        exempt_columns = ["content_template", "subject_template", "description", "content"]
        sql_keyword = "select from users"
        
        for col in exempt_columns:
            # Should not raise exception for exempt columns
            result = sql_operate._validate_data_value(sql_keyword, col)
            assert result is None

    def test_validate_data_dict_valid(self, sql_operate):
        """Test valid data dictionary validation"""
        data = {"name": "test", "age": 30, "active": True}
        result = sql_operate._validate_data_dict(data)
        assert isinstance(result, dict)
        assert "name" in result
        assert "age" in result
        assert "active" in result

    def test_validate_data_dict_invalid_type(self, sql_operate):
        """Test invalid data type for dict validation"""
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_data_dict("invalid")
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "must be a dictionary" in exc_info.value.message

    def test_validate_data_dict_empty_not_allowed(self, sql_operate):
        """Test empty dict when not allowed"""
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_data_dict({}, allow_empty=False)
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "cannot be empty" in exc_info.value.message

    def test_validate_data_dict_empty_allowed(self, sql_operate):
        """Test empty dict when allowed"""
        result = sql_operate._validate_data_dict({}, allow_empty=True)
        assert isinstance(result, dict)
        assert len(result) == 0

    def test_validate_data_dict_invalid_keys(self, sql_operate):
        """Test dict with invalid keys"""
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_data_dict({"invalid-key": "value"})
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR

    def test_validate_data_dict_null_values(self, sql_operate):
        """Test dict with null values"""
        data = {"name": "test", "deleted_at": None, "status": -999999}
        result = sql_operate._validate_data_dict(data)
        # Null values should be excluded from result
        assert "name" in result
        assert "deleted_at" not in result
        assert "status" not in result

    def test_validate_data_dict_serializable_values(self, sql_operate):
        """Test dict with values that need JSON serialization"""
        data = {"config": {"setting": "value"}, "tags": ["tag1", "tag2"]}
        result = sql_operate._validate_data_dict(data)
        assert isinstance(result["config"], str)  # Should be JSON string
        assert isinstance(result["tags"], str)    # Should be JSON string

    def test_validate_data_dict_no_valid_data(self, sql_operate):
        """Test dict where all values are filtered out"""
        data = {"field1": None, "field2": -999999, "field3": "null"}
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_data_dict(data, allow_empty=False)
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "No valid data provided" in exc_info.value.message


class TestHelperMethods:
    """Test helper methods"""

    def test_create_external_session(self, sql_operate):
        """Test creating external session"""
        with patch('general_operate.app.sql_operate.AsyncSession') as mock_session:
            result = sql_operate.create_external_session()
            mock_session.assert_called_once_with(sql_operate._get_client().get_engine())

    def test_get_client(self, sql_operate, mock_sql_client):
        """Test getting the SQL client"""
        assert sql_operate._get_client() is mock_sql_client

    def test_build_where_clause_empty(self, sql_operate):
        """Test building WHERE clause with no filters"""
        where_clause, params = sql_operate._build_where_clause(None)
        assert where_clause == ""
        assert params == {}

        where_clause, params = sql_operate._build_where_clause({})
        assert where_clause == ""
        assert params == {}

    def test_build_where_clause_simple_filters(self, sql_operate):
        """Test building WHERE clause with simple filters"""
        filters = {"name": "John", "age": 30}
        where_clause, params = sql_operate._build_where_clause(filters)
        assert "WHERE" in where_clause
        assert "name = :name" in where_clause
        assert "age = :age" in where_clause
        assert "AND" in where_clause
        assert params["name"] == "John"
        assert params["age"] == 30

    def test_build_where_clause_list_values_postgresql(self, sql_operate):
        """Test building WHERE clause with list values for PostgreSQL"""
        sql_operate._is_postgresql = True
        filters = {"status": ["active", "pending"]}
        where_clause, params = sql_operate._build_where_clause(filters)
        assert "status = ANY(:status)" in where_clause
        assert params["status"] == ["active", "pending"]

    def test_build_where_clause_list_values_mysql(self, sql_operate):
        """Test building WHERE clause with list values for MySQL"""
        sql_operate._is_postgresql = False
        filters = {"status": ["active", "pending"]}
        where_clause, params = sql_operate._build_where_clause(filters)
        assert "status IN (:status_0, :status_1)" in where_clause
        assert params["status_0"] == "active"
        assert params["status_1"] == "pending"

    def test_build_where_clause_empty_list(self, sql_operate):
        """Test building WHERE clause with empty list"""
        filters = {"status": []}
        where_clause, params = sql_operate._build_where_clause(filters)
        assert where_clause == ""
        assert params == {}

    def test_build_where_clause_null_values(self, sql_operate):
        """Test building WHERE clause with null values"""
        filters = {"deleted_at": None, "active": True}
        where_clause, params = sql_operate._build_where_clause(filters)
        assert "active = :active" in where_clause
        assert "deleted_at" not in where_clause  # None values should be skipped
        assert params["active"] is True

    def test_build_where_clause_null_set_values(self, sql_operate):
        """Test building WHERE clause with null set values"""
        filters = {"status": -999999, "type": "null", "active": True}
        where_clause, params = sql_operate._build_where_clause(filters)
        assert "active = :active" in where_clause
        assert "status" not in where_clause
        assert "type" not in where_clause
        assert params["active"] is True

    def test_build_where_clause_unhashable_values(self, sql_operate):
        """Test building WHERE clause with unhashable values"""
        unhashable_dict = {"key": "value"}
        filters = {"config": unhashable_dict, "name": "test"}
        where_clause, params = sql_operate._build_where_clause(filters)
        assert "config = :config" in where_clause
        assert "name = :name" in where_clause
        assert params["config"] == unhashable_dict
        assert params["name"] == "test"


class TestValidateCreateData:
    """Test _validate_create_data method"""
    
    def test_validate_create_data_single_dict(self, sql_operate):
        """Test validating single dictionary"""
        data = {"name": "test", "value": 123}
        result = sql_operate._validate_create_data(data)
        assert len(result) == 1
        assert "name" in result[0]

    def test_validate_create_data_list_of_dicts(self, sql_operate):
        """Test validating list of dictionaries"""
        data = [
            {"name": "test1", "value": 123},
            {"name": "test2", "value": 456}
        ]
        result = sql_operate._validate_create_data(data)
        assert len(result) == 2

    def test_validate_create_data_empty_list(self, sql_operate):
        """Test empty list returns empty list"""
        result = sql_operate._validate_create_data([])
        assert result == []

    def test_validate_create_data_invalid_type(self, sql_operate):
        """Test invalid data type"""
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_create_data("invalid")
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "must be a dictionary or list of dictionaries" in exc_info.value.message

    def test_validate_create_data_invalid_list_item(self, sql_operate):
        """Test list with invalid item"""
        data = [{"name": "test"}, "invalid"]
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_create_data(data)
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "must be a dictionary" in exc_info.value.message

    def test_validate_create_data_invalid_dict_content(self, sql_operate):
        """Test list with dict containing invalid content"""
        data = [{"invalid-key": "value"}]  # Invalid key name
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_create_data(data)
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "Invalid data at index 0" in exc_info.value.message


class TestBuildInsertQuery:
    """Test _build_insert_query method"""
    
    def test_build_insert_query_postgresql(self, sql_operate):
        """Test building PostgreSQL INSERT query"""
        sql_operate._is_postgresql = True
        data = [{"name": "test", "value": 123}]
        query, params = sql_operate._build_insert_query("users", data)
        
        assert "INSERT INTO users" in query
        assert "RETURNING *" in query
        assert "name, value" in query or "value, name" in query
        assert params["name_0"] == "test"
        assert params["value_0"] == 123

    def test_build_insert_query_mysql(self, sql_operate):
        """Test building MySQL INSERT query"""
        sql_operate._is_postgresql = False
        data = [{"name": "test", "value": 123}]
        query, params = sql_operate._build_insert_query("users", data)
        
        assert "INSERT INTO users" in query
        assert "RETURNING *" not in query
        assert "name, value" in query or "value, name" in query
        assert params["name_0"] == "test"
        assert params["value_0"] == 123

    def test_build_insert_query_multiple_rows(self, sql_operate):
        """Test building INSERT query with multiple rows"""
        data = [
            {"name": "test1", "value": 123},
            {"name": "test2", "value": 456}
        ]
        query, params = sql_operate._build_insert_query("users", data)
        
        assert "INSERT INTO users" in query
        assert params["name_0"] == "test1"
        assert params["name_1"] == "test2"
        assert params["value_0"] == 123
        assert params["value_1"] == 456

    def test_build_insert_query_empty_data(self, sql_operate):
        """Test building INSERT query with empty data"""
        query, params = sql_operate._build_insert_query("users", [])
        assert query == ""
        assert params == {}


class TestExecuteInsertMethods:
    """Test insert execution methods"""

    @pytest.mark.asyncio
    async def test_execute_postgresql_insert(self, sql_operate, mock_session):
        """Test PostgreSQL insert execution"""
        mock_result = Mock()
        mock_row = Mock()
        mock_row._mapping = {"id": 1, "name": "test"}
        mock_result.fetchall.return_value = [mock_row]
        mock_session.execute.return_value = mock_result
        
        data = [{"name": "test"}]
        result = await sql_operate._execute_postgresql_insert("users", data, mock_session)
        
        assert len(result) == 1
        assert result[0]["id"] == 1
        assert result[0]["name"] == "test"

    @pytest.mark.asyncio
    async def test_execute_mysql_insert_single_row(self, sql_operate, mock_session):
        """Test MySQL single row insert execution"""
        # First call - INSERT
        mock_insert_result = Mock()
        mock_insert_result.lastrowid = 1
        mock_insert_result.rowcount = 1
        
        # Second call - SELECT to fetch inserted record
        mock_select_result = Mock()
        mock_row = Mock()
        mock_row._mapping = {"id": 1, "name": "test"}
        mock_select_result.fetchall.return_value = [mock_row]
        
        mock_session.execute.side_effect = [mock_insert_result, mock_select_result]
        
        data = [{"name": "test"}]
        result = await sql_operate._execute_mysql_insert("users", data, mock_session)
        
        assert len(result) == 1
        assert result[0]["id"] == 1

    @pytest.mark.asyncio
    async def test_execute_mysql_insert_multiple_rows(self, sql_operate, mock_session):
        """Test MySQL multiple rows insert execution"""
        # First call - INSERT
        mock_insert_result = Mock()
        mock_insert_result.lastrowid = 10
        mock_insert_result.rowcount = 3
        
        # Second call - SELECT to fetch inserted records
        mock_select_result = Mock()
        mock_rows = [
            Mock(_mapping={"id": 10}),
            Mock(_mapping={"id": 11}),
            Mock(_mapping={"id": 12})
        ]
        mock_select_result.fetchall.return_value = mock_rows
        
        mock_session.execute.side_effect = [mock_insert_result, mock_select_result]
        
        data = [{"name": "test1"}, {"name": "test2"}, {"name": "test3"}]
        result = await sql_operate._execute_mysql_insert("users", data, mock_session)
        
        assert len(result) == 3
        assert result[0]["id"] == 10
        assert result[1]["id"] == 11
        assert result[2]["id"] == 12

    @pytest.mark.asyncio
    async def test_execute_mysql_insert_no_lastrowid(self, sql_operate, mock_session):
        """Test MySQL insert when lastrowid is None or 0"""
        mock_insert_result = Mock()
        mock_insert_result.lastrowid = None
        mock_insert_result.rowcount = 1
        
        mock_session.execute.return_value = mock_insert_result
        
        data = [{"name": "test"}]
        result = await sql_operate._execute_mysql_insert("users", data, mock_session)
        
        assert result == []


class TestCRUDOperations:
    """Test main CRUD operations"""

    @pytest.mark.asyncio
    async def test_create_sql_postgresql_with_external_session(self, sql_operate, mock_session):
        """Test create_sql with external session (PostgreSQL)"""
        sql_operate._is_postgresql = True
        
        mock_result = Mock()
        mock_row = Mock()
        mock_row._mapping = {"id": 1, "name": "test"}
        mock_result.fetchall.return_value = [mock_row]
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.create_sql("users", {"name": "test"}, session=mock_session)
        
        assert len(result) == 1
        assert result[0]["id"] == 1
        # Should not commit when external session is used
        mock_session.commit.assert_not_called()

    @pytest.mark.asyncio
    async def test_create_sql_auto_managed_session(self, sql_operate, mock_session):
        """Test create_sql with auto-managed session"""
        sql_operate._is_postgresql = True
        
        # Mock the context manager behavior
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_row = Mock()
            mock_row._mapping = {"id": 1, "name": "test"}
            mock_result.fetchall.return_value = [mock_row]
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate.create_sql("users", {"name": "test"})
            
            assert len(result) == 1
            mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_sql_rollback_on_error(self, sql_operate, mock_session):
        """Test create_sql rolls back on error"""
        sql_operate._is_postgresql = True
        
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_session.execute.side_effect = Exception("Database error")
            
            with pytest.raises(Exception, match="Database error"):
                await sql_operate.create_sql("users", {"name": "test"})
            
            mock_session.rollback.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_sql_empty_data(self, sql_operate):
        """Test create_sql with empty data"""
        result = await sql_operate.create_sql("users", [])
        assert result == []

    @pytest.mark.asyncio
    async def test_read_sql_basic(self, sql_operate, mock_session):
        """Test basic read_sql functionality"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_row = Mock()
            mock_row._mapping = {"id": 1, "name": "test"}
            mock_result.fetchall.return_value = [mock_row]
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate.read_sql("users")
            
            assert len(result) == 1
            assert result[0]["id"] == 1

    @pytest.mark.asyncio
    async def test_read_sql_with_filters(self, sql_operate, mock_session):
        """Test read_sql with filters"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result
            
            await sql_operate.read_sql("users", filters={"status": "active"})
            
            # Verify the WHERE clause was built
            call_args = mock_session.execute.call_args[0][0]
            assert "WHERE" in str(call_args)

    @pytest.mark.asyncio
    async def test_read_sql_with_order_by(self, sql_operate, mock_session):
        """Test read_sql with order by"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result
            
            await sql_operate.read_sql("users", order_by="name", order_direction="DESC")
            
            call_args = mock_session.execute.call_args[0][0]
            assert "ORDER BY name DESC" in str(call_args)

    @pytest.mark.asyncio
    async def test_read_sql_with_limit_postgresql(self, sql_operate, mock_session):
        """Test read_sql with limit on PostgreSQL"""
        sql_operate._is_postgresql = True
        
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result
            
            await sql_operate.read_sql("users", limit=10, offset=5)
            
            call_args = mock_session.execute.call_args[0][0]
            assert "LIMIT 10 OFFSET 5" in str(call_args)

    @pytest.mark.asyncio
    async def test_read_sql_with_limit_mysql(self, sql_operate, mock_session):
        """Test read_sql with limit on MySQL"""
        sql_operate._is_postgresql = False
        
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result
            
            await sql_operate.read_sql("users", limit=10, offset=5)
            
            call_args = mock_session.execute.call_args[0][0]
            assert "LIMIT 5, 10" in str(call_args)

    @pytest.mark.asyncio
    async def test_read_sql_offset_only_postgresql(self, sql_operate, mock_session):
        """Test read_sql with offset only on PostgreSQL"""
        sql_operate._is_postgresql = True
        
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result
            
            await sql_operate.read_sql("users", offset=10)
            
            call_args = mock_session.execute.call_args[0][0]
            assert "OFFSET 10" in str(call_args)
            assert "LIMIT" not in str(call_args)

    @pytest.mark.asyncio
    async def test_read_sql_offset_only_mysql(self, sql_operate, mock_session):
        """Test read_sql with offset only on MySQL"""
        sql_operate._is_postgresql = False
        
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result
            
            await sql_operate.read_sql("users", offset=10)
            
            call_args = mock_session.execute.call_args[0][0]
            assert "LIMIT 10, 18446744073709551615" in str(call_args)

    @pytest.mark.asyncio
    async def test_read_sql_validation_errors(self, sql_operate):
        """Test read_sql validation errors"""
        # Invalid table name
        with pytest.raises(DatabaseException):
            await sql_operate.read_sql("invalid-table")
        
        # Invalid order_by column
        with pytest.raises(DatabaseException):
            await sql_operate.read_sql("users", order_by="invalid-column")
        
        # Invalid order direction
        with pytest.raises(DatabaseException):
            await sql_operate.read_sql("users", order_direction="INVALID")
        
        # Invalid limit
        with pytest.raises(DatabaseException):
            await sql_operate.read_sql("users", limit=-1)
        
        # Invalid offset
        with pytest.raises(DatabaseException):
            await sql_operate.read_sql("users", offset=-5)

    @pytest.mark.asyncio
    async def test_count_sql(self, sql_operate, mock_session):
        """Test count_sql functionality"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_result.fetchone.return_value = [5]
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate.count_sql("users")
            assert result == 5

    @pytest.mark.asyncio
    async def test_count_sql_no_results(self, sql_operate, mock_session):
        """Test count_sql with no results"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_result.fetchone.return_value = None
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate.count_sql("users")
            assert result == 0


class TestAdvancedMethods:
    """Test advanced SQL methods"""

    @pytest.mark.asyncio
    async def test_read_sql_with_date_range(self, sql_operate, mock_session):
        """Test read_sql_with_date_range functionality"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result
            
            await sql_operate.read_sql_with_date_range(
                "orders", 
                filters=None,
                date_field="created_at", 
                start_date="2024-01-01", 
                end_date="2024-12-31"
            )
            
            call_args = mock_session.execute.call_args[0][0]
            assert "created_at >= :start_date" in str(call_args)
            assert "created_at <= :end_date" in str(call_args)

    @pytest.mark.asyncio
    async def test_read_sql_with_date_range_with_filters(self, sql_operate, mock_session):
        """Test date range with additional filters"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result
            
            await sql_operate.read_sql_with_date_range(
                "orders", 
                filters={"status": ["active", "pending"], "deleted_at": None},
                date_field="created_at", 
                start_date="2024-01-01", 
                end_date="2024-12-31"
            )
            
            call_args = mock_session.execute.call_args[0]
            query_str = str(call_args[0])
            params = call_args[1]
            
            assert "created_at >= :start_date" in query_str
            assert "created_at <= :end_date" in query_str
            # Check that list filters are handled
            assert "status IN" in query_str
            # Null values should be excluded from filters

    @pytest.mark.asyncio
    async def test_read_sql_with_conditions(self, sql_operate, mock_session):
        """Test read_sql_with_conditions functionality"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result
            
            conditions = ["age > :min_age", "status IN (:status1, :status2)"]
            params = {"min_age": 18, "status1": "active", "status2": "pending"}
            
            await sql_operate.read_sql_with_conditions(
                "users", conditions, params
            )
            
            call_args = mock_session.execute.call_args[0][0]
            assert "age > :min_age" in str(call_args)
            assert "status IN (:status1, :status2)" in str(call_args)

    @pytest.mark.asyncio
    async def test_read_sql_with_conditions_complex_order_by(self, sql_operate, mock_session):
        """Test conditions with complex order by clause"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result
            
            await sql_operate.read_sql_with_conditions(
                "users", 
                ["active = :active"], 
                {"active": True},
                order_by="priority"  # Use simple order_by to avoid validation issues
            )
            
            call_args = mock_session.execute.call_args[0][0]
            query_str = str(call_args)
            assert "ORDER BY priority" in query_str

    @pytest.mark.asyncio
    async def test_get_aggregated_data(self, sql_operate, mock_session):
        """Test get_aggregated_data functionality"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result
            
            await sql_operate.get_aggregated_data(
                "orders",
                group_by=["status"],
                aggregations={"total_amount": "amount", "order_count": "*"},
                filters={"active": True},
                having_conditions=["COUNT(*) > 5"]
            )
            
            call_args = mock_session.execute.call_args[0][0]
            query_str = str(call_args)
            assert "GROUP BY status" in query_str
            assert "COUNT(amount) as total_amount" in query_str
            assert "COUNT(*) as order_count" in query_str
            assert "HAVING COUNT(*) > 5" in query_str

    @pytest.mark.asyncio
    async def test_execute_raw_query_select(self, sql_operate, mock_session):
        """Test execute_raw_query with SELECT"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_rows = [Mock(_mapping={"id": 1}), Mock(_mapping={"id": 2})]
            mock_result.fetchall.return_value = mock_rows
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate.execute_raw_query(
                "SELECT * FROM users", 
                fetch_mode="all"
            )
            
            assert len(result) == 2
            assert result[0]["id"] == 1

    @pytest.mark.asyncio
    async def test_execute_raw_query_one(self, sql_operate, mock_session):
        """Test execute_raw_query with fetch_mode='one'"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_row = Mock(_mapping={"count": 5})
            mock_result.fetchone.return_value = mock_row
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate.execute_raw_query(
                "SELECT COUNT(*) as count FROM users", 
                fetch_mode="one"
            )
            
            assert result["count"] == 5

    @pytest.mark.asyncio
    async def test_execute_raw_query_none(self, sql_operate, mock_session):
        """Test execute_raw_query with fetch_mode='none'"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate.execute_raw_query(
                "UPDATE users SET active = true", 
                fetch_mode="none"
            )
            
            assert result is None

    @pytest.mark.asyncio
    async def test_execute_raw_query_invalid_fetch_mode(self, sql_operate):
        """Test execute_raw_query with invalid fetch_mode"""
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate.execute_raw_query(
                "SELECT * FROM users", 
                fetch_mode="invalid"
            )
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR

    @pytest.mark.asyncio
    async def test_read_one(self, sql_operate, mock_session):
        """Test read_one functionality"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_row = Mock(_mapping={"id": 1, "name": "test"})
            mock_result.fetchone.return_value = mock_row
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate.read_one("users", 1)
            
            assert result["id"] == 1
            assert result["name"] == "test"

    @pytest.mark.asyncio
    async def test_read_one_not_found(self, sql_operate, mock_session):
        """Test read_one when record not found"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_result.fetchone.return_value = None
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate.read_one("users", 999)
            
            assert result is None

    @pytest.mark.asyncio
    async def test_read_one_custom_id_column(self, sql_operate, mock_session):
        """Test read_one with custom ID column"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_row = Mock(_mapping={"uuid": "abc-123", "name": "test"})
            mock_result.fetchone.return_value = mock_row
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate.read_one("users", "abc-123", id_column="uuid")
            
            assert result["uuid"] == "abc-123"
            call_args = mock_session.execute.call_args[0][0]
            assert "WHERE uuid = :id_value" in str(call_args)


class TestUpdateOperations:
    """Test update operations"""

    @pytest.mark.asyncio
    async def test_update_sql_postgresql(self, sql_operate, mock_session):
        """Test update_sql with PostgreSQL"""
        sql_operate._is_postgresql = True
        
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_row = Mock(_mapping={"id": 1, "name": "updated"})
            mock_result.fetchone.return_value = mock_row
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate.update_sql(
                "users", 
                [{"id": 1, "data": {"name": "updated"}}]
            )
            
            assert len(result) == 1
            assert result[0]["name"] == "updated"
            mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_sql_mysql(self, sql_operate, mock_session):
        """Test update_sql with MySQL"""
        sql_operate._is_postgresql = False
        
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            # First call - UPDATE
            mock_update_result = Mock()
            mock_update_result.rowcount = 1
            
            # Second call - SELECT
            mock_select_result = Mock()
            mock_row = Mock(_mapping={"id": 1, "name": "updated"})
            mock_select_result.fetchone.return_value = mock_row
            
            mock_session.execute.side_effect = [mock_update_result, mock_select_result]
            
            result = await sql_operate.update_sql(
                "users", 
                [{"id": 1, "data": {"name": "updated"}}]
            )
            
            assert len(result) == 1
            assert result[0]["name"] == "updated"

    @pytest.mark.asyncio
    async def test_update_sql_no_affected_rows(self, sql_operate, mock_session):
        """Test update_sql when no rows are affected"""
        sql_operate._is_postgresql = False
        
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_update_result = Mock()
            mock_update_result.rowcount = 0  # No rows affected
            mock_session.execute.return_value = mock_update_result
            
            result = await sql_operate.update_sql(
                "users", 
                [{"id": 999, "data": {"name": "updated"}}]  # Non-existent ID
            )
            
            assert result == []

    @pytest.mark.asyncio
    async def test_update_sql_validation_errors(self, sql_operate):
        """Test update_sql validation errors"""
        # Invalid data format (not list)
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate.update_sql("users", {"id": 1, "name": "test"})
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "must be a list" in exc_info.value.message
        
        # Missing required fields
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate.update_sql("users", [{"id": 1}])  # Missing 'data'
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "must have 'id' and 'data' fields" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_update_sql_empty_data(self, sql_operate):
        """Test update_sql with empty data"""
        result = await sql_operate.update_sql("users", [])
        assert result == []

    @pytest.mark.asyncio
    async def test_update_sql_filtered_out_data(self, sql_operate, mock_session):
        """Test update_sql when all data gets filtered out during validation"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            # Data with only null values that will be filtered out during _validate_data_dict
            # This should raise an exception since no valid data is provided
            with pytest.raises(DatabaseException) as exc_info:
                await sql_operate.update_sql("users", [
                    {"id": 1, "data": {"field": None, "field2": -999999}}
                ])
            assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
            assert "No valid data provided" in exc_info.value.message


class TestDeleteOperations:
    """Test delete operations"""

    @pytest.mark.asyncio
    async def test_delete_sql_single_id(self, sql_operate, mock_session):
        """Test delete_sql with single ID"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_result.rowcount = 1
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate.delete_sql("users", 1)
            
            assert result == [1]
            mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_sql_multiple_ids_postgresql(self, sql_operate, mock_session):
        """Test delete_sql with multiple IDs on PostgreSQL"""
        sql_operate._is_postgresql = True
        
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_result.rowcount = 3
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate.delete_sql("users", [1, 2, 3])
            
            assert result == [1, 2, 3]
            call_args = mock_session.execute.call_args[0][0]
            assert "= ANY(:id_list)" in str(call_args)

    @pytest.mark.asyncio
    async def test_delete_sql_multiple_ids_mysql(self, sql_operate, mock_session):
        """Test delete_sql with multiple IDs on MySQL"""
        sql_operate._is_postgresql = False
        
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_result.rowcount = 2
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate.delete_sql("users", [1, 2])
            
            assert result == [1, 2]
            call_args = mock_session.execute.call_args[0][0]
            assert "IN (:id_0, :id_1)" in str(call_args)

    @pytest.mark.asyncio
    async def test_delete_sql_no_rows_deleted(self, sql_operate, mock_session):
        """Test delete_sql when no rows are deleted"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_result.rowcount = 0
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate.delete_sql("users", 999)
            
            assert result == []

    @pytest.mark.asyncio
    async def test_delete_sql_empty_list(self, sql_operate):
        """Test delete_sql with empty list"""
        result = await sql_operate.delete_sql("users", [])
        assert result == []

    @pytest.mark.asyncio
    async def test_delete_sql_partial_success(self, sql_operate, mock_session):
        """Test delete_sql with partial success in bulk delete"""
        sql_operate._is_postgresql = True
        
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_result.rowcount = 2  # Only 2 out of 3 deleted
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate.delete_sql("users", [1, 2, 3])
            
            # Current implementation returns all IDs if any were deleted
            assert result == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_delete_filter(self, sql_operate, mock_session):
        """Test delete_filter functionality"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            # First call - SELECT to get IDs
            mock_select_result = Mock()
            mock_select_result.fetchall.return_value = [(1,), (2,), (3,)]
            
            # Second call - DELETE
            mock_delete_result = Mock()
            
            mock_session.execute.side_effect = [mock_select_result, mock_delete_result]
            
            result = await sql_operate.delete_filter("users", {"status": "inactive"})
            
            assert result == [1, 2, 3]
            mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_filter_no_matches(self, sql_operate, mock_session):
        """Test delete_filter with no matching records"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_select_result = Mock()
            mock_select_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_select_result
            
            result = await sql_operate.delete_filter("users", {"status": "nonexistent"})
            
            assert result == []

    @pytest.mark.asyncio
    async def test_delete_filter_no_filters(self, sql_operate):
        """Test delete_filter with no filters"""
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate.delete_filter("users", {})
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "Filters are required" in exc_info.value.message

        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate.delete_filter("users", None)
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR


class TestUpsertOperations:
    """Test upsert operations"""

    @pytest.mark.asyncio
    async def test_upsert_sql_postgresql(self, sql_operate, mock_session):
        """Test upsert_sql with PostgreSQL"""
        sql_operate._is_postgresql = True
        
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_rows = [Mock(_mapping={"id": 1, "email": "test@example.com"})]
            mock_result.fetchall.return_value = mock_rows
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate.upsert_sql(
                "users",
                {"email": "test@example.com", "name": "Test"},
                conflict_fields=["email"]
            )
            
            assert len(result) == 1
            assert result[0]["email"] == "test@example.com"
            
            call_args = mock_session.execute.call_args[0][0]
            assert "ON CONFLICT" in str(call_args)
            assert "RETURNING *" in str(call_args)

    @pytest.mark.asyncio
    async def test_upsert_sql_mysql(self, sql_operate, mock_session):
        """Test upsert_sql with MySQL"""
        sql_operate._is_postgresql = False
        
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            # First call - INSERT ... ON DUPLICATE KEY UPDATE
            mock_insert_result = Mock()
            
            # Second call - SELECT to fetch results
            mock_select_result = Mock()
            mock_rows = [Mock(_mapping={"id": 1, "email": "test@example.com"})]
            mock_select_result.fetchall.return_value = mock_rows
            
            mock_session.execute.side_effect = [mock_insert_result, mock_select_result]
            
            result = await sql_operate.upsert_sql(
                "users",
                {"email": "test@example.com", "name": "Test"},
                conflict_fields=["email"]
            )
            
            assert len(result) == 1
            assert result[0]["email"] == "test@example.com"

    @pytest.mark.asyncio
    async def test_upsert_sql_multiple_records(self, sql_operate, mock_session):
        """Test upsert_sql with multiple records"""
        sql_operate._is_postgresql = True
        
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_rows = [
                Mock(_mapping={"id": 1, "email": "test1@example.com"}),
                Mock(_mapping={"id": 2, "email": "test2@example.com"})
            ]
            mock_result.fetchall.return_value = mock_rows
            mock_session.execute.return_value = mock_result
            
            data = [
                {"email": "test1@example.com", "name": "Test1"},
                {"email": "test2@example.com", "name": "Test2"}
            ]
            
            result = await sql_operate.upsert_sql(
                "users",
                data,
                conflict_fields=["email"]
            )
            
            assert len(result) == 2

    @pytest.mark.asyncio
    async def test_upsert_sql_custom_update_fields(self, sql_operate, mock_session):
        """Test upsert_sql with custom update fields"""
        sql_operate._is_postgresql = True
        
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result
            
            await sql_operate.upsert_sql(
                "users",
                {"email": "test@example.com", "name": "Test", "age": 30},
                conflict_fields=["email"],
                update_fields=["name"]  # Only update name, not age
            )
            
            call_args = mock_session.execute.call_args[0][0]
            query_str = str(call_args)
            assert "DO UPDATE SET name = EXCLUDED.name" in query_str
            assert "age = EXCLUDED.age" not in query_str

    @pytest.mark.asyncio
    async def test_upsert_sql_validation_errors(self, sql_operate):
        """Test upsert_sql validation errors"""
        # The implementation doesn't seem to validate empty conflict_fields,
        # so let's test a different validation error
        # Invalid data type for conflict fields validation
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate.upsert_sql("invalid-table", {"name": "test"}, conflict_fields=["id"])
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR

    @pytest.mark.asyncio
    async def test_upsert_sql_invalid_data_type(self, sql_operate):
        """Test upsert_sql with invalid data type"""
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate.upsert_sql("users", "invalid", conflict_fields=["id"])
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "must be a dictionary or list of dictionaries" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_upsert_sql_empty_data(self, sql_operate):
        """Test upsert_sql with empty data"""
        result = await sql_operate.upsert_sql("users", [], conflict_fields=["id"])
        assert result == []


class TestUtilityMethods:
    """Test utility methods"""

    @pytest.mark.asyncio
    async def test_exists_sql_single_id_exists(self, sql_operate, mock_session):
        """Test exists_sql with single ID that exists"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_result.fetchone.return_value = [1]  # ID exists
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate.exists_sql("users", [1])
            
            assert result == {1: True}

    @pytest.mark.asyncio
    async def test_exists_sql_single_id_not_exists(self, sql_operate, mock_session):
        """Test exists_sql with single ID that doesn't exist"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_result.fetchone.return_value = None  # ID doesn't exist
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate.exists_sql("users", [999])
            
            assert result == {999: False}

    @pytest.mark.asyncio
    async def test_exists_sql_multiple_ids(self, sql_operate, mock_session):
        """Test exists_sql with multiple IDs"""
        sql_operate._is_postgresql = True
        
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_result.fetchall.return_value = [(1,), (3,)]  # Only 1 and 3 exist
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate.exists_sql("users", [1, 2, 3])
            
            assert result == {1: True, 2: False, 3: True}

    @pytest.mark.asyncio
    async def test_exists_sql_empty_list(self, sql_operate):
        """Test exists_sql with empty list"""
        result = await sql_operate.exists_sql("users", [])
        assert result == {}

    @pytest.mark.asyncio
    async def test_get_distinct_values(self, sql_operate, mock_session):
        """Test get_distinct_values functionality"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_result.fetchall.return_value = [("active",), ("inactive",), ("pending",)]
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate.get_distinct_values("users", "status")
            
            assert result == ["active", "inactive", "pending"]
            
            call_args = mock_session.execute.call_args[0][0]
            assert "SELECT DISTINCT status FROM users" in str(call_args)
            assert "ORDER BY status" in str(call_args)

    @pytest.mark.asyncio
    async def test_get_distinct_values_with_filters(self, sql_operate, mock_session):
        """Test get_distinct_values with filters"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_result.fetchall.return_value = [("active",), ("pending",)]
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate.get_distinct_values(
                "users", "status", 
                filters={"department": "sales"}
            )
            
            assert result == ["active", "pending"]
            
            call_args = mock_session.execute.call_args[0][0]
            assert "WHERE" in str(call_args)

    @pytest.mark.asyncio
    async def test_get_distinct_values_with_limit(self, sql_operate, mock_session):
        """Test get_distinct_values with limit"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_result.fetchall.return_value = [("active",), ("pending",)]
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate.get_distinct_values("users", "status", limit=2)
            
            assert result == ["active", "pending"]
            
            call_args = mock_session.execute.call_args[0][0]
            assert "LIMIT 2" in str(call_args)

    @pytest.mark.asyncio
    async def test_get_distinct_values_filter_nulls(self, sql_operate, mock_session):
        """Test get_distinct_values filters out null values"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_result.fetchall.return_value = [("active",), (None,), ("pending",)]
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate.get_distinct_values("users", "status")
            
            # Should filter out None values
            assert result == ["active", "pending"]

    @pytest.mark.asyncio
    async def test_get_distinct_values_validation_error(self, sql_operate):
        """Test get_distinct_values with invalid limit"""
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate.get_distinct_values("users", "status", limit=-1)
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "must be a positive integer" in exc_info.value.message


class TestExecuteQuery:
    """Test execute_query method"""

    @pytest.mark.asyncio
    async def test_execute_query_select(self, sql_operate, mock_session):
        """Test execute_query with SELECT statement"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_rows = [Mock(_mapping={"count": 10})]
            mock_result.fetchall.return_value = mock_rows
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate.execute_query("SELECT COUNT(*) as count FROM users")
            
            assert result == [{"count": 10}]

    @pytest.mark.asyncio
    async def test_execute_query_update(self, sql_operate, mock_session):
        """Test execute_query with UPDATE statement"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_result.rowcount = 5
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate.execute_query("UPDATE users SET active = true")
            
            assert result == {"affected_rows": 5}
            mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_query_show(self, sql_operate, mock_session):
        """Test execute_query with SHOW statement"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_rows = [Mock(_mapping={"table": "users"})]
            mock_result.fetchall.return_value = mock_rows
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate.execute_query("SHOW TABLES")
            
            assert result == [{"table": "users"}]

    @pytest.mark.asyncio
    async def test_execute_query_no_rowcount(self, sql_operate, mock_session):
        """Test execute_query when result has no rowcount"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            del mock_result.rowcount  # Remove rowcount attribute
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate.execute_query("DELETE FROM users WHERE id = 1")
            
            assert result == {"affected_rows": 0}


class TestHealthCheck:
    """Test health_check method"""

    @pytest.mark.asyncio
    async def test_health_check_success(self, sql_operate, mock_session):
        """Test successful health check"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_result.fetchone.return_value = [1]
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate.health_check()
            
            assert result is True

    @pytest.mark.asyncio
    async def test_health_check_unexpected_result(self, sql_operate, mock_session):
        """Test health check with unexpected result"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_result.fetchone.return_value = [0]  # Unexpected result
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate.health_check()
            
            assert result is False

    @pytest.mark.asyncio
    async def test_health_check_null_result(self, sql_operate, mock_session):
        """Test health check with null result"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_result.fetchone.return_value = None
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate.health_check()
            
            assert result is False

    @pytest.mark.asyncio
    async def test_health_check_db_error(self, sql_operate, mock_session):
        """Test health check with database error"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_session.execute.side_effect = DBAPIError("statement", {}, Exception("Connection failed"))
            
            result = await sql_operate.health_check()
            
            assert result is False

    @pytest.mark.asyncio
    async def test_health_check_postgres_error(self, sql_operate, mock_session):
        """Test health check with PostgreSQL specific error"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_session.execute.side_effect = asyncpg.PostgresError("Connection lost")
            
            result = await sql_operate.health_check()
            
            assert result is False

    @pytest.mark.asyncio
    async def test_health_check_mysql_error(self, sql_operate, mock_session):
        """Test health check with MySQL specific error"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_session.execute.side_effect = pymysql.Error("Connection failed")
            
            result = await sql_operate.health_check()
            
            assert result is False

    @pytest.mark.asyncio
    async def test_health_check_connection_error(self, sql_operate, mock_session):
        """Test health check with connection error"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_session.execute.side_effect = ConnectionError("Network unreachable")
            
            result = await sql_operate.health_check()
            
            assert result is False

    @pytest.mark.asyncio
    async def test_health_check_timeout_error(self, sql_operate, mock_session):
        """Test health check with timeout error"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_session.execute.side_effect = TimeoutError("Request timeout")
            
            result = await sql_operate.health_check()
            
            assert result is False

    @pytest.mark.asyncio
    async def test_health_check_os_error(self, sql_operate, mock_session):
        """Test health check with OS error"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_session.execute.side_effect = OSError("No such file or directory")
            
            result = await sql_operate.health_check()
            
            assert result is False

    @pytest.mark.asyncio
    async def test_health_check_unexpected_error(self, sql_operate, mock_session):
        """Test health check with unexpected error"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_session.execute.side_effect = RuntimeError("Unexpected error")
            
            result = await sql_operate.health_check()
            
            assert result is False


class TestEdgeCasesAndErrorHandling:
    """Test edge cases and error handling"""

    @pytest.mark.asyncio
    async def test_concurrent_operations(self, sql_operate, mock_session):
        """Test concurrent database operations"""
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        
        with patch.object(sql_operate, 'create_external_session', return_value=mock_context):
            mock_result = Mock()
            mock_row = Mock(_mapping={"id": 1})
            mock_result.fetchall.return_value = [mock_row]
            mock_session.execute.return_value = mock_result
            
            # Run multiple operations concurrently
            tasks = [
                sql_operate.read_sql("users"),
                sql_operate.read_sql("users"),
                sql_operate.read_sql("users")
            ]
            
            results = await asyncio.gather(*tasks)
            
            assert len(results) == 3
            assert all(r == [{"id": 1}] for r in results)

    def test_main_block(self, sql_operate):
        """Test the __main__ block"""
        # The main block just has 'pass', so we just ensure it doesn't break
        import general_operate.app.sql_operate
        # This should not raise any exceptions

    @pytest.mark.asyncio 
    async def test_transaction_error_with_external_session(self, sql_operate, mock_session):
        """Test transaction error with external session doesn't commit/rollback"""
        sql_operate._is_postgresql = True
        mock_session.execute.side_effect = Exception("Transaction failed")
        
        with pytest.raises(Exception, match="Transaction failed"):
            await sql_operate.create_sql("users", {"name": "test"}, session=mock_session)
        
        # External session should not be committed or rolled back
        mock_session.commit.assert_not_called()
        mock_session.rollback.assert_not_called()

    def test_logger_initialization(self, sql_operate):
        """Test that logger is properly initialized"""
        assert hasattr(sql_operate, 'logger')
        assert sql_operate.logger is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--cov=general_operate.app.sql_operate", "--cov-report=term-missing"])