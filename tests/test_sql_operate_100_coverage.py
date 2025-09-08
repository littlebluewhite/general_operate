"""
Comprehensive tests for sql_operate.py to achieve 100% coverage.
Tests all methods, exception handlers, and edge cases.
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch, call
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy.exc import DBAPIError, IntegrityError, OperationalError, ProgrammingError
from sqlalchemy import Row
from sqlalchemy.engine.result import Result

from general_operate.app.sql_operate import SQLOperate
from general_operate.core.exceptions import DatabaseException, ErrorCode, ErrorContext


@pytest.fixture
def mock_client():
    """Mock SQL client."""
    mock = AsyncMock()
    return mock


@pytest.fixture 
def mock_postgresql_client():
    """Mock PostgreSQL client."""
    mock = AsyncMock()
    # Mock the engine_type to return PostgreSQL
    mock.engine_type = 'PostgreSQL'
    return mock


@pytest.fixture
def mock_mysql_client():
    """Mock MySQL client."""
    mock = AsyncMock()
    # Mock the engine_type to return MySQL  
    mock.engine_type = 'MySQL'
    return mock


@pytest.fixture
def sql_operate_postgresql(mock_postgresql_client):
    """SQLOperate instance with PostgreSQL client."""
    return SQLOperate(mock_postgresql_client)


@pytest.fixture
def sql_operate_mysql(mock_mysql_client):
    """SQLOperate instance with MySQL client."""
    return SQLOperate(mock_mysql_client)


@pytest.fixture
def sql_operate(mock_client):
    """SQLOperate instance with generic client."""
    return SQLOperate(mock_client)


class TestSQLOperateInit:
    """Test SQLOperate initialization."""
    
    def test_init_postgresql(self, mock_postgresql_client):
        """Test SQLOperate initialization with PostgreSQL."""
        sql_op = SQLOperate(mock_postgresql_client)
        
        assert sql_op._SQLOperate__sqlClient is mock_postgresql_client
        assert sql_op._SQLOperate__exc is DatabaseException
        assert sql_op._is_postgresql is True
        assert hasattr(sql_op, 'logger')
        assert sql_op.null_set == {-999999, 'null'}
    
    def test_init_mysql(self, mock_mysql_client):
        """Test SQLOperate initialization with MySQL."""
        sql_op = SQLOperate(mock_mysql_client)
        
        assert sql_op._SQLOperate__sqlClient is mock_mysql_client
        assert sql_op._is_postgresql is False


class TestSQLOperateExceptionHandler:
    """Test exception handler decorator and all exception branches."""
    
    @pytest.mark.asyncio
    async def test_exception_handler_dbapi_error(self, sql_operate):
        """Test exception handler for DBAPIError."""
        
        @sql_operate.exception_handler
        async def test_method(self):
            raise DBAPIError("statement", "params", "orig")
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_method(sql_operate)
        
        assert exc_info.value.code == ErrorCode.DB_CONNECTION_ERROR
        assert "Database API error" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_exception_handler_integrity_error(self, sql_operate):
        """Test exception handler for IntegrityError."""
        
        @sql_operate.exception_handler
        async def test_method(self):
            raise IntegrityError("statement", "params", "orig")
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_method(sql_operate)
        
        assert exc_info.value.code == ErrorCode.DB_INTEGRITY_ERROR
        assert "Database integrity constraint violation" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_exception_handler_operational_error_postgresql(self, sql_operate_postgresql):
        """Test exception handler for OperationalError with PostgreSQL."""
        
        @sql_operate_postgresql.exception_handler
        async def test_method(self):
            raise OperationalError("statement", "params", "orig")
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_method(sql_operate_postgresql)
        
        assert exc_info.value.code == ErrorCode.DB_CONNECTION_ERROR
        assert "PostgreSQL operational error" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_exception_handler_operational_error_mysql(self, sql_operate_mysql):
        """Test exception handler for OperationalError with MySQL."""
        
        @sql_operate_mysql.exception_handler
        async def test_method(self):
            raise OperationalError("statement", "params", "orig")
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_method(sql_operate_mysql)
        
        assert exc_info.value.code == ErrorCode.DB_CONNECTION_ERROR
        assert "MySQL operational error" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_exception_handler_programming_error_postgresql(self, sql_operate_postgresql):
        """Test exception handler for ProgrammingError with PostgreSQL."""
        
        @sql_operate_postgresql.exception_handler
        async def test_method(self):
            raise ProgrammingError("statement", "params", "orig")
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_method(sql_operate_postgresql)
        
        assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR
        assert "PostgreSQL programming error" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_exception_handler_programming_error_mysql(self, sql_operate_mysql):
        """Test exception handler for ProgrammingError with MySQL."""
        
        @sql_operate_mysql.exception_handler
        async def test_method(self):
            raise ProgrammingError("statement", "params", "orig")
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_method(sql_operate_mysql)
        
        assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR
        assert "MySQL programming error" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_exception_handler_value_error_with_sql(self, sql_operate):
        """Test exception handler for ValueError with SQL in message."""
        
        @sql_operate.exception_handler
        async def test_method(self):
            raise ValueError("SQL validation failed")
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_method(sql_operate)
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "SQL operation validation error" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_exception_handler_general_operate_exception_passthrough(self, sql_operate):
        """Test exception handler passes through GeneralOperateException."""
        
        original_exc = DatabaseException(
            code=ErrorCode.DB_QUERY_ERROR,
            message="Original error"
        )
        
        @sql_operate.exception_handler
        async def test_method(self):
            raise original_exc
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_method(sql_operate)
        
        assert exc_info.value is original_exc
    
    @pytest.mark.asyncio
    async def test_exception_handler_unknown_error(self, sql_operate):
        """Test exception handler for unknown error."""
        
        @sql_operate.exception_handler
        async def test_method(self):
            raise RuntimeError("Unknown error")
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_method(sql_operate)
        
        assert exc_info.value.code == ErrorCode.UNKNOWN_ERROR
        assert "Operation error" in str(exc_info.value)
    
    def test_exception_handler_sync_wrapper(self, sql_operate):
        """Test exception handler sync wrapper."""
        
        @sql_operate.exception_handler
        def sync_method(self):
            raise ValueError("Sync error SQL")
        
        with pytest.raises(DatabaseException) as exc_info:
            sync_method(sql_operate)
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR


class TestSQLOperateValidateIdentifier:
    """Test _validate_identifier method."""
    
    def test_validate_identifier_valid_table_name(self, sql_operate):
        """Test valid table name validation."""
        sql_operate._validate_identifier("users", "table")
        sql_operate._validate_identifier("user_data", "table")
        sql_operate._validate_identifier("UserData", "table")
    
    def test_validate_identifier_invalid_empty(self, sql_operate):
        """Test invalid empty identifier."""
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_identifier("", "table")
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
    
    def test_validate_identifier_invalid_none(self, sql_operate):
        """Test invalid None identifier."""
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_identifier(None, "table")
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
    
    def test_validate_identifier_invalid_pattern(self, sql_operate):
        """Test invalid identifier pattern."""
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_identifier("select * from", "table")
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
    
    def test_validate_identifier_invalid_injection(self, sql_operate):
        """Test SQL injection prevention."""
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_identifier("users; DROP TABLE", "table")
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR


class TestSQLOperateValidateDataValue:
    """Test _validate_data_value method."""
    
    def test_validate_data_value_valid_string(self, sql_operate):
        """Test valid string value."""
        result = sql_operate._validate_data_value("test", "name")
        assert result == "test"
    
    def test_validate_data_value_valid_integer(self, sql_operate):
        """Test valid integer value."""
        result = sql_operate._validate_data_value(42, "age")
        assert result == 42
    
    def test_validate_data_value_valid_float(self, sql_operate):
        """Test valid float value."""
        result = sql_operate._validate_data_value(3.14, "price")
        assert result == 3.14
    
    def test_validate_data_value_valid_decimal(self, sql_operate):
        """Test valid decimal value."""
        decimal_val = Decimal("10.50")
        result = sql_operate._validate_data_value(decimal_val, "amount")
        assert result == decimal_val
    
    def test_validate_data_value_valid_datetime(self, sql_operate):
        """Test valid datetime value."""
        dt = datetime.now(timezone.utc)
        result = sql_operate._validate_data_value(dt, "created_at")
        assert result == dt
    
    def test_validate_data_value_valid_boolean(self, sql_operate):
        """Test valid boolean value."""
        result = sql_operate._validate_data_value(True, "is_active")
        assert result is True
    
    def test_validate_data_value_valid_none(self, sql_operate):
        """Test valid None value."""
        result = sql_operate._validate_data_value(None, "optional_field")
        assert result is None
    
    def test_validate_data_value_invalid_type(self, sql_operate):
        """Test invalid data type."""
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_data_value({"invalid": "dict"}, "data")
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
    
    def test_validate_data_value_sql_injection_string(self, sql_operate):
        """Test SQL injection prevention in string values."""
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_data_value("'; DROP TABLE users; --", "name")
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
    
    def test_validate_data_value_sql_injection_keyword(self, sql_operate):
        """Test SQL keyword detection."""
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_data_value("DROP", "command")
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
    
    def test_validate_data_value_exempt_column(self, sql_operate):
        """Test exempt column allows SQL keywords."""
        # Should not raise exception for exempt columns
        result = sql_operate._validate_data_value("SELECT", "query")
        assert result == "SELECT"
        
        result = sql_operate._validate_data_value("DELETE", "action")
        assert result == "DELETE"
    
    def test_validate_data_value_conversion_error(self, sql_operate):
        """Test value conversion error handling."""
        class BadValue:
            def __str__(self):
                raise ValueError("Cannot convert")
        
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_data_value(BadValue(), "bad_field")
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR


class TestSQLOperateValidateDataDict:
    """Test _validate_data_dict method."""
    
    def test_validate_data_dict_valid_single(self, sql_operate):
        """Test valid single data dict."""
        data = {"name": "John", "age": 30}
        result = sql_operate._validate_data_dict(data, "create")
        
        assert result == [{"name": "John", "age": 30}]
    
    def test_validate_data_dict_valid_list(self, sql_operate):
        """Test valid data dict list."""
        data = [{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]
        result = sql_operate._validate_data_dict(data, "create")
        
        assert len(result) == 2
        assert result[0]["name"] == "John"
        assert result[1]["name"] == "Jane"
    
    def test_validate_data_dict_empty_not_allowed(self, sql_operate):
        """Test empty data dict when not allowed."""
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_data_dict({}, "create", allow_empty=False)
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
    
    def test_validate_data_dict_empty_allowed(self, sql_operate):
        """Test empty data dict when allowed."""
        result = sql_operate._validate_data_dict({}, "update", allow_empty=True)
        assert result == [{}]
    
    def test_validate_data_dict_invalid_type(self, sql_operate):
        """Test invalid data dict type."""
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_data_dict("invalid", "create")
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
    
    def test_validate_data_dict_invalid_value(self, sql_operate):
        """Test data dict with invalid value."""
        data = {"name": "John", "bad_field": {"invalid": "dict"}}
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_data_dict(data, "create")
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR


class TestSQLOperateBuildWhereClause:
    """Test _build_where_clause method."""
    
    def test_build_where_clause_simple(self, sql_operate):
        """Test simple where clause building."""
        filters = {"id": 1, "name": "John"}
        where_clause, params = sql_operate._build_where_clause(filters)
        
        assert "id = :id" in where_clause
        assert "name = :name" in where_clause
        assert " AND " in where_clause
        assert params == {"id": 1, "name": "John"}
    
    def test_build_where_clause_list_values(self, sql_operate):
        """Test where clause with list values."""
        filters = {"id": [1, 2, 3], "status": ["active", "pending"]}
        where_clause, params = sql_operate._build_where_clause(filters)
        
        assert "id IN (" in where_clause
        assert "status IN (" in where_clause
        assert ":id_0" in where_clause and ":id_1" in where_clause and ":id_2" in where_clause
        assert params["id_0"] == 1 and params["id_1"] == 2 and params["id_2"] == 3
    
    def test_build_where_clause_empty_filters(self, sql_operate):
        """Test where clause with empty filters."""
        where_clause, params = sql_operate._build_where_clause({})
        
        assert where_clause == ""
        assert params == {}
    
    def test_build_where_clause_none_filters(self, sql_operate):
        """Test where clause with None filters."""
        where_clause, params = sql_operate._build_where_clause(None)
        
        assert where_clause == ""
        assert params == {}
    
    def test_build_where_clause_empty_list_value(self, sql_operate):
        """Test where clause with empty list value."""
        filters = {"id": []}
        where_clause, params = sql_operate._build_where_clause(filters)
        
        assert where_clause == ""
        assert params == {}


class TestSQLOperateCreateSQL:
    """Test create_sql method."""
    
    @pytest.mark.asyncio
    async def test_create_sql_postgresql_single(self, sql_operate_postgresql, mock_postgresql_client):
        """Test create_sql with PostgreSQL single record."""
        # Mock the result
        mock_result = AsyncMock()
        mock_row = MagicMock()
        mock_row._asdict.return_value = {"id": 1, "name": "John"}
        mock_result.fetchall.return_value = [mock_row]
        
        # Mock session and execute
        mock_session = AsyncMock()
        mock_session.execute.return_value = mock_result
        mock_postgresql_client.get_session.return_value.__aenter__.return_value = mock_session
        
        data = {"name": "John", "age": 30}
        result = await sql_operate_postgresql.create_sql("users", data)
        
        assert len(result) == 1
        assert result[0]["id"] == 1
        assert result[0]["name"] == "John"
        
        # Verify query execution
        mock_session.execute.assert_called_once()
        call_args = mock_session.execute.call_args[0]
        assert "INSERT INTO users" in str(call_args[0])
        assert "RETURNING *" in str(call_args[0])
    
    @pytest.mark.asyncio
    async def test_create_sql_mysql_single(self, sql_operate_mysql, mock_mysql_client):
        """Test create_sql with MySQL single record."""
        # Mock the insert result
        mock_insert_result = AsyncMock()
        mock_insert_result.lastrowid = 1
        mock_insert_result.rowcount = 1
        
        # Mock the select result
        mock_select_result = AsyncMock()
        mock_row = MagicMock()
        mock_row._asdict.return_value = {"id": 1, "name": "John"}
        mock_select_result.fetchall.return_value = [mock_row]
        
        # Mock session
        mock_session = AsyncMock()
        mock_session.execute.side_effect = [mock_insert_result, mock_select_result]
        mock_mysql_client.get_session.return_value.__aenter__.return_value = mock_session
        
        data = {"name": "John", "age": 30}
        result = await sql_operate_mysql.create_sql("users", data)
        
        assert len(result) == 1
        assert result[0]["id"] == 1
        assert result[0]["name"] == "John"
        
        # Verify two queries were executed (insert + select)
        assert mock_session.execute.call_count == 2
    
    @pytest.mark.asyncio
    async def test_create_sql_batch_postgresql(self, sql_operate_postgresql, mock_postgresql_client):
        """Test create_sql with PostgreSQL batch insert."""
        # Mock the result
        mock_result = AsyncMock()
        mock_rows = [
            MagicMock(_asdict=lambda: {"id": 1, "name": "John"}),
            MagicMock(_asdict=lambda: {"id": 2, "name": "Jane"})
        ]
        mock_result.fetchall.return_value = mock_rows
        
        # Mock session
        mock_session = AsyncMock()
        mock_session.execute.return_value = mock_result
        mock_postgresql_client.get_session.return_value.__aenter__.return_value = mock_session
        
        data = [{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]
        result = await sql_operate_postgresql.create_sql("users", data)
        
        assert len(result) == 2
        assert result[0]["name"] == "John"
        assert result[1]["name"] == "Jane"
    
    @pytest.mark.asyncio
    async def test_create_sql_invalid_data(self, sql_operate):
        """Test create_sql with invalid data."""
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate.create_sql("users", "invalid")
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
    
    @pytest.mark.asyncio
    async def test_create_sql_with_session(self, sql_operate_postgresql):
        """Test create_sql with provided session."""
        mock_session = AsyncMock()
        mock_result = AsyncMock()
        mock_row = MagicMock()
        mock_row._asdict.return_value = {"id": 1, "name": "John"}
        mock_result.fetchall.return_value = [mock_row]
        mock_session.execute.return_value = mock_result
        
        data = {"name": "John", "age": 30}
        result = await sql_operate_postgresql.create_sql("users", data, session=mock_session)
        
        assert len(result) == 1
        assert result[0]["id"] == 1


class TestSQLOperateReadSQL:
    """Test read_sql method."""
    
    @pytest.mark.asyncio
    async def test_read_sql_simple(self, sql_operate, mock_client):
        """Test simple read_sql."""
        # Mock the result
        mock_result = AsyncMock()
        mock_rows = [
            MagicMock(_asdict=lambda: {"id": 1, "name": "John"}),
            MagicMock(_asdict=lambda: {"id": 2, "name": "Jane"})
        ]
        mock_result.fetchall.return_value = mock_rows
        
        # Mock session
        mock_session = AsyncMock()
        mock_session.execute.return_value = mock_result
        mock_client.get_session.return_value.__aenter__.return_value = mock_session
        
        result = await sql_operate.read_sql("users")
        
        assert len(result) == 2
        assert result[0]["name"] == "John"
        assert result[1]["name"] == "Jane"
        
        # Verify query
        mock_session.execute.assert_called_once()
        call_args = mock_session.execute.call_args[0]
        assert "SELECT * FROM users" in str(call_args[0])
    
    @pytest.mark.asyncio
    async def test_read_sql_with_filters(self, sql_operate, mock_client):
        """Test read_sql with filters."""
        mock_result = AsyncMock()
        mock_result.fetchall.return_value = []
        mock_session = AsyncMock()
        mock_session.execute.return_value = mock_result
        mock_client.get_session.return_value.__aenter__.return_value = mock_session
        
        filters = {"status": "active", "age": [25, 30, 35]}
        await sql_operate.read_sql("users", filters=filters)
        
        # Verify WHERE clause is built correctly
        call_args = mock_session.execute.call_args[0]
        query_str = str(call_args[0])
        assert "WHERE" in query_str
        assert "status = :status" in query_str
        assert "age IN (" in query_str
    
    @pytest.mark.asyncio
    async def test_read_sql_with_ordering(self, sql_operate, mock_client):
        """Test read_sql with ordering."""
        mock_result = AsyncMock()
        mock_result.fetchall.return_value = []
        mock_session = AsyncMock()
        mock_session.execute.return_value = mock_result
        mock_client.get_session.return_value.__aenter__.return_value = mock_session
        
        await sql_operate.read_sql("users", order_by="created_at", order_direction="DESC")
        
        call_args = mock_session.execute.call_args[0]
        query_str = str(call_args[0])
        assert "ORDER BY created_at DESC" in query_str
    
    @pytest.mark.asyncio
    async def test_read_sql_with_limit_offset(self, sql_operate, mock_client):
        """Test read_sql with limit and offset."""
        mock_result = AsyncMock()
        mock_result.fetchall.return_value = []
        mock_session = AsyncMock()
        mock_session.execute.return_value = mock_result
        mock_client.get_session.return_value.__aenter__.return_value = mock_session
        
        await sql_operate.read_sql("users", limit=10, offset=20)
        
        call_args = mock_session.execute.call_args[0]
        query_str = str(call_args[0])
        assert "LIMIT 10" in query_str
        assert "OFFSET 20" in query_str
    
    @pytest.mark.asyncio
    async def test_read_sql_invalid_table(self, sql_operate):
        """Test read_sql with invalid table name."""
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate.read_sql("users; DROP TABLE")
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR


class TestSQLOperateUpdateSQL:
    """Test update_sql method."""
    
    @pytest.mark.asyncio
    async def test_update_sql_single(self, sql_operate, mock_client):
        """Test update_sql with single record."""
        mock_result = AsyncMock()
        mock_result.rowcount = 1
        mock_session = AsyncMock()
        mock_session.execute.return_value = mock_result
        mock_client.get_session.return_value.__aenter__.return_value = mock_session
        
        update_data = {"1": {"name": "John Updated", "age": 31}}
        result = await sql_operate.update_sql("users", update_data, "id")
        
        assert result == 1
        mock_session.execute.assert_called_once()
        call_args = mock_session.execute.call_args[0]
        query_str = str(call_args[0])
        assert "UPDATE users" in query_str
        assert "SET name = :name" in query_str
        assert "WHERE id = :where_value" in query_str
    
    @pytest.mark.asyncio
    async def test_update_sql_batch(self, sql_operate, mock_client):
        """Test update_sql with batch records."""
        mock_result = AsyncMock()
        mock_result.rowcount = 2
        mock_session = AsyncMock()
        mock_session.execute.return_value = mock_result
        mock_client.get_session.return_value.__aenter__.return_value = mock_session
        
        update_data = {
            "1": {"name": "John Updated"},
            "2": {"name": "Jane Updated"}
        }
        result = await sql_operate.update_sql("users", update_data, "id")
        
        assert result == 2
        # Should execute one query per update
        assert mock_session.execute.call_count == 2
    
    @pytest.mark.asyncio
    async def test_update_sql_invalid_data(self, sql_operate):
        """Test update_sql with invalid data."""
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate.update_sql("users", "invalid", "id")
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
    
    @pytest.mark.asyncio
    async def test_update_sql_empty_data(self, sql_operate):
        """Test update_sql with empty data."""
        result = await sql_operate.update_sql("users", {}, "id")
        assert result == 0


class TestSQLOperateDeleteSQL:
    """Test delete_sql method."""
    
    @pytest.mark.asyncio
    async def test_delete_sql_single(self, sql_operate, mock_client):
        """Test delete_sql with single ID."""
        mock_result = AsyncMock()
        mock_result.fetchall.return_value = [MagicMock(_asdict=lambda: {"id": 1})]
        mock_session = AsyncMock()
        mock_session.execute.return_value = mock_result
        mock_client.get_session.return_value.__aenter__.return_value = mock_session
        
        result = await sql_operate.delete_sql("users", [1])
        
        assert result == [1]
        mock_session.execute.assert_called_once()
        call_args = mock_session.execute.call_args[0]
        query_str = str(call_args[0])
        assert "DELETE FROM users" in query_str
        assert "WHERE id IN (" in query_str
        assert "RETURNING id" in query_str
    
    @pytest.mark.asyncio
    async def test_delete_sql_batch(self, sql_operate, mock_client):
        """Test delete_sql with multiple IDs."""
        mock_result = AsyncMock()
        mock_result.fetchall.return_value = [
            MagicMock(_asdict=lambda: {"id": 1}),
            MagicMock(_asdict=lambda: {"id": 2})
        ]
        mock_session = AsyncMock()
        mock_session.execute.return_value = mock_result
        mock_client.get_session.return_value.__aenter__.return_value = mock_session
        
        result = await sql_operate.delete_sql("users", [1, 2, 3])
        
        assert result == [1, 2]  # Only 2 were actually deleted
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_sql_empty_ids(self, sql_operate):
        """Test delete_sql with empty ID list."""
        result = await sql_operate.delete_sql("users", [])
        assert result == []
    
    @pytest.mark.asyncio
    async def test_delete_sql_custom_column(self, sql_operate, mock_client):
        """Test delete_sql with custom ID column."""
        mock_result = AsyncMock()
        mock_result.fetchall.return_value = [MagicMock(_asdict=lambda: {"user_id": 1})]
        mock_session = AsyncMock()
        mock_session.execute.return_value = mock_result
        mock_client.get_session.return_value.__aenter__.return_value = mock_session
        
        result = await sql_operate.delete_sql("users", [1], id_column="user_id")
        
        assert result == [1]
        call_args = mock_session.execute.call_args[0]
        query_str = str(call_args[0])
        assert "WHERE user_id IN (" in query_str
        assert "RETURNING user_id" in query_str


class TestSQLOperateUpsertSQL:
    """Test upsert_sql method."""
    
    @pytest.mark.asyncio
    async def test_upsert_sql_postgresql(self, sql_operate_postgresql, mock_postgresql_client):
        """Test upsert_sql with PostgreSQL."""
        mock_result = AsyncMock()
        mock_result.fetchall.return_value = [MagicMock(_asdict=lambda: {"id": 1, "name": "John"})]
        mock_session = AsyncMock()
        mock_session.execute.return_value = mock_result
        mock_postgresql_client.get_session.return_value.__aenter__.return_value = mock_session
        
        data = {"email": "john@example.com", "name": "John", "age": 30}
        result = await sql_operate_postgresql.upsert_sql(
            "users", 
            data, 
            conflict_fields=["email"],
            update_fields=["name", "age"]
        )
        
        assert len(result) == 1
        assert result[0]["name"] == "John"
        
        call_args = mock_session.execute.call_args[0]
        query_str = str(call_args[0])
        assert "ON CONFLICT (email)" in query_str
        assert "DO UPDATE SET" in query_str
    
    @pytest.mark.asyncio
    async def test_upsert_sql_mysql(self, sql_operate_mysql, mock_mysql_client):
        """Test upsert_sql with MySQL."""
        mock_result = AsyncMock()
        mock_result.fetchall.return_value = [MagicMock(_asdict=lambda: {"id": 1, "name": "John"})]
        mock_session = AsyncMock()
        mock_session.execute.return_value = mock_result
        mock_mysql_client.get_session.return_value.__aenter__.return_value = mock_session
        
        data = {"email": "john@example.com", "name": "John", "age": 30}
        result = await sql_operate_mysql.upsert_sql(
            "users", 
            data, 
            conflict_fields=["email"],
            update_fields=["name", "age"]
        )
        
        assert len(result) == 1
        
        call_args = mock_session.execute.call_args[0]
        query_str = str(call_args[0])
        assert "ON DUPLICATE KEY UPDATE" in query_str
    
    @pytest.mark.asyncio
    async def test_upsert_sql_invalid_conflict_field(self, sql_operate):
        """Test upsert_sql with invalid conflict field."""
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate.upsert_sql(
                "users",
                {"name": "John"},
                conflict_fields=["invalid; DROP TABLE"],
                update_fields=["name"]
            )
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR


class TestSQLOperateExistsSQL:
    """Test exists_sql method."""
    
    @pytest.mark.asyncio
    async def test_exists_sql_found(self, sql_operate, mock_client):
        """Test exists_sql with existing records."""
        mock_result = AsyncMock()
        mock_result.fetchall.return_value = [
            MagicMock(_asdict=lambda: {"id": 1}),
            MagicMock(_asdict=lambda: {"id": 3})
        ]
        mock_session = AsyncMock()
        mock_session.execute.return_value = mock_result
        mock_client.get_session.return_value.__aenter__.return_value = mock_session
        
        result = await sql_operate.exists_sql("users", [1, 2, 3])
        
        assert result == {"existing": [1, 3], "missing": [2]}
        
        call_args = mock_session.execute.call_args[0]
        query_str = str(call_args[0])
        assert "SELECT id FROM users" in query_str
        assert "WHERE id IN (" in query_str
    
    @pytest.mark.asyncio
    async def test_exists_sql_none_found(self, sql_operate, mock_client):
        """Test exists_sql with no existing records."""
        mock_result = AsyncMock()
        mock_result.fetchall.return_value = []
        mock_session = AsyncMock()
        mock_session.execute.return_value = mock_result
        mock_client.get_session.return_value.__aenter__.return_value = mock_session
        
        result = await sql_operate.exists_sql("users", [1, 2])
        
        assert result == {"existing": [], "missing": [1, 2]}
    
    @pytest.mark.asyncio
    async def test_exists_sql_empty_ids(self, sql_operate):
        """Test exists_sql with empty ID list."""
        result = await sql_operate.exists_sql("users", [])
        assert result == {"existing": [], "missing": []}


class TestSQLOperateGetDistinctValues:
    """Test get_distinct_values method."""
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_simple(self, sql_operate, mock_client):
        """Test get_distinct_values without filters."""
        mock_result = AsyncMock()
        mock_result.fetchall.return_value = [
            MagicMock(_asdict=lambda: {"status": "active"}),
            MagicMock(_asdict=lambda: {"status": "inactive"})
        ]
        mock_session = AsyncMock()
        mock_session.execute.return_value = mock_result
        mock_client.get_session.return_value.__aenter__.return_value = mock_session
        
        result = await sql_operate.get_distinct_values("users", "status")
        
        assert result == ["active", "inactive"]
        
        call_args = mock_session.execute.call_args[0]
        query_str = str(call_args[0])
        assert "SELECT DISTINCT status FROM users" in query_str
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_with_filters(self, sql_operate, mock_client):
        """Test get_distinct_values with filters."""
        mock_result = AsyncMock()
        mock_result.fetchall.return_value = []
        mock_session = AsyncMock()
        mock_session.execute.return_value = mock_result
        mock_client.get_session.return_value.__aenter__.return_value = mock_session
        
        filters = {"department": "engineering"}
        await sql_operate.get_distinct_values("users", "status", filters=filters, limit=50)
        
        call_args = mock_session.execute.call_args[0]
        query_str = str(call_args[0])
        assert "WHERE department = :department" in query_str
        assert "LIMIT 50" in query_str
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_invalid_field(self, sql_operate):
        """Test get_distinct_values with invalid field name."""
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate.get_distinct_values("users", "field; DROP TABLE")
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR


class TestSQLOperateHealthCheck:
    """Test health_check method."""
    
    @pytest.mark.asyncio
    async def test_health_check_success(self, sql_operate, mock_client):
        """Test successful health check."""
        mock_result = AsyncMock()
        mock_result.fetchone.return_value = MagicMock(_asdict=lambda: {"result": 1})
        mock_session = AsyncMock()
        mock_session.execute.return_value = mock_result
        mock_client.get_session.return_value.__aenter__.return_value = mock_session
        
        result = await sql_operate.health_check()
        
        assert result is True
        
        call_args = mock_session.execute.call_args[0]
        query_str = str(call_args[0])
        assert "SELECT 1 as result" in query_str
    
    @pytest.mark.asyncio
    async def test_health_check_dbapi_error(self, sql_operate, mock_client):
        """Test health check with DBAPIError."""
        mock_session = AsyncMock()
        mock_session.execute.side_effect = DBAPIError("statement", "params", "orig")
        mock_client.get_session.return_value.__aenter__.return_value = mock_session
        
        with patch('structlog.get_logger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            result = await sql_operate.health_check()
        
        assert result is False
        mock_logger.warning.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_health_check_connection_error(self, sql_operate, mock_client):
        """Test health check with connection error."""
        from sqlalchemy.exc import DisconnectionError
        
        mock_session = AsyncMock()
        mock_session.execute.side_effect = DisconnectionError("Connection lost")
        mock_client.get_session.return_value.__aenter__.return_value = mock_session
        
        with patch('structlog.get_logger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            result = await sql_operate.health_check()
        
        assert result is False
        mock_logger.warning.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_health_check_unexpected_error(self, sql_operate, mock_client):
        """Test health check with unexpected error."""
        mock_session = AsyncMock()
        mock_session.execute.side_effect = Exception("Unexpected error")
        mock_client.get_session.return_value.__aenter__.return_value = mock_session
        
        with patch('structlog.get_logger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            result = await sql_operate.health_check()
        
        assert result is False
        mock_logger.error.assert_called_once()


class TestSQLOperateOtherMethods:
    """Test other SQL operate methods."""
    
    def test_get_client(self, sql_operate, mock_client):
        """Test _get_client method."""
        result = sql_operate._get_client()
        assert result is mock_client
    
    @pytest.mark.asyncio
    async def test_create_external_session(self, sql_operate, mock_client):
        """Test create_external_session method."""
        mock_session = AsyncMock()
        mock_client.get_session.return_value = mock_session
        
        result = sql_operate.create_external_session()
        
        assert result is mock_session
        mock_client.get_session.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_count_sql(self, sql_operate, mock_client):
        """Test count_sql method."""
        mock_result = AsyncMock()
        mock_result.fetchone.return_value = MagicMock(_asdict=lambda: {"count": 42})
        mock_session = AsyncMock()
        mock_session.execute.return_value = mock_result
        mock_client.get_session.return_value.__aenter__.return_value = mock_session
        
        result = await sql_operate.count_sql("users", {"status": "active"})
        
        assert result == 42
        
        call_args = mock_session.execute.call_args[0]
        query_str = str(call_args[0])
        assert "SELECT COUNT(*) as count FROM users" in query_str
        assert "WHERE status = :status" in query_str
    
    @pytest.mark.asyncio
    async def test_read_one(self, sql_operate, mock_client):
        """Test read_one method."""
        mock_result = AsyncMock()
        mock_result.fetchone.return_value = MagicMock(_asdict=lambda: {"id": 1, "name": "John"})
        mock_session = AsyncMock()
        mock_session.execute.return_value = mock_result
        mock_client.get_session.return_value.__aenter__.return_value = mock_session
        
        result = await sql_operate.read_one("users", 1)
        
        assert result == {"id": 1, "name": "John"}
        
        call_args = mock_session.execute.call_args[0]
        query_str = str(call_args[0])
        assert "SELECT * FROM users WHERE id = :id_value LIMIT 1" in query_str
    
    @pytest.mark.asyncio
    async def test_execute_query(self, sql_operate, mock_client):
        """Test execute_query method."""
        mock_result = AsyncMock()
        mock_result.fetchall.return_value = [MagicMock(_asdict=lambda: {"id": 1})]
        mock_session = AsyncMock()
        mock_session.execute.return_value = mock_result
        mock_client.get_session.return_value.__aenter__.return_value = mock_session
        
        query = "SELECT * FROM users WHERE status = :status"
        params = {"status": "active"}
        
        result = await sql_operate.execute_query(query, params)
        
        assert len(result) == 1
        assert result[0]["id"] == 1
        
        mock_session.execute.assert_called_once_with(query, params)


# Test edge cases and specific error paths
class TestSQLOperateEdgeCases:
    """Test edge cases and specific error paths."""
    
    @pytest.mark.asyncio
    async def test_mysql_insert_no_auto_increment(self, sql_operate_mysql, mock_mysql_client):
        """Test MySQL insert when no auto-increment is available."""
        mock_insert_result = AsyncMock()
        mock_insert_result.lastrowid = None  # No auto-increment
        mock_insert_result.rowcount = 1
        
        mock_session = AsyncMock()
        mock_session.execute.return_value = mock_insert_result
        mock_mysql_client.get_session.return_value.__aenter__.return_value = mock_session
        
        data = {"name": "John"}
        result = await sql_operate_mysql.create_sql("users", data)
        
        # Should return empty list when no auto-increment is available
        assert result == []
    
    @pytest.mark.asyncio
    async def test_build_insert_query_edge_cases(self, sql_operate):
        """Test _build_insert_query with edge cases."""
        # Test with single data item
        cleaned_data = [{"name": "John", "age": 30}]
        query, params = sql_operate._build_insert_query("users", cleaned_data)
        
        assert "INSERT INTO users" in query
        assert "(name, age)" in query
        assert "VALUES (:name_0, :age_0)" in query
        assert params["name_0"] == "John"
        assert params["age_0"] == 30
    
    def test_validate_create_data_edge_cases(self, sql_operate):
        """Test _validate_create_data with edge cases."""
        # Test with mixed valid and invalid data
        data = [
            {"name": "John", "age": 30},
            {"name": "'; DROP TABLE", "age": 25}  # Invalid
        ]
        
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_create_data(data)
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
    
    @pytest.mark.asyncio
    async def test_read_sql_with_date_range_validation_errors(self, sql_operate):
        """Test read_sql_with_date_range with validation errors."""
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate.read_sql_with_date_range(
                "users; DROP TABLE",  # Invalid table name
                {},
                "created_at",
                datetime.now(),
                datetime.now()
            )
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        
        # Test with invalid date field
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate.read_sql_with_date_range(
                "users",
                {},
                "created_at; DROP TABLE",  # Invalid field
                datetime.now(),
                datetime.now()
            )
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
    
    @pytest.mark.asyncio  
    async def test_delete_filter_method(self, sql_operate, mock_client):
        """Test delete_filter method."""
        mock_result = AsyncMock()
        mock_result.rowcount = 2
        mock_session = AsyncMock()
        mock_session.execute.return_value = mock_result
        mock_client.get_session.return_value.__aenter__.return_value = mock_session
        
        filters = {"status": "inactive", "age": [20, 25]}
        result = await sql_operate.delete_filter("users", filters)
        
        assert result == 2
        
        call_args = mock_session.execute.call_args[0]
        query_str = str(call_args[0])
        assert "DELETE FROM users" in query_str
        assert "WHERE status = :status" in query_str
        assert "age IN (" in query_str