"""
Comprehensive tests for sql_operate.py to achieve 100% coverage.
Tests all methods, exception handlers, and edge cases.
"""

import pytest
import pytest_asyncio
import asyncio
from unittest.mock import AsyncMock, MagicMock, Mock, patch, call
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
    # Use MagicMock for client since we need simple attribute access
    mock = MagicMock()
    # Mock the get_engine method to return a proper mock engine
    mock_engine = MagicMock()
    mock_engine.sync_engine = MagicMock()  # SQLAlchemy needs this attribute
    mock.get_engine.return_value = mock_engine
    
    return mock


@pytest.fixture 
def mock_postgresql_client():
    """Mock PostgreSQL client."""
    # Use MagicMock for client since we need simple attribute access
    mock = MagicMock()
    # Mock the engine_type to return PostgreSQL
    mock.engine_type = 'PostgreSQL'
    # Mock the get_engine method to return a proper mock engine
    mock_engine = MagicMock()
    mock_engine.sync_engine = MagicMock()  # SQLAlchemy needs this attribute
    mock.get_engine.return_value = mock_engine
    
    return mock


@pytest.fixture
def mock_mysql_client():
    """Mock MySQL client."""
    # Use MagicMock for client since we need simple attribute access
    mock = MagicMock()
    # Mock the engine_type to return MySQL  
    mock.engine_type = 'MySQL'
    # Mock the get_engine method to return a proper mock engine
    mock_engine = MagicMock()
    mock_engine.sync_engine = MagicMock()  # SQLAlchemy needs this attribute
    mock.get_engine.return_value = mock_engine
    
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
        
        assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR
        assert "orig" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_exception_handler_integrity_error(self, sql_operate):
        """Test exception handler for IntegrityError."""
        
        @sql_operate.exception_handler
        async def test_method(self):
            raise IntegrityError("statement", "params", "orig")
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_method(sql_operate)
        
        assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR
        assert "(builtins.str) orig" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_exception_handler_operational_error_postgresql(self, sql_operate_postgresql):
        """Test exception handler for OperationalError with PostgreSQL."""
        
        @sql_operate_postgresql.exception_handler
        async def test_method(self):
            raise OperationalError("statement", "params", "orig")
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_method(sql_operate_postgresql)
        
        assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR
        assert "orig" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_exception_handler_operational_error_mysql(self, sql_operate_mysql):
        """Test exception handler for OperationalError with MySQL."""
        
        @sql_operate_mysql.exception_handler
        async def test_method(self):
            raise OperationalError("statement", "params", "orig")
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_method(sql_operate_mysql)
        
        assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR
        assert "orig" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_exception_handler_programming_error_postgresql(self, sql_operate_postgresql):
        """Test exception handler for ProgrammingError with PostgreSQL."""
        
        @sql_operate_postgresql.exception_handler
        async def test_method(self):
            raise ProgrammingError("statement", "params", "orig")
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_method(sql_operate_postgresql)
        
        assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR
        assert "orig" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_exception_handler_programming_error_mysql(self, sql_operate_mysql):
        """Test exception handler for ProgrammingError with MySQL."""
        
        @sql_operate_mysql.exception_handler
        async def test_method(self):
            raise ProgrammingError("statement", "params", "orig")
        
        with pytest.raises(DatabaseException) as exc_info:
            await test_method(sql_operate_mysql)
        
        assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR
        assert "orig" in str(exc_info.value)
    
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
        assert result is None
    
    def test_validate_data_value_valid_integer(self, sql_operate):
        """Test valid integer value."""
        result = sql_operate._validate_data_value(42, "age")
        assert result is None
    
    def test_validate_data_value_valid_float(self, sql_operate):
        """Test valid float value."""
        result = sql_operate._validate_data_value(3.14, "price")
        assert result is None
    
    def test_validate_data_value_valid_decimal(self, sql_operate):
        """Test valid decimal value."""
        decimal_val = Decimal("10.50")
        result = sql_operate._validate_data_value(decimal_val, "amount")
        assert result is None
    
    def test_validate_data_value_valid_datetime(self, sql_operate):
        """Test valid datetime value."""
        dt = datetime.now(timezone.utc)
        result = sql_operate._validate_data_value(dt, "created_at")
        assert result is None
    
    def test_validate_data_value_valid_boolean(self, sql_operate):
        """Test valid boolean value."""
        result = sql_operate._validate_data_value(True, "is_active")
        assert result is None
    
    def test_validate_data_value_valid_none(self, sql_operate):
        """Test valid None value."""
        result = sql_operate._validate_data_value(None, "optional_field")
        assert result is None
    
    def test_validate_data_value_invalid_type(self, sql_operate):
        """Test invalid data type - non-serializable object."""
        class NonSerializable:
            def __init__(self):
                self.func = lambda x: x  # Functions are not JSON serializable
        
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_data_value({"func": NonSerializable().func}, "data")
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
        # Should not raise exception for exempt columns (content_template, subject_template, description, content)
        result = sql_operate._validate_data_value("SELECT * FROM users", "content_template")
        assert result is None
        
        result = sql_operate._validate_data_value("DELETE FROM table", "description")
        assert result is None
    
    def test_validate_data_value_conversion_error(self, sql_operate):
        """Test value conversion error handling."""
        class BadValue:
            def __str__(self):
                raise ValueError("Cannot convert")
        
        # BadValue objects are accepted since validation doesn't convert non-strings
        sql_operate._validate_data_value(BadValue(), "bad_field")  # Should not raise


class TestSQLOperateValidateDataDict:
    """Test _validate_data_dict method."""
    
    def test_validate_data_dict_valid_single(self, sql_operate):
        """Test valid single data dict."""
        data = {"name": "John", "age": 30}
        result = sql_operate._validate_data_dict(data, "create")
        
        assert result == {"name": "John", "age": 30}
    
    def test_validate_data_dict_valid_list(self, sql_operate):
        """Test invalid data dict list - method expects dict, not list."""
        data = [{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_data_dict(data, "create")
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
    
    def test_validate_data_dict_empty_not_allowed(self, sql_operate):
        """Test empty data dict when not allowed."""
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_data_dict({}, "create", allow_empty=False)
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
    
    def test_validate_data_dict_empty_allowed(self, sql_operate):
        """Test empty data dict when allowed."""
        result = sql_operate._validate_data_dict({}, "update", allow_empty=True)
        assert result == {}
    
    def test_validate_data_dict_invalid_type(self, sql_operate):
        """Test invalid data dict type."""
        with pytest.raises(DatabaseException) as exc_info:
            sql_operate._validate_data_dict("invalid", "create")
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
    
    def test_validate_data_dict_invalid_value(self, sql_operate):
        """Test data dict with invalid value - non-serializable."""
        class NonSerializable:
            def __init__(self):
                self.func = lambda x: x
        
        data = {"name": "John", "bad_field": {"func": NonSerializable().func}}
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
        # Mock the result - use MagicMock for result since fetchall() is not async
        mock_result = MagicMock()
        mock_result._is_cursor = False
        mock_row = MagicMock()
        mock_row._mapping = {"id": 1, "name": "John", "age": 30}
        mock_result.fetchall.return_value = [mock_row]
        
        # Mock session - use AsyncMock for session methods
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()
        
        # Mock the async context manager for create_external_session
        async_cm = AsyncMock()
        async_cm.__aenter__ = AsyncMock(return_value=mock_session)
        async_cm.__aexit__ = AsyncMock(return_value=None)
        sql_operate_postgresql.create_external_session = Mock(return_value=async_cm)
        
        data = {"name": "John", "age": 30}
        result = await sql_operate_postgresql.create_sql("users", data)
        
        assert len(result) == 1
        assert result[0]["name"] == "John"
        assert result[0]["age"] == 30
    
    @pytest.mark.asyncio
    async def test_create_sql_mysql_single(self, sql_operate_mysql, mock_mysql_client):
        """Test create_sql with MySQL single record."""
        # Mock the insert result
        mock_insert_result = MagicMock()
        mock_insert_result.lastrowid = 1
        mock_insert_result.rowcount = 1
        
        # Mock the select result
        mock_select_result = MagicMock()
        mock_row = MagicMock()
        mock_row._mapping = {"id": 1, "name": "John", "age": 30}
        mock_select_result.fetchall.return_value = [mock_row]
        
        # Mock session - use AsyncMock for session methods
        mock_session = AsyncMock()
        mock_session.execute.side_effect = [mock_insert_result, mock_select_result]
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()
        
        # Mock the async context manager for create_external_session
        async_cm = AsyncMock()
        async_cm.__aenter__ = AsyncMock(return_value=mock_session)
        async_cm.__aexit__ = AsyncMock(return_value=None)
        sql_operate_mysql.create_external_session = Mock(return_value=async_cm)
        
        data = {"name": "John", "age": 30}
        result = await sql_operate_mysql.create_sql("users", data)
        
        assert len(result) == 1
        assert result[0]["name"] == "John"
        assert result[0]["age"] == 30
        
        # Verify two queries were executed (insert + select)
        assert mock_session.execute.call_count == 2
    
    @pytest.mark.asyncio
    async def test_create_sql_batch_postgresql(self, sql_operate_postgresql, mock_postgresql_client):
        """Test create_sql with PostgreSQL batch insert."""
        # Mock the result
        mock_result = MagicMock()
        mock_result._is_cursor = False
        mock_rows = [
            MagicMock(_mapping={"id": 1, "name": "John", "age": 30}),
            MagicMock(_mapping={"id": 2, "name": "Jane", "age": 25})
        ]
        mock_result.fetchall.return_value = mock_rows
        
        # Mock session - use AsyncMock for session methods
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()
        
        # Mock the async context manager for create_external_session
        async_cm = AsyncMock()
        async_cm.__aenter__ = AsyncMock(return_value=mock_session)
        async_cm.__aexit__ = AsyncMock(return_value=None)
        sql_operate_postgresql.create_external_session = Mock(return_value=async_cm)
        
        data = [{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]
        result = await sql_operate_postgresql.create_sql("users", data)
        
        assert len(result) == 2
        assert result[0]["name"] == "John"
        assert result[1]["name"] == "Jane"
        assert result[0]["age"] == 30
        assert result[1]["age"] == 25
    
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
        mock_result = MagicMock()
        mock_result._is_cursor = False
        mock_row = MagicMock()
        mock_row._mapping = {"id": 1, "name": "John", "age": 30}
        mock_result.fetchall.return_value = [mock_row]
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        data = {"name": "John", "age": 30}
        result = await sql_operate_postgresql.create_sql("users", data, session=mock_session)
        
        assert len(result) == 1
        assert result[0]["name"] == "John"


class TestSQLOperateReadSQL:
    """Test read_sql method."""
    
    @pytest.mark.asyncio
    async def test_read_sql_simple(self, sql_operate, mock_client):
        """Test simple read_sql."""
        # Mock the result
        mock_result = MagicMock()
        mock_result._is_cursor = False
        mock_rows = [
            MagicMock(_mapping={"id": 1, "name": "John"}),
            MagicMock(_mapping={"id": 2, "name": "Jane"})
        ]
        mock_result.fetchall.return_value = mock_rows
        
        # Mock session
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()
        
        # Mock the async context manager for create_external_session
        async_cm = AsyncMock()
        async_cm.__aenter__ = AsyncMock(return_value=mock_session)
        async_cm.__aexit__ = AsyncMock(return_value=None)
        sql_operate.create_external_session = Mock(return_value=async_cm)
        
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
        mock_result = MagicMock()
        mock_result._is_cursor = False
        mock_result.fetchall.return_value = []
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()
        
        # Mock the async context manager for create_external_session
        async_cm = AsyncMock()
        async_cm.__aenter__ = AsyncMock(return_value=mock_session)
        async_cm.__aexit__ = AsyncMock(return_value=None)
        sql_operate.create_external_session = Mock(return_value=async_cm)
        
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
        mock_result = MagicMock()
        mock_result._is_cursor = False
        mock_result.fetchall.return_value = []
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)
        # Mock the async context manager for create_external_session
        async_cm = AsyncMock()
        async_cm.__aenter__ = AsyncMock(return_value=mock_session)
        async_cm.__aexit__ = AsyncMock(return_value=None)
        sql_operate.create_external_session = Mock(return_value=async_cm)
        
        await sql_operate.read_sql("users", order_by="created_at", order_direction="DESC")
        
        call_args = mock_session.execute.call_args[0]
        query_str = str(call_args[0])
        assert "ORDER BY created_at DESC" in query_str
    
    @pytest.mark.asyncio
    async def test_read_sql_with_limit_offset(self, sql_operate, mock_client):
        """Test read_sql with limit and offset."""
        mock_result = MagicMock()
        mock_result._is_cursor = False
        mock_result.fetchall.return_value = []
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)
        # Mock the async context manager for create_external_session
        async_cm = AsyncMock()
        async_cm.__aenter__ = AsyncMock(return_value=mock_session)
        async_cm.__aexit__ = AsyncMock(return_value=None)
        sql_operate.create_external_session = Mock(return_value=async_cm)
        
        await sql_operate.read_sql("users", limit=10, offset=20)
        
        call_args = mock_session.execute.call_args[0]
        query_str = str(call_args[0])
        # MySQL uses LIMIT offset, limit format
        assert ("LIMIT 10" in query_str) or ("LIMIT 20, 10" in query_str)
        assert ("OFFSET 20" in query_str) or ("LIMIT 20, 10" in query_str)
    
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
        mock_result = MagicMock()
        mock_result._is_cursor = False
        mock_result.rowcount = 1
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)
        # Mock the async context manager for create_external_session
        async_cm = AsyncMock()
        async_cm.__aenter__ = AsyncMock(return_value=mock_session)
        async_cm.__aexit__ = AsyncMock(return_value=None)
        sql_operate.create_external_session = Mock(return_value=async_cm)
        
        # Mock the select result for updated record
        mock_select_result = MagicMock()
        mock_row = MagicMock()
        mock_row._mapping = {"id": 1, "name": "John Updated", "age": 31}
        mock_select_result.fetchone.return_value = mock_row
        
        mock_session.execute.side_effect = [mock_result, mock_select_result]
        
        update_data = [{"id": 1, "data": {"name": "John Updated", "age": 31}}]
        result = await sql_operate.update_sql("users", update_data, "id")
        
        assert len(result) == 1
        assert result[0]["name"] == "John Updated"
        assert mock_session.execute.call_count == 2  # UPDATE + SELECT
        # Check the first call (UPDATE)
        first_call_args = mock_session.execute.call_args_list[0][0]
        update_query_str = str(first_call_args[0])
        assert "UPDATE users" in update_query_str
        assert "SET name = :name" in update_query_str
        assert "WHERE id = :where_value" in update_query_str
    
    @pytest.mark.asyncio
    async def test_update_sql_batch(self, sql_operate, mock_client):
        """Test update_sql with batch records."""
        # First UPDATE result
        mock_update1 = MagicMock()
        mock_update1._is_cursor = False
        mock_update1.rowcount = 1
        
        # Second UPDATE result
        mock_update2 = MagicMock()
        mock_update2._is_cursor = False
        mock_update2.rowcount = 1
        
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(side_effect=[mock_update1, mock_update2])
        # Mock the async context manager for create_external_session
        async_cm = AsyncMock()
        async_cm.__aenter__ = AsyncMock(return_value=mock_session)
        async_cm.__aexit__ = AsyncMock(return_value=None)
        sql_operate.create_external_session = Mock(return_value=async_cm)
        
        update_data = [
            {"id": "1", "data": {"name": "John Updated"}},
            {"id": "2", "data": {"name": "Jane Updated"}}
        ]
        result = await sql_operate.update_sql("users", update_data, "id")
        
        # Returns list with two empty dicts (no SELECT for batch update)
        assert len(result) == 2
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
        result = await sql_operate.update_sql("users", [], "id")
        assert result == []


class TestSQLOperateDeleteSQL:
    """Test delete_sql method."""
    
    @pytest.mark.asyncio
    async def test_delete_sql_single(self, sql_operate, mock_client):
        """Test delete_sql with single ID."""
        mock_result = MagicMock()
        mock_result._is_cursor = False
        mock_result.rowcount = 1  # Add rowcount for delete operation
        mock_row = MagicMock()
        mock_row._mapping = MagicMock(_asdict=lambda: {"id": 1})
        mock_result.fetchall.return_value = [mock_row]
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)
        # Mock the async context manager for create_external_session
        async_cm = AsyncMock()
        async_cm.__aenter__ = AsyncMock(return_value=mock_session)
        async_cm.__aexit__ = AsyncMock(return_value=None)
        sql_operate.create_external_session = Mock(return_value=async_cm)
        
        result = await sql_operate.delete_sql("users", [1])
        
        assert result == [1]
        mock_session.execute.assert_called_once()
        call_args = mock_session.execute.call_args[0]
        query_str = str(call_args[0])
        assert "DELETE FROM users" in query_str
        assert "WHERE id = :id_value" in query_str  # Single ID uses = instead of IN
        # No RETURNING clause - uses rowcount to determine success
    
    @pytest.mark.asyncio
    async def test_delete_sql_batch(self, sql_operate, mock_client):
        """Test delete_sql with multiple IDs."""
        mock_result = MagicMock()
        mock_result._is_cursor = False
        mock_result.rowcount = 2  # Add rowcount for batch delete
        mock_row1 = MagicMock()
        mock_row1._mapping = MagicMock(_asdict=lambda: {"id": 1})
        mock_row2 = MagicMock()
        mock_row2._mapping = MagicMock(_asdict=lambda: {"id": 2})
        
        mock_result.fetchall.return_value = [mock_row1, mock_row2]
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)
        # Mock the async context manager for create_external_session
        async_cm = AsyncMock()
        async_cm.__aenter__ = AsyncMock(return_value=mock_session)
        async_cm.__aexit__ = AsyncMock(return_value=None)
        sql_operate.create_external_session = Mock(return_value=async_cm)
        
        result = await sql_operate.delete_sql("users", [1, 2, 3])
        
        assert result == [1, 2, 3]  # Returns all requested IDs when rowcount > 0
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_sql_empty_ids(self, sql_operate):
        """Test delete_sql with empty ID list."""
        result = await sql_operate.delete_sql("users", [])
        assert result == []
    
    @pytest.mark.asyncio
    async def test_delete_sql_custom_column(self, sql_operate, mock_client):
        """Test delete_sql with custom ID column."""
        mock_result = MagicMock()
        mock_result._is_cursor = False
        mock_result.rowcount = 1  # Add rowcount for custom column delete
        mock_row = MagicMock()
        mock_row._mapping = MagicMock(_asdict=lambda: {"user_id": 1})
        mock_result.fetchall.return_value = [mock_row]
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)
        # Mock the async context manager for create_external_session
        async_cm = AsyncMock()
        async_cm.__aenter__ = AsyncMock(return_value=mock_session)
        async_cm.__aexit__ = AsyncMock(return_value=None)
        sql_operate.create_external_session = Mock(return_value=async_cm)
        
        result = await sql_operate.delete_sql("users", [1], id_column="user_id")
        
        assert result == [1]
        call_args = mock_session.execute.call_args[0]
        query_str = str(call_args[0])
        assert "WHERE user_id = :id_value" in query_str  # Uses generic :id_value parameter
        # No RETURNING clause - uses rowcount to determine success


class TestSQLOperateUpsertSQL:
    """Test upsert_sql method."""
    
    @pytest.mark.asyncio
    async def test_upsert_sql_postgresql(self, sql_operate_postgresql, mock_postgresql_client):
        """Test upsert_sql with PostgreSQL."""
        mock_result = MagicMock()
        mock_result._is_cursor = False
        mock_row = MagicMock()
        mock_row._mapping = {"id": 1, "email": "john@example.com", "name": "John", "age": 30}
        mock_result.fetchall.return_value = [mock_row]
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)
        # Mock the async context manager for create_external_session
        async_cm = AsyncMock()
        async_cm.__aenter__ = AsyncMock(return_value=mock_session)
        async_cm.__aexit__ = AsyncMock(return_value=None)
        sql_operate_postgresql.create_external_session = Mock(return_value=async_cm)
        
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
        # First result for INSERT query
        mock_insert_result = MagicMock()
        mock_insert_result._is_cursor = False
        mock_insert_result.rowcount = 1
        
        # Second result for SELECT query
        mock_select_result = MagicMock()
        mock_select_result._is_cursor = False
        mock_row = MagicMock()
        mock_row._mapping = {"id": 1, "email": "john@example.com", "name": "John", "age": 30}
        mock_select_result.fetchall.return_value = [mock_row]
        
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(side_effect=[mock_insert_result, mock_select_result])
        # Mock the async context manager for create_external_session
        async_cm = AsyncMock()
        async_cm.__aenter__ = AsyncMock(return_value=mock_session)
        async_cm.__aexit__ = AsyncMock(return_value=None)
        sql_operate_mysql.create_external_session = Mock(return_value=async_cm)
        
        data = {"email": "john@example.com", "name": "John", "age": 30}
        result = await sql_operate_mysql.upsert_sql(
            "users", 
            data, 
            conflict_fields=["email"],
            update_fields=["name", "age"]
        )
        
        assert len(result) == 1
        
        # Check the first call (INSERT query)
        first_call_args = mock_session.execute.call_args_list[0][0]
        insert_query_str = str(first_call_args[0])
        assert "ON DUPLICATE KEY UPDATE" in insert_query_str
    
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
        mock_result = MagicMock()
        mock_result._is_cursor = False
        mock_row1 = MagicMock()
        mock_row1._mapping = {"id": 1}
        mock_row1.__getitem__ = lambda _, i: 1 if i == 0 else None
        mock_row2 = MagicMock()
        mock_row2._mapping = {"id": 3}
        mock_row2.__getitem__ = lambda _, i: 3 if i == 0 else None
        
        mock_result.fetchall.return_value = [mock_row1, mock_row2]
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)
        # Mock the async context manager for create_external_session
        async_cm = AsyncMock()
        async_cm.__aenter__ = AsyncMock(return_value=mock_session)
        async_cm.__aexit__ = AsyncMock(return_value=None)
        sql_operate.create_external_session = Mock(return_value=async_cm)
        
        result = await sql_operate.exists_sql("users", [1, 2, 3])
        
        assert result == {1: True, 2: False, 3: True}
        
        call_args = mock_session.execute.call_args[0]
        query_str = str(call_args[0])
        assert "SELECT id FROM users" in query_str
        assert "WHERE id IN (" in query_str
    
    @pytest.mark.asyncio
    async def test_exists_sql_none_found(self, sql_operate, mock_client):
        """Test exists_sql with no existing records."""
        mock_result = MagicMock()
        mock_result._is_cursor = False
        mock_result.fetchall.return_value = []
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)
        # Mock the async context manager for create_external_session
        async_cm = AsyncMock()
        async_cm.__aenter__ = AsyncMock(return_value=mock_session)
        async_cm.__aexit__ = AsyncMock(return_value=None)
        sql_operate.create_external_session = Mock(return_value=async_cm)
        
        result = await sql_operate.exists_sql("users", [1, 2])
        
        assert result == {1: False, 2: False}
    
    @pytest.mark.asyncio
    async def test_exists_sql_empty_ids(self, sql_operate):
        """Test exists_sql with empty ID list."""
        result = await sql_operate.exists_sql("users", [])
        assert result == {}


class TestSQLOperateGetDistinctValues:
    """Test get_distinct_values method."""
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_simple(self, sql_operate, mock_client):
        """Test get_distinct_values without filters."""
        mock_result = MagicMock()
        mock_result._is_cursor = False
        mock_row1 = MagicMock()
        mock_row1._mapping = {"status": "active"}
        mock_row1.__getitem__ = lambda _, i: "active" if i == 0 else None
        mock_row2 = MagicMock()
        mock_row2._mapping = {"status": "inactive"}
        mock_row2.__getitem__ = lambda _, i: "inactive" if i == 0 else None
        
        mock_result.fetchall.return_value = [mock_row1, mock_row2]
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)
        # Mock the async context manager for create_external_session
        async_cm = AsyncMock()
        async_cm.__aenter__ = AsyncMock(return_value=mock_session)
        async_cm.__aexit__ = AsyncMock(return_value=None)
        sql_operate.create_external_session = Mock(return_value=async_cm)
        
        result = await sql_operate.get_distinct_values("users", field="status")
        
        assert result == ["active", "inactive"]
        
        call_args = mock_session.execute.call_args[0]
        query_str = str(call_args[0])
        assert "SELECT DISTINCT status FROM users" in query_str
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_with_filters(self, sql_operate, mock_client):
        """Test get_distinct_values with filters."""
        mock_result = MagicMock()
        mock_result._is_cursor = False
        mock_result.fetchall.return_value = []
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)
        # Mock the async context manager for create_external_session
        async_cm = AsyncMock()
        async_cm.__aenter__ = AsyncMock(return_value=mock_session)
        async_cm.__aexit__ = AsyncMock(return_value=None)
        sql_operate.create_external_session = Mock(return_value=async_cm)
        
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
            await sql_operate.get_distinct_values("users", field="field; DROP TABLE")
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR


class TestSQLOperateHealthCheck:
    """Test health_check method."""
    
    @pytest.mark.asyncio
    async def test_health_check_success(self, sql_operate, mock_client):
        """Test successful health check."""
        mock_result = MagicMock()
        mock_result._is_cursor = False
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda _, i: 1 if i == 0 else None
        mock_result.fetchone.return_value = mock_row
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)
        # Mock the async context manager for create_external_session
        async_cm = AsyncMock()
        async_cm.__aenter__ = AsyncMock(return_value=mock_session)
        async_cm.__aexit__ = AsyncMock(return_value=None)
        sql_operate.create_external_session = Mock(return_value=async_cm)
        
        result = await sql_operate.health_check()
        
        assert result is True
        
        call_args = mock_session.execute.call_args[0]
        query_str = str(call_args[0])
        assert "SELECT 1 as health_check" in query_str
    
    @pytest.mark.asyncio
    async def test_health_check_dbapi_error(self, sql_operate, mock_client):
        """Test health check with DBAPIError."""
        mock_session = AsyncMock()
        mock_session.execute.side_effect = DBAPIError("statement", "params", "orig")
        
        # Mock create_external_session instead of get_session
        async_cm = AsyncMock()
        async_cm.__aenter__ = AsyncMock(return_value=mock_session)
        async_cm.__aexit__ = AsyncMock(return_value=None)
        sql_operate.create_external_session = Mock(return_value=async_cm)
        
        with patch('structlog.get_logger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            result = await sql_operate.health_check()
        
        assert result is False
        mock_logger.warning.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_health_check_connection_error(self, sql_operate, mock_client):
        """Test health check with connection error."""
        from sqlalchemy.exc import DBAPIError
        
        mock_session = AsyncMock()
        mock_session.execute.side_effect = DBAPIError("statement", "params", "orig")
        
        # Mock create_external_session instead of get_session
        async_cm = AsyncMock()
        async_cm.__aenter__ = AsyncMock(return_value=mock_session)
        async_cm.__aexit__ = AsyncMock(return_value=None)
        sql_operate.create_external_session = Mock(return_value=async_cm)
        
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
        # Mock the async context manager for create_external_session
        async_cm = AsyncMock()
        async_cm.__aenter__ = AsyncMock(return_value=mock_session)
        async_cm.__aexit__ = AsyncMock(return_value=None)
        sql_operate.create_external_session = Mock(return_value=async_cm)
        
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
        from sqlalchemy.ext.asyncio import AsyncSession
        
        mock_engine = MagicMock()
        mock_client.get_engine.return_value = mock_engine
        
        result = sql_operate.create_external_session()
        
        assert isinstance(result, AsyncSession)
        mock_client.get_engine.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_count_sql(self, sql_operate, mock_client):
        """Test count_sql method."""
        mock_result = MagicMock()
        mock_result._is_cursor = False
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda _, i: 42 if i == 0 else None
        mock_result.fetchone.return_value = mock_row
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)
        # Mock the async context manager for create_external_session
        async_cm = AsyncMock()
        async_cm.__aenter__ = AsyncMock(return_value=mock_session)
        async_cm.__aexit__ = AsyncMock(return_value=None)
        sql_operate.create_external_session = Mock(return_value=async_cm)
        
        result = await sql_operate.count_sql("users", {"status": "active"})
        
        assert result == 42
        
        call_args = mock_session.execute.call_args[0]
        query_str = str(call_args[0])
        assert "SELECT COUNT(*) as total FROM users" in query_str
        assert "WHERE status = :status" in query_str
    
    @pytest.mark.asyncio
    async def test_read_one(self, sql_operate, mock_client):
        """Test read_one method."""
        mock_result = MagicMock()
        mock_result._is_cursor = False
        mock_row = MagicMock()
        mock_row._mapping = {"id": 1, "name": "John"}
        mock_result.fetchone.return_value = mock_row
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)
        # Mock the async context manager for create_external_session
        async_cm = AsyncMock()
        async_cm.__aenter__ = AsyncMock(return_value=mock_session)
        async_cm.__aexit__ = AsyncMock(return_value=None)
        sql_operate.create_external_session = Mock(return_value=async_cm)
        
        result = await sql_operate.read_one("users", id_value=1)
        
        assert result == {"id": 1, "name": "John"}
        
        call_args = mock_session.execute.call_args[0]
        query_str = str(call_args[0])
        assert "SELECT * FROM users WHERE id = :id_value" in query_str
    
    @pytest.mark.asyncio
    async def test_execute_query(self, sql_operate, mock_client):
        """Test execute_query method."""
        mock_result = MagicMock()
        mock_result._is_cursor = False
        mock_row = MagicMock()
        mock_row._mapping = {"id": 1, "name": "John"}
        mock_result.fetchall.return_value = [mock_row]
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)
        # Mock the async context manager for create_external_session
        async_cm = AsyncMock()
        async_cm.__aenter__ = AsyncMock(return_value=mock_session)
        async_cm.__aexit__ = AsyncMock(return_value=None)
        sql_operate.create_external_session = Mock(return_value=async_cm)
        
        query = "SELECT * FROM users WHERE status = :status"
        params = {"status": "active"}
        
        result = await sql_operate.execute_query(query, params)
        
        assert len(result) == 1
        assert result[0]["name"] == "John"
        
        # Check that execute was called with the right parameters
        call_args = mock_session.execute.call_args
        assert str(call_args[0][0]) == query
        assert call_args[0][1] == params


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
        # Mock the async context manager for create_external_session
        async_cm = AsyncMock()
        async_cm.__aenter__ = AsyncMock(return_value=mock_session)
        async_cm.__aexit__ = AsyncMock(return_value=None)
        sql_operate_mysql.create_external_session = Mock(return_value=async_cm)
        
        data = {"name": "John"}
        result = await sql_operate_mysql.create_sql("users", data)
        
        # Should return empty list when no auto-increment is available
        assert result == []
    
    @pytest.mark.asyncio
    async def test_build_insert_query_edge_cases(self, sql_operate):
        """Test _build_insert_query with edge cases."""
        # Test with single data item
        cleaned_data = [{"name": "John", "age": 30}]
        query, returned_data = sql_operate._build_insert_query("users", cleaned_data)
        
        assert "INSERT INTO users" in query
        assert "(name, age)" in query
        assert "VALUES (:name_0, :age_0)" in query
        # The method returns the cleaned_data_list, not params dict
        assert returned_data == cleaned_data
        assert returned_data[0]["name"] == "John"
        assert returned_data[0]["age"] == 30
    
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
        """Test read_sql with date range validation errors."""
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate.read_sql(
                "users; DROP TABLE",  # Invalid table name
                filters={},
                date_field="created_at",
                start_date=datetime.now(),
                end_date=datetime.now()
            )
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        
        # Test with invalid date field
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate.read_sql(
                "users",
                filters={},
                date_field="created_at; DROP TABLE",  # Invalid field
                start_date=datetime.now(),
                end_date=datetime.now()
            )
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
    
    @pytest.mark.asyncio  
    async def test_delete_filter_method(self, sql_operate, mock_client):
        """Test delete_filter method."""
        # First mock result for SELECT query to get IDs
        mock_select_result = MagicMock()
        mock_select_result._is_cursor = False
        mock_select_result.fetchall.return_value = [(1,), (2,)]  # Two IDs will be deleted
        
        # Second mock result for DELETE query
        mock_delete_result = MagicMock()
        mock_delete_result._is_cursor = False
        mock_delete_result.rowcount = 2
        
        mock_session = AsyncMock()
        # Return SELECT result first, then DELETE result
        mock_session.execute = AsyncMock(side_effect=[mock_select_result, mock_delete_result])
        
        # Mock the async context manager for create_external_session
        async_cm = AsyncMock()
        async_cm.__aenter__ = AsyncMock(return_value=mock_session)
        async_cm.__aexit__ = AsyncMock(return_value=None)
        sql_operate.create_external_session = Mock(return_value=async_cm)
        
        filters = {"status": "inactive", "age": [20, 25]}
        result = await sql_operate.delete_filter("users", filters)
        
        assert result == [1, 2]  # Returns list of deleted IDs
        
        call_args = mock_session.execute.call_args[0]
        query_str = str(call_args[0])
        assert "DELETE FROM users" in query_str
        assert "WHERE status = :status" in query_str
        assert "age IN (" in query_str