from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import pymysql
import pytest
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.exc import UnmappedInstanceError

# Import directly to avoid circular import issues
from general_operate.app.sql_operate import SQLOperate
from general_operate.utils.exception import GeneralOperateException


class MockSQLClient:
    """Mock SQLClient for testing"""

    def __init__(self, engine_type="postgresql"):
        self.engine_type = engine_type

    def get_engine(self):
        return MagicMock()


@pytest.fixture
def mock_sql_client():
    """Create a mock SQLClient"""
    return MockSQLClient("postgresql")


@pytest.fixture
def sql_operate_postgres(mock_sql_client):
    """Create a SQLOperate instance with PostgreSQL mock client"""
    mock_sql_client.engine_type = "postgresql"
    return SQLOperate(mock_sql_client)


@pytest.fixture
def sql_operate_mysql(mock_sql_client):
    """Create a SQLOperate instance with MySQL mock client"""
    mock_sql_client.engine_type = "mysql"
    return SQLOperate(mock_sql_client)


@pytest.fixture
def mock_session():
    """Create a mock AsyncSession"""
    session = AsyncMock(spec=AsyncSession)
    return session


class TestSQLOperateValidation:
    """Test validation methods"""

    def test_validate_identifier_valid(self, sql_operate_postgres):
        """Test valid identifier validation"""
        # Should not raise exception
        sql_operate_postgres._validate_identifier("valid_table_name", "table name")
        sql_operate_postgres._validate_identifier("column123", "column name")
        sql_operate_postgres._validate_identifier("_underscore_start", "identifier")

    def test_validate_identifier_invalid_empty(self, sql_operate_postgres):
        """Test invalid empty identifier"""
        with pytest.raises(GeneralOperateException) as exc_info:
            sql_operate_postgres._validate_identifier("", "table name")
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 100

    def test_validate_identifier_invalid_none(self, sql_operate_postgres):
        """Test invalid None identifier"""
        with pytest.raises(GeneralOperateException) as exc_info:
            sql_operate_postgres._validate_identifier(None, "table name")
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 100

    def test_validate_identifier_too_long(self, sql_operate_postgres):
        """Test identifier that's too long"""
        long_name = "a" * 65  # 65 characters
        with pytest.raises(GeneralOperateException) as exc_info:
            sql_operate_postgres._validate_identifier(long_name, "table name")
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 101

    def test_validate_identifier_invalid_characters(self, sql_operate_postgres):
        """Test identifier with invalid characters"""
        with pytest.raises(GeneralOperateException) as exc_info:
            sql_operate_postgres._validate_identifier("table-name", "table name")
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 102

        with pytest.raises(GeneralOperateException) as exc_info:
            sql_operate_postgres._validate_identifier("table name", "table name")
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 102

    def test_validate_data_value_valid(self, sql_operate_postgres):
        """Test valid data value validation"""
        # Should not raise exception
        sql_operate_postgres._validate_data_value("normal string", "column1")
        sql_operate_postgres._validate_data_value(123, "column2")
        sql_operate_postgres._validate_data_value(None, "column3")
        sql_operate_postgres._validate_data_value(-999999, "column4")  # null_set

    def test_validate_data_value_too_long(self, sql_operate_postgres):
        """Test data value that's too long"""
        long_value = "a" * 65536  # 65536 characters
        with pytest.raises(GeneralOperateException) as exc_info:
            sql_operate_postgres._validate_data_value(long_value, "column1")
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 103

    def test_validate_data_value_dangerous_sql(self, sql_operate_postgres):
        """Test data value with dangerous SQL keywords"""
        with pytest.raises(GeneralOperateException) as exc_info:
            sql_operate_postgres._validate_data_value("drop table users", "column1")
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 104

        with pytest.raises(GeneralOperateException) as exc_info:
            sql_operate_postgres._validate_data_value("select from users", "column1")
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 104

        with pytest.raises(GeneralOperateException) as exc_info:
            sql_operate_postgres._validate_data_value("test-- comment", "column1")
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 104

    def test_validate_data_dict_valid(self, sql_operate_postgres):
        """Test valid data dictionary validation"""
        data = {"name": "John", "age": 25, "email": "john@example.com"}
        result = sql_operate_postgres._validate_data_dict(data, "test operation")
        assert result == data

    def test_validate_data_dict_with_null_values(self, sql_operate_postgres):
        """Test data dictionary with null values"""
        data = {"name": "John", "age": -999999, "email": "null", "city": "NYC"}
        result = sql_operate_postgres._validate_data_dict(data, "test operation")
        assert result == {"name": "John", "city": "NYC"}

    def test_validate_data_dict_empty_not_allowed(self, sql_operate_postgres):
        """Test empty data dictionary when not allowed"""
        with pytest.raises(GeneralOperateException) as exc_info:
            sql_operate_postgres._validate_data_dict({}, "test operation")
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 106

    def test_validate_data_dict_empty_allowed(self, sql_operate_postgres):
        """Test empty data dictionary when allowed"""
        result = sql_operate_postgres._validate_data_dict(
            {}, "test operation", allow_empty=True
        )
        assert result == {}

    def test_validate_data_dict_not_dict(self, sql_operate_postgres):
        """Test non-dictionary data"""
        with pytest.raises(GeneralOperateException) as exc_info:
            sql_operate_postgres._validate_data_dict("not a dict", "test operation")
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 105


class TestSQLOperateHelpers:
    """Test helper methods"""

    def test_build_where_clause_empty(self, sql_operate_postgres):
        """Test building WHERE clause with empty filters"""
        where_clause, params = sql_operate_postgres._build_where_clause(None)
        assert where_clause == ""
        assert params == {}

        where_clause, params = sql_operate_postgres._build_where_clause({})
        assert where_clause == ""
        assert params == {}

    def test_build_where_clause_with_filters(self, sql_operate_postgres):
        """Test building WHERE clause with filters"""
        filters = {"name": "John", "age": 25}
        where_clause, params = sql_operate_postgres._build_where_clause(filters)

        assert where_clause.startswith(" WHERE ")
        assert "name = :name" in where_clause
        assert "age = :age" in where_clause
        assert " AND " in where_clause
        assert params == {"name": "John", "age": 25}

    def test_build_where_clause_with_null_values(self, sql_operate_postgres):
        """Test building WHERE clause with null values filtered out"""
        filters = {"name": "John", "age": -999999, "city": "null"}
        where_clause, params = sql_operate_postgres._build_where_clause(filters)

        assert where_clause == " WHERE name = :name"
        assert params == {"name": "John"}


class TestSQLOperateCreate:
    """Test create method"""

    @pytest.mark.asyncio
    async def test_create_postgresql_success(self, sql_operate_postgres):
        """Test successful create operation on PostgreSQL"""
        data = {"name": "John", "age": 25}

        with patch.object(sql_operate_postgres, "_create_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session

            # Mock successful result
            mock_result = MagicMock()
            mock_row = MagicMock()
            mock_row._mapping = {"id": 1, "name": "John", "age": 25}
            mock_result.fetchall.return_value = [mock_row]
            mock_session.execute.return_value = mock_result

            result = await sql_operate_postgres.create_sql("users", data)

            assert len(result) == 1
            assert result[0] == {"id": 1, "name": "John", "age": 25}
            mock_session.execute.assert_called_once()
            mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_mysql_success(self, sql_operate_mysql):
        """Test successful create operation on MySQL"""
        data = {"name": "John", "age": 25}

        with patch.object(sql_operate_mysql, "_create_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session

            # Mock INSERT result
            mock_insert_result = MagicMock()
            mock_insert_result.lastrowid = 1
            mock_insert_result.rowcount = 1

            # Mock SELECT result
            mock_select_result = MagicMock()
            mock_row = MagicMock()
            mock_row._mapping = {"id": 1, "name": "John", "age": 25}
            mock_select_result.fetchall.return_value = [mock_row]

            mock_session.execute.side_effect = [mock_insert_result, mock_select_result]

            result = await sql_operate_mysql.create_sql("users", data)

            assert len(result) == 1
            assert result[0] == {"id": 1, "name": "John", "age": 25}
            assert mock_session.execute.call_count == 2
            mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_invalid_table_name(self, sql_operate_postgres):
        """Test create with invalid table name"""
        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operate_postgres.create_sql("invalid-table", {"name": "John"})
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 102

    @pytest.mark.asyncio
    async def test_create_empty_data(self, sql_operate_postgres):
        """Test create with empty data"""
        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operate_postgres.create_sql("users", {})
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 113


class TestSQLOperateRead:
    """Test read method"""

    @pytest.mark.asyncio
    async def test_read_all_records(self, sql_operate_postgres):
        """Test reading all records without filters"""
        with patch.object(sql_operate_postgres, "_create_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session

            # Mock result
            mock_result = MagicMock()
            mock_rows = [
                MagicMock(_mapping={"id": 1, "name": "John"}),
                MagicMock(_mapping={"id": 2, "name": "Jane"}),
            ]
            mock_result.fetchall.return_value = mock_rows
            mock_session.execute.return_value = mock_result

            result = await sql_operate_postgres.read_sql("users")

            assert len(result) == 2
            assert result[0] == {"id": 1, "name": "John"}
            assert result[1] == {"id": 2, "name": "Jane"}

    @pytest.mark.asyncio
    async def test_read_with_filters(self, sql_operate_postgres):
        """Test reading with filters"""
        filters = {"name": "John"}

        with patch.object(sql_operate_postgres, "_create_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session

            mock_result = MagicMock()
            mock_row = MagicMock(_mapping={"id": 1, "name": "John"})
            mock_result.fetchall.return_value = [mock_row]
            mock_session.execute.return_value = mock_result

            result = await sql_operate_postgres.read_sql("users", filters)

            assert len(result) == 1
            assert result[0] == {"id": 1, "name": "John"}

    @pytest.mark.asyncio
    async def test_read_with_ordering(self, sql_operate_postgres):
        """Test reading with ordering"""
        with patch.object(sql_operate_postgres, "_create_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session

            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result

            await sql_operate_postgres.read_sql(
                "users", order_by="name", order_direction="DESC"
            )

            # Check that ORDER BY was included in query
            call_args = mock_session.execute.call_args[0]
            query_text = str(call_args[0])
            assert "ORDER BY name DESC" in query_text

    @pytest.mark.asyncio
    async def test_read_with_pagination_postgresql(self, sql_operate_postgres):
        """Test reading with pagination on PostgreSQL"""
        with patch.object(sql_operate_postgres, "_create_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session

            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result

            await sql_operate_postgres.read_sql("users", limit=10, offset=5)

            # Check that LIMIT and OFFSET were included
            call_args = mock_session.execute.call_args[0]
            query_text = str(call_args[0])
            assert "LIMIT 10 OFFSET 5" in query_text

    @pytest.mark.asyncio
    async def test_read_with_pagination_mysql(self, sql_operate_mysql):
        """Test reading with pagination on MySQL"""
        with patch.object(sql_operate_mysql, "_create_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session

            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result

            await sql_operate_mysql.read_sql("users", limit=10, offset=5)

            # Check that MySQL syntax was used
            call_args = mock_session.execute.call_args[0]
            query_text = str(call_args[0])
            assert "LIMIT 5, 10" in query_text

    @pytest.mark.asyncio
    async def test_read_invalid_order_direction(self, sql_operate_postgres):
        """Test read with invalid order direction"""
        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operate_postgres.read_sql("users", order_direction="INVALID")
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 108

    @pytest.mark.asyncio
    async def test_read_invalid_limit(self, sql_operate_postgres):
        """Test read with invalid limit"""
        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operate_postgres.read_sql("users", limit=0)
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 109

    @pytest.mark.asyncio
    async def test_read_invalid_offset(self, sql_operate_postgres):
        """Test read with invalid offset"""
        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operate_postgres.read_sql("users", offset=-1)
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 110


class TestSQLOperateUpdate:
    """Test update method"""

    @pytest.mark.asyncio
    async def test_update_postgresql_success(self, sql_operate_postgres):
        """Test successful update operation on PostgreSQL"""
        data = {"name": "John Updated", "age": 26}

        with patch.object(sql_operate_postgres, "_create_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session

            # Mock successful result
            mock_result = MagicMock()
            mock_row = MagicMock()
            mock_row._mapping = {"id": 1, "name": "John Updated", "age": 26}
            mock_result.fetchone.return_value = mock_row
            mock_session.execute.return_value = mock_result

            result = await sql_operate_postgres.update_sql("users", [{"id": 1, "data": data}])

            assert len(result) == 1
            assert result[0] == {"id": 1, "name": "John Updated", "age": 26}
            mock_session.execute.assert_called_once()
            mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_mysql_success(self, sql_operate_mysql):
        """Test successful update operation on MySQL"""
        data = {"name": "John Updated", "age": 26}

        with patch.object(sql_operate_mysql, "_create_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session

            # Mock UPDATE result
            mock_update_result = MagicMock()
            mock_update_result.rowcount = 1

            # Mock SELECT result
            mock_select_result = MagicMock()
            mock_row = MagicMock()
            mock_row._mapping = {"id": 1, "name": "John Updated", "age": 26}
            mock_select_result.fetchone.return_value = mock_row

            mock_session.execute.side_effect = [mock_update_result, mock_select_result]

            result = await sql_operate_mysql.update_sql("users", [{"id": 1, "data": data}])

            assert len(result) == 1
            assert result[0] == {"id": 1, "name": "John Updated", "age": 26}
            assert mock_session.execute.call_count == 2
            mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_record_not_found(self, sql_operate_postgres):
        """Test update when record is not found"""
        data = {"name": "John Updated"}

        with patch.object(sql_operate_postgres, "_create_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session

            # Mock no result
            mock_result = MagicMock()
            mock_result.fetchone.return_value = None
            mock_session.execute.return_value = mock_result

            result = await sql_operate_postgres.update_sql("users", [{"id": 999, "data": data}])

            # Since no record was found, the result should be empty
            assert len(result) == 0


class TestSQLOperateDelete:
    """Test delete method"""

    @pytest.mark.asyncio
    async def test_delete_success(self, sql_operate_postgres):
        """Test successful delete operation"""
        with patch.object(sql_operate_postgres, "_create_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session

            # Mock successful delete
            mock_result = MagicMock()
            mock_result.rowcount = 1
            mock_session.execute.return_value = mock_result

            result = await sql_operate_postgres.delete_sql("users", 1)

            assert len(result) == 1
            assert 1 in result
            mock_session.execute.assert_called_once()
            mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_not_found(self, sql_operate_postgres):
        """Test delete when record is not found"""
        with patch.object(sql_operate_postgres, "_create_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session

            # Mock no rows affected
            mock_result = MagicMock()
            mock_result.rowcount = 0
            mock_session.execute.return_value = mock_result

            result = await sql_operate_postgres.delete_sql("users", 999)

            assert len(result) == 0


class TestSQLOperateDeleteMany:
    """Test delete_many method"""

    @pytest.mark.asyncio
    async def test_delete_many_success(self, sql_operate_postgres):
        """Test successful delete_many operation"""
        filters = {"status": "inactive", "age": 18}

        with patch.object(sql_operate_postgres, "_create_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session

            # Mock successful delete - first the select query, then the delete query
            mock_select_result = MagicMock()
            mock_select_result.fetchall.return_value = [(1,), (2,), (3,)]  # 3 IDs to delete
            mock_delete_result = MagicMock()
            mock_delete_result.rowcount = 3
            mock_session.execute.side_effect = [mock_select_result, mock_delete_result]

            result = await sql_operate_postgres.delete_filter("users", filters)

            assert result == [1, 2, 3]  # Should return list of deleted IDs
            assert mock_session.execute.call_count == 2  # SELECT then DELETE
            mock_session.commit.assert_called_once()

            # Verify the queries contain WHERE clause with filters
            call_args_list = mock_session.execute.call_args_list
            select_query_text = str(call_args_list[0][0][0])
            delete_query_text = str(call_args_list[1][0][0])
            assert "SELECT id FROM users WHERE" in select_query_text
            assert "DELETE FROM users WHERE" in delete_query_text

    @pytest.mark.asyncio
    async def test_delete_many_no_matches(self, sql_operate_postgres):
        """Test delete_many when no records match"""
        filters = {"status": "nonexistent"}

        with patch.object(sql_operate_postgres, "_create_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session

            # Mock no rows found in select
            mock_select_result = MagicMock()
            mock_select_result.fetchall.return_value = []  # No IDs found
            mock_session.execute.return_value = mock_select_result

            result = await sql_operate_postgres.delete_filter("users", filters)

            assert result == []  # Should return empty list

    @pytest.mark.asyncio
    async def test_delete_many_empty_filters(self, sql_operate_postgres):
        """Test delete_many with empty filters"""
        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operate_postgres.delete_filter("users", {})
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 111

    @pytest.mark.asyncio
    async def test_delete_many_none_filters(self, sql_operate_postgres):
        """Test delete_many with None filters"""
        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operate_postgres.delete_filter("users", None)
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 111

    @pytest.mark.asyncio
    async def test_delete_many_invalid_table_name(self, sql_operate_postgres):
        """Test delete_many with invalid table name"""
        filters = {"status": "inactive"}
        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operate_postgres.delete_filter("invalid-table", filters)
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 102

    @pytest.mark.asyncio
    async def test_delete_many_with_null_values_filtered(self, sql_operate_postgres):
        """Test delete_many with null values in filters (should be filtered out)"""
        filters = {"status": "inactive", "age": -999999, "city": "null"}

        with patch.object(sql_operate_postgres, "_create_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session

            # Mock select returning 2 IDs, then delete
            mock_select_result = MagicMock()
            mock_select_result.fetchall.return_value = [(5,), (6,)]  # 2 IDs to delete
            mock_delete_result = MagicMock()
            mock_delete_result.rowcount = 2
            mock_session.execute.side_effect = [mock_select_result, mock_delete_result]

            result = await sql_operate_postgres.delete_filter("users", filters)

            assert result == [5, 6]  # Should return list of deleted IDs

            # Verify only non-null values are in the query
            call_args = mock_session.execute.call_args[0]
            query_text = str(call_args[0])
            assert "status = :status" in query_text
            assert "age = :age" not in query_text  # Should be filtered out
            assert "city = :city" not in query_text  # Should be filtered out

    @pytest.mark.asyncio
    async def test_delete_many_dangerous_values(self, sql_operate_postgres):
        """Test delete_many with dangerous SQL values"""
        filters = {"name": "drop table users"}

        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operate_postgres.delete_filter("users", filters)
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 104


class TestSQLOperateCount:
    """Test count method"""

    @pytest.mark.asyncio
    async def test_count_all_records(self, sql_operate_postgres):
        """Test counting all records"""
        with patch.object(sql_operate_postgres, "_create_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session

            # Mock count result
            mock_result = MagicMock()
            mock_row = [5]  # COUNT(*) returns a single value
            mock_result.fetchone.return_value = mock_row
            mock_session.execute.return_value = mock_result

            result = await sql_operate_postgres.count("users")

            assert result == 5

    @pytest.mark.asyncio
    async def test_count_with_filters(self, sql_operate_postgres):
        """Test counting with filters"""
        filters = {"active": True}

        with patch.object(sql_operate_postgres, "_create_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session

            mock_result = MagicMock()
            mock_row = [3]
            mock_result.fetchone.return_value = mock_row
            mock_session.execute.return_value = mock_result

            result = await sql_operate_postgres.count("users", filters)

            assert result == 3


class TestSQLOperateHealthCheck:
    """Test health_check method"""

    @pytest.mark.asyncio
    async def test_health_check_success(self, sql_operate_postgres):
        """Test successful health check"""
        with patch.object(sql_operate_postgres, "_create_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session

            # Mock successful SELECT 1
            mock_result = MagicMock()
            mock_result.fetchone.return_value = [1]
            mock_session.execute.return_value = mock_result

            result = await sql_operate_postgres.health_check()

            assert result is True

    @pytest.mark.asyncio
    async def test_health_check_failure(self, sql_operate_postgres):
        """Test health check failure"""
        with patch.object(sql_operate_postgres, "_create_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session

            # Mock database error
            mock_session.execute.side_effect = DBAPIError(
                "Connection failed", None, None
            )

            result = await sql_operate_postgres.health_check()

            assert result is False


class TestSQLOperateExceptionHandler:
    """Test exception handling - testing the decorator directly"""

    def test_exception_handler_asyncpg_error(self, sql_operate_postgres):
        """Test exception handler with asyncpg error"""
        # Mock asyncpg error with proper args
        pg_error = asyncpg.PostgresError("Unique constraint violation")
        pg_error.sqlstate = "23505"  # Unique violation
        db_error = DBAPIError("PostgreSQL error", None, pg_error)

        # Test the exception handler wrapper directly
        @sql_operate_postgres.exception_handler
        def test_func(self):
            raise db_error

        with pytest.raises(GeneralOperateException) as exc_info:
            test_func(sql_operate_postgres)

        assert exc_info.value.status_code == 487  # Generic database error status code
        assert exc_info.value.message_code == "UNKNOWN"  # Generic error code

    def test_exception_handler_mysql_error(self, sql_operate_mysql):
        """Test exception handler with MySQL error"""
        # Mock MySQL error
        mysql_error = pymysql.Error(1062, "Duplicate entry")
        db_error = DBAPIError("MySQL error", None, mysql_error)

        # Test the exception handler wrapper directly
        @sql_operate_mysql.exception_handler
        def test_func(self):
            raise db_error

        with pytest.raises(GeneralOperateException) as exc_info:
            test_func(sql_operate_mysql)

        assert exc_info.value.status_code == 486
        assert exc_info.value.message_code == 1062

    def test_exception_handler_unmapped_instance_error(self, sql_operate_postgres):
        """Test exception handler with UnmappedInstanceError"""

        # Test the exception handler wrapper directly
        @sql_operate_postgres.exception_handler
        def test_func(self):
            raise UnmappedInstanceError("Unmapped instance")

        with pytest.raises(GeneralOperateException) as exc_info:
            test_func(sql_operate_postgres)

        assert exc_info.value.status_code == 486
        assert exc_info.value.message_code == 2


@pytest.mark.asyncio
async def test_integration_workflow(sql_operate_postgres):
    """Test a complete workflow of SQL operations"""
    with patch.object(sql_operate_postgres, "_create_session") as mock_session_ctx:
        mock_session = AsyncMock()
        mock_session_ctx.return_value.__aenter__.return_value = mock_session

        # Mock responses for different operations
        mock_results = {
            "create": MagicMock(_mapping={"id": 1, "name": "John", "age": 25}),
            "read": [MagicMock(_mapping={"id": 1, "name": "John", "age": 25})],
            "update": MagicMock(_mapping={"id": 1, "name": "John Updated", "age": 26}),
            "count": [5],
            "delete": MagicMock(rowcount=1),
        }

        def mock_execute_side_effect(*args, **kwargs):
            query = str(args[0]).upper()
            if "INSERT" in query:
                result = MagicMock()
                result.fetchall.return_value = [mock_results["create"]]
                return result
            elif "SELECT COUNT" in query:
                result = MagicMock()
                result.fetchone.return_value = mock_results["count"]
                return result
            elif "SELECT" in query:
                result = MagicMock()
                result.fetchall.return_value = mock_results["read"]
                return result
            elif "UPDATE" in query:
                result = MagicMock()
                result.fetchone.return_value = mock_results["update"]
                return result
            elif "DELETE" in query:
                return mock_results["delete"]
            else:
                return MagicMock()

        mock_session.execute.side_effect = mock_execute_side_effect

        # 1. Create a user
        user_data = {"name": "John", "age": 25}
        created_user = await sql_operate_postgres.create_sql("users", user_data)
        assert len(created_user) == 1
        assert created_user[0]["name"] == "John"
        assert created_user[0]["age"] == 25

        # 2. Read users
        users = await sql_operate_postgres.read_sql("users")
        assert len(users) == 1
        assert users[0]["name"] == "John"

        # 3. Count users
        count = await sql_operate_postgres.count("users")
        assert count == 5

        # 4. Update user
        update_data = {"name": "John Updated", "age": 26}
        updated_users = await sql_operate_postgres.update_sql("users", [{"id": 1, "data": update_data}])
        assert len(updated_users) == 1
        assert updated_users[0]["name"] == "John Updated"
        assert updated_users[0]["age"] == 26

        # 5. Delete user
        deleted_ids = await sql_operate_postgres.delete_sql("users", 1)
        assert len(deleted_ids) == 1
        assert 1 in deleted_ids

        # 6. Health check
        with patch.object(mock_session, "execute") as mock_health_execute:
            mock_health_result = MagicMock()
            mock_health_result.fetchone.return_value = [1]
            mock_health_execute.return_value = mock_health_result

            health = await sql_operate_postgres.health_check()
            assert health is True
