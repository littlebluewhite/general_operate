from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

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

        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
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

        with patch.object(sql_operate_mysql, "create_external_session") as mock_session_ctx:
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
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
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

        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
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
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
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
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
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
        with patch.object(sql_operate_mysql, "create_external_session") as mock_session_ctx:
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

        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
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

        with patch.object(sql_operate_mysql, "create_external_session") as mock_session_ctx:
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

        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
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
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
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
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
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

        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
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

        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
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

        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
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
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session

            # Mock count result
            mock_result = MagicMock()
            mock_row = [5]  # COUNT(*) returns a single value
            mock_result.fetchone.return_value = mock_row
            mock_session.execute.return_value = mock_result

            result = await sql_operate_postgres.count_sql("users")

            assert result == 5

    @pytest.mark.asyncio
    async def test_count_with_filters(self, sql_operate_postgres):
        """Test counting with filters"""
        filters = {"active": True}

        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session

            mock_result = MagicMock()
            mock_row = [3]
            mock_result.fetchone.return_value = mock_row
            mock_session.execute.return_value = mock_result

            result = await sql_operate_postgres.count_sql("users", filters)

            assert result == 3


class TestSQLOperateHealthCheck:
    """Test health_check method"""

    @pytest.mark.asyncio
    async def test_health_check_success(self, sql_operate_postgres):
        """Test successful health check"""
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
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
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session

            # Mock database error
            mock_session.execute.side_effect = DBAPIError(
                "Connection failed", None, None
            )

            result = await sql_operate_postgres.health_check()

            assert result is False

@pytest.mark.asyncio
async def test_integration_workflow(sql_operate_postgres):
    """Test a complete workflow of SQL operations"""
    with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
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
        count = await sql_operate_postgres.count_sql("users")
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


class TestSQLOperateReadWithDateRange:
    """Test read_sql_with_date_range method"""

    @pytest.mark.asyncio
    async def test_read_with_date_range_basic(self, sql_operate_postgres):
        """Test basic date range query"""
        from datetime import datetime
        
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 12, 31)
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_rows = [
                MagicMock(_mapping={"id": 1, "name": "Record 1", "created_at": start_date}),
                MagicMock(_mapping={"id": 2, "name": "Record 2", "created_at": end_date}),
            ]
            mock_result.fetchall.return_value = mock_rows
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate_postgres.read_sql_with_date_range(
                "events", 
                start_date=start_date,
                end_date=end_date
            )
            
            assert len(result) == 2
            assert result[0]["name"] == "Record 1"
            assert result[1]["name"] == "Record 2"
            
            # Verify query contains date range conditions
            call_args = mock_session.execute.call_args[0]
            query_text = str(call_args[0])
            assert "created_at >= :start_date" in query_text
            assert "created_at <= :end_date" in query_text

    @pytest.mark.asyncio
    async def test_read_with_date_range_custom_field(self, sql_operate_postgres):
        """Test date range query with custom date field"""
        from datetime import datetime
        
        start_date = datetime(2023, 1, 1)
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result
            
            await sql_operate_postgres.read_sql_with_date_range(
                "orders",
                date_field="order_date",
                start_date=start_date
            )
            
            # Verify custom date field is used
            call_args = mock_session.execute.call_args[0]
            query_text = str(call_args[0])
            assert "order_date >= :start_date" in query_text

    @pytest.mark.asyncio
    async def test_read_with_date_range_and_filters(self, sql_operate_postgres):
        """Test date range query with additional filters"""
        from datetime import datetime
        
        filters = {"status": "active", "category": "electronics"}
        start_date = datetime(2023, 1, 1)
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result
            
            await sql_operate_postgres.read_sql_with_date_range(
                "products",
                filters=filters,
                start_date=start_date
            )
            
            # Verify both filters and date range are included
            call_args = mock_session.execute.call_args[0]
            query_text = str(call_args[0])
            assert "status = :filter_status" in query_text
            assert "category = :filter_category" in query_text
            assert "created_at >= :start_date" in query_text
            assert " AND " in query_text

    @pytest.mark.asyncio
    async def test_read_with_date_range_list_filters(self, sql_operate_postgres):
        """Test date range query with list filters (IN clause)"""
        from datetime import datetime
        
        filters = {"status": ["active", "pending", "completed"]}
        start_date = datetime(2023, 1, 1)
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result
            
            await sql_operate_postgres.read_sql_with_date_range(
                "orders",
                filters=filters,
                start_date=start_date
            )
            
            # Verify IN clause is generated correctly
            call_args = mock_session.execute.call_args[0]
            query_text = str(call_args[0])
            assert "status IN (:filter_status_0, :filter_status_1, :filter_status_2)" in query_text
            assert "created_at >= :start_date" in query_text

    @pytest.mark.asyncio
    async def test_read_with_date_range_null_filters(self, sql_operate_postgres):
        """Test date range query with null values in filters (should be filtered out)"""
        from datetime import datetime
        
        filters = {"name": "John", "age": -999999, "city": "null"}
        start_date = datetime(2023, 1, 1)
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result
            
            await sql_operate_postgres.read_sql_with_date_range(
                "users",
                filters=filters,
                start_date=start_date
            )
            
            # Verify only non-null values are in the query
            call_args = mock_session.execute.call_args[0]
            query_text = str(call_args[0])
            assert "name = :filter_name" in query_text
            assert "age = :filter_age" not in query_text  # Should be filtered out
            assert "city = :filter_city" not in query_text  # Should be filtered out
            assert "created_at >= :start_date" in query_text

    @pytest.mark.asyncio
    async def test_read_with_date_range_only_start_date(self, sql_operate_postgres):
        """Test date range query with only start date"""
        from datetime import datetime
        
        start_date = datetime(2023, 1, 1)
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result
            
            await sql_operate_postgres.read_sql_with_date_range(
                "events",
                start_date=start_date
            )
            
            # Verify only start date condition is added
            call_args = mock_session.execute.call_args[0]
            query_text = str(call_args[0])
            assert "created_at >= :start_date" in query_text
            assert "created_at <= :end_date" not in query_text

    @pytest.mark.asyncio
    async def test_read_with_date_range_only_end_date(self, sql_operate_postgres):
        """Test date range query with only end date"""
        from datetime import datetime
        
        end_date = datetime(2023, 12, 31)
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result
            
            await sql_operate_postgres.read_sql_with_date_range(
                "events",
                end_date=end_date
            )
            
            # Verify only end date condition is added
            call_args = mock_session.execute.call_args[0]
            query_text = str(call_args[0])
            assert "created_at <= :end_date" in query_text
            assert "created_at >= :start_date" not in query_text

    @pytest.mark.asyncio
    async def test_read_with_date_range_no_dates(self, sql_operate_postgres):
        """Test date range query with no date filters (should work like regular read)"""
        filters = {"status": "active"}
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result
            
            await sql_operate_postgres.read_sql_with_date_range(
                "events",
                filters=filters
            )
            
            # Verify no date conditions are added
            call_args = mock_session.execute.call_args[0]
            query_text = str(call_args[0])
            assert "status = :filter_status" in query_text
            assert "created_at >= :start_date" not in query_text
            assert "created_at <= :end_date" not in query_text

    @pytest.mark.asyncio
    async def test_read_with_date_range_with_ordering(self, sql_operate_postgres):
        """Test date range query with ordering"""
        from datetime import datetime
        
        start_date = datetime(2023, 1, 1)
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result
            
            await sql_operate_postgres.read_sql_with_date_range(
                "events",
                start_date=start_date,
                order_by="created_at",
                order_direction="ASC"
            )
            
            # Verify ORDER BY is included
            call_args = mock_session.execute.call_args[0]
            query_text = str(call_args[0])
            assert "ORDER BY created_at ASC" in query_text

    @pytest.mark.asyncio
    async def test_read_with_date_range_pagination_postgresql(self, sql_operate_postgres):
        """Test date range query with pagination on PostgreSQL"""
        from datetime import datetime
        
        start_date = datetime(2023, 1, 1)
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result
            
            await sql_operate_postgres.read_sql_with_date_range(
                "events",
                start_date=start_date,
                limit=10,
                offset=5
            )
            
            # Check PostgreSQL pagination syntax
            call_args = mock_session.execute.call_args[0]
            query_text = str(call_args[0])
            assert "LIMIT 10 OFFSET 5" in query_text

    @pytest.mark.asyncio
    async def test_read_with_date_range_pagination_mysql(self, sql_operate_mysql):
        """Test date range query with pagination on MySQL"""
        from datetime import datetime
        
        start_date = datetime(2023, 1, 1)
        
        with patch.object(sql_operate_mysql, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result
            
            await sql_operate_mysql.read_sql_with_date_range(
                "events",
                start_date=start_date,
                limit=10,
                offset=5
            )
            
            # Check MySQL pagination syntax
            call_args = mock_session.execute.call_args[0]
            query_text = str(call_args[0])
            assert "LIMIT 5, 10" in query_text

    @pytest.mark.asyncio
    async def test_read_with_date_range_invalid_table_name(self, sql_operate_postgres):
        """Test date range query with invalid table name"""
        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operate_postgres.read_sql_with_date_range("invalid-table")
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 102

    @pytest.mark.asyncio
    async def test_read_with_date_range_invalid_date_field(self, sql_operate_postgres):
        """Test date range query with invalid date field"""
        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operate_postgres.read_sql_with_date_range("events", date_field="invalid-field")
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 102

    @pytest.mark.asyncio
    async def test_read_with_date_range_invalid_order_by(self, sql_operate_postgres):
        """Test date range query with invalid order_by column"""
        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operate_postgres.read_sql_with_date_range("events", order_by="invalid-column")
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 102

    @pytest.mark.asyncio
    async def test_read_with_date_range_invalid_order_direction(self, sql_operate_postgres):
        """Test date range query with invalid order direction"""
        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operate_postgres.read_sql_with_date_range("events", order_direction="INVALID")
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 108

    @pytest.mark.asyncio
    async def test_read_with_date_range_invalid_limit(self, sql_operate_postgres):
        """Test date range query with invalid limit"""
        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operate_postgres.read_sql_with_date_range("events", limit=0)
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 109

    @pytest.mark.asyncio
    async def test_read_with_date_range_invalid_offset(self, sql_operate_postgres):
        """Test date range query with invalid offset"""
        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operate_postgres.read_sql_with_date_range("events", offset=-1)
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 110

    @pytest.mark.asyncio
    async def test_read_with_date_range_invalid_filter_column(self, sql_operate_postgres):
        """Test date range query with invalid filter column name"""
        filters = {"invalid-column": "value"}
        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operate_postgres.read_sql_with_date_range("events", filters=filters)
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 102

    @pytest.mark.asyncio
    async def test_read_with_date_range_with_external_session(self, sql_operate_postgres):
        """Test date range query with external session"""
        from datetime import datetime
        
        start_date = datetime(2023, 1, 1)
        external_session = AsyncMock(spec=AsyncSession)
        
        mock_result = MagicMock()
        mock_rows = [MagicMock(_mapping={"id": 1, "name": "Record 1"})]
        mock_result.fetchall.return_value = mock_rows
        external_session.execute.return_value = mock_result
        
        result = await sql_operate_postgres.read_sql_with_date_range(
            "events",
            start_date=start_date,
            session=external_session
        )
        
        assert len(result) == 1
        assert result[0]["name"] == "Record 1"
        
        # Verify external session was used
        external_session.execute.assert_called_once()
        # External session should not be committed/rolled back
        external_session.commit.assert_not_called()
        external_session.rollback.assert_not_called()

    @pytest.mark.asyncio
    async def test_read_with_date_range_various_date_types(self, sql_operate_postgres):
        """Test date range query with various date types"""
        from datetime import datetime, date
        import time
        
        # Test with different date types
        test_cases = [
            # datetime object
            {"start": datetime(2023, 1, 1), "end": datetime(2023, 12, 31)},
            # date object
            {"start": date(2023, 1, 1), "end": date(2023, 12, 31)},
            # timestamp
            {"start": 1672531200, "end": 1704067199},  # 2023 timestamps
            # string dates
            {"start": "2023-01-01", "end": "2023-12-31"},
        ]
        
        for i, dates in enumerate(test_cases):
            with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
                mock_session = AsyncMock()
                mock_session_ctx.return_value.__aenter__.return_value = mock_session
                
                mock_result = MagicMock()
                mock_result.fetchall.return_value = []
                mock_session.execute.return_value = mock_result
                
                await sql_operate_postgres.read_sql_with_date_range(
                    "events",
                    start_date=dates["start"],
                    end_date=dates["end"]
                )
                
                # Verify the function was called successfully
                mock_session.execute.assert_called_once()
                
                # Verify the query contains date range conditions
                call_args = mock_session.execute.call_args[0]
                query_text = str(call_args[0])
                assert "created_at >= :start_date" in query_text
                assert "created_at <= :end_date" in query_text


class TestSQLOperateReadWithConditions:
    """Test read_sql_with_conditions method"""

    @pytest.mark.asyncio
    async def test_read_with_conditions_basic(self, sql_operate_postgres):
        """Test basic conditional query"""
        conditions = ["age > :min_age", "status = :status"]
        params = {"min_age": 18, "status": "active"}
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_rows = [
                MagicMock(_mapping={"id": 1, "name": "Alice", "age": 25, "status": "active"}),
                MagicMock(_mapping={"id": 2, "name": "Bob", "age": 30, "status": "active"}),
            ]
            mock_result.fetchall.return_value = mock_rows
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate_postgres.read_sql_with_conditions(
                "users", conditions, params
            )
            
            assert len(result) == 2
            assert result[0]["name"] == "Alice"
            assert result[1]["name"] == "Bob"
            
            # Verify query structure
            call_args = mock_session.execute.call_args[0]
            query_text = str(call_args[0])
            assert "SELECT * FROM users WHERE" in query_text
            assert "age > :min_age AND status = :status" in query_text

    @pytest.mark.asyncio
    async def test_read_with_conditions_no_conditions(self, sql_operate_postgres):
        """Test conditional query with empty conditions"""
        conditions = []
        params = {}
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate_postgres.read_sql_with_conditions(
                "users", conditions, params
            )
            
            # Verify query doesn't have WHERE clause
            call_args = mock_session.execute.call_args[0]
            query_text = str(call_args[0])
            assert "SELECT * FROM users" in query_text
            assert "WHERE" not in query_text

    @pytest.mark.asyncio
    async def test_read_with_conditions_ordering(self, sql_operate_postgres):
        """Test conditional query with ordering"""
        conditions = ["status = :status"]
        params = {"status": "active"}
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result
            
            await sql_operate_postgres.read_sql_with_conditions(
                "users", conditions, params, order_by="name", order_direction="ASC"
            )
            
            # Verify ORDER BY is included
            call_args = mock_session.execute.call_args[0]
            query_text = str(call_args[0])
            assert "ORDER BY name ASC" in query_text

    @pytest.mark.asyncio
    async def test_read_with_conditions_simple_ordering(self, sql_operate_postgres):
        """Test conditional query with simple ordering"""
        conditions = ["status = :status"]
        params = {"status": "active"}
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result
            
            await sql_operate_postgres.read_sql_with_conditions(
                "users", conditions, params, order_by="priority"
            )
            
            # Verify ORDER BY is included correctly
            call_args = mock_session.execute.call_args[0]
            query_text = str(call_args[0])
            assert "ORDER BY priority" in query_text
            assert "ORDER BY priority DESC, created_at ASC DESC" not in query_text

    @pytest.mark.asyncio
    async def test_read_with_conditions_pagination_postgresql(self, sql_operate_postgres):
        """Test conditional query with pagination on PostgreSQL"""
        conditions = ["status = :status"]
        params = {"status": "active"}
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result
            
            await sql_operate_postgres.read_sql_with_conditions(
                "users", conditions, params, limit=10, offset=5
            )
            
            # Check PostgreSQL pagination syntax
            call_args = mock_session.execute.call_args[0]
            query_text = str(call_args[0])
            assert "LIMIT 10 OFFSET 5" in query_text

    @pytest.mark.asyncio
    async def test_read_with_conditions_pagination_mysql(self, sql_operate_mysql):
        """Test conditional query with pagination on MySQL"""
        conditions = ["status = :status"]
        params = {"status": "active"}
        
        with patch.object(sql_operate_mysql, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result
            
            await sql_operate_mysql.read_sql_with_conditions(
                "users", conditions, params, limit=10, offset=5
            )
            
            # Check MySQL pagination syntax
            call_args = mock_session.execute.call_args[0]
            query_text = str(call_args[0])
            assert "LIMIT 5, 10" in query_text

    @pytest.mark.asyncio
    async def test_read_with_conditions_only_offset_postgresql(self, sql_operate_postgres):
        """Test conditional query with only offset (no limit) on PostgreSQL"""
        conditions = ["status = :status"]
        params = {"status": "active"}
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result
            
            await sql_operate_postgres.read_sql_with_conditions(
                "users", conditions, params, offset=10
            )
            
            # Check PostgreSQL offset-only syntax
            call_args = mock_session.execute.call_args[0]
            query_text = str(call_args[0])
            assert "OFFSET 10" in query_text
            assert "LIMIT" not in query_text

    @pytest.mark.asyncio
    async def test_read_with_conditions_only_offset_mysql(self, sql_operate_mysql):
        """Test conditional query with only offset (no limit) on MySQL"""
        conditions = ["status = :status"]
        params = {"status": "active"}
        
        with patch.object(sql_operate_mysql, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result
            
            await sql_operate_mysql.read_sql_with_conditions(
                "users", conditions, params, offset=10
            )
            
            # Check MySQL requires LIMIT with large number for offset-only
            call_args = mock_session.execute.call_args[0]
            query_text = str(call_args[0])
            assert "LIMIT 10, 18446744073709551615" in query_text

    @pytest.mark.asyncio
    async def test_read_with_conditions_invalid_table_name(self, sql_operate_postgres):
        """Test conditional query with invalid table name"""
        conditions = ["status = :status"]
        params = {"status": "active"}
        
        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operate_postgres.read_sql_with_conditions(
                "invalid-table", conditions, params
            )
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 102

    @pytest.mark.asyncio
    async def test_read_with_conditions_invalid_order_by(self, sql_operate_postgres):
        """Test conditional query with invalid order_by column"""
        conditions = ["status = :status"]
        params = {"status": "active"}
        
        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operate_postgres.read_sql_with_conditions(
                "users", conditions, params, order_by="invalid-column"
            )
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 102

    @pytest.mark.asyncio
    async def test_read_with_conditions_invalid_order_direction(self, sql_operate_postgres):
        """Test conditional query with invalid order direction"""
        conditions = ["status = :status"]
        params = {"status": "active"}
        
        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operate_postgres.read_sql_with_conditions(
                "users", conditions, params, order_direction="INVALID"
            )
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 108

    @pytest.mark.asyncio
    async def test_read_with_conditions_invalid_limit(self, sql_operate_postgres):
        """Test conditional query with invalid limit"""
        conditions = ["status = :status"]
        params = {"status": "active"}
        
        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operate_postgres.read_sql_with_conditions(
                "users", conditions, params, limit=0
            )
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 109

    @pytest.mark.asyncio
    async def test_read_with_conditions_invalid_offset(self, sql_operate_postgres):
        """Test conditional query with invalid offset"""
        conditions = ["status = :status"]
        params = {"status": "active"}
        
        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operate_postgres.read_sql_with_conditions(
                "users", conditions, params, offset=-1
            )
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 110

    @pytest.mark.asyncio
    async def test_read_with_conditions_with_external_session(self, sql_operate_postgres):
        """Test conditional query with external session"""
        conditions = ["status = :status"]
        params = {"status": "active"}
        external_session = AsyncMock(spec=AsyncSession)
        
        mock_result = MagicMock()
        mock_rows = [MagicMock(_mapping={"id": 1, "name": "John"})]
        mock_result.fetchall.return_value = mock_rows
        external_session.execute.return_value = mock_result
        
        result = await sql_operate_postgres.read_sql_with_conditions(
            "users", conditions, params, session=external_session
        )
        
        assert len(result) == 1
        assert result[0]["name"] == "John"
        
        # Verify external session was used
        external_session.execute.assert_called_once()
        # External session should not be committed/rolled back
        external_session.commit.assert_not_called()
        external_session.rollback.assert_not_called()


class TestSQLOperateGetAggregatedData:
    """Test get_aggregated_data method"""

    @pytest.mark.asyncio
    async def test_get_aggregated_data_basic(self, sql_operate_postgres):
        """Test basic aggregation query"""
        group_by = ["department", "status"]
        aggregations = {"count": "*", "avg_salary": "salary"}
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_rows = [
                MagicMock(_mapping={"department": "Engineering", "status": "active", "count": 10, "avg_salary": 5}),
                MagicMock(_mapping={"department": "Marketing", "status": "active", "count": 5, "avg_salary": 3}),
            ]
            mock_result.fetchall.return_value = mock_rows
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate_postgres.get_aggregated_data(
                "employees", group_by, aggregations
            )
            
            assert len(result) == 2
            assert result[0]["department"] == "Engineering"
            assert result[0]["count"] == 10
            assert result[1]["department"] == "Marketing"
            assert result[1]["count"] == 5
            
            # Verify query structure
            call_args = mock_session.execute.call_args[0]
            query_text = str(call_args[0])
            assert "SELECT department, status, COUNT(*) as count, COUNT(salary) as avg_salary" in query_text
            assert "FROM employees" in query_text
            assert "GROUP BY department, status" in query_text

    @pytest.mark.asyncio
    async def test_get_aggregated_data_default_aggregation(self, sql_operate_postgres):
        """Test aggregation with default aggregations (count only)"""
        group_by = ["category"]
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_rows = [MagicMock(_mapping={"category": "Books", "count": 25})]
            mock_result.fetchall.return_value = mock_rows
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate_postgres.get_aggregated_data(
                "products", group_by
            )
            
            assert len(result) == 1
            assert result[0]["category"] == "Books"
            assert result[0]["count"] == 25
            
            # Verify default aggregation is used
            call_args = mock_session.execute.call_args[0]
            query_text = str(call_args[0])
            assert "COUNT(*) as count" in query_text

    @pytest.mark.asyncio
    async def test_get_aggregated_data_with_filters(self, sql_operate_postgres):
        """Test aggregation with filters"""
        group_by = ["department"]
        aggregations = {"total": "*"}
        filters = {"status": "active", "salary_grade": "senior"}
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result
            
            await sql_operate_postgres.get_aggregated_data(
                "employees", group_by, aggregations, filters
            )
            
            # Verify WHERE clause is included
            call_args = mock_session.execute.call_args[0]
            query_text = str(call_args[0])
            assert "WHERE" in query_text
            assert "status = :status" in query_text
            assert "salary_grade = :salary_grade" in query_text
            assert "GROUP BY department" in query_text

    @pytest.mark.asyncio
    async def test_get_aggregated_data_with_having(self, sql_operate_postgres):
        """Test aggregation with HAVING conditions"""
        group_by = ["department"]
        aggregations = {"count": "*"}
        having_conditions = ["COUNT(*) > 5", "AVG(salary) > 50000"]
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_session.execute.return_value = mock_result
            
            await sql_operate_postgres.get_aggregated_data(
                "employees", group_by, aggregations, having_conditions=having_conditions
            )
            
            # Verify HAVING clause is included
            call_args = mock_session.execute.call_args[0]
            query_text = str(call_args[0])
            assert "HAVING COUNT(*) > 5 AND AVG(salary) > 50000" in query_text

    @pytest.mark.asyncio
    async def test_get_aggregated_data_invalid_table_name(self, sql_operate_postgres):
        """Test aggregation with invalid table name"""
        group_by = ["department"]
        
        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operate_postgres.get_aggregated_data(
                "invalid-table", group_by
            )
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 102

    @pytest.mark.asyncio
    async def test_get_aggregated_data_invalid_group_by_field(self, sql_operate_postgres):
        """Test aggregation with invalid group by field"""
        group_by = ["valid_field", "invalid-field"]
        
        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operate_postgres.get_aggregated_data(
                "employees", group_by
            )
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 102

    @pytest.mark.asyncio
    async def test_get_aggregated_data_invalid_aggregation_field(self, sql_operate_postgres):
        """Test aggregation with invalid aggregation field"""
        group_by = ["department"]
        aggregations = {"count": "invalid-field"}
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            with pytest.raises(GeneralOperateException) as exc_info:
                await sql_operate_postgres.get_aggregated_data(
                    "employees", group_by, aggregations
                )
            assert exc_info.value.status_code == 400
            assert exc_info.value.message_code == 102

    @pytest.mark.asyncio
    async def test_get_aggregated_data_with_external_session(self, sql_operate_postgres):
        """Test aggregation with external session"""
        group_by = ["category"]
        external_session = AsyncMock(spec=AsyncSession)
        
        mock_result = MagicMock()
        mock_rows = [MagicMock(_mapping={"category": "Books", "count": 10})]
        mock_result.fetchall.return_value = mock_rows
        external_session.execute.return_value = mock_result
        
        result = await sql_operate_postgres.get_aggregated_data(
            "products", group_by, session=external_session
        )
        
        assert len(result) == 1
        assert result[0]["category"] == "Books"
        
        # Verify external session was used
        external_session.execute.assert_called_once()
        # External session should not be committed/rolled back
        external_session.commit.assert_not_called()
        external_session.rollback.assert_not_called()


class TestSQLOperateExecuteRawQuery:
    """Test execute_raw_query method"""

    @pytest.mark.asyncio
    async def test_execute_raw_query_fetch_all(self, sql_operate_postgres):
        """Test raw query with fetch_all mode"""
        query = "SELECT * FROM users WHERE age > :min_age"
        params = {"min_age": 18}
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_rows = [
                MagicMock(_mapping={"id": 1, "name": "Alice", "age": 25}),
                MagicMock(_mapping={"id": 2, "name": "Bob", "age": 30}),
            ]
            mock_result.fetchall.return_value = mock_rows
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate_postgres.execute_raw_query(
                query, params, fetch_mode="all"
            )
            
            assert len(result) == 2
            assert result[0]["name"] == "Alice"
            assert result[1]["name"] == "Bob"

    @pytest.mark.asyncio
    async def test_execute_raw_query_fetch_one(self, sql_operate_postgres):
        """Test raw query with fetch_one mode"""
        query = "SELECT * FROM users WHERE id = :user_id"
        params = {"user_id": 1}
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_row = MagicMock(_mapping={"id": 1, "name": "Alice", "age": 25})
            mock_result.fetchone.return_value = mock_row
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate_postgres.execute_raw_query(
                query, params, fetch_mode="one"
            )
            
            assert result["name"] == "Alice"
            assert result["age"] == 25

    @pytest.mark.asyncio
    async def test_execute_raw_query_fetch_one_no_result(self, sql_operate_postgres):
        """Test raw query with fetch_one mode and no result"""
        query = "SELECT * FROM users WHERE id = :user_id"
        params = {"user_id": 999}
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_result.fetchone.return_value = None
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate_postgres.execute_raw_query(
                query, params, fetch_mode="one"
            )
            
            assert result is None

    @pytest.mark.asyncio
    async def test_execute_raw_query_fetch_none(self, sql_operate_postgres):
        """Test raw query with fetch_none mode (e.g., for INSERT/UPDATE/DELETE)"""
        query = "UPDATE users SET status = :status WHERE department = :dept"
        params = {"status": "inactive", "dept": "marketing"}
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate_postgres.execute_raw_query(
                query, params, fetch_mode="none"
            )
            
            assert result is None

    @pytest.mark.asyncio
    async def test_execute_raw_query_no_params(self, sql_operate_postgres):
        """Test raw query without parameters"""
        query = "SELECT COUNT(*) as total FROM users"
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_rows = [MagicMock(_mapping={"total": 42})]
            mock_result.fetchall.return_value = mock_rows
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate_postgres.execute_raw_query(query)
            
            assert len(result) == 1
            assert result[0]["total"] == 42
            
            # Verify empty params dict was used
            call_args = mock_session.execute.call_args[0]
            assert call_args[1] == {}  # Empty params dict

    @pytest.mark.asyncio
    async def test_execute_raw_query_invalid_fetch_mode(self, sql_operate_postgres):
        """Test raw query with invalid fetch mode"""
        query = "SELECT * FROM users"
        
        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operate_postgres.execute_raw_query(
                query, fetch_mode="invalid"
            )
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 111

    @pytest.mark.asyncio
    async def test_execute_raw_query_with_external_session(self, sql_operate_postgres):
        """Test raw query with external session"""
        query = "SELECT * FROM users LIMIT 1"
        external_session = AsyncMock(spec=AsyncSession)
        
        mock_result = MagicMock()
        mock_rows = [MagicMock(_mapping={"id": 1, "name": "Alice"})]
        mock_result.fetchall.return_value = mock_rows
        external_session.execute.return_value = mock_result
        
        result = await sql_operate_postgres.execute_raw_query(
            query, session=external_session
        )
        
        assert len(result) == 1
        assert result[0]["name"] == "Alice"
        
        # Verify external session was used
        external_session.execute.assert_called_once()
        # External session should not be committed/rolled back
        external_session.commit.assert_not_called()
        external_session.rollback.assert_not_called()


class TestSQLOperateReadOne:
    """Test read_one method"""

    @pytest.mark.asyncio
    async def test_read_one_found(self, sql_operate_postgres):
        """Test read_one when record is found"""
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_row = MagicMock(_mapping={"id": 1, "name": "Alice", "email": "alice@example.com"})
            mock_result.fetchone.return_value = mock_row
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate_postgres.read_one("users", 1)
            
            assert result["id"] == 1
            assert result["name"] == "Alice"
            assert result["email"] == "alice@example.com"
            
            # Verify query structure
            call_args = mock_session.execute.call_args[0]
            query_text = str(call_args[0])
            assert "SELECT * FROM users WHERE id = :id_value" in query_text
            assert call_args[1] == {"id_value": 1}

    @pytest.mark.asyncio
    async def test_read_one_not_found(self, sql_operate_postgres):
        """Test read_one when record is not found"""
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_result.fetchone.return_value = None
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate_postgres.read_one("users", 999)
            
            assert result is None

    @pytest.mark.asyncio
    async def test_read_one_custom_id_column(self, sql_operate_postgres):
        """Test read_one with custom ID column"""
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_row = MagicMock(_mapping={"uuid": "abc123", "name": "Bob"})
            mock_result.fetchone.return_value = mock_row
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate_postgres.read_one(
                "users", "abc123", id_column="uuid"
            )
            
            assert result["uuid"] == "abc123"
            assert result["name"] == "Bob"
            
            # Verify custom ID column is used
            call_args = mock_session.execute.call_args[0]
            query_text = str(call_args[0])
            assert "SELECT * FROM users WHERE uuid = :id_value" in query_text

    @pytest.mark.asyncio
    async def test_read_one_invalid_table_name(self, sql_operate_postgres):
        """Test read_one with invalid table name"""
        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operate_postgres.read_one("invalid-table", 1)
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 102

    @pytest.mark.asyncio
    async def test_read_one_invalid_id_column(self, sql_operate_postgres):
        """Test read_one with invalid ID column name"""
        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operate_postgres.read_one(
                "users", 1, id_column="invalid-column"
            )
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 102

    @pytest.mark.asyncio
    async def test_read_one_with_external_session(self, sql_operate_postgres):
        """Test read_one with external session"""
        external_session = AsyncMock(spec=AsyncSession)
        
        mock_result = MagicMock()
        mock_row = MagicMock(_mapping={"id": 1, "name": "Alice"})
        mock_result.fetchone.return_value = mock_row
        external_session.execute.return_value = mock_result
        
        result = await sql_operate_postgres.read_one(
            "users", 1, session=external_session
        )
        
        assert result["id"] == 1
        assert result["name"] == "Alice"
        
        # Verify external session was used
        external_session.execute.assert_called_once()
        # External session should not be committed/rolled back
        external_session.commit.assert_not_called()
        external_session.rollback.assert_not_called()
