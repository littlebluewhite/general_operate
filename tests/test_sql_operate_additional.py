"""
Additional comprehensive tests for SQL operations to improve coverage
"""

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


class TestSQLOperateExecuteQuery:
    """Test execute_query method"""

    @pytest.mark.asyncio
    async def test_execute_query_select(self, sql_operate_postgres):
        """Test execute_query with SELECT statement"""
        query = "SELECT name, email FROM users WHERE active = :active"
        params = {"active": True}
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_rows = [
                MagicMock(_mapping={"name": "Alice", "email": "alice@example.com"}),
                MagicMock(_mapping={"name": "Bob", "email": "bob@example.com"}),
            ]
            mock_result.fetchall.return_value = mock_rows
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate_postgres.execute_query(query, params)
            
            assert len(result) == 2
            assert result[0]["name"] == "Alice"
            assert result[1]["name"] == "Bob"
            
            # Verify session was not committed (SELECT doesn't need commit)
            mock_session.commit.assert_not_called()

    @pytest.mark.asyncio
    async def test_execute_query_insert(self, sql_operate_postgres):
        """Test execute_query with INSERT statement"""
        query = "INSERT INTO users (name, email) VALUES (:name, :email)"
        params = {"name": "Charlie", "email": "charlie@example.com"}
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_result.rowcount = 1
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate_postgres.execute_query(query, params)
            
            assert result == {"affected_rows": 1}
            
            # Verify session was committed (non-SELECT needs commit)
            mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_query_show(self, sql_operate_postgres):
        """Test execute_query with SHOW statement"""
        query = "SHOW TABLES"
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_rows = [
                MagicMock(_mapping={"Tables_in_db": "users"}),
                MagicMock(_mapping={"Tables_in_db": "products"}),
            ]
            mock_result.fetchall.return_value = mock_rows
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate_postgres.execute_query(query)
            
            assert len(result) == 2
            assert result[0]["Tables_in_db"] == "users"
            assert result[1]["Tables_in_db"] == "products"
            
            # SHOW is treated like SELECT
            mock_session.commit.assert_not_called()

    @pytest.mark.asyncio
    async def test_execute_query_no_params(self, sql_operate_postgres):
        """Test execute_query without parameters"""
        query = "SELECT COUNT(*) as total FROM users"
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            mock_result = MagicMock()
            mock_rows = [MagicMock(_mapping={"total": 100})]
            mock_result.fetchall.return_value = mock_rows
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate_postgres.execute_query(query)
            
            assert len(result) == 1
            assert result[0]["total"] == 100
            
            # Verify empty dict was used for params
            call_args = mock_session.execute.call_args[0]
            assert call_args[1] == {}

    @pytest.mark.asyncio
    async def test_execute_query_result_without_rowcount(self, sql_operate_postgres):
        """Test execute_query when result doesn't have rowcount attribute"""
        query = "CREATE INDEX idx_users_email ON users(email)"
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            # Mock result without rowcount attribute
            mock_result = MagicMock(spec=[])
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate_postgres.execute_query(query)
            
            assert result == {"affected_rows": 0}
            mock_session.commit.assert_called_once()


class TestSQLOperateAdvancedFeatures:
    """Test advanced SQL features and edge cases"""

    @pytest.mark.asyncio
    async def test_create_external_session(self, sql_operate_postgres):
        """Test creating external session"""
        session = sql_operate_postgres.create_external_session()
        
        # Verify session is created from the client's engine
        assert session is not None
        # The _get_client method returns the actual client, not a mock

    def test_get_client(self, sql_operate_postgres):
        """Test _get_client method"""
        client = sql_operate_postgres._get_client()
        
        assert client is not None
        assert client == sql_operate_postgres._SQLOperate__sqlClient

    @pytest.mark.asyncio
    async def test_validate_data_dict_with_json_serializable_list(self, sql_operate_postgres):
        """Test data validation with JSON-serializable list"""
        data = {
            "name": "John",
            "tags": ["python", "javascript", "react"],
            "metadata": {"created_by": "admin", "version": 1}
        }
        
        result = sql_operate_postgres._validate_data_dict(data, "test operation")
        
        # Lists and dicts should be JSON-serialized
        assert result["name"] == "John"
        assert "tags" in result
        assert "metadata" in result
        # Verify they're JSON strings
        import json
        assert json.loads(result["tags"]) == ["python", "javascript", "react"]
        assert json.loads(result["metadata"]) == {"created_by": "admin", "version": 1}

    @pytest.mark.asyncio
    async def test_validate_data_dict_with_non_serializable_list_data(self, sql_operate_postgres):
        """Test data validation with non-JSON-serializable list data"""
        class NonSerializable:
            pass
        
        data = {
            "name": "John",
            "complex_list": [NonSerializable()]  # List with non-serializable object
        }
        
        with pytest.raises(GeneralOperateException) as exc_info:
            sql_operate_postgres._validate_data_dict(data, "test operation")
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 108

    @pytest.mark.asyncio
    async def test_validate_data_dict_no_valid_data(self, sql_operate_postgres):
        """Test data validation when all values are filtered out as null"""
        data = {
            "field1": -999999,  # null_set value
            "field2": "null",   # null_set value
            "field3": None      # None value
        }
        
        with pytest.raises(GeneralOperateException) as exc_info:
            sql_operate_postgres._validate_data_dict(data, "test operation")
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 107

    @pytest.mark.asyncio
    async def test_validate_data_value_with_unhashable_types(self, sql_operate_postgres):
        """Test data value validation with unhashable types (lists, dicts)"""
        # These should not raise exceptions
        sql_operate_postgres._validate_data_value([1, 2, 3], "list_field")
        sql_operate_postgres._validate_data_value({"key": "value"}, "dict_field")
        sql_operate_postgres._validate_data_value(set([1, 2, 3]), "set_field")

    @pytest.mark.asyncio
    async def test_health_check_unexpected_result(self, sql_operate_postgres):
        """Test health check with unexpected result"""
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            # Mock unexpected result (not 1)
            mock_result = MagicMock()
            mock_result.fetchone.return_value = [0]  # Unexpected value
            mock_session.execute.return_value = mock_result
            
            with patch('structlog.get_logger') as mock_logger:
                mock_log = MagicMock()
                mock_logger.return_value = mock_log
                
                result = await sql_operate_postgres.health_check()
                
                assert result is False
                mock_log.warning.assert_called_once()

    @pytest.mark.asyncio
    async def test_health_check_connection_error(self, sql_operate_postgres):
        """Test health check with connection error"""
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            # Mock connection error
            mock_session.execute.side_effect = ConnectionError("Connection lost")
            
            with patch('structlog.get_logger') as mock_logger:
                mock_log = MagicMock()
                mock_logger.return_value = mock_log
                
                result = await sql_operate_postgres.health_check()
                
                assert result is False
                mock_log.warning.assert_called_once()
                assert "Connection error" in str(mock_log.warning.call_args)

    @pytest.mark.asyncio
    async def test_health_check_with_asyncpg_error(self, sql_operate_postgres):
        """Test health check with asyncpg PostgresError"""
        import asyncpg
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            # Mock asyncpg error
            mock_session.execute.side_effect = asyncpg.PostgresError("PostgreSQL error")
            
            with patch('structlog.get_logger') as mock_logger:
                mock_log = MagicMock()
                mock_logger.return_value = mock_log
                
                result = await sql_operate_postgres.health_check()
                
                assert result is False
                mock_log.warning.assert_called_once()
                assert "Database health check failed" in str(mock_log.warning.call_args)

    @pytest.mark.asyncio
    async def test_build_where_clause_with_list_filters_postgresql(self, sql_operate_postgres):
        """Test WHERE clause building with list filters on PostgreSQL"""
        filters = {
            "status": ["active", "pending", "completed"],
            "department": ["engineering", "marketing"]
        }
        
        where_clause, params = sql_operate_postgres._build_where_clause(filters)
        
        assert " WHERE " in where_clause
        assert "status = ANY(:status)" in where_clause
        assert "department = ANY(:department)" in where_clause
        assert " AND " in where_clause
        
        # PostgreSQL uses arrays natively
        assert params["status"] == ["active", "pending", "completed"]
        assert params["department"] == ["engineering", "marketing"]

    @pytest.mark.asyncio
    async def test_build_where_clause_with_list_filters_mysql(self, sql_operate_mysql):
        """Test WHERE clause building with list filters on MySQL"""
        filters = {
            "status": ["active", "pending"],
            "priority": [1, 2, 3]
        }
        
        where_clause, params = sql_operate_mysql._build_where_clause(filters)
        
        assert " WHERE " in where_clause
        assert "status IN (:status_0, :status_1)" in where_clause
        assert "priority IN (:priority_0, :priority_1, :priority_2)" in where_clause
        assert " AND " in where_clause
        
        # MySQL expands parameters manually
        assert params["status_0"] == "active"
        assert params["status_1"] == "pending"
        assert params["priority_0"] == 1
        assert params["priority_1"] == 2
        assert params["priority_2"] == 3

    @pytest.mark.asyncio
    async def test_build_where_clause_with_empty_list(self, sql_operate_postgres):
        """Test WHERE clause building with empty list (should be ignored)"""
        filters = {
            "status": [],  # Empty list should be ignored
            "name": "John"  # Regular filter
        }
        
        where_clause, params = sql_operate_postgres._build_where_clause(filters)
        
        assert where_clause == " WHERE name = :name"
        assert params == {"name": "John"}

    @pytest.mark.asyncio
    async def test_build_where_clause_with_unhashable_values(self, sql_operate_postgres):
        """Test WHERE clause building with unhashable values"""
        filters = {
            "metadata": {"key": "value"},  # Unhashable dict
            "tags": ["python", "javascript"]  # List
        }
        
        where_clause, params = sql_operate_postgres._build_where_clause(filters)
        
        assert " WHERE " in where_clause
        assert "metadata = :metadata" in where_clause
        assert "tags = ANY(:tags)" in where_clause
        assert " AND " in where_clause
        
        assert params["metadata"] == {"key": "value"}
        assert params["tags"] == ["python", "javascript"]

    def test_null_set_handling(self, sql_operate_postgres):
        """Test null_set values are properly handled"""
        # Test the null_set contains expected values
        assert -999999 in sql_operate_postgres.null_set
        assert "null" in sql_operate_postgres.null_set
        
        # Test validation with null_set values
        sql_operate_postgres._validate_data_value(-999999, "test_field")
        sql_operate_postgres._validate_data_value("null", "test_field")

    def test_database_type_detection(self, sql_operate_postgres, sql_operate_mysql):
        """Test database type detection for PostgreSQL vs MySQL"""
        assert sql_operate_postgres._is_postgresql is True
        assert sql_operate_mysql._is_postgresql is False


class TestSQLOperateTransactionManagement:
    """Test transaction management with external sessions"""

    @pytest.mark.asyncio
    async def test_create_with_external_session_success(self, sql_operate_postgres):
        """Test create operation with external session (successful)"""
        data = {"name": "John", "age": 25}
        external_session = AsyncMock(spec=AsyncSession)
        
        # Mock successful result
        mock_result = MagicMock()
        mock_row = MagicMock()
        mock_row._mapping = {"id": 1, "name": "John", "age": 25}
        mock_result.fetchall.return_value = [mock_row]
        external_session.execute.return_value = mock_result
        
        result = await sql_operate_postgres.create_sql(
            "users", data, session=external_session
        )
        
        assert len(result) == 1
        assert result[0] == {"id": 1, "name": "John", "age": 25}
        
        # External session should be used but not committed/rolled back
        external_session.execute.assert_called_once()
        external_session.commit.assert_not_called()
        external_session.rollback.assert_not_called()

    @pytest.mark.asyncio
    async def test_auto_session_rollback_on_error(self, sql_operate_postgres):
        """Test auto-managed session rollback on error"""
        data = {"name": "John", "age": 25}
        
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            # Mock exception during execution
            mock_session.execute.side_effect = Exception("Database error")
            
            with pytest.raises(Exception, match="Database error"):
                await sql_operate_postgres.create_sql("users", data)
            
            # Verify rollback was called
            mock_session.rollback.assert_called_once()
            mock_session.commit.assert_not_called()

    @pytest.mark.asyncio
    async def test_create_with_list_data_validation_error(self, sql_operate_postgres):
        """Test create with list data that has validation errors"""
        data = [
            {"name": "John", "age": 25},
            {"name": "drop table users", "age": 30},  # Invalid SQL injection attempt
            {"name": "Alice", "age": 22}
        ]
        
        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operate_postgres.create_sql("users", data)
        
        assert exc_info.value.status_code == 400
        assert "Invalid data at index 1" in exc_info.value.message
        assert exc_info.value.message_code == 113

    @pytest.mark.asyncio
    async def test_create_with_invalid_data_type(self, sql_operate_postgres):
        """Test create with invalid data type (not dict or list)"""
        data = "invalid_data_type"
        
        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operate_postgres.create_sql("users", data)
        
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 114

    @pytest.mark.asyncio
    async def test_create_with_non_dict_item_in_list(self, sql_operate_postgres):
        """Test create with list containing non-dict items"""
        data = [
            {"name": "John", "age": 25},
            "not_a_dict",  # Invalid item
            {"name": "Alice", "age": 22}
        ]
        
        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operate_postgres.create_sql("users", data)
        
        assert exc_info.value.status_code == 400
        assert "Item at index 1 must be a dictionary" in exc_info.value.message
        assert exc_info.value.message_code == 115

    @pytest.mark.asyncio
    async def test_update_with_invalid_data_format(self, sql_operate_postgres):
        """Test update with invalid data format (not list)"""
        data = {"id": 1, "name": "John"}  # Should be a list
        
        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operate_postgres.update_sql("users", data)
        
        assert exc_info.value.status_code == 400
        assert exc_info.value.message_code == 117

    @pytest.mark.asyncio
    async def test_update_with_missing_required_fields(self, sql_operate_postgres):
        """Test update with missing required fields in data"""
        data = [{"name": "John"}]  # Missing 'id' and 'data' fields
        
        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operate_postgres.update_sql("users", data)
        
        assert exc_info.value.status_code == 400
        assert "must have 'id' and 'data' fields" in exc_info.value.message
        assert exc_info.value.message_code == 116

    @pytest.mark.asyncio
    async def test_update_with_validation_error(self, sql_operate_postgres):
        """Test update with data validation error"""
        data = [{"id": 1, "data": {"name": "drop table users"}}]  # Invalid SQL injection
        
        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operate_postgres.update_sql("users", data)
        
        assert exc_info.value.status_code == 400
        assert "Invalid update data at index 0" in exc_info.value.message
        assert exc_info.value.message_code == 118


class TestSQLOperateEdgeCasesAndErrorHandling:
    """Test edge cases and comprehensive error handling"""

    @pytest.mark.asyncio
    async def test_create_mysql_without_returning_support(self, sql_operate_mysql):
        """Test create on MySQL with multiple records (no RETURNING support)"""
        data = [
            {"name": "John", "age": 25},
            {"name": "Alice", "age": 30}
        ]
        
        with patch.object(sql_operate_mysql, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            # Mock INSERT result with multiple rows
            mock_insert_result = MagicMock()
            mock_insert_result.lastrowid = 100
            mock_insert_result.rowcount = 2
            
            # Mock SELECT result for fetching inserted records
            mock_select_result = MagicMock()
            mock_rows = [
                MagicMock(_mapping={"id": 100, "name": "John", "age": 25}),
                MagicMock(_mapping={"id": 101, "name": "Alice", "age": 30}),
            ]
            mock_select_result.fetchall.return_value = mock_rows
            
            mock_session.execute.side_effect = [mock_insert_result, mock_select_result]
            
            result = await sql_operate_mysql.create_sql("users", data)
            
            assert len(result) == 2
            assert result[0]["name"] == "John"
            assert result[1]["name"] == "Alice"
            
            # Verify two queries were executed (INSERT + SELECT)
            assert mock_session.execute.call_count == 2

    @pytest.mark.asyncio
    async def test_create_mysql_no_lastrowid(self, sql_operate_mysql):
        """Test create on MySQL when lastrowid is not available"""
        data = {"name": "John", "age": 25}
        
        with patch.object(sql_operate_mysql, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            # Mock INSERT result without lastrowid
            mock_insert_result = MagicMock()
            mock_insert_result.lastrowid = None
            mock_insert_result.rowcount = 1
            
            mock_session.execute.return_value = mock_insert_result
            
            result = await sql_operate_mysql.create_sql("users", data)
            
            # Should return empty list when no lastrowid
            assert result == []
            
            # Only INSERT should be executed, no SELECT
            mock_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_count_no_result(self, sql_operate_postgres):
        """Test count when no result is returned"""
        with patch.object(sql_operate_postgres, "create_external_session") as mock_session_ctx:
            mock_session = AsyncMock()
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            
            # Mock count result with no row
            mock_result = MagicMock()
            mock_result.fetchone.return_value = None
            mock_session.execute.return_value = mock_result
            
            result = await sql_operate_postgres.count_sql("users")
            
            assert result == 0


if __name__ == "__main__":
    pass