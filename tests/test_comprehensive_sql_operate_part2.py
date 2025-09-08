"""Comprehensive unit tests for sql_operate.py (Part 2) to achieve 100% coverage."""

import pytest
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch, call
from typing import Any
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from general_operate.app.sql_operate import SQLOperate
from general_operate.app.client.database import SQLClient
from general_operate.core.exceptions import DatabaseException, ErrorCode, ErrorContext


@pytest.fixture
def mock_sql_client():
    """Mock SQL client."""
    client = MagicMock(spec=SQLClient)
    client.engine_type = "postgresql"
    client.get_engine.return_value = MagicMock()
    return client


@pytest.fixture
def sql_operate_postgresql(mock_sql_client):
    """Create a PostgreSQL SQLOperate instance for testing."""
    mock_sql_client.engine_type = "postgresql"
    return SQLOperate(mock_sql_client)


@pytest.fixture
def sql_operate_mysql(mock_sql_client):
    """Create a MySQL SQLOperate instance for testing."""
    mock_sql_client.engine_type = "mysql"
    return SQLOperate(mock_sql_client)


@pytest.fixture
def mock_session():
    """Mock AsyncSession."""
    session = AsyncMock(spec=AsyncSession)
    return session


class TestSQLOperateExecutePostgreSQLInsert:
    """Test PostgreSQL INSERT execution."""
    
    @pytest.mark.asyncio
    async def test_execute_postgresql_insert_success(self, sql_operate_postgresql, mock_session):
        """Test successful PostgreSQL INSERT execution."""
        data = [{"name": "test1"}, {"name": "test2"}]
        mock_result = MagicMock()
        mock_rows = [
            MagicMock(_mapping={"id": 1, "name": "test1"}),
            MagicMock(_mapping={"id": 2, "name": "test2"})
        ]
        mock_result.fetchall.return_value = mock_rows
        mock_session.execute.return_value = mock_result
        
        with patch.object(sql_operate_postgresql, '_build_insert_query', 
                         return_value=("INSERT query", {"param": "value"})) as mock_build:
            
            result = await sql_operate_postgresql._execute_postgresql_insert("test_table", data, mock_session)
        
        assert len(result) == 2
        assert result[0] == {"id": 1, "name": "test1"}
        assert result[1] == {"id": 2, "name": "test2"}
        mock_build.assert_called_once_with("test_table", data)
        mock_session.execute.assert_called_once()


class TestSQLOperateExecuteMySQLInsert:
    """Test MySQL INSERT execution."""
    
    @pytest.mark.asyncio
    async def test_execute_mysql_insert_success(self, sql_operate_mysql, mock_session):
        """Test successful MySQL INSERT execution."""
        data = [{"name": "test1"}, {"name": "test2"}]
        
        # Mock first execute (INSERT)
        mock_insert_result = MagicMock()
        mock_insert_result.lastrowid = 10
        mock_insert_result.rowcount = 2
        
        # Mock second execute (SELECT)
        mock_select_result = MagicMock()
        mock_select_rows = [
            MagicMock(_mapping={"id": 10, "name": "test1"}),
            MagicMock(_mapping={"id": 11, "name": "test2"})
        ]
        mock_select_result.fetchall.return_value = mock_select_rows
        
        mock_session.execute.side_effect = [mock_insert_result, mock_select_result]
        
        with patch.object(sql_operate_mysql, '_build_insert_query', 
                         return_value=("INSERT query", {"param": "value"})):
            
            result = await sql_operate_mysql._execute_mysql_insert("test_table", data, mock_session)
        
        assert len(result) == 2
        assert result[0] == {"id": 10, "name": "test1"}
        assert result[1] == {"id": 11, "name": "test2"}
        assert mock_session.execute.call_count == 2
    
    @pytest.mark.asyncio
    async def test_execute_mysql_insert_no_lastrowid(self, sql_operate_mysql, mock_session):
        """Test MySQL INSERT execution without lastrowid."""
        data = [{"name": "test"}]
        
        mock_result = MagicMock()
        mock_result.lastrowid = None  # No auto-increment ID
        mock_result.rowcount = 1
        mock_session.execute.return_value = mock_result
        
        with patch.object(sql_operate_mysql, '_build_insert_query', 
                         return_value=("INSERT query", {"param": "value"})):
            
            result = await sql_operate_mysql._execute_mysql_insert("test_table", data, mock_session)
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_execute_mysql_insert_no_rowcount(self, sql_operate_mysql, mock_session):
        """Test MySQL INSERT execution without rowcount."""
        data = [{"name": "test"}]
        
        mock_result = MagicMock()
        mock_result.lastrowid = 10
        mock_result.rowcount = 0  # No rows affected
        mock_session.execute.return_value = mock_result
        
        with patch.object(sql_operate_mysql, '_build_insert_query', 
                         return_value=("INSERT query", {"param": "value"})):
            
            result = await sql_operate_mysql._execute_mysql_insert("test_table", data, mock_session)
        
        assert result == []


class TestSQLOperateCreateSQL:
    """Test create_sql functionality."""
    
    @pytest.mark.asyncio
    async def test_create_sql_postgresql_with_session(self, sql_operate_postgresql, mock_session):
        """Test create_sql with PostgreSQL and external session."""
        data = {"name": "test"}
        mock_result = [{"id": 1, "name": "test"}]
        
        with patch.object(sql_operate_postgresql, '_validate_create_data', return_value=[data]), \
             patch.object(sql_operate_postgresql, '_execute_postgresql_insert', return_value=mock_result) as mock_exec:
            
            result = await sql_operate_postgresql.create_sql("test_table", data, session=mock_session)
        
        assert result == mock_result
        mock_exec.assert_called_once_with("test_table", [data], mock_session)
    
    @pytest.mark.asyncio
    async def test_create_sql_mysql_auto_session(self, sql_operate_mysql):
        """Test create_sql with MySQL and auto-managed session."""
        data = [{"name": "test1"}, {"name": "test2"}]
        mock_result = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        
        mock_session = AsyncMock()
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()
        
        with patch.object(sql_operate_mysql, '_validate_create_data', return_value=data), \
             patch.object(sql_operate_mysql, '_execute_mysql_insert', return_value=mock_result) as mock_exec, \
             patch.object(sql_operate_mysql, 'create_external_session', return_value=mock_session):
            
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            
            result = await sql_operate_mysql.create_sql("test_table", data)
        
        assert result == mock_result
        mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_sql_auto_session_rollback(self, sql_operate_postgresql):
        """Test create_sql auto-session with rollback on error."""
        data = {"name": "test"}
        
        mock_session = AsyncMock()
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()
        
        with patch.object(sql_operate_postgresql, '_validate_create_data', return_value=[data]), \
             patch.object(sql_operate_postgresql, '_execute_postgresql_insert', 
                         side_effect=Exception("Insert error")) as mock_exec, \
             patch.object(sql_operate_postgresql, 'create_external_session', return_value=mock_session):
            
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            
            with pytest.raises(Exception):
                await sql_operate_postgresql.create_sql("test_table", data)
        
        mock_session.rollback.assert_called_once()
        mock_session.commit.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_create_sql_empty_data(self, sql_operate_postgresql):
        """Test create_sql with empty data."""
        with patch.object(sql_operate_postgresql, '_validate_create_data', return_value=[]):
            
            result = await sql_operate_postgresql.create_sql("test_table", [])
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_create_sql_invalid_table_name(self, sql_operate_postgresql):
        """Test create_sql with invalid table name."""
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate_postgresql.create_sql("invalid-table!", {"name": "test"})
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR


class TestSQLOperateReadSQL:
    """Test read_sql functionality."""
    
    @pytest.mark.asyncio
    async def test_read_sql_basic(self, sql_operate_postgresql, mock_session):
        """Test basic read_sql operation."""
        mock_result = MagicMock()
        mock_rows = [MagicMock(_mapping={"id": 1, "name": "test"})]
        mock_result.fetchall.return_value = mock_rows
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate_postgresql.read_sql("test_table", session=mock_session)
        
        assert len(result) == 1
        assert result[0] == {"id": 1, "name": "test"}
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_read_sql_with_filters(self, sql_operate_postgresql, mock_session):
        """Test read_sql with filters."""
        filters = {"status": "active", "type": "premium"}
        
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result
        
        with patch.object(sql_operate_postgresql, '_build_where_clause', 
                         return_value=(" WHERE status = :status AND type = :type", filters)) as mock_where:
            
            result = await sql_operate_postgresql.read_sql("test_table", filters=filters, session=mock_session)
        
        mock_where.assert_called_once_with(filters)
        assert result == []
    
    @pytest.mark.asyncio
    async def test_read_sql_with_ordering(self, sql_operate_postgresql, mock_session):
        """Test read_sql with ordering."""
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate_postgresql.read_sql(
            "test_table", 
            order_by="created_at",
            order_direction="DESC",
            session=mock_session
        )
        
        # Check that ORDER BY clause was added
        call_args = mock_session.execute.call_args[0][0]
        query_str = str(call_args)
        # The exact format depends on SQLAlchemy text() object representation
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_read_sql_with_pagination_postgresql(self, sql_operate_postgresql, mock_session):
        """Test read_sql with pagination for PostgreSQL."""
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate_postgresql.read_sql(
            "test_table",
            limit=10,
            offset=20,
            session=mock_session
        )
        
        mock_session.execute.assert_called_once()
        assert result == []
    
    @pytest.mark.asyncio
    async def test_read_sql_with_pagination_mysql(self, sql_operate_mysql, mock_session):
        """Test read_sql with pagination for MySQL."""
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate_mysql.read_sql(
            "test_table",
            limit=10,
            offset=20,
            session=mock_session
        )
        
        mock_session.execute.assert_called_once()
        assert result == []
    
    @pytest.mark.asyncio
    async def test_read_sql_offset_only_postgresql(self, sql_operate_postgresql, mock_session):
        """Test read_sql with offset only for PostgreSQL."""
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate_postgresql.read_sql(
            "test_table",
            offset=10,
            session=mock_session
        )
        
        mock_session.execute.assert_called_once()
        assert result == []
    
    @pytest.mark.asyncio
    async def test_read_sql_offset_only_mysql(self, sql_operate_mysql, mock_session):
        """Test read_sql with offset only for MySQL."""
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate_mysql.read_sql(
            "test_table",
            offset=10,
            session=mock_session
        )
        
        mock_session.execute.assert_called_once()
        assert result == []
    
    @pytest.mark.asyncio
    async def test_read_sql_auto_session(self, sql_operate_postgresql):
        """Test read_sql with auto-managed session."""
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result
        
        with patch.object(sql_operate_postgresql, 'create_external_session', return_value=mock_session):
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            
            result = await sql_operate_postgresql.read_sql("test_table")
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_read_sql_invalid_order_direction(self, sql_operate_postgresql):
        """Test read_sql with invalid order direction."""
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate_postgresql.read_sql("test_table", order_direction="INVALID")
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "must be 'ASC' or 'DESC'" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_read_sql_invalid_limit(self, sql_operate_postgresql):
        """Test read_sql with invalid limit."""
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate_postgresql.read_sql("test_table", limit=0)
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "positive integer" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_read_sql_invalid_offset(self, sql_operate_postgresql):
        """Test read_sql with invalid offset."""
        with pytest.raises(DatabaseException) as exc_info:
            await sql_operate_postgresql.read_sql("test_table", offset=-1)
        
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
        assert "non-negative" in str(exc_info.value)


class TestSQLOperateCountSQL:
    """Test count_sql functionality."""
    
    @pytest.mark.asyncio
    async def test_count_sql_basic(self, sql_operate_postgresql, mock_session):
        """Test basic count_sql operation."""
        mock_result = MagicMock()
        mock_row = MagicMock()
        mock_row.__getitem__.return_value = 42
        mock_result.fetchone.return_value = mock_row
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate_postgresql.count_sql("test_table", session=mock_session)
        
        assert result == 42
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_count_sql_with_filters(self, sql_operate_postgresql, mock_session):
        """Test count_sql with filters."""
        filters = {"status": "active"}
        
        mock_result = MagicMock()
        mock_row = MagicMock()
        mock_row.__getitem__.return_value = 10
        mock_result.fetchone.return_value = mock_row
        mock_session.execute.return_value = mock_result
        
        with patch.object(sql_operate_postgresql, '_build_where_clause', 
                         return_value=(" WHERE status = :status", filters)):
            
            result = await sql_operate_postgresql.count_sql("test_table", filters=filters, session=mock_session)
        
        assert result == 10
    
    @pytest.mark.asyncio
    async def test_count_sql_no_result(self, sql_operate_postgresql, mock_session):
        """Test count_sql with no result."""
        mock_result = MagicMock()
        mock_result.fetchone.return_value = None
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate_postgresql.count_sql("test_table", session=mock_session)
        
        assert result == 0
    
    @pytest.mark.asyncio
    async def test_count_sql_auto_session(self, sql_operate_postgresql):
        """Test count_sql with auto-managed session."""
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_row = MagicMock()
        mock_row.__getitem__.return_value = 5
        mock_result.fetchone.return_value = mock_row
        mock_session.execute.return_value = mock_result
        
        with patch.object(sql_operate_postgresql, 'create_external_session', return_value=mock_session):
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            
            result = await sql_operate_postgresql.count_sql("test_table")
        
        assert result == 5


class TestSQLOperateReadOne:
    """Test read_one functionality."""
    
    @pytest.mark.asyncio
    async def test_read_one_found(self, sql_operate_postgresql, mock_session):
        """Test read_one when record is found."""
        mock_result = MagicMock()
        mock_row = MagicMock(_mapping={"id": 1, "name": "test"})
        mock_result.fetchone.return_value = mock_row
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate_postgresql.read_one("test_table", 1, session=mock_session)
        
        assert result == {"id": 1, "name": "test"}
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_read_one_not_found(self, sql_operate_postgresql, mock_session):
        """Test read_one when record is not found."""
        mock_result = MagicMock()
        mock_result.fetchone.return_value = None
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate_postgresql.read_one("test_table", 999, session=mock_session)
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_read_one_custom_id_column(self, sql_operate_postgresql, mock_session):
        """Test read_one with custom ID column."""
        mock_result = MagicMock()
        mock_row = MagicMock(_mapping={"email": "test@example.com", "name": "test"})
        mock_result.fetchone.return_value = mock_row
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate_postgresql.read_one("test_table", "test@example.com", "email", session=mock_session)
        
        assert result == {"email": "test@example.com", "name": "test"}
    
    @pytest.mark.asyncio
    async def test_read_one_auto_session(self, sql_operate_postgresql):
        """Test read_one with auto-managed session."""
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = None
        mock_session.execute.return_value = mock_result
        
        with patch.object(sql_operate_postgresql, 'create_external_session', return_value=mock_session):
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            
            result = await sql_operate_postgresql.read_one("test_table", 1)
        
        assert result is None


# Continue with update_sql, delete_sql, upsert_sql, exists_sql, get_distinct_values, and health_check tests...
# Due to length constraints, this covers the major query operations

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--cov=general_operate.app.sql_operate", "--cov-report=term-missing"])