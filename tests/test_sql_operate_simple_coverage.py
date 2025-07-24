"""
Simple test cases to improve SQL operate coverage
"""

import pytest
from unittest.mock import AsyncMock, MagicMock

from general_operate.app.sql_operate import SQLOperate
from general_operate.utils.exception import GeneralOperateException


class MockSQLClient:
    """Mock SQLClient for testing"""

    def __init__(self, engine_type="postgresql"):
        self.engine_type = engine_type

    def get_engine(self):
        return MagicMock()


@pytest.fixture
def sql_operate():
    """Create a SQLOperate instance"""
    mock_client = MockSQLClient("postgresql")
    return SQLOperate(mock_client)


class TestSQLOperateSimpleCoverage:
    """Simple test cases to improve coverage"""

    def test_validate_data_dict_with_json_serialization(self, sql_operate):
        """Test JSON serialization in _validate_data_dict - covers lines 154-155"""
        # Test with list and dict data that needs JSON serialization
        data = {"list_field": [1, 2, 3], "dict_field": {"key": "value"}}
        
        result = sql_operate._validate_data_dict(data, operation="create", allow_empty=False)
        
        # The list and dict should be JSON serialized
        import json
        assert result["list_field"] == json.dumps([1, 2, 3])
        assert result["dict_field"] == json.dumps({"key": "value"})

    def test_validate_data_dict_with_unhashable_value(self, sql_operate):
        """Test handling of unhashable values - covers line 164"""
        # Test with None values and empty result
        data = {"field1": None}
        
        result = sql_operate._validate_data_dict(data, operation="create", allow_empty=True)
        
        # Should handle None values properly
        assert isinstance(result, dict)
        
    def test_build_where_clause_empty_conditions(self, sql_operate):
        """Test build_where_clause with None values - covers lines 216, 227"""
        # Test with None values that should be skipped
        conditions = {"field1": None, "field2": None}
        
        where_clause, params = sql_operate._build_where_clause(conditions)
        
        # Should return empty string when no valid conditions
        assert where_clause == ""
        assert params == {}

    def test_build_where_clause_with_valid_conditions(self, sql_operate):
        """Test build_where_clause with valid conditions"""
        conditions = {"field1": "value1", "field2": 123}
        
        where_clause, params = sql_operate._build_where_clause(conditions)
        
        # Should build proper WHERE clause
        assert where_clause.startswith(" WHERE ")
        assert len(params) > 0

    @pytest.mark.asyncio
    async def test_create_sql_empty_data_list(self, sql_operate):
        """Test create_sql with empty data list - covers line 263"""
        mock_session = AsyncMock()
        
        # Empty data list should return empty list
        result = await sql_operate.create_sql("test_table", [], session=mock_session)
        assert result == []

    @pytest.mark.asyncio 
    async def test_read_sql_with_conditions(self, sql_operate):
        """Test read_sql with where conditions"""
        mock_session = AsyncMock()
        mock_result = MagicMock()
        
        # Create mock row objects with _mapping attribute
        mock_row = MagicMock()
        mock_row._mapping = {"id": 1, "name": "test"}
        mock_result.fetchall.return_value = [mock_row]
        mock_session.execute.return_value = mock_result
        
        # Test with where conditions (filters parameter)
        result = await sql_operate.read_sql(
            table_name="test_table",
            filters={"active": True},
            session=mock_session
        )
        
        assert mock_session.execute.called
        assert result == [{"id": 1, "name": "test"}]

    @pytest.mark.asyncio
    async def test_count_sql_basic(self, sql_operate):
        """Test count_sql basic functionality"""
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_row = MagicMock()
        mock_row.__getitem__ = MagicMock(return_value=5)
        mock_result.fetchone.return_value = mock_row
        mock_session.execute.return_value = mock_result
        
        result = await sql_operate.count_sql(
            table_name="test_table",
            session=mock_session
        )
        
        assert mock_session.execute.called
        assert result == 5

    @pytest.mark.asyncio
    async def test_health_check_success(self, sql_operate):
        """Test health check success"""
        # Mock the create_external_session method
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_row = MagicMock()
        mock_row.__getitem__ = MagicMock(return_value=1)
        mock_result.fetchone.return_value = mock_row
        mock_session.execute.return_value = mock_result
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        
        # Mock the create_external_session method
        sql_operate.create_external_session = MagicMock(return_value=mock_session)
        
        result = await sql_operate.health_check()
        assert result is True

    def test_validate_identifier_valid(self, sql_operate):
        """Test _validate_identifier with valid identifiers"""
        # Should not raise exception for valid identifiers
        sql_operate._validate_identifier("valid_table_name")
        sql_operate._validate_identifier("column_name123")
        
        # Test should pass without exceptions

    def test_validate_identifier_invalid(self, sql_operate):
        """Test _validate_identifier with invalid identifiers"""
        with pytest.raises(GeneralOperateException):
            sql_operate._validate_identifier("invalid-name;DROP TABLE")
            
        with pytest.raises(GeneralOperateException):
            sql_operate._validate_identifier("table'; DROP TABLE users; --")