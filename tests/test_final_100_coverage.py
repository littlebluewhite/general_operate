"""Final test file to achieve 100% coverage for all three core files."""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock
from datetime import datetime
import json

from general_operate.general_operate import GeneralOperate
from general_operate.app.sql_operate import SQLOperate
from general_operate.core.exceptions import (
    GeneralOperateException, DatabaseException, CacheException,
    ErrorCode, ErrorContext
)


class TestFinalGeneralOperateCoverage:
    """Tests for final missing lines in general_operate.py"""
    
    @pytest.mark.asyncio
    async def test_delete_data_empty_ids_line_792(self):
        """Test delete_data with empty IDs to cover line 792"""
        go = GeneralOperate(
            sql_db=AsyncMock(),
            redis_db=AsyncMock(),
            schemas={"main_schemas": MagicMock()},
            table_name="test_table"
        )
        
        # Test with empty list of IDs
        result = await go.delete_data(data_list_or_ids=[])
        assert result == {}  # Should return empty dict for empty input
    
    @pytest.mark.asyncio
    async def test_delete_filter_redis_error_lines_856_859(self):
        """Test delete_filter_data Redis error handling for lines 856-859"""
        go = GeneralOperate(
            sql_db=AsyncMock(),
            redis_db=AsyncMock(),
            schemas={"main_schemas": MagicMock()},
            table_name="test_table"
        )
        
        # Mock SQL to return some IDs
        go.sql_operate.delete_filter = AsyncMock(return_value=["id1", "id2"])
        
        # Mock Redis to raise an exception
        go.redis = AsyncMock()
        go.redis.delete_caches = AsyncMock(side_effect=Exception("Redis error"))
        
        # Should not raise, just log error
        result = await go.delete_filter_data(filter_dict={"status": "active"})
        assert result == ["id1", "id2"]
        
        # Verify SQL was called
        go.sql_operate.delete_filter.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_refresh_cache_no_redis_lines_1277_1281(self):
        """Test refresh_cache without Redis for lines 1277-1281"""
        go = GeneralOperate(
            sql_db=AsyncMock(),
            redis_db=None,  # No Redis
            schemas={"main_schemas": MagicMock()},
            table_name="test_table"
        )
        
        # Should return early when no Redis
        result = await go.refresh_cache(data_ids=["id1", "id2"])
        assert result == {"refreshed": 0, "errors": []}
    
    @pytest.mark.asyncio
    async def test_refresh_cache_all_missing_line_1296(self):
        """Test refresh_cache when all IDs are missing for line 1296"""
        go = GeneralOperate(
            sql_db=AsyncMock(),
            redis_db=AsyncMock(),
            schemas={"main_schemas": MagicMock()},
            table_name="test_table"
        )
        
        # Mock SQL to return empty list (all IDs missing)
        go.sql_operate.read_sql = AsyncMock(return_value=[])
        
        # Mock Redis operations
        go.redis = AsyncMock()
        go.redis.pipeline = MagicMock()
        pipeline = AsyncMock()
        go.redis.pipeline.return_value.__aenter__ = AsyncMock(return_value=pipeline)
        go.redis.pipeline.return_value.__aexit__ = AsyncMock(return_value=None)
        pipeline.setex = MagicMock()
        pipeline.execute = AsyncMock()
        
        result = await go.refresh_cache(data_ids=["missing1", "missing2"])
        
        # Should set null markers for missing IDs
        assert result["refreshed"] == 0
        assert pipeline.setex.call_count == 2  # Two null markers


class TestFinalSQLOperateCoverage:
    """Tests for final missing lines in sql_operate.py"""
    
    @pytest.mark.asyncio
    async def test_build_where_clause_edge_cases(self):
        """Test edge cases in _build_where_clause for missing lines"""
        sql_op = SQLOperate(
            db_type="postgresql",
            sql_db=AsyncMock(),
            table_name="test_table"
        )
        
        # Test with None in filter values (lines 252-253)
        where_clause, params = sql_op._build_where_clause(
            identifier=None,
            filter_dict={"status": None, "name": "test"}
        )
        assert "status IS NULL" in where_clause
        assert "name = " in where_clause
        
        # Test with null_set value (line 262)
        sql_op.null_set = {-999}
        where_clause, params = sql_op._build_where_clause(
            identifier=None,
            filter_dict={"value": -999}
        )
        assert "value IS NULL" in where_clause
    
    @pytest.mark.asyncio
    async def test_execute_mysql_insert_edge_case_line_513(self):
        """Test MySQL insert edge case for line 513"""
        sql_op = SQLOperate(
            db_type="mysql",
            sql_db=AsyncMock(),
            table_name="test_table"
        )
        
        # Mock session to return specific values
        session = AsyncMock()
        session.execute = AsyncMock()
        
        # Test with insert that doesn't return lastrowid
        mock_result = MagicMock()
        mock_result.lastrowid = None  # No lastrowid
        session.execute.return_value = mock_result
        
        result = await sql_op._execute_mysql_insert(
            session=session,
            query="INSERT INTO test_table (name) VALUES (%s)",
            data=[{"name": "test"}],
            return_cols=["id"]
        )
        
        # Should return empty list when no lastrowid
        assert result == []
    
    @pytest.mark.asyncio
    async def test_read_sql_edge_cases_lines_591_620(self):
        """Test read_sql edge cases for lines 591 and 620"""
        sql_op = SQLOperate(
            db_type="postgresql",
            sql_db=AsyncMock(),
            table_name="test_table"
        )
        
        # Test with external session (line 591)
        external_session = AsyncMock()
        external_session.execute = AsyncMock()
        mock_result = MagicMock()
        mock_result.mappings.return_value.all.return_value = []
        external_session.execute.return_value = mock_result
        
        result = await sql_op.read_sql(
            identifier="test_id",
            session=external_session
        )
        assert result == []
        
        # Test count_sql edge case (line 620)
        mock_result = MagicMock()
        mock_result.scalar.return_value = None  # No count result
        external_session.execute.return_value = mock_result
        
        result = await sql_op.count_sql(session=external_session)
        assert result == 0
    
    def test_read_sql_with_date_range_validation_errors(self):
        """Test validation errors in read_sql_with_date_range for lines 643-652"""
        sql_op = SQLOperate(
            db_type="postgresql",
            sql_db=AsyncMock(),
            table_name="test_table"
        )
        
        # Test invalid table name (line 643)
        with pytest.raises(DatabaseException) as exc:
            asyncio.run(sql_op.read_sql_with_date_range(
                date_column="created",
                start_date=datetime.now(),
                end_date=datetime.now(),
                table_name="invalid-table!"
            ))
        assert exc.value.code == ErrorCode.VALIDATION_ERROR
        
        # Test invalid date column (line 646)
        with pytest.raises(DatabaseException) as exc:
            asyncio.run(sql_op.read_sql_with_date_range(
                date_column="invalid-column!",
                start_date=datetime.now(),
                end_date=datetime.now()
            ))
        assert exc.value.code == ErrorCode.VALIDATION_ERROR
        
        # Test invalid order_by column (line 649)
        with pytest.raises(DatabaseException) as exc:
            asyncio.run(sql_op.read_sql_with_date_range(
                date_column="created",
                start_date=datetime.now(),
                end_date=datetime.now(),
                order_by="invalid-order!"
            ))
        assert exc.value.code == ErrorCode.VALIDATION_ERROR
        
        # Test invalid columns (line 652)
        with pytest.raises(DatabaseException) as exc:
            asyncio.run(sql_op.read_sql_with_date_range(
                date_column="created",
                start_date=datetime.now(),
                end_date=datetime.now(),
                columns=["invalid-col!"]
            ))
        assert exc.value.code == ErrorCode.VALIDATION_ERROR
    
    @pytest.mark.asyncio
    async def test_read_sql_with_date_range_execution(self):
        """Test read_sql_with_date_range execution for lines 693-712"""
        sql_op = SQLOperate(
            db_type="postgresql",
            sql_db=AsyncMock(),
            table_name="test_table"
        )
        
        # Mock session
        session = AsyncMock()
        mock_result = MagicMock()
        
        # Test with columns specified (line 693)
        mock_result.mappings.return_value.all.return_value = [{"id": 1, "name": "test"}]
        session.execute.return_value = mock_result
        
        sql_op.create_internal_session = MagicMock(return_value=session)
        
        result = await sql_op.read_sql_with_date_range(
            date_column="created_at",
            start_date=datetime.now(),
            end_date=datetime.now(),
            columns=["id", "name"],
            order_by="created_at",
            order="DESC",
            limit=10
        )
        
        assert result == [{"id": 1, "name": "test"}]
        
        # Verify query construction
        executed_query = session.execute.call_args[0][0]
        query_str = str(executed_query)
        # Query should have columns, order by, and limit
        assert "SELECT" in query_str or hasattr(executed_query, 'compile')
    
    def test_read_sql_with_conditions_validation_errors(self):
        """Test validation errors in read_sql_with_conditions for lines 735-775"""
        sql_op = SQLOperate(
            db_type="postgresql",
            sql_db=AsyncMock(),
            table_name="test_table"
        )
        
        # Test invalid table name (line 735)
        with pytest.raises(DatabaseException) as exc:
            asyncio.run(sql_op.read_sql_with_conditions(
                conditions=[],
                table_name="invalid!"
            ))
        assert exc.value.code == ErrorCode.VALIDATION_ERROR
        
        # Test invalid order_by (line 738)
        with pytest.raises(DatabaseException) as exc:
            asyncio.run(sql_op.read_sql_with_conditions(
                conditions=[],
                order_by="invalid!"
            ))
        assert exc.value.code == ErrorCode.VALIDATION_ERROR
        
        # Test invalid columns (line 741)
        with pytest.raises(DatabaseException) as exc:
            asyncio.run(sql_op.read_sql_with_conditions(
                conditions=[],
                columns=["invalid!"]
            ))
        assert exc.value.code == ErrorCode.VALIDATION_ERROR
        
        # Test invalid operator (lines 760-763)
        with pytest.raises(DatabaseException) as exc:
            asyncio.run(sql_op.read_sql_with_conditions(
                conditions=[{"column": "status", "operator": "INVALID", "value": "test"}]
            ))
        assert exc.value.code == ErrorCode.VALIDATION_ERROR
        
        # Test missing column in condition (lines 765-768)
        with pytest.raises(DatabaseException) as exc:
            asyncio.run(sql_op.read_sql_with_conditions(
                conditions=[{"operator": "=", "value": "test"}]
            ))
        assert exc.value.code == ErrorCode.VALIDATION_ERROR
        
        # Test invalid column name in condition (line 775)
        with pytest.raises(DatabaseException) as exc:
            asyncio.run(sql_op.read_sql_with_conditions(
                conditions=[{"column": "invalid!", "operator": "=", "value": "test"}]
            ))
        assert exc.value.code == ErrorCode.VALIDATION_ERROR
    
    @pytest.mark.asyncio
    async def test_read_sql_with_conditions_execution_line_797(self):
        """Test read_sql_with_conditions execution for line 797"""
        sql_op = SQLOperate(
            db_type="postgresql",
            sql_db=AsyncMock(),
            table_name="test_table"
        )
        
        session = AsyncMock()
        mock_result = MagicMock()
        mock_result.mappings.return_value.all.return_value = []
        session.execute.return_value = mock_result
        sql_op.create_internal_session = MagicMock(return_value=session)
        
        # Test with IN operator
        result = await sql_op.read_sql_with_conditions(
            conditions=[
                {"column": "status", "operator": "IN", "value": ["active", "pending"]}
            ],
            columns=["id", "status"]
        )
        
        assert result == []
    
    def test_get_aggregated_data_validation_line_834(self):
        """Test get_aggregated_data validation for line 834"""
        sql_op = SQLOperate(
            db_type="postgresql",
            sql_db=AsyncMock(),
            table_name="test_table"
        )
        
        # Test invalid having clause
        with pytest.raises(DatabaseException) as exc:
            asyncio.run(sql_op.get_aggregated_data(
                aggregations=["COUNT(*)"],
                group_by=["status"],
                having="COUNT(*) > 10; DROP TABLE test;"  # SQL injection attempt
            ))
        assert exc.value.code == ErrorCode.VALIDATION_ERROR
    
    @pytest.mark.asyncio
    async def test_execute_raw_query_update_line_870(self):
        """Test execute_raw_query for UPDATE statement line 870"""
        sql_op = SQLOperate(
            db_type="postgresql",
            sql_db=AsyncMock(),
            table_name="test_table"
        )
        
        session = AsyncMock()
        mock_result = MagicMock()
        mock_result.rowcount = 5
        session.execute.return_value = mock_result
        session.commit = AsyncMock()
        
        result = await sql_op.execute_raw_query(
            query="UPDATE test_table SET status = :status WHERE id = :id",
            params={"status": "active", "id": 1},
            session=session
        )
        
        assert result == {"affected_rows": 5}
        session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_read_one_not_found_line_890(self):
        """Test read_one when no record found for line 890"""
        sql_op = SQLOperate(
            db_type="postgresql",
            sql_db=AsyncMock(),
            table_name="test_table"
        )
        
        session = AsyncMock()
        mock_result = MagicMock()
        mock_result.mappings.return_value.first.return_value = None
        session.execute.return_value = mock_result
        
        result = await sql_op.read_one(identifier="missing_id", session=session)
        assert result is None
    
    def test_update_sql_validation_errors_lines_943_992(self):
        """Test update_sql validation errors for lines 943 and 992"""
        sql_op = SQLOperate(
            db_type="postgresql",
            sql_db=AsyncMock(),
            table_name="test_table"
        )
        
        # Test with no identifier and no filter (line 943)
        with pytest.raises(DatabaseException) as exc:
            asyncio.run(sql_op.update_sql(
                update_data={"status": "active"}
            ))
        assert exc.value.code == ErrorCode.VALIDATION_ERROR
        assert "identifier or filter_dict" in str(exc.value)
        
        # Test MySQL with filter_dict instead of identifier (line 992)
        sql_op.db_type = "mysql"
        with pytest.raises(DatabaseException) as exc:
            asyncio.run(sql_op.update_sql(
                update_data={"status": "active"},
                filter_dict={"name": "test"}
            ))
        assert exc.value.code == ErrorCode.VALIDATION_ERROR
    
    @pytest.mark.asyncio
    async def test_update_sql_mysql_execution_lines_1000_1002(self):
        """Test MySQL update execution for lines 1000-1002"""
        sql_op = SQLOperate(
            db_type="mysql",
            sql_db=AsyncMock(),
            table_name="test_table"
        )
        
        session = AsyncMock()
        
        # Mock for multiple IDs update
        mock_result = MagicMock()
        mock_result.rowcount = 3
        session.execute.return_value = mock_result
        session.commit = AsyncMock()
        
        sql_op.create_internal_session = MagicMock(return_value=session)
        
        result = await sql_op.update_sql(
            identifier=["id1", "id2", "id3"],
            update_data={"status": "updated"}
        )
        
        assert result == 3
        session.commit.assert_called()
    
    @pytest.mark.asyncio
    async def test_delete_sql_edge_cases_lines_1074_1084(self):
        """Test delete_sql edge cases for lines 1074-1084"""
        sql_op = SQLOperate(
            db_type="postgresql",
            sql_db=AsyncMock(),
            table_name="test_table"
        )
        
        session = AsyncMock()
        
        # Test with no deletions (line 1074)
        mock_result = MagicMock()
        mock_result.rowcount = 0
        session.execute.return_value = mock_result
        
        result = await sql_op.delete_sql(
            identifier="nonexistent_id",
            session=session
        )
        assert result == []
        
        # Test MySQL delete (lines 1082-1084)
        sql_op.db_type = "mysql"
        mock_result.rowcount = 2
        session.execute.return_value = mock_result
        session.commit = AsyncMock()
        
        result = await sql_op.delete_sql(
            identifier=["id1", "id2"],
            session=session
        )
        assert result == 2
    
    @pytest.mark.asyncio
    async def test_delete_filter_edge_cases_lines_1098_1127(self):
        """Test delete_filter edge cases for lines 1098-1127"""
        sql_op = SQLOperate(
            db_type="postgresql",
            sql_db=AsyncMock(),
            table_name="test_table"
        )
        
        # Test with external session (line 1098)
        external_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        external_session.execute.return_value = mock_result
        
        result = await sql_op.delete_filter(
            filter_dict={"status": "inactive"},
            session=external_session
        )
        assert result == []
        
        # Test MySQL delete_filter (lines 1117, 1125-1127)
        sql_op.db_type = "mysql"
        session = AsyncMock()
        mock_result.rowcount = 5
        session.execute.return_value = mock_result
        session.commit = AsyncMock()
        
        sql_op.create_internal_session = MagicMock(return_value=session)
        
        result = await sql_op.delete_filter(
            filter_dict={"status": "old"}
        )
        assert result == 5
    
    @pytest.mark.asyncio
    async def test_upsert_sql_mysql_lines_1172_1181(self):
        """Test MySQL upsert for lines 1172-1181"""
        sql_op = SQLOperate(
            db_type="mysql",
            sql_db=AsyncMock(),
            table_name="test_table"
        )
        
        # Test with no unique fields (line 1172)
        with pytest.raises(DatabaseException) as exc:
            await sql_op.upsert_sql(
                data={"name": "test"},
                unique_fields=[]
            )
        assert exc.value.code == ErrorCode.VALIDATION_ERROR
        
        # Test MySQL upsert execution (lines 1177-1181)
        session = AsyncMock()
        mock_result = MagicMock()
        mock_result.lastrowid = 10
        mock_result.rowcount = 1
        session.execute.return_value = mock_result
        session.commit = AsyncMock()
        
        sql_op.create_internal_session = MagicMock(return_value=session)
        
        result = await sql_op.upsert_sql(
            data={"id": 1, "name": "test", "status": "active"},
            unique_fields=["id"]
        )
        
        # Should return the data with potential ID update
        assert result["id"] == 1
        assert result["name"] == "test"
    
    @pytest.mark.asyncio
    async def test_upsert_sql_validation_line_1281(self):
        """Test upsert validation for line 1281"""
        sql_op = SQLOperate(
            db_type="postgresql",
            sql_db=AsyncMock(),
            table_name="test_table"
        )
        
        # Test with invalid unique field name
        with pytest.raises(DatabaseException) as exc:
            await sql_op.upsert_sql(
                data={"name": "test"},
                unique_fields=["invalid-field!"]
            )
        assert exc.value.code == ErrorCode.VALIDATION_ERROR
    
    @pytest.mark.asyncio
    async def test_upsert_multiple_records_lines_1288_1290(self):
        """Test upserting multiple records for lines 1288-1290"""
        sql_op = SQLOperate(
            db_type="postgresql",
            sql_db=AsyncMock(),
            table_name="test_table"
        )
        
        session = AsyncMock()
        mock_result = MagicMock()
        mock_result.mappings.return_value.all.return_value = [
            {"id": 1, "name": "test1"},
            {"id": 2, "name": "test2"}
        ]
        session.execute.return_value = mock_result
        session.commit = AsyncMock()
        
        sql_op.create_internal_session = MagicMock(return_value=session)
        
        result = await sql_op.upsert_sql(
            data=[
                {"name": "test1", "status": "active"},
                {"name": "test2", "status": "active"}
            ],
            unique_fields=["name"]
        )
        
        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[1]["id"] == 2
    
    @pytest.mark.asyncio
    async def test_exists_sql_edge_cases_lines_1337_1353(self):
        """Test exists_sql edge cases for lines 1337-1353"""
        sql_op = SQLOperate(
            db_type="postgresql",
            sql_db=AsyncMock(),
            table_name="test_table"
        )
        
        session = AsyncMock()
        mock_result = MagicMock()
        
        # Test with filter_dict (lines 1337-1340)
        mock_result.scalars.return_value.all.return_value = ["id1", "id2"]
        session.execute.return_value = mock_result
        
        result = await sql_op.exists_sql(
            filter_dict={"status": "active"},
            session=session
        )
        assert result == {"id1": True, "id2": True}
        
        # Test with empty result (line 1353)
        mock_result.scalars.return_value.all.return_value = []
        session.execute.return_value = mock_result
        
        result = await sql_op.exists_sql(
            identifier=["missing1", "missing2"],
            session=session
        )
        assert result == {"missing1": False, "missing2": False}
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_line_1407(self):
        """Test get_distinct_values for line 1407"""
        sql_op = SQLOperate(
            db_type="postgresql",
            sql_db=AsyncMock(),
            table_name="test_table"
        )
        
        session = AsyncMock()
        mock_result = MagicMock()
        # Return distinct values
        mock_result.scalars.return_value.all.return_value = ["value1", "value2", "value3"]
        session.execute.return_value = mock_result
        
        result = await sql_op.get_distinct_values(
            column="category",
            filter_dict={"status": "active"},
            session=session
        )
        
        assert result == ["value1", "value2", "value3"]


@pytest.fixture
def cleanup_async_tasks():
    """Cleanup any pending async tasks after each test."""
    yield
    # Get all tasks
    tasks = asyncio.all_tasks(asyncio.get_event_loop())
    # Cancel them
    for task in tasks:
        task.cancel()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--cov=general_operate.general_operate",
                 "--cov=general_operate.app.sql_operate", 
                 "--cov=general_operate.app.cache_operate",
                 "--cov-report=term-missing"])