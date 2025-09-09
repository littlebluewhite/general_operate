"""Test new database operation methods"""

import pytest
import pytest_asyncio
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch, call
from general_operate.general_operate import GeneralOperate
from general_operate.app.sql_operate import SQLOperate
from general_operate.core.exceptions import GeneralOperateException, ErrorCode


class MockOperator(GeneralOperate):
    """Test implementation of GeneralOperate"""
    
    def get_module(self):
        """Return mock module"""
        module = MagicMock()
        module.table_name = "test_table"
        module.main_schemas = MagicMock()
        module.create_schemas = MagicMock()
        module.update_schemas = MagicMock()
        return module


@pytest_asyncio.fixture
async def operator():
    """Create test operator instance"""
    db_client = MagicMock()
    db_client.engine_type = "postgresql"
    db_client.get_engine = MagicMock()
    
    redis_client = AsyncMock()
    
    op = MockOperator(
        database_client=db_client,
        redis_client=redis_client
    )
    return op


@pytest_asyncio.fixture
async def sql_operator():
    """Create SQL operator instance"""
    client = MagicMock()
    client.engine_type = "postgresql"
    client.get_engine = MagicMock()
    
    op = SQLOperate(client)
    return op


class TestSQLOperateMethods:
    """Test new SQL operate methods"""
    
    @pytest.mark.asyncio
    async def test_upsert_sql_postgresql(self, sql_operator):
        """Test upsert_sql for PostgreSQL"""
        # Mock session and execute
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()
        
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            MagicMock(_mapping={"id": 1, "name": "test", "value": "updated"})
        ]
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        with patch.object(sql_operator, 'create_external_session', return_value=mock_session):
            # Test data
            data = [{"id": 1, "name": "test", "value": "new"}]
            conflict_fields = ["id"]
            update_fields = ["value"]
            
            # Execute upsert
            result = await sql_operator.upsert_sql(
                "test_table",
                data,
                conflict_fields,
                update_fields
            )
            
            # Verify result
            assert len(result) == 1
            assert result[0]["id"] == 1
            assert result[0]["value"] == "updated"
            
            # Verify SQL was executed
            mock_session.execute.assert_called_once()
            call_args = mock_session.execute.call_args[0]
            sql_query = call_args[0].text
            
            # Check that it uses PostgreSQL syntax
            assert "ON CONFLICT" in sql_query
            assert "DO UPDATE SET" in sql_query
    
    @pytest.mark.asyncio
    async def test_exists_sql(self, sql_operator):
        """Test exists_sql method"""
        # Mock session and execute
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            (1,), (3,)  # IDs 1 and 3 exist
        ]
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        with patch.object(sql_operator, 'create_external_session', return_value=mock_session):
            # Test with multiple IDs
            id_values = [1, 2, 3, 4]
            result = await sql_operator.exists_sql(
                "test_table",
                id_values
            )
            
            # Verify result
            assert result[1] is True
            assert result[2] is False
            assert result[3] is True
            assert result[4] is False
    
    @pytest.mark.asyncio
    async def test_get_distinct_values(self, sql_operator):
        """Test get_distinct_values method"""
        # Mock session and execute
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            ("value1",),
            ("value2",),
            ("value3",),
            (None,)  # Should be filtered out
        ]
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        with patch.object(sql_operator, 'create_external_session', return_value=mock_session):
            # Test without filters
            result = await sql_operator.get_distinct_values(
                "test_table",
                "category"
            )
            
            # Verify result
            assert len(result) == 3
            assert "value1" in result
            assert "value2" in result
            assert "value3" in result
            assert None not in result
            
            # Verify SQL was executed
            mock_session.execute.assert_called_once()
            call_args = mock_session.execute.call_args[0]
            sql_query = call_args[0].text
            
            # Check SQL structure
            assert "SELECT DISTINCT category" in sql_query
            assert "ORDER BY category" in sql_query


class TestGeneralOperateMethods:
    """Test new GeneralOperate methods"""
    
    @pytest.mark.asyncio
    async def test_upsert_data(self, operator):
        """Test upsert_data method"""
        # Mock the upsert_sql method
        mock_records = [
            {"id": 1, "name": "test1", "value": "new1"},
            {"id": 2, "name": "test2", "value": "new2"}
        ]
        
        with patch.object(operator, 'upsert_sql', return_value=mock_records) as mock_upsert:
            with patch.object(operator, 'delete_caches', return_value=None):
                # Mock schema validation
                operator.create_schemas.return_value = MagicMock(
                    model_dump=lambda: {"id": 1, "name": "test1", "value": "new1"}
                )
                operator.main_schemas.side_effect = lambda **kwargs: MagicMock(**kwargs)
                
                # Test data
                data = [
                    {"id": 1, "name": "test1", "value": "new1"},
                    {"id": 2, "name": "test2", "value": "new2"}
                ]
                
                # Execute upsert
                result = await operator.upsert_data(
                    data,
                    conflict_fields=["id"],
                    update_fields=["value"]
                )
                
                # Verify result
                assert len(result) == 2
                
                # Verify upsert_sql was called
                mock_upsert.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_exists_check(self, operator):
        """Test exists_check method"""
        # Mock cache check
        operator.redis = AsyncMock()
        operator.redis.exists.return_value = False
        
        # Mock get_caches to return empty (cache miss)
        with patch.object(operator, 'get_caches', return_value=None):
            # Mock exists_sql to return True
            with patch.object(operator, 'exists_sql', return_value={123: True}):
                result = await operator.exists_check(123)
                assert result is True
        
        # Test with cache hit
        with patch.object(operator, 'get_caches', return_value=[{"id": 456}]):
            result = await operator.exists_check(456)
            assert result is True
        
        # Test with null marker
        operator.redis.exists.return_value = True
        result = await operator.exists_check(789)
        assert result is False
    
    @pytest.mark.asyncio
    async def test_batch_exists(self, operator):
        """Test batch_exists method"""
        # Mock cache operations
        operator.redis = AsyncMock()
        
        # Mock pipeline operations
        mock_pipeline = AsyncMock()
        mock_pipeline.exists = MagicMock()  # Pipeline methods don't return values immediately
        mock_pipeline.execute = AsyncMock(return_value=[False, True, False])  # null marker results
        # Set up pipeline as async context manager
        mock_pipeline.__aenter__ = AsyncMock(return_value=mock_pipeline)
        mock_pipeline.__aexit__ = AsyncMock(return_value=None)
        operator.redis.pipeline = MagicMock(return_value=mock_pipeline)
        
        # Mock get_caches
        async def mock_get_caches(table, ids):
            if "1" in ids:
                return {"1": {"id": 1}}
            return {}
        
        with patch.object(operator, 'get_caches', side_effect=mock_get_caches):
            # Mock exists_sql for unchecked IDs
            with patch.object(operator, 'exists_sql', return_value={3: True}):
                result = await operator.batch_exists({1, 2, 3})
                
                # Verify results
                assert result[1] is True  # Found in cache
                assert result[2] is False  # Has null marker
                assert result[3] is True  # Found in database
    
    @pytest.mark.asyncio
    async def test_refresh_cache(self, operator):
        """Test refresh_cache method"""
        # Mock Redis pipeline for null marker operations
        if operator.redis:
            mock_pipeline = AsyncMock()
            mock_pipeline.delete = MagicMock()
            mock_pipeline.setex = MagicMock()
            mock_pipeline.execute = AsyncMock(return_value=[])
            operator.redis.pipeline = MagicMock(return_value=mock_pipeline)
        
        # Mock cache operations
        with patch.object(operator, 'delete_caches', return_value=None):
            # Mock SQL read
            mock_sql_results = [
                {"id": 1, "name": "test1"},
                {"id": 2, "name": "test2"}
            ]
            with patch.object(operator, 'read_sql', return_value=mock_sql_results):
                with patch.object(operator, 'store_caches', return_value=None):
                    # Refresh cache for IDs 1, 2, 3 (3 doesn't exist)
                    result = await operator.refresh_cache({1, 2, 3})
                    
                    # Verify results
                    assert result["refreshed"] == 2
                    assert result["not_found"] == 1
                    assert result["errors"] == 0
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_with_cache(self, operator):
        """Test get_distinct_values with caching"""
        operator.redis = AsyncMock()
        
        # Test cache hit
        cached_values = '["value1", "value2", "value3"]'
        operator.redis.get.return_value = cached_values
        
        result = await operator.get_distinct_values("category", cache_ttl=300)
        
        # Should return cached values
        assert result == ["value1", "value2", "value3"]
        
        # Test cache miss
        operator.redis.get.return_value = None
        operator.redis.setex = AsyncMock()
        
        # Mock the SQLOperate.get_distinct_values call
        with patch('general_operate.general_operate.SQLOperate.get_distinct_values', 
                   return_value=["new1", "new2"]):
            result = await operator.get_distinct_values("category", cache_ttl=300)
            
            # Should return database values
            assert result == ["new1", "new2"]
            
            # Should cache the result
            operator.redis.setex.assert_called_once()


class TestSQLOperateMySQL:
    """Test SQL operate methods with MySQL-specific behavior"""
    
    @pytest_asyncio.fixture
    async def mysql_operator(self):
        """Create MySQL SQL operator instance"""
        client = MagicMock()
        client.engine_type = "mysql"  # MySQL engine
        client.get_engine = MagicMock()
        
        op = SQLOperate(client)
        return op
    
    @pytest.mark.asyncio
    async def test_upsert_sql_mysql(self, mysql_operator):
        """Test upsert_sql for MySQL with ON DUPLICATE KEY UPDATE"""
        # Mock session and execute
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()
        
        # First execute for INSERT
        mock_result = MagicMock()
        # Second execute for SELECT to fetch results
        mock_fetch_result = MagicMock()
        mock_fetch_result.fetchall.return_value = [
            MagicMock(_mapping={"id": 1, "name": "test", "value": "updated"})
        ]
        mock_session.execute = AsyncMock(side_effect=[mock_result, mock_fetch_result])
        
        with patch.object(mysql_operator, 'create_external_session', return_value=mock_session):
            # Test data
            data = [{"id": 1, "name": "test", "value": "new"}]
            conflict_fields = ["id"]
            update_fields = ["value"]
            
            # Execute upsert
            result = await mysql_operator.upsert_sql(
                "test_table",
                data,
                conflict_fields,
                update_fields
            )
            
            # Verify result
            assert len(result) == 1
            assert result[0]["id"] == 1
            
            # Verify MySQL syntax was used
            calls = mock_session.execute.call_args_list
            insert_sql = calls[0][0][0].text
            assert "ON DUPLICATE KEY UPDATE" in insert_sql
            assert "VALUES(value)" in insert_sql
    
    @pytest.mark.asyncio
    async def test_exists_sql_mysql_batch(self, mysql_operator):
        """Test exists_sql with multiple IDs in MySQL"""
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [(1,), (3,), (5,)]
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        with patch.object(mysql_operator, 'create_external_session', return_value=mock_session):
            # Test with multiple IDs
            id_values = [1, 2, 3, 4, 5]
            result = await mysql_operator.exists_sql(
                "test_table",
                id_values
            )
            
            # Verify MySQL IN clause was used
            call_args = mock_session.execute.call_args[0]
            sql_query = call_args[0].text
            assert " IN (" in sql_query
            
            # Verify results
            assert result[1] is True
            assert result[2] is False
            assert result[3] is True
            assert result[4] is False
            assert result[5] is True


class TestErrorHandling:
    """Test error handling and edge cases"""
    
    @pytest.mark.asyncio
    async def test_upsert_invalid_data(self, sql_operator):
        """Test upsert with invalid data types"""
        with pytest.raises(GeneralOperateException):
            await sql_operator.upsert_sql(
                "test_table",
                "invalid_string",  # Should be list or dict
                ["id"],
                ["value"]
            )
    
    @pytest.mark.asyncio
    async def test_sql_injection_prevention(self, sql_operator):
        """Test SQL injection prevention in identifiers"""
        # Test with SQL injection attempt in table name
        with pytest.raises(GeneralOperateException) as exc_info:
            await sql_operator.upsert_sql(
                "test_table; DROP TABLE users--",
                [{"id": 1}],
                ["id"],
                ["value"]
            )
        assert exc_info.value.code == ErrorCode.VALIDATION_ERROR
    
    @pytest.mark.asyncio
    async def test_exists_sql_empty_list(self, sql_operator):
        """Test exists_sql with empty ID list"""
        result = await sql_operator.exists_sql("test_table", [])
        assert result == {}
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_with_nulls(self, sql_operator):
        """Test get_distinct_values filters out NULL values"""
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            ("value1",),
            (None,),
            ("value2",),
            (None,),
            ("value3",)
        ]
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        with patch.object(sql_operator, 'create_external_session', return_value=mock_session):
            result = await sql_operator.get_distinct_values(
                "test_table",
                "category"
            )
            
            # NULL values should be filtered out
            assert len(result) == 3
            assert None not in result
            assert result == ["value1", "value2", "value3"]
    
    @pytest.mark.asyncio
    async def test_transaction_rollback(self, sql_operator):
        """Test transaction rollback on error"""
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()
        
        # Simulate database error
        mock_session.execute = AsyncMock(side_effect=Exception("Database error"))
        
        with patch.object(sql_operator, 'create_external_session', return_value=mock_session):
            with pytest.raises(Exception):
                await sql_operator.upsert_sql(
                    "test_table",
                    [{"id": 1, "value": "test"}],
                    ["id"],
                    ["value"]
                )
            
            # Verify rollback was called
            mock_session.rollback.assert_called_once()
            # Verify commit was not called
            mock_session.commit.assert_not_called()


class TestConcurrency:
    """Test concurrent access scenarios"""
    
    @pytest.mark.asyncio
    async def test_concurrent_upserts(self, sql_operator):
        """Test multiple concurrent upsert operations"""
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        mock_session.commit = AsyncMock()
        
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            MagicMock(_mapping={"id": i, "value": f"value_{i}"})
            for i in range(1, 6)
        ]
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        with patch.object(sql_operator, 'create_external_session', return_value=mock_session):
            # Create multiple concurrent upsert tasks
            tasks = []
            for i in range(5):
                task = sql_operator.upsert_sql(
                    "test_table",
                    [{"id": i+1, "value": f"value_{i+1}"}],
                    ["id"],
                    ["value"]
                )
                tasks.append(task)
            
            # Execute concurrently
            results = await asyncio.gather(*tasks)
            
            # Verify all operations completed
            assert len(results) == 5
            for i, result in enumerate(results):
                assert len(result) == 5  # Each gets all results from mock
    
    @pytest.mark.asyncio
    async def test_concurrent_exists_checks(self, operator):
        """Test concurrent exists checks with caching"""
        operator.redis = AsyncMock()
        operator.redis.exists.return_value = False
        
        # Mock different cache states for concurrent checks
        cache_results = [None, [{"id": 2}], None, [{"id": 4}], None]
        with patch.object(operator, 'get_caches', side_effect=cache_results):
            with patch.object(operator, 'exists_sql', return_value={1: True, 3: True, 5: False}):
                # Create concurrent exists checks
                tasks = [
                    operator.exists_check(1),
                    operator.exists_check(2),
                    operator.exists_check(3),
                    operator.exists_check(4),
                    operator.exists_check(5)
                ]
                
                # Execute concurrently
                results = await asyncio.gather(*tasks)
                
                # Verify results
                assert results[0] is True  # From database
                assert results[1] is True  # From cache
                assert results[2] is True  # From database
                assert results[3] is True  # From cache
                assert results[4] is False  # From database


class TestPerformance:
    """Test performance with large datasets"""
    
    @pytest.mark.asyncio
    async def test_batch_upsert_performance(self, sql_operator):
        """Test upsert with large batch of records"""
        # Create large dataset
        large_data = [
            {"id": i, "name": f"name_{i}", "value": f"value_{i}"}
            for i in range(1000)
        ]
        
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        mock_session.commit = AsyncMock()
        
        # Mock results
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            MagicMock(_mapping=item) for item in large_data
        ]
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        with patch.object(sql_operator, 'create_external_session', return_value=mock_session):
            # Execute large batch upsert
            result = await sql_operator.upsert_sql(
                "test_table",
                large_data,
                ["id"],
                ["value"]
            )
            
            # Verify all records processed
            assert len(result) == 1000
            
            # Verify SQL was executed only once (batch operation)
            assert mock_session.execute.call_count == 1
    
    @pytest.mark.asyncio
    async def test_batch_exists_performance(self, operator):
        """Test batch exists check with large ID set"""
        # Create large ID set
        large_id_set = set(range(1, 1001))
        
        operator.redis = AsyncMock()
        operator.redis.exists.return_value = False
        
        # Mock cache miss for all
        with patch.object(operator, 'get_caches', return_value=None):
            # Mock database check
            db_results = {i: i % 2 == 0 for i in large_id_set}
            with patch.object(operator, 'exists_sql', return_value=db_results):
                result = await operator.batch_exists(large_id_set)
                
                # Verify all IDs checked
                assert len(result) == 1000
                
                # Verify correct results
                for i in range(1, 1001):
                    assert result[i] == (i % 2 == 0)


class TestCacheConsistency:
    """Test cache consistency and invalidation"""
    
    @pytest.mark.asyncio
    async def test_upsert_invalidates_cache(self, operator):
        """Test that upsert properly invalidates cache"""
        with patch.object(operator, 'upsert_sql', return_value=[{"id": 1, "value": "new"}]):
            with patch.object(operator, 'delete_caches', return_value=None) as mock_delete:
                # Mock schema validation
                operator.create_schemas.return_value = MagicMock(
                    model_dump=lambda: {"id": 1, "value": "new"}
                )
                operator.main_schemas.side_effect = lambda **kwargs: MagicMock(**kwargs)
                
                await operator.upsert_data(
                    [{"id": 1, "value": "new"}],
                    conflict_fields=["id"],
                    update_fields=["value"]
                )
                
                # Verify cache was invalidated (called twice - once for each ID)
                assert mock_delete.call_count == 2
    
    @pytest.mark.asyncio
    async def test_refresh_cache_consistency(self, operator):
        """Test refresh_cache maintains consistency"""
        # Mock Redis pipeline
        if operator.redis:
            mock_pipeline = AsyncMock()
            mock_pipeline.delete = MagicMock()
            mock_pipeline.setex = MagicMock()
            mock_pipeline.execute = AsyncMock(return_value=[])
            operator.redis.pipeline = MagicMock(return_value=mock_pipeline)
        
        with patch.object(operator, 'delete_caches', return_value=None):
            # Mock SQL read - some IDs exist, some don't
            mock_sql_results = [
                {"id": 1, "name": "test1"},
                {"id": 3, "name": "test3"}
            ]
            with patch.object(operator, 'read_sql', return_value=mock_sql_results):
                with patch.object(operator, 'store_caches', return_value=None) as mock_store:
                    # Refresh cache for IDs 1, 2, 3
                    result = await operator.refresh_cache({1, 2, 3})
                    
                    # Verify correct items were cached
                    mock_store.assert_called_once()
                    stored_data = mock_store.call_args[0][1]
                    assert len(stored_data) == 2
                    
                    # Verify results
                    assert result["refreshed"] == 2
                    assert result["not_found"] == 1
    
    @pytest.mark.asyncio
    async def test_get_distinct_values_cache_ttl(self, operator):
        """Test get_distinct_values respects cache TTL"""
        operator.redis = AsyncMock()
        operator.redis.get.return_value = None
        operator.redis.setex = AsyncMock()
        
        with patch('general_operate.general_operate.SQLOperate.get_distinct_values', 
                   return_value=["value1", "value2"]):
            # Request with specific TTL
            await operator.get_distinct_values("category", cache_ttl=600)
            
            # Verify cache was set with correct TTL
            operator.redis.setex.assert_called_once()
            call_args = operator.redis.setex.call_args[0]
            assert call_args[1] == 600  # TTL value


class TestIntegration:
    """Integration tests for method interactions"""
    
    @pytest.mark.asyncio
    async def test_upsert_and_exists_integration(self, operator):
        """Test upsert followed by exists check"""
        # Setup
        operator.redis = AsyncMock()
        operator.create_schemas.return_value = MagicMock(
            model_dump=lambda: {"id": 1, "name": "test", "value": "new"}
        )
        operator.main_schemas.side_effect = lambda **kwargs: MagicMock(**kwargs)
        
        with patch.object(operator, 'upsert_sql', return_value=[{"id": 1, "name": "test"}]):
            with patch.object(operator, 'delete_caches', return_value=None):
                # Perform upsert
                await operator.upsert_data(
                    [{"id": 1, "name": "test", "value": "new"}],
                    conflict_fields=["id"],
                    update_fields=["value"]
                )
        
        # Now check existence
        operator.redis.exists.return_value = False
        with patch.object(operator, 'get_caches', return_value=[{"id": 1, "name": "test"}]):
            result = await operator.exists_check(1)
            assert result is True
    
    @pytest.mark.asyncio
    async def test_batch_operations_integration(self, operator):
        """Test batch exists followed by refresh cache"""
        operator.redis = AsyncMock()
        
        # Mock pipeline for batch exists
        mock_pipeline = AsyncMock()
        mock_pipeline.exists = MagicMock()
        mock_pipeline.delete = MagicMock()
        mock_pipeline.setex = MagicMock()
        mock_pipeline.execute = AsyncMock(return_value=[False, False, False])
        operator.redis.pipeline = MagicMock(return_value=mock_pipeline)
        
        # First, batch exists check
        with patch.object(operator, 'get_caches', return_value={}):
            with patch.object(operator, 'exists_sql', return_value={1: True, 2: False, 3: True}):
                exists_results = await operator.batch_exists({1, 2, 3})
        
        # Then refresh cache for non-existent items
        non_existent = {k for k, v in exists_results.items() if not v}
        
        with patch.object(operator, 'delete_caches', return_value=None):
            with patch.object(operator, 'read_sql', return_value=[]):
                with patch.object(operator, 'store_caches', return_value=None):
                    result = await operator.refresh_cache(non_existent)
                    
                    # Verify null markers set for non-existent items
                    assert result["not_found"] == len(non_existent)