from unittest.mock import AsyncMock, MagicMock

import pytest
from redis import RedisError

from general_operate.app.cache_operate import CacheOperate


@pytest.fixture
def mock_redis():
    """Create a mock Redis client"""
    from unittest.mock import MagicMock
    import redis.asyncio
    
    mock_redis = AsyncMock(spec=redis.asyncio.Redis)
    
    # Create mock pipeline context manager
    class MockPipeline:
        def __init__(self):
            self.commands = []
            self.execute = AsyncMock()
            
        def setex(self, key, ttl, value):
            self.commands.append(('setex', key, ttl, value))
            
        def set(self, key, value):
            self.commands.append(('set', key, value))
            
        def get(self, key):
            self.commands.append(('get', key))
            
        async def _default_execute(self):
            # Return mock values based on the commands
            results = []
            for cmd in self.commands:
                if cmd[0] == 'get':
                    # Mock get responses
                    key = cmd[1]
                    if 'field1' in key:
                        results.append('{"string_field": "test_value", "number": 42}')
                    elif 'field2' in key:
                        results.append('{"dict_field": {"nested": "value"}, "list_field": [1, 2, 3]}')
                    elif 'user1' in key:
                        results.append('{"name": "John"}')
                    elif 'age' in key:
                        results.append('{"value": 25}')
                    else:
                        results.append(None)
                else:
                    results.append(True)
            return results
            
        async def __aenter__(self):
            return self
            
        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return None
    
    # Set up pipeline method
    pipeline_instance = MockPipeline()
    pipeline_instance.execute.side_effect = pipeline_instance._default_execute
    mock_redis.pipeline = MagicMock(return_value=pipeline_instance)
    
    # Set up other methods with proper return values
    mock_redis.exists = AsyncMock(return_value=1)
    mock_redis.delete = AsyncMock(return_value=1)
    mock_redis.hdel = AsyncMock(return_value=1)
    mock_redis.hexists = AsyncMock(return_value=1)
    mock_redis.hset = AsyncMock(return_value=1)
    mock_redis.hmget = AsyncMock(return_value=[])
    mock_redis.hgetall = AsyncMock(return_value={})
    mock_redis.hlen = AsyncMock(return_value=0)
    mock_redis.setex = AsyncMock()
    mock_redis.get = AsyncMock()
    mock_redis.ttl = AsyncMock(return_value=3600)
    mock_redis.expire = AsyncMock(return_value=True)
    mock_redis.ping = AsyncMock(return_value=True)
    
    return mock_redis


@pytest.fixture
def redis_operate(mock_redis):
    """Create a RedisOperate instance with mock Redis"""
    return CacheOperate(mock_redis)


class TestRedisOperate:
    """Test cases for RedisOperate class"""

    @pytest.mark.asyncio
    async def test_get_with_empty_keys(self, redis_operate):
        """Test get method with empty keys"""
        result = await redis_operate.get_caches("test_table", set())
        assert result == []

    @pytest.mark.asyncio
    async def test_get_with_valid_keys(self, redis_operate, mock_redis):
        """Test get method with valid keys"""
        # Pipeline will be used for get operations
        result = await redis_operate.get_caches("test_table", {"field1", "field2", "field3"})

        # Check that pipeline was called
        mock_redis.pipeline.assert_called_once_with(transaction=False)

        # Check results - should get field1 and field2 (field3 returns None)
        assert len(result) == 2
        # Verify the returned data structure
        assert any("string_field" in item for item in result)
        assert any("dict_field" in item for item in result)

    @pytest.mark.asyncio
    async def test_set_with_empty_data(self, redis_operate):
        """Test set method with empty data"""
        result = await redis_operate.store_caches("test_table", {})
        assert result is False

    @pytest.mark.asyncio
    async def test_set_with_various_data_types(self, redis_operate, mock_redis):
        """Test set method with different data types"""

        test_data = {
            "field1": {"string_field": "test_value", "number": 42},
            "field2": {"dict_field": {"nested": "value"}, "list_field": [1, 2, 3]}
        }

        result = await redis_operate.store_caches("test_table", test_data, ttl_seconds=3600)

        # Verify pipeline was used
        mock_redis.pipeline.assert_called_once_with(transaction=False)
        
        # Verify setex was called twice (once for each field) 
        pipeline_mock = mock_redis.pipeline.return_value
        assert len([cmd for cmd in pipeline_mock.commands if cmd[0] == 'setex']) == 2
        
        # Verify execute was called
        pipeline_mock.execute.assert_called_once()

        assert result is True

    @pytest.mark.asyncio
    async def test_delete_with_empty_keys(self, redis_operate):
        """Test delete method with empty keys"""
        result = await redis_operate.delete_caches("test_table", set())
        assert result == 0

    @pytest.mark.asyncio
    async def test_delete_with_valid_keys(self, redis_operate, mock_redis):
        """Test delete method with valid keys"""
        mock_redis.delete.return_value = 2

        result = await redis_operate.delete_caches("test_table", {"field1", "field2"})

        # Verify delete was called correctly
        mock_redis.delete.assert_called_once()
        assert result == 2

    @pytest.mark.asyncio
    async def test_exists_field_exists(self, redis_operate, mock_redis):
        """Test exists method when field exists"""
        mock_redis.exists.return_value = 1

        result = await redis_operate.cache_exists("test_table", "field1")

        mock_redis.exists.assert_called_once_with("test_table:field1")
        assert result is True

    @pytest.mark.asyncio
    async def test_exists_field_not_exists(self, redis_operate, mock_redis):
        """Test exists method when field doesn't exist"""
        mock_redis.exists.return_value = 0

        result = await redis_operate.cache_exists("test_table", "field1")

        mock_redis.exists.assert_called_once_with("test_table:field1")
        assert result is False

    @pytest.mark.asyncio
    async def test_health_check_success(self, redis_operate, mock_redis):
        """Test health_check when Redis is healthy"""
        mock_redis.ping.return_value = True

        result = await redis_operate.health_check()

        assert result is True

    @pytest.mark.asyncio
    async def test_health_check_failure(self, redis_operate, mock_redis):
        """Test health_check when Redis is not healthy"""
        mock_redis.ping.side_effect = RedisError("Connection failed")

        result = await redis_operate.health_check()

        assert result is False

    @pytest.mark.asyncio
    async def test_exception_handler_json_decode_error(self, redis_operate, mock_redis):
        """Test exception handler with JSON decode error"""
        # Create a custom pipeline that returns invalid JSON
        class InvalidJSONPipeline:
            def __init__(self):
                self.commands = []
                
            def get(self, key):
                self.commands.append(('get', key))
                
            async def execute(self):
                return ["invalid json"]  # This will cause JSONDecodeError
                
            async def __aenter__(self):
                return self
                
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                return None
        
        mock_redis.pipeline = MagicMock(return_value=InvalidJSONPipeline())

        # The get method handles invalid JSON gracefully by logging warning and continuing
        result = await redis_operate.get_caches("test_table", {"field1"})
        # Should return empty list as invalid JSON is skipped
        assert result == []


@pytest.mark.asyncio
async def test_integration_workflow(redis_operate, mock_redis):
    """Test a complete workflow of operations"""
    # Setup mock responses for non-pipeline operations
    mock_redis.exists.return_value = 1
    mock_redis.delete.return_value = 1

    # Create a fresh pipeline for each call to avoid state issues
    def create_fresh_pipeline(transaction=False):
        class FreshMockPipeline:
            def __init__(self):
                self.commands = []
                
            def setex(self, key, ttl, value):
                self.commands.append(('setex', key, ttl, value))
                
            def set(self, key, value):
                self.commands.append(('set', key, value))
                
            def get(self, key):
                self.commands.append(('get', key))
                
            async def execute(self):
                results = []
                for cmd in self.commands:
                    if cmd[0] == 'get':
                        key = cmd[1]
                        if 'user1' in key:
                            results.append('{"name": "John"}')
                        elif 'age' in key:
                            results.append('{"value": 25}')
                        else:
                            results.append(None)
                    else:
                        results.append(True)
                return results
                
            async def __aenter__(self):
                return self
                
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                return None
        
        return FreshMockPipeline()
    
    mock_redis.pipeline.side_effect = create_fresh_pipeline

    # 1. Set some data
    data = {"user1": {"name": "John"}, "age": {"value": 25}}
    assert await redis_operate.store_caches("users", data) is True

    # 2. Check if field exists  
    assert await redis_operate.cache_exists("users", "user1") is True

    # 3. Get specific fields
    result = await redis_operate.get_caches("users", {"user1", "age"})
    assert len(result) == 2

    # 4. Delete a field
    deleted = await redis_operate.delete_caches("users", {"age"})
    assert deleted == 1


@pytest.mark.asyncio
async def test_store_cache_single_item(redis_operate, mock_redis):
    """Test store_cache method with single item"""
    test_data = {"test": "value"}
    
    await redis_operate.store_cache("prefix", "identifier", test_data, 3600)
    
    # Verify setex was called
    mock_redis.setex.assert_called_once()
    args = mock_redis.setex.call_args[0]
    assert args[0] == "prefix:identifier"  # key
    assert args[1] == 3600  # ttl
    assert '"test": "value"' in args[2]  # serialized data


@pytest.mark.asyncio
async def test_health_check_redis_error(redis_operate, mock_redis):
    """Test health check with Redis error"""
    from redis import ConnectionError as RedisConnectionError
    mock_redis.ping.side_effect = RedisConnectionError("Connection failed")
    
    result = await redis_operate.health_check()
    assert result is False


@pytest.mark.asyncio
async def test_health_check_unexpected_error(redis_operate, mock_redis):
    """Test health check with unexpected error"""
    mock_redis.ping.side_effect = ValueError("Unexpected error")
    
    result = await redis_operate.health_check()
    assert result is False
