from unittest.mock import AsyncMock

import pytest
from redis import RedisError

from general_operate import GeneralOperateException
from general_operate.app.cache_operate import CacheOperate


@pytest.fixture
def mock_redis():
    """Create a mock Redis client"""
    return AsyncMock()


@pytest.fixture
def redis_operate(mock_redis):
    """Create a RedisOperate instance with mock Redis"""
    return CacheOperate(mock_redis)


class TestRedisOperate:
    """Test cases for RedisOperate class"""

    @pytest.mark.asyncio
    async def test_get_with_empty_keys(self, redis_operate):
        """Test get method with empty keys"""
        result = await redis_operate.get("test_table", set())
        assert result == []

    @pytest.mark.asyncio
    async def test_get_with_valid_keys(self, redis_operate, mock_redis):
        """Test get method with valid keys"""
        mock_redis.hmget.return_value = ["value1", '{"key": "value2"}', None]

        result = await redis_operate.get("test_table", {"field1", "field2", "field3"})

        # Check that hmget was called correctly
        mock_redis.hmget.assert_called_once()

        # Check results - note that order might vary due to set
        assert len(result) == 2
        values = [list(item.values())[0] for item in result]
        assert "value1" in values
        assert {"key": "value2"} in values

    @pytest.mark.asyncio
    async def test_get_with_redis_error(self, redis_operate, mock_redis):
        """Test get method handling Redis error"""
        mock_redis.hmget.side_effect = RedisError("Error 123: Connection failed")

        with pytest.raises(GeneralOperateException) as exc_info:
            await redis_operate.get("test_table", {"field1"})

        assert exc_info.value.status_code == 487
        assert exc_info.value.message_code == 123

    @pytest.mark.asyncio
    async def test_set_with_empty_data(self, redis_operate):
        """Test set method with empty data"""
        result = await redis_operate.set("test_table", {})
        assert result is False

    @pytest.mark.asyncio
    async def test_set_with_various_data_types(self, redis_operate, mock_redis):
        """Test set method with different data types"""
        mock_redis.hset.return_value = 3

        test_data = {
            "string_field": "test_value",
            "dict_field": {"nested": "value"},
            "list_field": [1, 2, 3],
            "number_field": 42,
        }

        result = await redis_operate.set("test_table", test_data)

        # Verify hset was called
        mock_redis.hset.assert_called_once()
        call_args = mock_redis.hset.call_args

        # Check the mapping argument
        mapping = call_args.kwargs["mapping"]
        assert mapping["string_field"] == "test_value"
        assert mapping["dict_field"] == '{"nested": "value"}'
        assert mapping["list_field"] == "[1, 2, 3]"
        assert mapping["number_field"] == "42"

        assert result is True

    @pytest.mark.asyncio
    async def test_delete_with_empty_keys(self, redis_operate):
        """Test delete method with empty keys"""
        result = await redis_operate.delete_cache("test_table", set())
        assert result == 0

    @pytest.mark.asyncio
    async def test_delete_with_valid_keys(self, redis_operate, mock_redis):
        """Test delete method with valid keys"""
        mock_redis.hdel.return_value = 2

        result = await redis_operate.delete_cache("test_table", {"field1", "field2"})

        # Verify hdel was called correctly
        mock_redis.hdel.assert_called_once()
        assert result == 2

    @pytest.mark.asyncio
    async def test_exists_field_exists(self, redis_operate, mock_redis):
        """Test exists method when field exists"""
        mock_redis.hexists.return_value = 1

        result = await redis_operate.exists("test_table", "field1")

        mock_redis.hexists.assert_called_once_with("test_table", "field1")
        assert result is True

    @pytest.mark.asyncio
    async def test_exists_field_not_exists(self, redis_operate, mock_redis):
        """Test exists method when field doesn't exist"""
        mock_redis.hexists.return_value = 0

        result = await redis_operate.exists("test_table", "field1")

        assert result is False

    @pytest.mark.asyncio
    async def test_get_all_empty_hash(self, redis_operate, mock_redis):
        """Test get_all method with empty hash"""
        mock_redis.hgetall.return_value = {}

        result = await redis_operate.get_all("test_table")

        assert result == {}

    @pytest.mark.asyncio
    async def test_get_all_with_mixed_values(self, redis_operate, mock_redis):
        """Test get_all method with mixed value types"""
        mock_redis.hgetall.return_value = {
            "field1": "simple_string",
            "field2": '{"nested": "json"}',
            "field3": "[1, 2, 3]",
            "field4": None,
        }

        result = await redis_operate.get_all("test_table")

        assert result["field1"] == "simple_string"
        assert result["field2"] == {"nested": "json"}
        assert result["field3"] == [1, 2, 3]
        assert "field4" not in result

    @pytest.mark.asyncio
    async def test_count(self, redis_operate, mock_redis):
        """Test count method"""
        mock_redis.hlen.return_value = 5

        result = await redis_operate.count("test_table")

        mock_redis.hlen.assert_called_once_with("test_table")
        assert result == 5

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
        # Mock a method that would trigger JSON decode error
        mock_redis.hmget.return_value = ["invalid json"]

        # Patch json.loads to raise JSONDecodeError
        import json

        original_loads = json.loads

        def mock_loads(s):
            if s == "invalid json":
                raise json.JSONDecodeError("Test error", "", 0)
            return original_loads(s)

        json.loads = mock_loads

        try:
            # The get method handles invalid JSON gracefully by keeping it as string
            # So this test doesn't actually trigger JSONDecodeError anymore
            result = await redis_operate.get("test_table", {"field1"})
            # Verify it kept the invalid JSON as string
            assert len(result) == 1
            assert list(result[0].values())[0] == "invalid json"
        finally:
            json.loads = original_loads

    @pytest.mark.asyncio
    async def test_exception_handler_generic_exception(self, redis_operate, mock_redis):
        """Test exception handler with generic exception"""
        mock_redis.hmget.side_effect = ValueError("Unexpected error")

        with pytest.raises(GeneralOperateException) as exc_info:
            await redis_operate.get("test_table", {"field1"})

        assert exc_info.value.status_code == 487
        assert exc_info.value.message_code == 999
        assert "Unexpected error" in str(exc_info.value.message)


@pytest.mark.asyncio
async def test_integration_workflow(redis_operate, mock_redis):
    """Test a complete workflow of operations"""
    # Setup mock responses
    mock_redis.hset.return_value = 2
    mock_redis.hexists.return_value = 1
    mock_redis.hmget.return_value = ['{"name": "John"}', "25"]
    mock_redis.hlen.return_value = 2
    mock_redis.hdel.return_value = 1

    # 1. Set some data
    data = {"user1": {"name": "John"}, "age": 25}
    assert await redis_operate.set("users", data) is True

    # 2. Check if field exists
    assert await redis_operate.exists("users", "user1") is True

    # 3. Get specific fields
    result = await redis_operate.get("users", {"user1", "age"})
    assert len(result) == 2

    # 4. Count fields
    count = await redis_operate.count("users")
    assert count == 2

    # 5. Delete a field
    deleted = await redis_operate.delete_cache("users", {"age"})
    assert deleted == 1
