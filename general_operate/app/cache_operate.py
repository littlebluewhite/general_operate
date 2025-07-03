import asyncio
import functools
import json
import re
from enum import Enum
from typing import Any

import redis
from fastapi.encoders import jsonable_encoder
from redis import RedisError

from ..utils.exception import GeneralOperateException


class CacheOperate:
    def __init__(self, redis_db: redis.asyncio.Redis, exc=GeneralOperateException):
        self.redis = redis_db
        self.__exc = exc

    @staticmethod
    def exception_handler(func):
        def handle_exceptions(self, e):
            """Common exception handling logic"""
            if isinstance(e, RedisError):
                error_message = str(e)
                pattern = r"Error (\d+)"
                match = re.search(pattern, error_message)
                if match:
                    error_code = match.group(1)
                    raise self.__exc(
                        status_code=487,
                        message=error_message,
                        message_code=int(error_code),
                    )
                else:
                    raise self.__exc(
                        status_code=487, message=error_message, message_code=3
                    )
            elif isinstance(e, json.JSONDecodeError):
                raise self.__exc(
                    status_code=487,
                    message="redis operation json decode error",
                    message_code=4,
                )
            elif isinstance(e, self.__exc):
                raise e
            else:
                raise self.__exc(status_code=487, message=str(e), message_code=999)

        @functools.wraps(func)
        async def async_wrapper(self, *args, **kwargs):
            try:
                return await func(self, *args, **kwargs)
            except Exception as e:
                handle_exceptions(self, e)

        @functools.wraps(func)
        def sync_wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                handle_exceptions(self, e)

        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    @exception_handler
    async def get(self, table_name: str, keys: set) -> list:
        """Get multiple field values from a Redis hash"""
        if not keys:
            return []

        # Convert set to list for Redis operation
        keys_list = list(keys)

        # Use HMGET to get multiple fields at once
        values = await self.redis.hmget(table_name, keys_list)

        # Build result list with key-value pairs
        result = []
        for key, value in zip(keys_list, values, strict=False):
            if value is not None:
                # Try to parse JSON if possible
                try:
                    parsed_value = json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    parsed_value = value
                result.append({key: parsed_value})

        return result

    @exception_handler
    async def set_cache(self, table_name: str, data: dict) -> bool:
        """Set multiple field-value pairs in a Redis hash"""
        if not data:
            return False

        # Prepare data for Redis HSET
        # Convert values to JSON strings if they're not already strings
        redis_data = {}
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                redis_data[key] = json.dumps(value)
            elif isinstance(value, Enum):
                redis_data[key] = jsonable_encoder(value)
            else:
                redis_data[key] = str(value)

        # Use HSET to set multiple fields at once
        result = await self.redis.hset(table_name, mapping=redis_data)

        # Return True if at least one field was set
        return result >= 0

    @exception_handler
    async def delete_cache(self, table_name: str, keys: set) -> int:
        """Delete multiple fields from a Redis hash"""
        if not keys:
            return 0

        # Convert set to list for Redis operation
        keys_list = list(keys)

        # Use HDEL to delete multiple fields at once
        deleted_count = await self.redis.hdel(table_name, *keys_list)

        # Return the number of fields deleted
        return deleted_count

    @exception_handler
    async def exists(self, table_name: str, key: str) -> bool:
        """Check if a field exists in a Redis hash"""
        result = await self.redis.hexists(table_name, key)
        return bool(result)

    @exception_handler
    async def get_all(self, table_name: str) -> dict[str, Any]:
        """Get all field-value pairs from a Redis hash"""
        result = await self.redis.hgetall(table_name)

        # Process the result to parse JSON values
        processed_result = {}
        for key, value in result.items():
            if value is not None:
                try:
                    # Try to parse JSON
                    processed_result[key] = json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    # Keep original value if not JSON
                    processed_result[key] = value

        return processed_result

    @exception_handler
    async def count(self, table_name: str) -> int:
        """Get the number of fields in a Redis hash"""
        count = await self.redis.hlen(table_name)
        return count

    @exception_handler
    async def delete_keys(self, keys: list[str] | set[str]) -> int:
        """Delete standalone Redis keys (not hash fields)"""
        if not keys:
            return 0
        
        # Convert to list if needed
        keys_list = list(keys) if isinstance(keys, set) else keys
        
        # Use DEL to delete standalone keys
        deleted_count = await self.redis.delete(*keys_list)
        
        # Return the number of keys deleted
        return deleted_count

    @exception_handler
    async def set_null_key(self, key: str, expiry_seconds: int = 300) -> bool:
        """Set a null marker key with expiry"""
        result = await self.redis.setex(key, expiry_seconds, "1")
        return result is True

    @exception_handler
    async def delete_null_key(self, key: str) -> bool:
        """Delete a null marker key"""
        result = await self.redis.delete(key)
        return result > 0

    @exception_handler
    async def health_check(self) -> bool:
        """Check if Redis connection is healthy"""
        try:
            # Ping the Redis server
            response = await self.redis.ping()
            return response is True
        except Exception:
            return False
