import json
import random
from datetime import datetime, UTC
from typing import Any
import structlog

import redis
from redis import RedisError

from ..utils.exception import GeneralOperateException


class CacheOperate:
    def __init__(self, redis_db: redis.asyncio.Redis, exc=GeneralOperateException):
        self.redis = redis_db
        self.__exc = exc

        self.logger = structlog.get_logger().bind(
            operator=self.__class__.__name__
        )

    async def get_caches(self, prefix: str, identifiers: set[str]) -> list[dict[str, Any]]:
        keys = [f"{prefix}:{identifier}" for identifier in identifiers]

        async with self.redis.pipeline(transaction=False) as pipe:
            for key in keys:
                # no await
                pipe.get(key)
            values = await pipe.execute()

        result = []
        for identifier, raw in zip(identifiers, values):
            if raw:
                try:
                    result.append(json.loads(raw))
                except json.JSONDecodeError:
                    self.logger.warning(f"Invalid JSON in cache for {prefix}:{identifier}")
        return result

    async def get_cache(self, prefix: str, identifier: str) -> dict[str, Any] | None:
        """從 Redis 獲取資料"""
        key = f"{prefix}:{identifier}"

        try:
            serialized_data = await self.redis.get(key)

            if serialized_data is None:
                return None

            # 反序列化資料
            data = json.loads(serialized_data)

            self.logger.debug(
                "Data retrieved from Redis",
                key=key,
                prefix=prefix
            )
            return data

        except (json.JSONDecodeError, Exception) as e:
            self.logger.error(
                "Failed to retrieve data from Redis",
                key=key,
                error=str(e)
            )
            return None

    async def store_caches(self, prefix: str,
                           identifier_key_with_data: dict[str, Any], ttl_seconds: int | None = None) -> bool:
        if not identifier_key_with_data:
            return False
            
        async with self.redis.pipeline(transaction=False) as pipe:
            for identifier, data in identifier_key_with_data.items():
                key, set_ttl, serialized_data = await self.__store_cache_inner(
                    prefix, identifier, data, ttl_seconds
                )
                # no await
                pipe.setex(key, set_ttl, serialized_data)
            await pipe.execute()
        return True

    async def store_cache(
            self, prefix: str, identifier: str, data: dict[str, Any], ttl_seconds: int | None = None
    ) -> None:
        """儲存資料到 Redis"""

        key, set_ttl, serialized_data = await self.__store_cache_inner(
            prefix, identifier, data, ttl_seconds
        )
        await self.redis.setex(key, set_ttl, serialized_data)

        self.logger.debug(
            "Data stored in Redis",
            key=key,
            prefix=prefix,
            ttl_seconds=ttl_seconds
        )

    @staticmethod
    async def __store_cache_inner(
            prefix: str, identifier: str, data: dict[str, Any], ttl_seconds: int | None = None
    ) -> tuple[str, int, str]:
        key = f"{prefix}:{identifier}"
        # 添加元數據
        enriched_data = {
            **data,
            "_created_at": datetime.now(UTC).isoformat(),
            "prefix": prefix,
            "_identifier": identifier
        }
        # 序列化資料
        serialized_data = json.dumps(enriched_data, ensure_ascii=False)
        # 儲存到 Redis
        set_ttl = ttl_seconds if ttl_seconds else random.randint(2000, 5000)
        return (key, set_ttl, serialized_data)

    async def delete_caches(self, prefix: str, identifiers: set[str]) -> int:
        if not identifiers:
            return 0
        keys = [f"{prefix}:{identifier}" for identifier in identifiers]
        deleted_count = await self.redis.delete(*keys)

        self.logger.debug(
            "Batch delete from Redis",
            keys=keys,
            deleted_count=deleted_count
        )

        return deleted_count

    async def delete_cache(self, prefix: str, identifier: str) -> bool:
        """從 Redis 刪除資料"""
        key = f"{prefix}:{identifier}"
        result = await self.redis.delete(key)

        self.logger.debug(
            "Data deleted from Redis",
            key=key,
            prefix=prefix,
            deleted=result > 0
        )

        return result > 0

    async def cache_exists(self, prefix: str, identifier: str) -> bool:
        """檢查資料是否存在"""
        key = f"{prefix}:{identifier}"
        return await self.redis.exists(key) > 0


    async def cache_extend_ttl(
            self,
            prefix: str,
            identifier: str,
            additional_seconds: int
    ) -> bool:
        """延長資料過期時間"""

        key = f"{prefix}:{identifier}"

        # 獲取當前 TTL
        current_ttl = await self.redis.ttl(key)

        if current_ttl <= 0:
            return False  # Key 不存在或已過期

        # 設置新的 TTL
        new_ttl = current_ttl + additional_seconds
        result = await self.redis.expire(key, new_ttl)

        self.logger.debug(
            "TTL extended",
            key=key,
            previous_ttl=current_ttl,
            new_ttl=new_ttl
        )
        return result

    async def set_null_key(self, key: str, expiry_seconds: int = 300) -> bool:
        """Set a null marker key with expiry"""
        result = await self.redis.setex(key, expiry_seconds, "1")
        return result is True

    async def delete_null_key(self, key: str) -> bool:
        """Delete a null marker key"""
        result = await self.redis.delete(key)
        return result > 0

    async def health_check(self) -> bool:
        """Check if Redis connection is healthy"""
        try:
            # Ping the Redis server
            response = await self.redis.ping()
            return response is True
        except (redis.ConnectionError, redis.TimeoutError, redis.ResponseError, RedisError) as redis_err:
            # Log specific Redis errors but don't raise - health check should return boolean
            logger = structlog.get_logger()
            logger.warning(f"Redis health check failed: {type(redis_err).__name__}: {redis_err}")
            return False
        except Exception as e:
            # Log unexpected errors
            logger = structlog.get_logger()
            logger.error(f"Unexpected error in Redis health check: {type(e).__name__}: {e}")
            return False
