import asyncio
import functools
import hashlib
import json
import random
import re
from datetime import datetime, UTC
from typing import Any
import structlog

import redis
from redis import RedisError

from ..core.exceptions import CacheException, ErrorCode, ErrorContext
from ..utils.json_encoder import EnhancedJSONEncoder


class CacheOperate:
    def __init__(self, redis_db: redis.asyncio.Redis):
        self.redis = redis_db
        self.__exc = CacheException

        self.logger = structlog.get_logger().bind(
            operator=self.__class__.__name__
        )

    @staticmethod
    def exception_handler(func):
        """Exception handler decorator for GeneralOperate methods"""
        def handle_exceptions(self, e):
            """Common exception handling logic"""
            # redis error
            if isinstance(e, redis.ConnectionError):
                raise self.__exc(
                    code=ErrorCode.CACHE_CONNECTION_ERROR,
                    message=f"Redis connection error: {str(e)}",
                    context=ErrorContext(operation="cache_operation", resource="redis"),
                    cause=e
                )
            elif isinstance(e, redis.TimeoutError):
                raise self.__exc(
                    code=ErrorCode.CACHE_CONNECTION_ERROR,
                    message=f"Redis timeout error: {str(e)}",
                    context=ErrorContext(operation="cache_operation", resource="redis", details={"error_type": "timeout"}),
                    cause=e
                )
            elif isinstance(e, redis.ResponseError):
                raise self.__exc(
                    code=ErrorCode.CACHE_KEY_ERROR,
                    message=f"Redis response error: {str(e)}",
                    context=ErrorContext(operation="cache_operation", resource="redis", details={"error_type": "response"}),
                    cause=e
                )
            elif isinstance(e, RedisError):
                error_message = str(e)
                pattern = r"Error (\d+)"
                match = re.search(pattern, error_message)
                details = {"error_type": "redis_general"}
                if match:
                    details["redis_error_code"] = match.group(1)
                
                raise self.__exc(
                    code=ErrorCode.CACHE_CONNECTION_ERROR,
                    message=f"Redis error: {error_message}",
                    context=ErrorContext(operation="cache_operation", resource="redis", details=details),
                    cause=e
                )
            # decode error
            elif isinstance(e, json.JSONDecodeError):
                raise self.__exc(
                    code=ErrorCode.CACHE_SERIALIZATION_ERROR,
                    message=f"JSON decode error: {str(e)}",
                    context=ErrorContext(operation="cache_operation", resource="redis", details={"error_type": "json_decode"}),
                    cause=e
                )
            elif isinstance(e, (ValueError, TypeError, AttributeError)) and "SQL" in str(e):
                # Data validation or SQL-related errors
                raise self.__exc(
                    code=ErrorCode.VALIDATION_ERROR,
                    message=f"SQL operation validation error: {str(e)}",
                    context=ErrorContext(operation="cache_operation", resource="redis", details={"error_type": "sql_validation"}),
                    cause=e
                )
            elif isinstance(e, self.__exc):
                # If it's already a GeneralOperateException, raise it directly
                raise e
            else:
                # For truly unexpected exceptions
                raise self.__exc(
                    code=ErrorCode.UNKNOWN_ERROR,
                    message=f"Operation error: {str(e)}",
                    context=ErrorContext(operation="cache_operation", resource="redis", details={"error_type": "unexpected"}),
                    cause=e
                )
        @functools.wraps(func)
        async def async_wrapper(self, *args, **kwargs):
            try:
                return await func(self, *args, **kwargs)
            except Exception as e:
                # Log unexpected exceptions with full details
                self.logger.error(f"Unexpected error in {func.__name__}: {type(e).__name__}: {str(e)}", exc_info=True)
                handle_exceptions(self, e)

        @functools.wraps(func)
        def sync_wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                # For other exceptions, return self.__exc instance
                handle_exceptions(self, e)

        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper


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
        serialized_data = json.dumps(enriched_data, ensure_ascii=False, cls=EnhancedJSONEncoder)
        # 儲存到 Redis
        set_ttl = ttl_seconds if ttl_seconds else random.randint(2000, 5000)
        return key, set_ttl, serialized_data

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
        """set a null marker key with expiry"""
        if not self.redis:
            return False
        result = await self.redis.setex(key, expiry_seconds, "1")
        return result is True

    async def delete_null_key(self, key: str) -> bool:
        """Delete a null marker key"""
        if not self.redis:
            return False
        result = await self.redis.delete(key)
        return result > 0

    async def check_null_markers_batch(self, table_name: str, id_values: list[Any]) -> tuple[dict[Any, bool], list[Any]]:
        """Batch check null markers for multiple IDs.
        
        Args:
            table_name: The table name
            id_values: List of IDs to check
            
        Returns:
            Tuple of:
            - Dictionary mapping ID to whether it has a null marker (True = null marker exists = record doesn't exist)
            - List of IDs that don't have null markers (need further checking)
        """
        if not self.redis or not id_values:
            return {}, list(id_values)
        
        try:
            # Build null marker keys
            null_keys = {}
            async with self.redis.pipeline(transaction=False) as pipe:
                for id_val in id_values:
                    null_key = self.build_null_marker_key(table_name, id_val)
                    null_keys[id_val] = null_key
                    pipe.exists(null_key)  # Queue the command
                
                # Execute all null marker checks at once
                null_results = await pipe.execute()
            
            # Process results
            null_marker_status = {}
            non_null_ids = []
            
            for id_val, is_null_marked in zip(id_values, null_results):
                null_marker_status[id_val] = bool(is_null_marked)
                if not is_null_marked:
                    non_null_ids.append(id_val)
            
            return null_marker_status, non_null_ids
            
        except Exception as e:
            self.logger.warning(f"Failed to check null markers batch: {e}")
            # On failure, assume all need checking
            return {}, list(id_values)

    async def set_null_markers_batch(self, table_name: str, id_values: list[Any], expiry_seconds: int = 300) -> dict[Any, bool]:
        """Batch set null markers for multiple IDs.
        
        Args:
            table_name: The table name
            id_values: List of IDs to set null markers for
            expiry_seconds: TTL for the null markers (default 5 minutes)
            
        Returns:
            Dictionary mapping ID to whether the null marker was successfully set
        """
        if not self.redis or not id_values:
            return {id_val: False for id_val in id_values}
        
        try:
            results = {}
            async with self.redis.pipeline(transaction=False) as pipe:
                for id_val in id_values:
                    null_key = self.build_null_marker_key(table_name, id_val)
                    pipe.setex(null_key, expiry_seconds, "1")
                
                # Execute all set operations
                set_results = await pipe.execute()
            
            # Process results
            for id_val, result in zip(id_values, set_results):
                results[id_val] = result is True
            
            return results
            
        except Exception as e:
            self.logger.warning(f"Failed to set null markers batch: {e}")
            return {id_val: False for id_val in id_values}

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
    
    # Cache Key Building Utilities
    def build_cache_key(self, table_name: str, record_id: Any) -> str:
        """Build a cache key for a given table and record ID.
        
        Args:
            table_name: The name of the table
            record_id: The record ID
            
        Returns:
            Formatted cache key string
        """
        return f"{table_name}:{record_id}"
    
    def build_null_marker_key(self, table_name: str, record_id: Any) -> str:
        """Build a null marker cache key for a given table and record ID.
        
        Args:
            table_name: The name of the table
            record_id: The record ID
            
        Returns:
            Formatted null marker key string
        """
        return f"{table_name}:{record_id}:null"
    
    @staticmethod
    def process_in_batches(items: list, batch_size: int = 100):
        """Generator to process items in batches.
        
        Args:
            items: list of items to process
            batch_size: Size of each batch (default: 100)
            
        Yields:
            Batches of items
        """
        for i in range(0, len(items), batch_size):
            yield items[i:i + batch_size]
    
    # Cache Management Operations
    async def cache_warming(
        self, 
        table_name: str,
        fetch_function,
        limit: int = 1000, 
        offset: int = 0
    ) -> dict:
        """Warm up cache by loading data from SQL in batches.
        
        Args:
            table_name: Name of the table to warm cache for
            fetch_function: Async function to fetch data from SQL
            limit: Maximum number of records to load
            offset: Number of records to skip
            
        Returns:
            Dictionary with warming statistics
        """
        batch_size = min(limit, 500)  # Limit batch size to prevent memory issues
        current_offset = offset
        total_loaded = 0

        while True:
            # Read batch from SQL using provided function
            batch_results = await fetch_function(
                table_name=table_name, 
                limit=batch_size, 
                offset=current_offset
            )

            if not batch_results:
                # No more data to load
                break

            # Prepare cache data
            cache_data_to_set = {}
            for sql_row in batch_results:
                if sql_row and "id" in sql_row:
                    cache_data_to_set[str(sql_row["id"])] = sql_row

            # Batch write to cache
            if cache_data_to_set:
                await self.store_caches(table_name, cache_data_to_set)
                total_loaded += len(cache_data_to_set)

            # Check if we've reached the limit
            if len(batch_results) < batch_size or total_loaded >= limit:
                break

            current_offset += batch_size

        return {
            "success": True,
            "records_loaded": total_loaded,
            "message": f"Successfully warmed cache with {total_loaded} records",
        }
    
    async def cache_clear(self, table_name: str) -> dict:
        """Clear all cache data for a table.
        
        Args:
            table_name: Name of the table to clear cache for
            
        Returns:
            Dictionary with clear operation result
        """
        # Clear main cache
        await self.redis.delete(table_name)

        # Clear null markers using pattern matching
        pattern = f"{table_name}:*:null"
        keys = await self.redis.keys(pattern)
        if keys:
            await self.redis.delete(*keys)

        return {"success": True, "message": "Cache cleared successfully"}
    
    async def refresh_cache(
        self,
        table_name: str,
        id_values: set,
        fetch_function
    ) -> dict:
        """Reload specific records from database to cache.
        
        Args:
            table_name: Name of the table
            id_values: set of ID values to refresh in cache
            fetch_function: Async function to fetch data from SQL
            
        Returns:
            Dictionary with refresh statistics
        """
        if not id_values:
            return {"refreshed": 0, "not_found": 0, "errors": 0}
        
        operation_context = f"refresh_cache(table={table_name}, ids={len(id_values)})"
        self.logger.debug(f"Starting {operation_context}")
        
        refreshed = 0
        not_found = 0
        errors = 0
        
        try:
            # Clear existing cache entries and null markers
            cache_keys = {str(id_val) for id_val in id_values}
            await self.delete_caches(table_name, cache_keys)
            
            # Clear null markers using pipeline for better performance
            try:
                async with self.redis.pipeline(transaction=False) as pipe:
                    for id_val in id_values:
                        null_key = self.build_null_marker_key(table_name, id_val)
                        pipe.delete(null_key)  # Queue delete operation
                    await pipe.execute()  # Execute all deletes at once
            except (redis.RedisError, ConnectionError) as e:
                # Log but don't fail - cache clear is best effort
                self.logger.debug(f"Failed to clear null markers in bulk: {e}")
            
            # Fetch fresh data from database
            sql_results = await fetch_function(
                table_name=table_name,
                filters={"id": list(id_values)}
            )
            
            if sql_results:
                # Prepare cache data
                cache_data_to_set = {}
                found_ids = set()
                
                for sql_row in sql_results:
                    if sql_row and "id" in sql_row:
                        cache_data_to_set[str(sql_row["id"])] = sql_row
                        found_ids.add(sql_row["id"])
                        refreshed += 1
                
                # Store in cache
                if cache_data_to_set:
                    await self.store_caches(table_name, cache_data_to_set)
                
                # Mark missing IDs with null markers using pipeline
                missing_ids = id_values - found_ids
                if missing_ids:
                    try:
                        async with self.redis.pipeline(transaction=False) as pipe:
                            for missing_id in missing_ids:
                                null_key = self.build_null_marker_key(table_name, missing_id)
                                pipe.setex(null_key, 300, "1")  # Queue setex operation
                                not_found += 1
                            await pipe.execute()  # Execute all at once
                    except (redis.RedisError, ConnectionError) as e:
                        self.logger.debug(f"Failed to set null markers in bulk: {e}")
                        not_found = len(missing_ids)
                else:
                    not_found = len(missing_ids)
            else:
                # All IDs not found - use pipeline for setting null markers
                try:
                    async with self.redis.pipeline(transaction=False) as pipe:
                        for id_val in id_values:
                            null_key = self.build_null_marker_key(table_name, id_val)
                            pipe.setex(null_key, 300, "1")  # Queue setex operation
                            not_found += 1
                        await pipe.execute()  # Execute all at once
                except (redis.RedisError, ConnectionError) as e:
                    self.logger.debug(f"Failed to set null markers in bulk: {e}")
                    not_found = len(id_values)
                    
        except Exception as e:
            self.logger.error(f"Error in {operation_context}: {str(e)}")
            errors = len(id_values) - refreshed - not_found
        
        result = {
            "refreshed": refreshed,
            "not_found": not_found,
            "errors": errors
        }
        
        self.logger.debug(f"Completed {operation_context}: {result}")
        return result
    
    # Cache Pattern Handlers
    async def process_cache_lookups(
        self,
        table_name: str,
        id_values: set
    ) -> tuple[list, set, set, list]:
        """Process cache lookups for given IDs.
        
        Args:
            table_name: Name of the table
            id_values: set of ID values to look up
            
        Returns:
            tuple of (results, cache_miss_ids, null_marked_ids, failed_cache_ops)
        """
        results = []
        cache_miss_ids = set()
        null_marked_ids = set()
        failed_cache_ops = []
        
        for id_key in id_values:
            try:
                # Check cache first to avoid database hit
                # Null markers indicate records that were previously checked and don't exist
                null_key = self.build_null_marker_key(table_name, id_key)
                is_null_marked = await self.redis.exists(null_key)

                if is_null_marked:
                    # Record was previously checked and doesn't exist in database
                    # Skip further processing to avoid unnecessary DB queries
                    null_marked_ids.add(id_key)
                    continue

                # Attempt to retrieve data from cache
                # Cache hit means we can avoid expensive database query
                cached_data = await self.get_caches(table_name, {str(id_key)})
                if cached_data:
                    # Data found in cache
                    results.extend(cached_data)
                else:
                    cache_miss_ids.add(id_key)

            except (redis.RedisError, Exception) as cache_err:
                # Cache failures are non-fatal - fall back to database
                # Track failed operations to avoid retry attempts
                self.logger.warning(f"Cache operation failed for {table_name}:{id_key}: {cache_err}")
                failed_cache_ops.append(id_key)
                cache_miss_ids.add(id_key)
                
        return results, cache_miss_ids, null_marked_ids, failed_cache_ops
    
    async def handle_cache_misses(
        self,
        table_name: str,
        cache_miss_ids: set,
        failed_cache_ops: list,
        fetch_function,
        schema_validator=None
    ) -> tuple[list, set]:
        """Handle cache misses by fetching from SQL and processing results.
        
        Args:
            table_name: Name of the table
            cache_miss_ids: set of IDs that were not in cache
            failed_cache_ops: list of IDs where cache operations failed
            fetch_function: Async function to fetch data from SQL
            schema_validator: Optional function to validate/transform data
            
        Returns:
            tuple of (schema_results, found_ids)
        """
        if not cache_miss_ids:
            return [], set()
            
        sql_results = await fetch_function(cache_miss_ids)
        schema_results = []
        found_ids = set()
        cache_data_to_set = {}

        for sql_row in sql_results:
            if sql_row:
                try:
                    # Apply schema validation if provided
                    if schema_validator:
                        schema_data = schema_validator(sql_row)
                        schema_results.append(schema_data)
                    else:
                        schema_results.append(sql_row)

                    # Prepare for cache update (skip if cache operation previously failed)
                    # This avoids attempting cache operations that are likely to fail again
                    if sql_row["id"] not in failed_cache_ops:
                        found_ids.add(sql_row["id"])
                        cache_data_to_set[str(sql_row["id"])] = sql_row
                except Exception as schema_err:
                    self.logger.error(f"Schema validation failed for SQL result {sql_row.get('id', 'unknown')}: {schema_err}")
                    continue

        # Update cache with fetched data
        if cache_data_to_set:
            await self.update_cache_after_fetch(table_name, cache_data_to_set)
            
        return schema_results, found_ids
    
    async def update_cache_after_fetch(
        self,
        table_name: str,
        cache_data: dict
    ) -> None:
        """Update cache with data fetched from SQL.
        
        Args:
            table_name: Name of the table
            cache_data: Dictionary mapping cache keys to data
        """
        try:
            cache_success = await self.store_caches(table_name, cache_data)
            if not cache_success:
                self.logger.warning(f"Cache write failed for {len(cache_data)} items")
        except Exception as cache_err:
            # Cache update failures are non-fatal - log and continue
            self.logger.warning(f"Cache write error: {cache_err}")
    
    async def mark_missing_records(
        self,
        table_name: str,
        missing_ids: set,
        failed_cache_ops: list
    ) -> None:
        """Mark records that don't exist in database with null markers.
        
        Args:
            table_name: Name of the table
            missing_ids: set of IDs that don't exist in database
            failed_cache_ops: list of IDs where cache operations failed
        """
        for missing_id in missing_ids:
            # Skip IDs where cache operations previously failed
            if missing_id not in failed_cache_ops:
                try:
                    null_key = self.build_null_marker_key(table_name, missing_id)
                    # set null marker with 5-minute expiry to avoid repeated DB lookups
                    await self.set_null_key(null_key, 300)
                except Exception as null_err:
                    self.logger.warning(f"Failed to set null marker for {missing_id}: {null_err}")
    
    async def clear_update_caches(
        self,
        table_name: str,
        cache_keys: list,
        operation_context: str
    ) -> list:
        """Clear cache entries before/after update operations.
        
        Args:
            table_name: Name of the table
            cache_keys: list of cache keys to clear
            operation_context: Context string for logging
            
        Returns:
            list of cache deletion errors (for logging)
        """
        cache_delete_errors = []
        
        if not cache_keys:
            return cache_delete_errors
            
        try:
            # Delete main cache entries
            deleted_count = await self.delete_caches(table_name, set(cache_keys))
            self.logger.debug(f"Cache cleanup: deleted {deleted_count} entries")

            # Delete null markers for ID-based cache keys
            for record_id in cache_keys:
                null_key = self.build_null_marker_key(table_name, record_id)
                try:
                    await self.delete_null_key(null_key)
                except Exception as null_err:
                    cache_delete_errors.append(f"null marker {record_id}: {null_err}")

        except Exception as cache_err:
            cache_delete_errors.append(f"cache cleanup: {cache_err}")
            self.logger.warning(f"Cache cleanup failed in {operation_context}: {cache_err}")
            
        return cache_delete_errors
    
    async def get_distinct_values_cached(
        self,
        table_name: str,
        field: str,
        filters: dict[str, Any] = None,
        cache_ttl: int = 300,
        fetch_function=None
    ) -> list[Any]:
        """Get distinct values from a field with optional caching.
        
        Args:
            table_name: Name of the table
            field: The field name to get distinct values from
            filters: Optional filters to apply
            cache_ttl: Cache TTL in seconds (default: 300, set to 0 to disable caching)
            fetch_function: Async function to fetch distinct values from SQL
            
        Returns:
            list of distinct values from the specified field
        """
        operation_context = f"get_distinct_values_cached(table={table_name}, field={field})"
        self.logger.debug(f"Starting {operation_context}")
        
        # Generate cache key for this query
        cache_key = None
        if cache_ttl > 0:
            filter_str = json.dumps(filters, sort_keys=True) if filters else ""
            cache_key = f"{table_name}:distinct:{field}:{hashlib.md5(filter_str.encode()).hexdigest()}"
            
            # Try to get from cache
            try:
                cached_result = await self.redis.get(cache_key)
                if cached_result:
                    result = json.loads(cached_result)
                    self.logger.debug(f"{operation_context} - Found in cache, returning {len(result)} values")
                    return result
            except Exception as cache_err:
                self.logger.warning(f"Cache read failed in {operation_context}: {cache_err}")
        
        # Get from database if not provided
        if not fetch_function:
            raise ValueError("fetch_function is required when cache miss occurs")
            
        try:
            distinct_values = await fetch_function(
                table_name,
                field,
                filters
            )
            
            # Cache the result if caching is enabled
            if cache_key and cache_ttl > 0:
                try:
                    await self.redis.setex(
                        cache_key,
                        cache_ttl,
                        json.dumps(distinct_values, cls=EnhancedJSONEncoder)
                    )
                except Exception as cache_err:
                    self.logger.warning(f"Cache write failed in {operation_context}: {cache_err}")
            
            self.logger.debug(f"Completed {operation_context}, found {len(distinct_values)} distinct values")
            return distinct_values
            
        except Exception as e:
            self.logger.error(f"Database query failed in {operation_context}: {str(e)}")
            raise
