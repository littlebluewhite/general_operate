import asyncio
import functools
import json

import redis

from . import GeneralOperateException
from .app.cache_operate import CacheOperate
from .app.client.influxdb import InfluxDB
from .app.influxdb_operate import InfluxOperate
from .app.sql_operate import SQLOperate


class GeneralOperate(CacheOperate, SQLOperate, InfluxOperate):
    def __init__(
        self,
        module,
        database_client,
        redis_client: redis.asyncio.Redis,
        influxdb: InfluxDB = None,
        exc=GeneralOperateException,
    ):
        self.module = module
        self.table_name = module.table_name
        self.main_schemas = module.main_schemas
        self.create_schemas = module.create_schemas
        self.update_schemas = module.update_schemas
        self.__exc = exc
        CacheOperate.__init__(self, redis_client, exc)
        SQLOperate.__init__(self, database_client, exc)
        InfluxOperate.__init__(self, influxdb)

    @staticmethod
    def exception_handler(func):
        """Exception handler decorator for GeneralOperate methods"""

        @functools.wraps(func)
        async def async_wrapper(self, *args, **kwargs):
            try:
                return await func(self, *args, **kwargs)
            except GeneralOperateException as e:
                # If it's already a GeneralOperateException, return it directly
                return e
            except Exception as e:
                # For other exceptions, return self.__exc instance
                return self.__exc(status_code=500, message=str(e), message_code=9999)

        @functools.wraps(func)
        def sync_wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except GeneralOperateException as e:
                # If it's already a GeneralOperateException, return it directly
                return e
            except Exception as e:
                # For other exceptions, return self.__exc instance
                return self.__exc(status_code=500, message=str(e), message_code=9999)

        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    @exception_handler
    async def health_check(self):
        """Check health of SQL and cache connections"""
        # Check SQL health
        sql_health = await SQLOperate.health_check(self)

        # Check cache health
        cache_health = await CacheOperate.health_check(self)

        return sql_health and cache_health

    @exception_handler
    async def cache_warming(self, limit: int = 1000, offset: int = 0):
        """Warm up cache by loading data from SQL in batches"""
        batch_size = min(limit, 500)  # Limit batch size to prevent memory issues
        current_offset = offset
        total_loaded = 0

        while True:
            # Read batch from SQL
            batch_results = await self.read_sql(
                table_name=self.table_name, limit=batch_size, offset=current_offset
            )

            if not batch_results:
                # No more data to load
                break

            # Prepare cache data
            cache_data_to_set = {}
            for sql_row in batch_results:
                if sql_row and "id" in sql_row:
                    cache_data_to_set[str(sql_row["id"])] = json.dumps(sql_row)

            # Batch write to cache
            if cache_data_to_set:
                await self.set(self.table_name, cache_data_to_set)
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

    @exception_handler
    async def cache_clear(self):
        """Clear all cache data for this table"""
        # Clear main cache
        await self.redis.delete(self.table_name)

        # Clear null markers using pattern matching
        pattern = f"{self.table_name}:*:null"
        keys = await self.redis.keys(pattern)
        if keys:
            await self.redis.delete(*keys)

        return {"success": True, "message": "Cache cleared successfully"}

    @exception_handler
    async def read_data(self, id_value: set):
        """Read data with cache-first strategy and null value protection"""
        try:
            results = []
            cache_miss_ids = set()
            null_marked_ids = set()

            # 1. Try to get data from cache
            for id_key in id_value:
                # Check for null marker first
                null_key = f"{self.table_name}:{id_key}:null"
                is_null_marked = await self.redis.exists(null_key)

                if is_null_marked:
                    null_marked_ids.add(id_key)
                    continue

                # Try to get actual data from cache
                cached_data = await self.get(self.table_name, {str(id_key)})
                if cached_data:
                    # Parse cached data and convert to schema
                    for cache_item in cached_data:
                        data_value = list(cache_item.values())[0]
                        if isinstance(data_value, str):
                            try:
                                data_value = json.loads(data_value)
                            except json.JSONDecodeError:
                                pass

                        # Convert to main schema
                        schema_data = self.main_schemas(**data_value)
                        results.append(schema_data)
                else:
                    cache_miss_ids.add(id_key)

            # 2. For cache misses, read from SQL
            if cache_miss_ids:
                # Use SQL read method from parent class
                sql_results = await self.read_sql(
                    table_name=self.table_name,
                    filters={"id": list(cache_miss_ids)}
                    if len(cache_miss_ids) > 1
                    else None,
                )

                if len(cache_miss_ids) == 1:
                    # For single ID, use read_one
                    single_id = next(iter(cache_miss_ids))
                    single_result = await self.read_one(self.table_name, single_id)
                    sql_results = [single_result] if single_result else []

                # Process SQL results
                found_ids = set()
                cache_data_to_set = {}

                for sql_row in sql_results:
                    if sql_row:
                        # Convert to main schema
                        schema_data = self.main_schemas(**sql_row)
                        results.append(schema_data)

                        # Prepare for cache storage
                        found_ids.add(sql_row["id"])
                        cache_data_to_set[str(sql_row["id"])] = json.dumps(sql_row)

                # 3. Cache the found data
                if cache_data_to_set:
                    await self.set(self.table_name, cache_data_to_set)

                # 4. Mark missing IDs with null values (5 minutes expiry)
                missing_ids = cache_miss_ids - found_ids
                for missing_id in missing_ids:
                    null_key = f"{self.table_name}:{missing_id}:null"
                    await self.redis.setex(null_key, 300, "1")  # 5 minutes

            return results

        except Exception:
            # Fallback strategy: read directly from SQL
            return await self._read_data_fallback(id_value)

    async def _read_data_fallback(self, id_value: set):
        """Fallback strategy when cache fails - read directly from SQL"""
        try:
            sql_results = []
            for id_key in id_value:
                result = await self.read_one(self.table_name, id_key)
                if result:
                    schema_data = self.main_schemas(**result)
                    sql_results.append(schema_data)
            return sql_results
        except Exception:
            return []

    @exception_handler
    async def create_data(self, data: list):
        """Create data in SQL only (no cache write) - uses bulk insert for better performance"""
        if not data:
            return []

        # Validate input data
        validated_data = []
        for item in data:
            if not isinstance(item, dict):
                continue

            # Validate with create schema
            try:
                create_item = self.create_schemas(**item)
                validated_data.append(create_item.model_dump())
            except Exception:
                # Skip invalid items, could add logging here
                continue

        if not validated_data:
            return []

        try:
            # Use bulk create for better performance
            created_records = await self.create_sql(self.table_name, validated_data)

            # Convert all records to main schema
            results = []
            for record in created_records:
                if record:
                    schema_data = self.main_schemas(**record)
                    results.append(schema_data)

            return results

        except Exception:
            # Fallback: try individual creates
            return await self._create_data_fallback(validated_data)

    async def _create_data_fallback(self, validated_data: list):
        """Fallback strategy for create_data - tries individual creates using create"""
        results = []
        for item_data in validated_data:
            try:
                # Use create for individual items for consistency
                created_records = await self.create_sql(self.table_name, [item_data])
                if created_records:
                    schema_data = self.main_schemas(**created_records[0])
                    results.append(schema_data)
            except Exception:
                # Skip failed creates
                continue
        return results

    @exception_handler
    async def update_data(self, data: dict):
        """Update data with bulk operations and double delete cache strategy"""
        if not data:
            return []

        # Prepare data for bulk update
        update_list = []
        cache_keys_to_delete = []

        for id_key, update_info in data.items():
            try:
                # Convert id to appropriate type
                record_id = int(id_key) if str(id_key).isdigit() else id_key

                # Validate update data
                if not isinstance(update_info, dict):
                    continue

                try:
                    # Validate with update schema
                    update_item = self.update_schemas(**update_info)
                    validated_update_data = update_item.model_dump(exclude_unset=True)

                    # Add to bulk update list
                    update_list.append({"id": record_id, "data": validated_update_data})

                    # Prepare cache keys for deletion
                    cache_keys_to_delete.append(str(record_id))

                except Exception:
                    # Skip invalid update data
                    continue

            except Exception:
                # Skip this update if any validation fails
                continue

        if not update_list:
            return []

        # 1. First delete from cache (bulk operation)
        try:
            if cache_keys_to_delete:
                await CacheOperate.delete_cache(self, self.table_name, set(cache_keys_to_delete))

                # Also delete null markers if they exist
                for record_id in cache_keys_to_delete:
                    null_key = f"{self.table_name}:{record_id}:null"
                    try:
                        await self.redis.delete(null_key)
                    except Exception:
                        pass
        except Exception:
            # Cache delete failed, but continue with SQL update
            pass

        # 2. Bulk update SQL data
        try:
            updated_records = await self.update_sql(self.table_name, update_list)

            # Convert to main schemas
            results = []
            for record in updated_records:
                if record:
                    schema_data = self.main_schemas(**record)
                    results.append(schema_data)

            # 3. Second delete from cache (ensure consistency)
            try:
                if cache_keys_to_delete:
                    await CacheOperate.delete_cache(self, self.table_name, set(cache_keys_to_delete))
            except Exception:
                # Second cache delete failed, but data is updated
                pass

            return results

        except Exception:
            # If bulk update fails, fall back to individual updates
            return await self._update_data_fallback(update_list)

    async def _update_data_fallback(self, update_list: list):
        """Fallback strategy for update_data when bulk update fails"""
        results = []
        for update_item in update_list:
            try:
                record_id = update_item["id"]
                validated_update_data = update_item["data"]

                # Individual cache delete
                try:
                    await CacheOperate.delete_cache(self, self.table_name, {str(record_id)})
                    null_key = f"{self.table_name}:{record_id}:null"
                    await self.redis.delete(null_key)
                except Exception:
                    pass

                # Individual SQL update using bulk method with single item
                updated_records = await self.update_sql(
                    self.table_name, [{"id": record_id, "data": validated_update_data}]
                )

                if updated_records:
                    schema_data = self.main_schemas(**updated_records[0])
                    results.append(schema_data)

                    # Second cache delete
                    try:
                        await CacheOperate.delete_cache(self, self.table_name, {str(record_id)})
                    except Exception:
                        pass

            except Exception:
                # Skip failed updates
                continue

        return results

    @exception_handler
    async def delete_data(self, id_value: set):
        """Delete data from both SQL and cache using bulk operations, return successfully deleted IDs"""
        if not id_value:
            return []

        # Convert and validate IDs
        validated_ids = []
        for id_key in id_value:
            try:
                # Convert id to appropriate type
                record_id = int(id_key) if str(id_key).isdigit() else id_key
                validated_ids.append(record_id)
            except Exception:
                # Skip invalid IDs
                continue

        if not validated_ids:
            return []

        try:
            # 1. Use bulk delete from SQL
            successfully_deleted_ids = await self.delete_sql(
                self.table_name, validated_ids
            )

            if successfully_deleted_ids:
                # 2. Bulk delete from cache
                try:
                    cache_keys = {
                        str(record_id) for record_id in successfully_deleted_ids
                    }
                    await CacheOperate.delete_cache(self, self.table_name, cache_keys)
                except Exception:
                    # Cache delete failed, but SQL delete succeeded
                    pass

                # 3. Delete null markers if they exist
                for record_id in successfully_deleted_ids:
                    try:
                        null_key = f"{self.table_name}:{record_id}:null"
                        await self.redis.delete(null_key)
                    except Exception:
                        # Null marker delete failed, but main delete succeeded
                        pass

            return successfully_deleted_ids

        except Exception:
            # If bulk delete fails, fall back to individual deletes
            return await self._delete_data_fallback(validated_ids)

    async def _delete_data_fallback(self, validated_ids: list):
        """Fallback strategy for delete_data when bulk delete fails"""
        successfully_deleted_ids = []

        for record_id in validated_ids:
            try:
                # Individual SQL delete (returns list of deleted IDs)
                sql_deleted_ids = await self.delete_sql(self.table_name, record_id)

                if sql_deleted_ids and record_id in sql_deleted_ids:
                    successfully_deleted_ids.append(record_id)

                    # Individual cache delete
                    try:
                        await CacheOperate.delete_cache(self, self.table_name, {str(record_id)})
                    except Exception:
                        pass

                    # Delete null marker
                    try:
                        null_key = f"{self.table_name}:{record_id}:null"
                        await self.redis.delete(null_key)
                    except Exception:
                        pass

            except Exception:
                # Skip failed deletes
                continue

        return successfully_deleted_ids

    @exception_handler
    async def delete_filter_data(self, filters: dict):
        """Delete multiple records based on filter conditions"""
        if not filters:
            return []

        try:
            # Use delete_many from SQL operate
            deleted_count = await self.delete_filter(self.table_name, filters)

            if deleted_count > 0:
                # If some records were deleted, clear related cache
                # Since we don't know exact IDs, we need to be more aggressive
                try:
                    # Option 1: Clear entire table cache (simple but less efficient)
                    await self.redis.delete(self.table_name)

                    # Option 2: Try to clear potential null markers
                    # This is more complex and might require additional logic

                except Exception:
                    # Cache clear failed, but SQL delete succeeded
                    pass

            return deleted_count

        except Exception:
            return 0
