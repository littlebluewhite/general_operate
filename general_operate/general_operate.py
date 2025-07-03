import asyncio
import functools
import json
from typing import Any

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
                # If it's already a GeneralOperateException, raise it directly
                raise e
            except Exception as e:
                # For other exceptions, raise self.__exc instance
                raise self.__exc(status_code=500, message=str(e), message_code=9999)

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
                await self.set_cache(self.table_name, cache_data_to_set)
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
    async def read_data_by_id(self, id_value: set):
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
                    await self.set_cache(self.table_name, cache_data_to_set)

                # 4. Mark missing IDs with null values (5 minutes expiry)
                missing_ids = cache_miss_ids - found_ids
                for missing_id in missing_ids:
                    null_key = f"{self.table_name}:{missing_id}:null"
                    await self.set_null_key(null_key, 300)  # 5 minutes

            return results

        except Exception:
            # Fallback strategy: read directly from SQL
            return await self._read_data_fallback(id_value)

    @exception_handler
    async def read_data_by_filter(self, filters: dict[str, Any], limit: int | None = None, offset: int = 0):
        """Read data by filter conditions with optional pagination"""
        try:
            # Use SQL read method with filters
            sql_results = await self.read_sql(
                table_name=self.table_name,
                filters=filters,
                limit=limit,
                offset=offset
            )

            # Convert to main schemas
            results = []
            for sql_row in sql_results:
                if sql_row:
                    schema_data = self.main_schemas(**sql_row)
                    results.append(schema_data)

            return results

        except self.__exc as e:
            raise e

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
        except self.__exc as e:
            raise e

    @exception_handler
    async def create_data(self, data: list, session=None):
        """Create data in SQL only (no cache write) - uses bulk insert for better performance
        
        Args:
            data: List of data to create
            session: Optional AsyncSession for transaction management
        """
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

        # Use bulk create for better performance, pass session if provided
        created_records = await self.create_sql(self.table_name, validated_data, session=session)

        # Convert all records to main schema
        results = []
        for record in created_records:
            if record:
                schema_data = self.main_schemas(**record)
                results.append(schema_data)

        return results

    @exception_handler
    async def create_by_foreign_key(self, foreign_key_field: str, foreign_key_value: Any, data: list[Any], session=None) -> list[Any]:
        """
        為指定的外鍵值創建關聯實體

        此方法簡化了創建具有外鍵關係的實體的過程，自動為每個實體設置外鍵值。

        Args:
            foreign_key_field: 外鍵字段名稱（例如："tutorial_id"）
            foreign_key_value: 外鍵值（例如：123）
            data: 要創建的數據列表，不需要包含外鍵字段
            session: Optional AsyncSession for transaction management

        Returns:
            list[Any]: 創建成功的實體列表

        Raises:
            GeneralOperateException: 當創建操作失敗時
        """
        if not data:
            return []

        # 導入數據構建函數
        from .utils.build_data import build_create_data

        # 準備創建數據，為每個項目添加外鍵
        create_data_list = []
        for item in data:
            item_dict = build_create_data(item)
            item_dict[foreign_key_field] = foreign_key_value
            create_data_list.append(item_dict)

        # 批量創建實體
        return await self.create_data(create_data_list, session=session)

    @exception_handler
    async def update_data(self, data: list[dict[str, Any]], where_field: str = "id", session=None):
        """Update data with bulk operations and double delete cache strategy
        
        Args:
            data: List of dictionaries containing update data, each must include the where_field
            where_field: Field name to use in WHERE clause (default: "id")
            session: Optional AsyncSession for transaction management
        """
        if not data:
            return []

        # Prepare data for bulk update
        update_list = []
        cache_keys_to_delete = []

        for update_item in data:
            if where_field not in update_item:
                raise self.__exc(status_code=400, message=f"Missing '{where_field}' field in update data", message_code=214)

            where_value = update_item[where_field]

            try:
                # Validate with update schema
                update_schema_item = self.update_schemas(**update_item)

                validated_update_data = update_schema_item.model_dump(exclude_unset=True)

                # Extract update fields (excluding the where_field)
                validated_update_data = {k: v for k, v in validated_update_data.items() if k != where_field}

                # Validate update data
                if not validated_update_data:
                    continue

                # Add to bulk update list in new format
                update_list.append({where_field: where_value, "data": validated_update_data})

                # Prepare cache keys for deletion (only for id-based updates)
                if where_field == "id":
                    cache_keys_to_delete.append(str(where_value))

            except Exception as e:
                # Skip invalid update data
                raise self.__exc(
                    status_code=400, message=str(e), message_code=215)

        if not update_list:
            return []

        # 1. First delete from cache (bulk operation)
        try:
            if cache_keys_to_delete:
                await CacheOperate.delete_cache(self, self.table_name, set(cache_keys_to_delete))

                # Also delete null markers if they exist (only for id-based updates)
                for record_id in cache_keys_to_delete:
                    null_key = f"{self.table_name}:{record_id}:null"
                    try:
                        await self.delete_null_key(null_key)
                    except Exception:
                        pass
        except Exception:
            # Cache delete failed, but continue with SQL update
            pass

        # 2. Bulk update SQL data, pass session if provided
        updated_records = await self.update_sql(self.table_name, update_list, where_field, session=session)

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

        if len(results) != len(data):
            raise self.__exc(status_code=400, message="not all data can update", message_code=217)

        return results


    @exception_handler
    async def update_by_foreign_key(self, foreign_key_field: str, foreign_key_value: Any, data: list[Any], session=None) -> None:
        """
        通用的外鍵關聯數據更新方法，支持批量 CRUD 操作

        此方法基於 ID 規則處理相關實體的創建、更新和刪除操作：
        - id == 0 or None: 創建新記錄
        - id > 0: 更新現有記錄（如果存在）
        - id < 0: 刪除記錄，使用 abs(id)（如果存在）

        Args:
            foreign_key_field: 外鍵字段名稱（例如："tutorial_id"）
            foreign_key_value: 外鍵值（例如：123）
            data: 要更新的數據列表，遵循上述 ID 規則
            session: Optional AsyncSession for transaction management

        Raises:
            GeneralOperateException: 當操作失敗時
        """
        if not data:
            return

        # 導入比較函數
        from .utils.build_data import compare_related_items

        # 定義錯誤處理函數
        def handle_missing_update(item_id: int) -> None:
            """處理不存在的更新目標"""
            print(f"Warning: Attempted to update non-existent record with ID {item_id}")

        def handle_missing_delete(item_id: int) -> None:
            """處理不存在的刪除目標"""
            print(f"Warning: Attempted to delete non-existent record with ID {item_id}")

        # 獲取現有記錄
        existing_items_models = await self.read_data_by_filter(
            filters={foreign_key_field: foreign_key_value}
        )
        existing_items = [item_model.model_dump() for item_model in existing_items_models]

        # 比較現有與新數據以確定操作
        to_create, to_update, to_delete_ids = compare_related_items(
            existing_items=existing_items,
            new_items=data,
            foreign_key_field=foreign_key_field,
            foreign_key_value=foreign_key_value,
            handle_missing_update=handle_missing_update,
            handle_missing_delete=handle_missing_delete
        )

        # 執行刪除操作（首先執行以避免衝突）
        if to_delete_ids:
            await self.delete_data(id_value=set(to_delete_ids), session=session)

        # 執行更新操作
        if to_update:
            # Convert to_update list to the new format (list of dicts with id included)
            await self.update_data(to_update, session=session)

        # 執行創建操作
        if to_create:
            await self.create_data(to_create, session=session)


    @exception_handler
    async def delete_data(self, id_value: set, session=None):
        """Delete data from both SQL and cache using bulk operations, return successfully deleted IDs
        
        Args:
            id_value: Set of IDs to delete
            session: Optional AsyncSession for transaction management
        """
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

        # 1. Use bulk delete from SQL
        successfully_deleted_ids = await self.delete_sql(
            self.table_name, validated_ids, session=session
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
                    await self.delete_null_key(null_key)
                except Exception:
                    # Null marker delete failed, but main delete succeeded
                    pass

        return successfully_deleted_ids


    @exception_handler
    async def delete_filter_data(self, filters: dict, session=None):
        """Delete multiple records based on filter conditions
        
        Args:
            filters: Dictionary of filter conditions
            session: Optional AsyncSession for transaction management
        """
        if not filters:
            return []

        try:
            # Use delete_filter from SQL operate - now returns list of deleted IDs
            deleted_ids = await self.delete_filter(self.table_name, filters, session=session)

            if deleted_ids:
                # Delete specific cache entries for deleted records
                try:
                    # Delete cache entries for specific IDs
                    cache_keys = {str(record_id) for record_id in deleted_ids}
                    await self.delete_cache(self.table_name, cache_keys)

                    # Delete null markers for deleted IDs
                    for record_id in deleted_ids:
                        null_key = f"{self.table_name}:{record_id}:null"
                        try:
                            await self.delete_null_key(null_key)
                        except Exception:
                            pass

                except Exception:
                    # Cache delete failed, but SQL delete succeeded
                    pass

            return deleted_ids

        except Exception:
            return []
