import asyncio
import functools
import json
import re
from abc import ABC, abstractmethod
from datetime import datetime, UTC
from typing import Any, Generic, TypeVar
from contextlib import asynccontextmanager

import pymysql
import redis
import structlog
from redis import RedisError
from sqlalchemy.dialects.postgresql.asyncpg import AsyncAdapt_asyncpg_dbapi
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm.exc import UnmappedInstanceError

from . import GeneralOperateException
from .app.cache_operate import CacheOperate
from .app.client.influxdb import InfluxDB
from .app.influxdb_operate import InfluxOperate
from .app.sql_operate import SQLOperate

T = TypeVar('T')


class GeneralOperate(CacheOperate, SQLOperate, InfluxOperate, Generic[T], ABC):
    """
    Base class for all database operators
    Provides unified interface for SQL, cache, and time-series data operations
    """

    def __init__(
        self,
        database_client,
        redis_client: redis.asyncio.Redis,
        influxdb: InfluxDB = None,
        exc=GeneralOperateException,
    ):
        # Get module from subclass
        module = self.get_module()

        self.module = module
        self.table_name = module.table_name
        self.main_schemas = module.main_schemas
        self.create_schemas = module.create_schemas
        self.update_schemas = module.update_schemas
        self.__exc = exc

        # Initialize parent classes
        CacheOperate.__init__(self, redis_client, exc)
        SQLOperate.__init__(self, database_client, exc)
        InfluxOperate.__init__(self, influxdb)

        # Set up logging
        self.logger = structlog.get_logger().bind(
            operator=self.__class__.__name__,
            table=self.table_name
        )

    @asynccontextmanager
    async def transaction(self):
        """
        Transaction context manager for multi-table operations with ACID compliance.

        This ensures that tutorial and subtable operations are executed within the same
        database transaction, providing rollback capabilities if any operation fails.

        Usage:
            async with self.transaction() as session:
                # Perform operations with session parameter
                await self.tutorial_operate.create_data(data, session=session)
                await self.subtable_operate.create_data(subtable_data, session=session)
        """
        session = self.create_external_session()
        try:
            async with session.begin():
                yield session
        finally:
            await session.close()

    @abstractmethod
    def get_module(self):
        """
        Subclasses must implement this to return their schema module
        
        Example:
            from shared.schemas.notification import module as notification_module
            return notification_module
        """
        pass

    @staticmethod
    def exception_handler(func):
        """Exception handler decorator for GeneralOperate methods"""
        def handle_exceptions(self, e):
            """Common exception handling logic"""
            # redis error
            if isinstance(e, redis.ConnectionError):
                raise self.__exc(
                    status_code=487,
                    message=f"Redis connection error: {str(e)}",
                    message_code=3001,
                )
            elif isinstance(e, redis.TimeoutError):
                raise self.__exc(
                    status_code=487,
                    message=f"Redis timeout error: {str(e)}",
                    message_code=3002,
                )
            elif isinstance(e, redis.ResponseError):
                raise self.__exc(
                    status_code=487,
                    message=f"Redis response error: {str(e)}",
                    message_code=3003,
                )
            elif isinstance(e, RedisError):
                error_message = str(e)
                pattern = r"Error (\d+)"
                match = re.search(pattern, error_message)
                if match:
                    error_code = match.group(1)
                    raise self.__exc(
                        status_code=492,
                        message=f"Redis error: {error_message}",
                        message_code=int(error_code),
                    )
                else:
                    raise self.__exc(
                        status_code=487, message=error_message, message_code=3004
                    )
            # decode error
            elif isinstance(e, json.JSONDecodeError):
                raise self.__exc(
                    status_code=491,
                    message=f"JSON decode error: {str(e)}",
                    message_code=3005,
                )
            elif isinstance(e, DBAPIError):
                if isinstance(e.orig, AsyncAdapt_asyncpg_dbapi.Error):
                    pg_error = e.orig
                    message = str(pg_error).replace("\n", " ").replace("\r", " ").split(': ', 1)[1]
                    code = getattr(pg_error, "sqlstate", "1") or "1"
                    raise self.__exc(
                        status_code=489, message=message, message_code=code
                    )
                elif isinstance(e.orig, pymysql.Error):
                    code, msg = e.orig.args
                    raise self.__exc(status_code=486, message=msg, message_code=code)
                else:
                    # Generic database error handling
                    message = str(e).replace("\n", " ").replace("\r", " ")
                    raise self.__exc(
                        status_code=487, message=message, message_code="UNKNOWN"
                    )
            elif isinstance(e, UnmappedInstanceError):
                raise self.__exc(
                    status_code=486,
                    message="id: one or more of ids is not exist",
                    message_code=2,
                )
            elif isinstance(e, (ValueError, TypeError, AttributeError)) and "SQL" in str(e):
                # Data validation or SQL-related errors
                raise self.__exc(
                    status_code=400,
                    message=f"SQL operation validation error: {str(e)}",
                    message_code=299
                )
            elif isinstance(e, GeneralOperateException):
                # If it's already a GeneralOperateException, raise it directly
                raise e
            else:
                # For truly unexpected exceptions
                raise self.__exc(
                    status_code=500,
                    message=f"Operation error: {str(e)}",
                    message_code=9999,
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
    async def read_data_by_id(self, id_value: set) -> list[T]:
        """Read data with cache-first strategy and null value protection"""
        if not id_value:
            return []

        operation_context = f"read_data_by_id(table={self.table_name}, ids={len(id_value)})"
        self.logger.debug(f"Starting {operation_context}")

        try:
            results = []
            cache_miss_ids = set()
            null_marked_ids = set()
            failed_cache_ops = []

            # 1. Try to get data from cache
            for id_key in id_value:
                try:
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
                                except json.JSONDecodeError as json_err:
                                    self.logger.warning(f"Invalid JSON in cache for {self.table_name}:{id_key}: {json_err}")
                                    cache_miss_ids.add(id_key)
                                    continue

                            # Convert to main schema
                            try:
                                schema_data = self.main_schemas(**data_value)
                                results.append(schema_data)
                            except Exception as schema_err:
                                self.logger.warning(f"Schema validation failed for {self.table_name}:{id_key}: {schema_err}")
                                cache_miss_ids.add(id_key)
                    else:
                        cache_miss_ids.add(id_key)

                except (redis.RedisError, Exception) as cache_err:
                    self.logger.warning(f"Cache operation failed for {self.table_name}:{id_key}: {cache_err}")
                    failed_cache_ops.append(id_key)
                    cache_miss_ids.add(id_key)

            # 2. For cache misses, read from SQL
            if cache_miss_ids:
                try:
                    sql_results = await self._fetch_from_sql(cache_miss_ids)

                    # Process SQL results
                    found_ids = set()
                    cache_data_to_set = {}

                    for sql_row in sql_results:
                        if sql_row:
                            try:
                                # Convert to main schema
                                schema_data = self.main_schemas(**sql_row)
                                results.append(schema_data)

                                # Prepare for cache storage (only if cache is working)
                                if sql_row["id"] not in failed_cache_ops:
                                    found_ids.add(sql_row["id"])
                                    cache_data_to_set[str(sql_row["id"])] = json.dumps(sql_row, default=str)
                            except Exception as schema_err:
                                self.logger.error(f"Schema validation failed for SQL result {sql_row.get('id', 'unknown')}: {schema_err}")
                                continue

                    # 3. Cache the found data (with error resilience)
                    if cache_data_to_set:
                        try:
                            cache_success = await self.set_cache(self.table_name, cache_data_to_set)
                            if not cache_success:
                                self.logger.warning(f"Cache write failed for {len(cache_data_to_set)} items in {operation_context}")
                        except Exception as cache_err:
                            self.logger.warning(f"Cache write error in {operation_context}: {cache_err}")

                    # 4. Mark missing IDs with null values (5 minutes expiry)
                    missing_ids = cache_miss_ids - found_ids
                    for missing_id in missing_ids:
                        if missing_id not in failed_cache_ops:
                            try:
                                null_key = f"{self.table_name}:{missing_id}:null"
                                await self.set_null_key(null_key, 300)  # 5 minutes
                            except Exception as null_err:
                                self.logger.warning(f"Failed to set null marker for {missing_id}: {null_err}")

                except Exception as sql_err:
                    self.logger.error(f"SQL fallback failed in {operation_context}: {sql_err}")
                    raise self.__exc(
                        status_code=500,
                        message=f"Both cache and SQL operations failed: {str(sql_err)}",
                        message_code=9996
                    )

            self.logger.debug(f"Completed {operation_context}, returned {len(results)} records")
            return results

        except self.__exc:
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error in {operation_context}: {str(e)}")
            raise self.__exc(
                status_code=500,
                message=f"Unexpected error during data read: {str(e)}",
                message_code=9995
            )

    @exception_handler
    async def read_data_by_filter(self, filters: dict[str, Any], limit: int | None = None, offset: int = 0) -> list[T]:
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

    async def _fetch_from_sql(self, id_value: set) -> list[dict[str, Any]]:
        """Fetch data from SQL with optimized bulk operations"""
        if not id_value:
            return []

        try:
            if len(id_value) == 1:
                # Single ID, use read_one for better error reporting
                single_id = next(iter(id_value))
                single_result = await self.read_one(self.table_name, single_id)
                return [single_result] if single_result else []
            else:
                # Multiple IDs, use bulk read with filters
                sql_results = await self.read_sql(
                    table_name=self.table_name,
                    filters={"id": list(id_value)}
                )
                return sql_results if sql_results else []

        except self.__exc:
            raise
        except Exception as e:
            self.logger.error(f"SQL fetch failed for {len(id_value)} IDs in table {self.table_name}: {str(e)}")
            raise self.__exc(
                status_code=500,
                message=f"Database read operation failed: {str(e)}",
                message_code=9994
            )

    @exception_handler
    async def create_data(self, data: list[dict], session=None) -> list[T]:
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
            except (TypeError, ValueError) as e:
                # Schema validation failed - skip invalid items
                self.logger.warning(f"Schema validation failed for item in create_data: {type(e).__name__}: {str(e)}")
                continue
            except AttributeError as e:
                # Invalid attribute access - skip
                self.logger.warning(f"Invalid attribute in create_data item: {str(e)}")
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
    async def update_data(self, data: list[dict[str, Any]], where_field: str = "id", session=None) -> list[T]:
        """Update data with bulk operations and enhanced cache consistency

        Args:
            data: List of dictionaries containing update data, each must include the where_field
            where_field: Field name to use in WHERE clause (default: "id")
            session: Optional AsyncSession for transaction management
        """
        if not data:
            return []

        operation_context = f"update_data(table={self.table_name}, records={len(data)}, field={where_field})"
        self.logger.debug(f"Starting {operation_context}")

        # Prepare data for bulk update
        update_list = []
        cache_keys_to_delete = []
        validation_errors = []

        for idx, update_item in enumerate(data):
            if where_field not in update_item:
                validation_errors.append(f"Item {idx}: Missing '{where_field}' field")
                continue

            where_value = update_item[where_field]

            try:
                # Validate with update schema
                update_schema_item = self.update_schemas(**update_item)
                validated_update_data = update_schema_item.model_dump(exclude_unset=True)

                # Extract update fields (excluding the where_field)
                validated_update_data = {k: v for k, v in validated_update_data.items() if k != where_field}

                # Validate update data
                if not validated_update_data:
                    self.logger.warning(f"Item {idx}: No valid update fields after validation")
                    continue

                # Add to bulk update list in new format
                update_list.append({where_field: where_value, "data": validated_update_data})

                # Prepare cache keys for deletion (only for id-based updates)
                if where_field == "id":
                    cache_keys_to_delete.append(str(where_value))

            except Exception as e:
                validation_errors.append(f"Item {idx}: {str(e)}")

        # Report validation errors if any
        if validation_errors:
            error_msg = "Validation errors: " + "; ".join(validation_errors)
            self.logger.error(f"{operation_context} - {error_msg}")
            raise self.__exc(status_code=400, message=error_msg, message_code=215)

        if not update_list:
            self.logger.warning(f"{operation_context} - No valid update data after validation")
            return []

        cache_delete_errors = []

        # 1. Pre-update cache cleanup (best effort)
        if cache_keys_to_delete:
            try:
                deleted_count = await CacheOperate.delete_cache(self, self.table_name, set(cache_keys_to_delete))
                self.logger.debug(f"Pre-update cache cleanup: deleted {deleted_count} entries")

                # Also delete null markers if they exist (only for id-based updates)
                for record_id in cache_keys_to_delete:
                    null_key = f"{self.table_name}:{record_id}:null"
                    try:
                        await self.delete_null_key(null_key)
                    except Exception as null_err:
                        cache_delete_errors.append(f"null marker {record_id}: {null_err}")

            except Exception as cache_err:
                cache_delete_errors.append(f"pre-update cleanup: {cache_err}")
                self.logger.warning(f"Pre-update cache cleanup failed in {operation_context}: {cache_err}")

        # 2. Bulk update SQL data
        try:
            updated_records = await self.update_sql(self.table_name, update_list, where_field, session=session)
        except Exception as sql_err:
            self.logger.error(f"SQL update failed in {operation_context}: {sql_err}")
            raise

        # Convert to main schemas with error handling
        results = []
        schema_errors = []

        for idx, record in enumerate(updated_records):
            if record:
                try:
                    schema_data = self.main_schemas(**record)
                    results.append(schema_data)
                except Exception as schema_err:
                    schema_errors.append(f"Record {idx}: {schema_err}")
                    self.logger.error(f"Schema conversion failed for updated record {record.get('id', idx)}: {schema_err}")

        # 3. Post-update cache cleanup (ensure consistency)
        if cache_keys_to_delete:
            try:
                deleted_count = await CacheOperate.delete_cache(self, self.table_name, set(cache_keys_to_delete))
                self.logger.debug(f"Post-update cache cleanup: deleted {deleted_count} entries")
            except Exception as cache_err:
                cache_delete_errors.append(f"post-update cleanup: {cache_err}")
                self.logger.warning(f"Post-update cache cleanup failed in {operation_context}: {cache_err}")

        # Log cache operation issues (non-fatal)
        if cache_delete_errors:
            self.logger.warning(f"Cache delete errors in {operation_context}: {'; '.join(cache_delete_errors)}")

        # Validate that all records were updated successfully
        if len(results) != len(update_list):
            missing_count = len(update_list) - len(results)
            error_msg = f"Update incomplete: {missing_count} of {len(update_list)} records failed to update"
            if schema_errors:
                error_msg += f". Schema errors: {'; '.join(schema_errors)}"

            self.logger.error(f"{operation_context} - {error_msg}")
            raise self.__exc(status_code=400, message=error_msg, message_code=217)

        self.logger.debug(f"Completed {operation_context}, updated {len(results)} records")
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
            except (ValueError, TypeError) as e:
                # Skip invalid IDs - log for debugging
                self.logger.debug(f"Skipping invalid ID in delete_data: {id_key} - {type(e).__name__}")
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
            except (redis.RedisError, AttributeError) as e:
                # Cache delete failed, but SQL delete succeeded - log but don't fail
                self.logger.warning(f"Cache cleanup failed in delete_data: {type(e).__name__}: {str(e)}")
                pass

            # 3. Delete null markers if they exist
            for record_id in successfully_deleted_ids:
                try:
                    null_key = f"{self.table_name}:{record_id}:null"
                    await self.delete_null_key(null_key)
                except (redis.RedisError, AttributeError) as e:
                    # Null marker delete failed, but main delete succeeded - non-critical
                    self.logger.debug(f"Failed to delete null marker for {record_id}: {type(e).__name__}")
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
                        except (redis.RedisError, AttributeError) as e:
                            # Non-critical error - log but continue
                            self.logger.debug(f"Failed to delete null marker in delete_filter_data: {type(e).__name__}")
                            pass

                except (redis.RedisError, AttributeError) as e:
                    # Cache delete failed, but SQL delete succeeded - non-critical
                    self.logger.warning(f"Cache cleanup failed in delete_filter_data: {type(e).__name__}")
                    pass

            return deleted_ids

        except (DBAPIError, self.__exc) as e:
            # Database or known errors - re-raise
            raise e
        except (ValueError, TypeError, KeyError) as e:
            # Data validation errors
            self.logger.error(f"Filter validation error in delete_filter_data: {type(e).__name__}: {str(e)}")
            return []
        except Exception as e:
            # Generic exception handling for delete_filter_data - return empty list
            self.logger.error(f"Error in delete_filter_data: {type(e).__name__}: {str(e)}")
            return []

    async def store_cache_data(
            self, prefix: str, identifier: str, data: dict[str, Any], ttl_seconds: int | None = None
    ) -> None:
        """儲存資料到 Redis"""
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
        await self.redis.setex(key, ttl_seconds, serialized_data)

        self.logger.debug(
            "Data stored in Redis",
            key=key,
            prefix=prefix,
            ttl_seconds=ttl_seconds
        )

    async def get_cache_data(self, prefix: str, identifier: str) -> dict[str, Any] | None:
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

    async def delete_cache_data(self, prefix: str, identifier: str) -> bool:
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