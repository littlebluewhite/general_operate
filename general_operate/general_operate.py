from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar
from contextlib import asynccontextmanager
import redis
import structlog
from sqlalchemy.exc import DBAPIError
from . import GeneralOperateException, ErrorCode, ErrorContext
from .app.cache_operate import CacheOperate
from .app.client.influxdb import InfluxDB
from .app.influxdb_operate import InfluxOperate
from .app.sql_operate import SQLOperate
from .core import handle_errors

T = TypeVar('T')


class GeneralOperate(CacheOperate, SQLOperate, InfluxOperate, Generic[T], ABC):
    """
    Base class for all database operators
    Provides unified interface for SQL, cache, time-series data, and Kafka event operations
    
    Now supports four data operation layers:
    1. SQL: Primary data storage with ACID transactions
    2. Cache (Redis): High-performance data caching
    3. InfluxDB: Time-series data storage  
    4. Kafka: Event-driven messaging and streaming (new)
    """

    def __init__(
        self,
        database_client = None,
        redis_client: redis.asyncio.Redis = None,
        influxdb: InfluxDB = None,
        kafka_config: dict[str, Any] | None = None,
    ):
        # Get module from subclass
        module = self.get_module()

        self.table_name = None
        self.main_schemas = None
        self.create_schemas = None
        self.update_schemas = None
        if module is not None:
            self.module = module
            self.table_name = module.table_name
            self.main_schemas = module.main_schemas
            self.create_schemas = module.create_schemas
            self.update_schemas = module.update_schemas
        self.__exc = GeneralOperateException

        # Initialize parent classes
        if redis_client is not None:
            CacheOperate.__init__(self, redis_client)
        if database_client is not None:
            SQLOperate.__init__(self, database_client)
        if influxdb is not None:
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

    def _build_cache_key(self, record_id: Any) -> str:
        """Build a cache key for a given record ID.
        
        Args:
            record_id: The record ID
            
        Returns:
            Formatted cache key string
        """
        # Delegate to CacheOperate
        if hasattr(self, 'redis') and self.redis:
            return CacheOperate.build_cache_key(self, self.table_name, record_id)
        return f"{self.table_name}:{record_id}"
    
    def _build_null_marker_key(self, record_id: Any) -> str:
        """Build a null marker cache key for a given record ID.
        
        Args:
            record_id: The record ID
            
        Returns:
            Formatted null marker key string
        """
        # Delegate to CacheOperate
        if hasattr(self, 'redis') and self.redis:
            return CacheOperate.build_null_marker_key(self, self.table_name, record_id)
        return f"{self.table_name}:{record_id}:null"
    
    def _validate_with_schema(self, data: dict, schema_type: str = "main") -> Any:
        """Validate data against specified schema type.
        
        Args:
            data: Dictionary to validate
            schema_type: Type of schema ("main", "create", "update")
            
        Returns:
            Validated schema object
            
        Raises:
            Exception: If validation fails
        """
        schema_map = {
            "main": self.main_schemas,
            "create": self.create_schemas,
            "update": self.update_schemas
        }
        schema = schema_map.get(schema_type, self.main_schemas)
        return schema(**data)
    
    @staticmethod
    def _process_in_batches(items: list, batch_size: int = 100):
        """Generator to process items in batches.
        
        Args:
            items: List of items to process
            batch_size: Size of each batch (default: 100)
            
        Yields:
            Batches of items
        """
        # Delegate to CacheOperate's static method
        return CacheOperate.process_in_batches(items, batch_size)

    @abstractmethod
    def get_module(self):
        """
        Subclasses must implement this to return their schema module
        
        Example:
            from shared.schemas.notification import module as notification_module
            return notification_module
        """
        pass

    @handle_errors(operation="health_check")
    async def health_check(self):
        """Check health of SQL and cache connections"""
        # Check SQL health
        sql_health = await SQLOperate.health_check(self)

        # Check cache health
        cache_health = await CacheOperate.health_check(self)

        return sql_health and cache_health

    @handle_errors(operation="cache_warming")
    async def cache_warming(self, limit: int = 1000, offset: int = 0):
        """Warm up cache by loading data from SQL in batches"""
        # Delegate to CacheOperate with SQL fetch function
        if hasattr(self, 'redis') and self.redis:
            return await CacheOperate.cache_warming(
                self,
                table_name=self.table_name,
                fetch_function=self.read_sql,
                limit=limit,
                offset=offset
            )
        
        # Fallback implementation if no cache
        return {
            "success": False,
            "records_loaded": 0,
            "message": "Cache not available",
        }

    @handle_errors(operation="cache_clear")
    async def cache_clear(self):
        """Clear all cache data for this table"""
        # Delegate to CacheOperate
        if hasattr(self, 'redis') and self.redis:
            return await CacheOperate.cache_clear(self, self.table_name)
        
        return {"success": False, "message": "Cache not available"}

    async def _process_cache_lookups(self, id_value: set) -> tuple[list, set, set, list]:
        """Process cache lookups for given IDs.
        
        Returns:
            Tuple of (results, cache_miss_ids, null_marked_ids, failed_cache_ops)
        """
        if not hasattr(self, 'redis') or not self.redis:
            # No cache available, all are cache misses
            return [], id_value, set(), []
            
        # Get raw cache results from CacheOperate
        cache_results, cache_miss_ids, null_marked_ids, failed_cache_ops = await CacheOperate.process_cache_lookups(
            self,
            table_name=self.table_name,
            id_values=id_value
        )
        
        # Apply schema validation to cached data
        results = []
        for cache_item in cache_results:
            try:
                schema_data = self.main_schemas(**cache_item)
                results.append(schema_data)
            except Exception as schema_err:
                self.logger.warning(f"Schema validation failed for cached item: {schema_err}")
                # Add to cache miss to refetch from DB
                if 'id' in cache_item:
                    cache_miss_ids.add(cache_item['id'])
                
        return results, cache_miss_ids, null_marked_ids, failed_cache_ops
    
    async def _handle_cache_misses(self, cache_miss_ids: set, failed_cache_ops: list) -> tuple[list, set]:
        """Handle cache misses by fetching from SQL and processing results.
        
        Returns:
            Tuple of (schema_results, found_ids)
        """
        if not cache_miss_ids:
            return [], set()
        
        # Use CacheOperate's handler with schema validator
        if hasattr(self, 'redis') and self.redis:
            def schema_validator(_sql_row):
                return self.main_schemas(**_sql_row)
            
            return await CacheOperate.handle_cache_misses(
                self,
                table_name=self.table_name,
                cache_miss_ids=cache_miss_ids,
                failed_cache_ops=failed_cache_ops,
                fetch_function=self._fetch_from_sql,
                schema_validator=schema_validator
            )
        
        # Fallback: just fetch from SQL without caching
        sql_results = await self._fetch_from_sql(cache_miss_ids)
        schema_results = []
        found_ids = set()
        
        for sql_row in sql_results:
            if sql_row:
                try:
                    schema_data = self.main_schemas(**sql_row)
                    schema_results.append(schema_data)
                    found_ids.add(sql_row["id"])
                except Exception as schema_err:
                    self.logger.error(f"Schema validation failed: {schema_err}")
        
        return schema_results, found_ids
    
    async def _update_cache_after_fetch(self, cache_data: dict) -> None:
        """Update cache with data fetched from SQL.
        
        Args:
            cache_data: Dictionary mapping cache keys to data
        """
        if hasattr(self, 'redis') and self.redis:
            await CacheOperate.update_cache_after_fetch(
                self,
                table_name=self.table_name,
                cache_data=cache_data
            )
    
    async def _mark_missing_records(self, missing_ids: set, failed_cache_ops: list) -> None:
        """Mark records that don't exist in database with null markers.
        
        Args:
            missing_ids: Set of IDs that don't exist in database
            failed_cache_ops: List of IDs where cache operations failed
        """
        if hasattr(self, 'redis') and self.redis:
            await CacheOperate.mark_missing_records(
                self,
                table_name=self.table_name,
                missing_ids=missing_ids,
                failed_cache_ops=failed_cache_ops
            )

    @handle_errors(operation="read_data_by_id")
    async def read_data_by_id(self, id_value: set) -> list[T]:
        """Read data with cache-first strategy and null value protection.
        
        This method implements a multi-tier data retrieval strategy:
        1. Check cache for existing data and null markers
        2. For cache misses, fetch from SQL database
        3. Update cache with fetched data
        4. Mark non-existent records to avoid repeated lookups
        """
        if not id_value:
            return []

        operation_context = f"read_data_by_id(table={self.table_name}, ids={len(id_value)})"
        self.logger.debug(f"Starting {operation_context}")

        try:
            # Step 1: Process cache lookups
            results, cache_miss_ids, null_marked_ids, failed_cache_ops = await self._process_cache_lookups(id_value)
            
            # Step 2: Handle cache misses with SQL fallback
            if cache_miss_ids:
                try:
                    sql_results, found_ids = await self._handle_cache_misses(cache_miss_ids, failed_cache_ops)
                    results.extend(sql_results)
                    
                    # Step 3: Mark missing records with null markers
                    missing_ids = cache_miss_ids - found_ids
                    await self._mark_missing_records(missing_ids, failed_cache_ops)
                    
                except Exception as sql_err:
                    self.logger.error(f"SQL fallback failed in {operation_context}: {sql_err}")
                    raise self.__exc(
                        code=ErrorCode.UNKNOWN_ERROR,
                        message=f"Both cache and SQL operations failed: {str(sql_err)}",
                        context=ErrorContext(operation="read_data_by_id", resource=self.table_name, details={"fallback_failed": True}),
                        cause=sql_err
                    )

            self.logger.debug(f"Completed {operation_context}, returned {len(results)} records")
            return results

        except self.__exc:
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error in {operation_context}: {str(e)}")
            raise self.__exc(
                code=ErrorCode.UNKNOWN_ERROR,
                message=f"Unexpected error during data read: {str(e)}",
                context=ErrorContext(operation="read_data_by_id", resource=self.table_name),
                cause=e
            )

    @handle_errors(operation="read_data_by_filter")
    async def read_data_by_filter(
        self, 
        filters: dict[str, Any], 
        limit: int | None = None, 
        offset: int = 0,
        order_by: str | None = None,
        order_direction: str = "ASC",
        date_field: str | None = None,
        start_date: Any = None,
        end_date: Any = None,
        session: Any = None
    ) -> list[T]:
        """Read data by filter conditions with optional pagination, ordering and date range filtering
        
        Args:
            filters: Filter conditions
            limit: Maximum number of records to return
            offset: Number of records to skip
            order_by: Column to order by
            order_direction: Order direction (ASC or DESC)
            date_field: Optional date field for range filtering
            start_date: Optional start date for range filtering
            end_date: Optional end date for range filtering
            session: Optional external AsyncSession
            
        Returns:
            List of records as model instances
        """
        try:
            # Use SQL read method with filters
            sql_results = await self.read_sql(
                table_name=self.table_name,
                filters=filters,
                limit=limit,
                offset=offset,
                order_by=order_by,
                order_direction=order_direction,
                date_field=date_field,
                start_date=start_date,
                end_date=end_date,
                session=session
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
                code=ErrorCode.DB_QUERY_ERROR,
                message=f"Database read operation failed: {str(e)}",
                context=ErrorContext(operation="fetch_from_sql", resource=self.table_name),
                cause=e
            )

    @handle_errors(operation="create_data")
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

    @handle_errors(operation="create_by_foreign_key")
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

    async def _validate_update_data(self, data: list[dict[str, Any]], where_field: str) -> tuple[list, list, list]:
        """Validate update data and prepare for bulk operations.
        
        Returns:
            Tuple of (update_list, cache_keys_to_delete, validation_errors)
        """
        update_list = []
        cache_keys_to_delete = []
        validation_errors = []

        for idx, update_item in enumerate(data):
            # Check for required where_field
            if where_field not in update_item:
                validation_errors.append(f"Item {idx}: Missing '{where_field}' field")
                continue

            where_value = update_item[where_field]

            try:
                # Validate against update schema
                update_schema_item = self.update_schemas(**update_item)
                validated_update_data = update_schema_item.model_dump(exclude_unset=True)

                # Remove where_field from update data to avoid updating it
                validated_update_data = {k: v for k, v in validated_update_data.items() if k != where_field}

                # Skip items with no valid update fields
                if not validated_update_data:
                    self.logger.warning(f"Item {idx}: No valid update fields after validation")
                    continue

                # Prepare for bulk update
                update_list.append({where_field: where_value, "data": validated_update_data})

                # Track cache keys for ID-based updates only
                if where_field == "id":
                    cache_keys_to_delete.append(str(where_value))

            except Exception as e:
                validation_errors.append(f"Item {idx}: {str(e)}")
                
        return update_list, cache_keys_to_delete, validation_errors
    
    async def _clear_update_caches(self, cache_keys: list, operation_context: str) -> list:
        """Clear cache entries before/after update operations.
        
        Returns:
            List of cache deletion errors (for logging)
        """
        if hasattr(self, 'redis') and self.redis:
            return await CacheOperate.clear_update_caches(
                self,
                table_name=self.table_name,
                cache_keys=cache_keys,
                operation_context=operation_context
            )
        return []
    
    async def _convert_update_results(self, updated_records: list) -> tuple[list, list]:
        """Convert updated records to schema objects.
        
        Returns:
            Tuple of (results, schema_errors)
        """
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
                    
        return results, schema_errors

    @handle_errors(operation="update_data")
    async def update_data(self, data: list[dict[str, Any]], where_field: str = "id", session=None) -> list[T]:
        """Update data with bulk operations and enhanced cache consistency.
        
        This method performs bulk updates with the following strategy:
        1. Validate all update data against schemas
        2. Clear cache entries before update (for consistency)
        3. Execute bulk SQL update
        4. Clear cache entries after update (ensure consistency)
        5. Return updated records as schema objects

        Args:
            data: List of dictionaries containing update data, each must include the where_field
            where_field: Field name to use in WHERE clause (default: "id")
            session: Optional AsyncSession for transaction management
        """
        if not data:
            return []

        operation_context = f"update_data(table={self.table_name}, records={len(data)}, field={where_field})"
        self.logger.debug(f"Starting {operation_context}")

        # Step 1: Validate and prepare update data
        update_list, cache_keys_to_delete, validation_errors = await self._validate_update_data(data, where_field)
        
        # Handle validation errors
        if validation_errors:
            error_msg = "Validation errors: " + "; ".join(validation_errors)
            self.logger.error(f"{operation_context} - {error_msg}")
            raise self.__exc(
                code=ErrorCode.VALIDATION_ERROR,
                message=error_msg,
                context=ErrorContext(operation="update_data", resource=self.table_name)
            )

        if not update_list:
            self.logger.warning(f"{operation_context} - No valid update data after validation")
            return []

        # Step 2: Pre-update cache cleanup (best effort)
        cache_delete_errors = await self._clear_update_caches(cache_keys_to_delete, operation_context)

        # Step 3: Execute bulk SQL update
        try:
            updated_records = await self.update_sql(self.table_name, update_list, where_field, session=session)
        except Exception as sql_err:
            self.logger.error(f"SQL update failed in {operation_context}: {sql_err}")
            raise

        # Step 4: Convert results to schema objects
        results, schema_errors = await self._convert_update_results(updated_records)

        # Step 5: Post-update cache cleanup (ensure consistency)
        post_cache_errors = await self._clear_update_caches(cache_keys_to_delete, operation_context)
        cache_delete_errors.extend(post_cache_errors)

        # Log cache operation issues (non-fatal)
        if cache_delete_errors:
            self.logger.warning(f"Cache delete errors in {operation_context}: {'; '.join(cache_delete_errors)}")

        # Validate update completeness
        if len(results) != len(update_list):
            missing_count = len(update_list) - len(results)
            error_msg = f"Update incomplete: {missing_count} of {len(update_list)} records failed to update"
            if schema_errors:
                error_msg += f". Schema errors: {'; '.join(schema_errors)}"

            self.logger.error(f"{operation_context} - {error_msg}")
            raise self.__exc(
                code=ErrorCode.VALIDATION_ERROR,
                message=error_msg,
                context=ErrorContext(operation="update_data", resource=self.table_name)
            )

        self.logger.debug(f"Completed {operation_context}, updated {len(results)} records")
        return results

    @handle_errors(operation="update_by_foreign_key")
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


    @handle_errors(operation="delete_data")
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
                # Skip None values explicitly
                if id_key is None:
                    self.logger.debug(f"Skipping None ID in delete_data")
                    continue
                    
                # Convert id to appropriate type - more strict validation
                if str(id_key).isdigit():
                    record_id = int(id_key)
                elif isinstance(id_key, (str, int, float)):
                    record_id = id_key
                else:
                    # Skip objects that aren't basic types
                    self.logger.debug(f"Skipping non-basic type ID in delete_data: {id_key} - {type(id_key).__name__}")
                    continue
                    
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
                await CacheOperate.delete_caches(self, self.table_name, cache_keys)
            except Exception as e:
                # Cache delete failed, but SQL delete succeeded - log but don't fail
                self.logger.warning(f"Cache cleanup failed in delete_data: {type(e).__name__}: {str(e)}")
                pass

            # 3. Delete null markers if they exist
            for record_id in successfully_deleted_ids:
                try:
                    null_key = f"{self.table_name}:{record_id}:null"
                    await self.delete_null_key(null_key)
                except Exception as e:
                    # Null marker delete failed, but main delete succeeded - non-critical
                    self.logger.debug(f"Failed to delete null marker for {record_id}: {type(e).__name__}")
                    pass

        return successfully_deleted_ids


    @handle_errors(operation="delete_filter_data")
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
                    await self.delete_caches(self.table_name, cache_keys)

                    # Delete null markers for deleted IDs
                    for record_id in deleted_ids:
                        null_key = f"{self.table_name}:{record_id}:null"
                        try:
                            await self.delete_null_key(null_key)
                        except Exception as e:
                            # Non-critical error - log but continue
                            self.logger.debug(f"Failed to delete null marker in delete_filter_data: {type(e).__name__}")
                            pass

                except Exception as e:
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

    @handle_errors(operation="count_data")
    async def count_data(
        self,
        filters: dict[str, Any] | None = None,
        date_field: str | None = None,
        start_date: Any = None,
        end_date: Any = None,
        session=None
    ) -> int:
        """Count records in the table with optional filters and date range
        
        Args:
            filters: Optional dictionary of filter conditions
            date_field: Optional date field for range filtering
            start_date: Optional start date for range filtering (inclusive)
            end_date: Optional end date for range filtering (inclusive)
            session: Optional AsyncSession for transaction management
            
        Returns:
            int: The count of records matching the filters
        """
        operation_context = f"count_data(table={self.table_name}, filters={filters})"
        self.logger.debug(f"Starting {operation_context}")
        
        try:
            # Use count_sql from SQLOperate
            count = await self.count_sql(
                table_name=self.table_name,
                filters=filters,
                date_field=date_field,
                start_date=start_date,
                end_date=end_date,
                session=session
            )
            
            self.logger.debug(f"Completed {operation_context}, count={count}")
            return count
            
        except self.__exc:
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error in {operation_context}: {str(e)}")
            raise self.__exc(
                code=ErrorCode.UNKNOWN_ERROR,
                message=f"Unexpected error during count operation: {str(e)}",
                context=ErrorContext(operation="count_data", resource=self.table_name),
                cause=e
            )

    async def store_cache_data(
            self, prefix: str, identifier: str, data: dict[str, Any], ttl_seconds: int | None = None
    ) -> None:
        """Store data to Redis cache - backward compatibility wrapper"""
        if hasattr(self, 'redis') and self.redis:
            await CacheOperate.store_cache(self, prefix, identifier, data, ttl_seconds)
        else:
            self.logger.warning("Cache not available for store_cache_data")

    async def get_cache_data(self, prefix: str, identifier: str) -> dict[str, Any] | None:
        """Get data from Redis cache - backward compatibility wrapper"""
        if hasattr(self, 'redis') and self.redis:
            return await CacheOperate.get_cache(self, prefix, identifier)
        return None

    async def delete_cache_data(self, prefix: str, identifier: str) -> bool:
        """Delete data from Redis cache - backward compatibility wrapper"""
        if hasattr(self, 'redis') and self.redis:
            return await CacheOperate.delete_cache(self, prefix, identifier)
        return False

    @handle_errors(operation="upsert_data")
    async def upsert_data(
        self,
        data: list[dict],
        conflict_fields: list[str],
        update_fields: list[str] = None,
        session=None
    ) -> list[T]:
        """Insert or update data with schema validation and cache management
        
        Args:
            data: List of dictionaries containing data to upsert
            conflict_fields: Fields that determine uniqueness (e.g., ["id"] or ["user_id", "item_id"])
            update_fields: Fields to update on conflict (if None, updates all fields except conflict_fields)
            session: Optional AsyncSession for transaction management
            
        Returns:
            List of upserted records as schema objects
        """
        if not data:
            return []
        
        operation_context = f"upsert_data(table={self.table_name}, records={len(data)}, conflict_fields={conflict_fields})"
        self.logger.debug(f"Starting {operation_context}")
        
        # Validate input data
        validated_data = []
        cache_keys_to_delete = []
        
        for idx, item in enumerate(data):
            if not isinstance(item, dict):
                self.logger.warning(f"Item {idx}: Not a dictionary, skipping")
                continue
            
            try:
                # Validate with create schema (for new records) or update schema (for updates)
                # Try create schema first for full validation
                try:
                    validated_item = self.create_schemas(**item)
                    validated_dict = validated_item.model_dump()
                except:
                    # Fall back to update schema for partial updates
                    validated_item = self.update_schemas(**item)
                    validated_dict = validated_item.model_dump(exclude_unset=True)
                
                validated_data.append(validated_dict)
                
                # Prepare cache keys for deletion if ID is in conflict fields
                if "id" in conflict_fields and "id" in validated_dict:
                    cache_keys_to_delete.append(str(validated_dict["id"]))
                    
            except Exception as e:
                self.logger.warning(f"Schema validation failed for item {idx}: {str(e)}")
                continue
        
        if not validated_data:
            self.logger.warning(f"{operation_context} - No valid data after validation")
            return []
        
        # Clear cache for affected records (best effort)
        if cache_keys_to_delete:
            try:
                await self.delete_caches(self.table_name, set(cache_keys_to_delete))
                
                # Also delete null markers
                for record_id in cache_keys_to_delete:
                    null_key = f"{self.table_name}:{record_id}:null"
                    try:
                        await self.delete_null_key(null_key)
                    except:
                        pass
            except Exception as cache_err:
                self.logger.warning(f"Pre-upsert cache cleanup failed: {cache_err}")
        
        # Perform upsert operation
        try:
            upserted_records = await self.upsert_sql(
                self.table_name,
                validated_data,
                conflict_fields,
                update_fields,
                session
            )
        except Exception as sql_err:
            self.logger.error(f"SQL upsert failed in {operation_context}: {sql_err}")
            raise
        
        # Convert to main schemas
        results = []
        for record in upserted_records:
            if record:
                try:
                    schema_data = self.main_schemas(**record)
                    results.append(schema_data)
                except Exception as schema_err:
                    self.logger.error(f"Schema conversion failed for upserted record: {schema_err}")
        
        # Clear cache again to ensure consistency
        if cache_keys_to_delete:
            try:
                await self.delete_caches(self.table_name, set(cache_keys_to_delete))
            except:
                pass
        
        self.logger.debug(f"Completed {operation_context}, upserted {len(results)} records")
        return results

    @handle_errors(operation="exists_check")
    async def exists_check(self, id_value: Any, session=None) -> bool:
        """Quick check if a single record exists
        
        Args:
            id_value: The ID value to check
            session: Optional AsyncSession
            
        Returns:
            True if record exists, False otherwise
        """
        operation_context = f"exists_check(table={self.table_name}, id={id_value})"
        self.logger.debug(f"Starting {operation_context}")
        
        # Check cache first
        if self.redis:
            try:
                # Check for null marker using CacheOperate
                null_marker_status, non_null_ids = await CacheOperate.check_null_markers_batch(
                    self, self.table_name, [id_value]
                )
                
                if null_marker_status.get(id_value, False):
                    self.logger.debug(f"{operation_context} - Found null marker, returning False")
                    return False
                
                # Try to get from cache
                cached_data = await self.get_caches(self.table_name, {str(id_value)})
                if cached_data:
                    self.logger.debug(f"{operation_context} - Found in cache, returning True")
                    return True
            except Exception as cache_err:
                self.logger.warning(f"Cache check failed in {operation_context}: {cache_err}")
        
        # Check database
        try:
            result = await self.exists_sql(
                self.table_name,
                [id_value],
                session=session
            )
            exists = result.get(id_value, False)
            
            # Update cache based on result
            if not exists and self.redis:
                # Set null marker for non-existent record using CacheOperate
                try:
                    await CacheOperate.set_null_markers_batch(
                        self, self.table_name, [id_value], expiry_seconds=300
                    )
                except Exception as e:
                    self.logger.debug(f"Failed to set null marker for {id_value}: {e}")
            
            self.logger.debug(f"Completed {operation_context}, exists={exists}")
            return exists
            
        except Exception as e:
            self.logger.error(f"Database check failed in {operation_context}: {str(e)}")
            raise

    @handle_errors(operation="batch_exists")
    async def batch_exists(self, id_values: set, session=None) -> dict[Any, bool]:
        """Check existence of multiple records
        
        Args:
            id_values: Set of ID values to check
            session: Optional AsyncSession
            
        Returns:
            Dictionary mapping each ID to its existence status
        """
        if not id_values:
            return {}
        
        operation_context = f"batch_exists(table={self.table_name}, ids={len(id_values)})"
        self.logger.debug(f"Starting {operation_context}")
        
        # Initialize result
        result = {}
        unchecked_ids = set()
        
        # Check cache first using CacheOperate batch methods
        if self.redis:
            try:
                # Check null markers using CacheOperate batch method
                null_marker_status, non_null_ids = await CacheOperate.check_null_markers_batch(
                    self, self.table_name, list(id_values)
                )
                
                # Process null marker results
                for id_val, has_null_marker in null_marker_status.items():
                    if has_null_marker:
                        result[id_val] = False  # Null marker exists = record doesn't exist
                
                # Check cache for non-null IDs
                if non_null_ids:
                    cached_data = await self.get_caches(self.table_name, {str(id_val) for id_val in non_null_ids})
                    for id_val in non_null_ids:
                        if str(id_val) in cached_data:
                            result[id_val] = True
                        else:
                            unchecked_ids.add(id_val)
                            
            except Exception as cache_err:
                self.logger.warning(f"Cache batch check failed: {cache_err}")
                # Fallback: all IDs need database check
                unchecked_ids = set(id_values)
        else:
            # No Redis, check all in database
            unchecked_ids = set(id_values)
        
        # Check database for remaining IDs
        if unchecked_ids:
            try:
                db_result = await self.exists_sql(
                    self.table_name,
                    list(unchecked_ids),
                    session=session
                )
                
                # Merge results
                non_existent_ids = []
                for id_val in unchecked_ids:
                    exists = db_result.get(id_val, False)
                    result[id_val] = exists
                    if not exists:
                        non_existent_ids.append(id_val)
                
                # Batch set null markers for non-existent records
                if non_existent_ids and self.redis:
                    try:
                        await CacheOperate.set_null_markers_batch(
                            self, self.table_name, non_existent_ids, expiry_seconds=300
                        )
                    except Exception as e:
                        # Log but don't fail the operation for cache issues
                        self.logger.debug(f"Failed to set null markers batch: {e}")
                            
            except Exception as e:
                self.logger.error(f"Database check failed in {operation_context}: {str(e)}")
                # For unchecked IDs, default to False
                for id_val in unchecked_ids:
                    if id_val not in result:
                        result[id_val] = False
        
        self.logger.debug(f"Completed {operation_context}, checked {len(result)} IDs")
        return result

    @handle_errors(operation="refresh_cache")
    async def refresh_cache(self, id_values: set) -> dict:
        """Reload specific records from database to cache
        
        Args:
            id_values: Set of ID values to refresh in cache
            
        Returns:
            Dictionary with refresh statistics
        """
        if not id_values:
            return {"refreshed": 0, "not_found": 0, "errors": 0}
        
        # Delegate to CacheOperate
        if hasattr(self, 'redis') and self.redis:
            return await CacheOperate.refresh_cache(
                self,
                table_name=self.table_name,
                id_values=id_values,
                fetch_function=self.read_sql
            )
        
        # No cache available
        return {"refreshed": 0, "not_found": 0, "errors": len(id_values)}

    @handle_errors(operation="get_distinct_values")
    async def get_distinct_values(
        self,
        field: str,
        filters: dict[str, Any] = None,
        cache_ttl: int = 300,
        session=None
    ) -> list[Any]:
        """Get distinct values from a field with optional caching
        
        Args:
            field: The field name to get distinct values from
            filters: Optional filters to apply
            cache_ttl: Cache TTL in seconds (default: 300, set to 0 to disable caching)
            session: Optional AsyncSession
            
        Returns:
            List of distinct values from the specified field
        """
        # Use CacheOperate's cached version if cache is available
        if hasattr(self, 'redis') and self.redis and cache_ttl > 0:
            # Create a fetch function that wraps SQLOperate.get_distinct_values
            async def fetch_distinct(table_name, _field, _filters):
                return await SQLOperate.get_distinct_values(
                    self,
                    table_name,
                    _field,
                    _filters,
                    session=session
                )
            
            return await CacheOperate.get_distinct_values_cached(
                self,
                table_name=self.table_name,
                field=field,
                filters=filters,
                cache_ttl=cache_ttl,
                fetch_function=fetch_distinct
            )
        
        # Direct database query without caching
        return await SQLOperate.get_distinct_values(
            self,
            self.table_name,
            field,
            filters,
            session=session
        )
