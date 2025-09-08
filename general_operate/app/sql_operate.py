import asyncio
import functools
import json
import re
from typing import Any

import asyncpg
import pymysql
import structlog
from sqlalchemy import text
from sqlalchemy.dialects.postgresql.asyncpg import AsyncAdapt_asyncpg_dbapi
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.exc import UnmappedInstanceError

from ..app.client.database import SQLClient
from ..core.exceptions import DatabaseException, ErrorCode, ErrorContext


class SQLOperate:
    def __init__(self, client: SQLClient):
        self.__exc = DatabaseException
        self.__sqlClient = client
        self.null_set = {-999999, "null"}

        # SQL injection protection patterns
        self._valid_identifier_pattern = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

        # Detect database type for compatibility
        self._is_postgresql = client.engine_type.lower() == "postgresql"

        # Initialize logger
        self.logger = structlog.get_logger().bind(
            operator=self.__class__.__name__
        )


    @staticmethod
    def exception_handler(func):
        """Exception handler decorator for GeneralOperate methods"""
        def handle_exceptions(self, e):
            """Common exception handling logic"""
            # decode error
            if isinstance(e, json.JSONDecodeError):
                raise self.__exc(
                    code=ErrorCode.VALIDATION_ERROR,
                    message=f"JSON decode error: {str(e)}",
                    context=ErrorContext(operation="sql_operation", resource="database", details={"error_type": "json_decode"}),
                    cause=e
                )
            elif isinstance(e, DBAPIError):
                if isinstance(e.orig, AsyncAdapt_asyncpg_dbapi.Error):
                    pg_error = e.orig
                    message = str(pg_error).replace("\n", " ").replace("\r", " ").split(': ', 1)[1]
                    sqlstate = getattr(pg_error, "sqlstate", "1") or "1"
                    raise self.__exc(
                        code=ErrorCode.DB_QUERY_ERROR,
                        message=message,
                        context=ErrorContext(operation="sql_operation", resource="postgresql", details={"sqlstate": sqlstate}),
                        cause=e
                    )
                elif isinstance(e.orig, pymysql.Error):
                    mysql_code, msg = e.orig.args
                    raise self.__exc(
                        code=ErrorCode.DB_QUERY_ERROR,
                        message=msg,
                        context=ErrorContext(operation="sql_operation", resource="mysql", details={"mysql_error_code": mysql_code}),
                        cause=e
                    )
                else:
                    # Generic database error handling
                    message = str(e).replace("\n", " ").replace("\r", " ")
                    raise self.__exc(
                        code=ErrorCode.DB_QUERY_ERROR,
                        message=message,
                        context=ErrorContext(operation="sql_operation", resource="database", details={"error_type": "generic_db"}),
                        cause=e
                    )
            elif isinstance(e, UnmappedInstanceError):
                raise self.__exc(
                    code=ErrorCode.DB_QUERY_ERROR,
                    message="id: one or more of ids is not exist",
                    context=ErrorContext(operation="sql_operation", resource="database", details={"error_type": "unmapped_instance"}),
                    cause=e
                )
            elif isinstance(e, (ValueError, TypeError, AttributeError)) and "SQL" in str(e):
                # Data validation or SQL-related errors
                raise self.__exc(
                    code=ErrorCode.VALIDATION_ERROR,
                    message=f"SQL operation validation error: {str(e)}",
                    context=ErrorContext(operation="sql_operation", resource="database", details={"error_type": "sql_validation"}),
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
                    context=ErrorContext(operation="sql_operation", resource="database", details={"error_type": "unexpected"}),
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


    def _validate_identifier(
            self, identifier: str, identifier_type: str = "identifier"
    ) -> None:
        """Validate SQL identifiers (table names, column names) to prevent SQL injection"""
        if not identifier or not isinstance(identifier, str):
            raise self.__exc(
                code=ErrorCode.VALIDATION_ERROR,
                message=f"Invalid {identifier_type}: must be a non-empty string",
                context=ErrorContext(operation="validate_identifier", details={"identifier_type": identifier_type})
            )

        if len(identifier) > 64:  # Standard SQL identifier length limit
            raise self.__exc(
                code=ErrorCode.VALIDATION_ERROR,
                message=f"Invalid {identifier_type}: too long (max 64 characters)",
                context=ErrorContext(operation="validate_identifier", details={"identifier_type": identifier_type, "length": len(identifier)})
            )

        if not self._valid_identifier_pattern.match(identifier):
            raise self.__exc(
                code=ErrorCode.VALIDATION_ERROR,
                message=f"Invalid {identifier_type}: contains invalid characters",
                context=ErrorContext(operation="validate_identifier", details={"identifier_type": identifier_type, "identifier": identifier})
            )

    def _validate_data_value(self, value: Any, column_name: str) -> None:
        """Validate data values to prevent issues with special characters or types"""
        if value is None:
            return  # Allow null values

        # Check if value is in null_set, but only for hashable types
        try:
            if value in self.null_set:
                return  # Allow null values
        except TypeError:
            # Value is unhashable (like list, dict), which is fine - continue validation
            pass

        # For lists and dicts, they should be serialized to JSON for database storage
        # This validation just ensures they are valid Python objects that can be serialized
        if isinstance(value, (list, dict)):
            try:
                import json
                json.dumps(value)  # Test if it can be serialized
            except (TypeError, ValueError) as e:
                raise self.__exc(
                    code=ErrorCode.VALIDATION_ERROR,
                    message=f"Value for column '{column_name}' contains non-serializable data: {e}",
                    context=ErrorContext(operation="validate_data_value", details={"column_name": column_name}),
                    cause=e
                )

        # Check for extremely long strings that might cause issues
        if isinstance(value, str) and len(value) > 65535:  # TEXT field limit in MySQL
            raise self.__exc(
                code=ErrorCode.VALIDATION_ERROR,
                message=f"Value for column '{column_name}' is too long (max 65535 characters)",
                context=ErrorContext(operation="validate_data_value", details={"column_name": column_name, "length": len(value)})
            )

        # Check for potentially dangerous SQL keywords in string values
        if isinstance(value, str):
            dangerous_patterns = ["--", "/*", "*/", "xp_", "sp_"]
            sql_keywords = [
                "exec",
                "execute",
                "drop table",
                "delete from",
                "insert into",
                "update set",
                "select from",
                "union select",
                "alter table",
                "create table",
            ]
            lower_value = value.lower().strip()

            # 對於 template / description 類的大字串跳過關鍵字偵測
            exempt_columns = {"content_template", "subject_template", "description", "content"}
            if column_name not in exempt_columns:
                # 檢查危險字元 / 注釋模式
                for pattern in dangerous_patterns:
                    if pattern in lower_value:
                        raise self.__exc(
                            code=ErrorCode.VALIDATION_ERROR,
                            message=f"Value for column '{column_name}' contains potentially dangerous SQL characters",
                            context=ErrorContext(operation="validate_data_value", details={"column_name": column_name, "pattern": pattern})
                        )
                # 使用 word boundary 的正規表示式降低誤判
                for keyword in sql_keywords:
                    if re.search(rf"\b{re.escape(keyword)}\b", lower_value):
                        raise self.__exc(
                            code=ErrorCode.VALIDATION_ERROR,
                            message=f"Value for column '{column_name}' contains potentially dangerous SQL keyword '{keyword}'",
                            context=ErrorContext(operation="validate_data_value", details={"column_name": column_name, "keyword": keyword})
                        )

    def _validate_data_dict(
            self,
            data: dict[str, Any],
            operation: str = "operation",
            allow_empty: bool = False,
    ) -> dict[str, Any]:
        """Validate a data dictionary for database operations"""
        if not isinstance(data, dict):
            raise self.__exc(code=ErrorCode.VALIDATION_ERROR, message=f"Data for {operation} must be a dictionary", context=ErrorContext(operation="validation"))

        if not data and not allow_empty:
            raise self.__exc(code=ErrorCode.VALIDATION_ERROR, message=f"Data for {operation} cannot be empty", context=ErrorContext(operation="validation"))

        validated_data = {}
        for key, value in data.items():
            # Validate column name
            self._validate_identifier(key, "column name")
            # Validate data value
            self._validate_data_value(value, key)
            # Only include non-null values
            if value is None:
                continue
            try:
                if value not in self.null_set:
                    # Serialize lists and dicts to JSON for database storage
                    if isinstance(value, (list, dict)):
                        import json
                        validated_data[key] = json.dumps(value)
                    else:
                        validated_data[key] = value
            except TypeError:
                # Value is unhashable (list/dict), serialize it
                if isinstance(value, (list, dict)):
                    import json
                    validated_data[key] = json.dumps(value)
                else:
                    validated_data[key] = value

        if not validated_data and not allow_empty:
            raise self.__exc(code=ErrorCode.VALIDATION_ERROR, message=f"No valid data provided for {operation}", context=ErrorContext(operation="validation"))
        return validated_data

    def create_external_session(self) -> AsyncSession:
        """Create a new AsyncSession for external transaction management
        
        Returns:
            AsyncSession: A new session that can be used for transaction management
        """
        return AsyncSession(self.__sqlClient.get_engine())

    def _build_where_clause(
            self, filters: dict[str, Any] | None
    ) -> tuple[str, dict[str, Any]]:
        """Build WHERE clause and parameters from filters"""
        if not filters:
            return "", {}

        # Don't clean filters for list values - handle them separately
        where_clauses = []
        params = {}

        for key, value in filters.items():
            # Validate column name
            self._validate_identifier(key, "column name")

            if isinstance(value, list):
                # Handle list values with IN clause
                if not value:  # Empty list - skip this filter
                    continue
                # Database-specific handling for list parameters
                if self._is_postgresql:
                    # PostgreSQL has native array support with ANY() operator
                    # This is more efficient than expanding parameters
                    where_clauses.append(f"{key} = ANY(:{key})")
                    params[key] = value  # PostgreSQL handles lists natively
                else:
                    # MySQL doesn't have array support, so we expand parameters
                    # This creates individual parameters for each list item
                    placeholders = [f":{key}_{i}" for i in range(len(value))]
                    where_clauses.append(f"{key} IN ({', '.join(placeholders)})")
                    for i, v in enumerate(value):
                        params[f"{key}_{i}"] = v
            else:
                # Handle single values
                # Validate the single value
                self._validate_data_value(value, key)
                if value is None:
                    continue
                try:
                    if value not in self.null_set:
                        where_clauses.append(f"{key} = :{key}")
                        params[key] = value
                except TypeError:
                    # Value is unhashable, add it anyway
                    where_clauses.append(f"{key} = :{key}")
                    params[key] = value

        if not where_clauses:
            return "", {}

        where_clause = " WHERE " + " AND ".join(where_clauses)
        return where_clause, params

    def _get_client(self) -> SQLClient:
        return self.__sqlClient

    def _validate_create_data(self, data: dict[str, Any] | list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Validate and prepare data for insertion.
        
        Args:
            data: Single record or list of records to validate
            
        Returns:
            List of validated and cleaned data dictionaries
        """
        # Normalize input to list format
        if isinstance(data, dict):
            data_list = [data]
        elif isinstance(data, list):
            data_list = data
        else:
            raise self.__exc(
                code=ErrorCode.VALIDATION_ERROR,
                message="Data must be a dictionary or list of dictionaries",
                context=ErrorContext(operation="validation")
            )

        if not data_list:
            return []

        # Validate each item in the list
        cleaned_data_list = []
        for idx, item in enumerate(data_list):
            if not isinstance(item, dict):
                raise self.__exc(
                    code=ErrorCode.VALIDATION_ERROR,
                    message=f"Item at index {idx} must be a dictionary",
                    context=ErrorContext(operation="validation")
                )

            try:
                cleaned_data = self._validate_data_dict(
                    item, f"create operation (item {idx})"
                )
                cleaned_data_list.append(cleaned_data)
            except Exception as e:
                # Re-raise with more context
                raise self.__exc(
                    code=ErrorCode.VALIDATION_ERROR,
                    message=f"Invalid data at index {idx}: {str(e)}",
                    context=ErrorContext(operation="validation")
                )

        return cleaned_data_list
    
    def _build_insert_query(self, table_name: str, cleaned_data_list: list[dict]) -> tuple[str, dict]:
        """Build INSERT query with parameters.
        
        Args:
            table_name: Name of the table
            cleaned_data_list: List of validated data dictionaries
            
        Returns:
            Tuple of (query_string, parameters_dict)
        """
        if not cleaned_data_list:
            return "", {}
            
        # Extract columns from first item (all should have same structure)
        columns = list(cleaned_data_list[0].keys())
        columns_str = ", ".join(columns)

        # Build VALUES clause with numbered parameters
        values_clauses = []
        params = {}
        
        for i, data_item in enumerate(cleaned_data_list):
            value_placeholders = []
            for col in columns:
                param_name = f"{col}_{i}"
                value_placeholders.append(f":{param_name}")
                params[param_name] = data_item.get(col)
            values_clauses.append(f"({', '.join(value_placeholders)})")

        # Build final query based on database type
        if self._is_postgresql:
            # PostgreSQL supports RETURNING clause for getting inserted records
            query = f"INSERT INTO {table_name} ({columns_str}) VALUES {', '.join(values_clauses)} RETURNING *"
        else:
            # MySQL doesn't support RETURNING
            query = f"INSERT INTO {table_name} ({columns_str}) VALUES {', '.join(values_clauses)}"
            
        return query, params
    
    async def _execute_postgresql_insert(
        self, table_name: str, cleaned_data_list: list[dict], session: AsyncSession
    ) -> list[dict[str, Any]]:
        """Execute INSERT for PostgreSQL with RETURNING clause.
        
        Args:
            table_name: Name of the table
            cleaned_data_list: List of validated data
            session: Database session
            
        Returns:
            List of inserted records
        """
        query, params = self._build_insert_query(table_name, cleaned_data_list)
        result = await session.execute(text(query), params)
        rows = result.fetchall()
        return [dict(row._mapping) for row in rows]
    
    async def _execute_mysql_insert(
        self, table_name: str, cleaned_data_list: list[dict], session: AsyncSession
    ) -> list[dict[str, Any]]:
        """Execute INSERT for MySQL and fetch inserted records.
        
        Args:
            table_name: Name of the table
            cleaned_data_list: List of validated data
            session: Database session
            
        Returns:
            List of inserted records
        """
        query, params = self._build_insert_query(table_name, cleaned_data_list)
        result = await session.execute(text(query), params)
        
        # Get the first inserted ID and row count
        first_id = result.lastrowid
        row_count = result.rowcount
        
        inserted_records = []
        
        # Fetch inserted records if we have auto-increment IDs
        if first_id and row_count > 0:
            # Assuming sequential auto-increment IDs
            id_list = list(range(first_id, first_id + row_count))
            
            # Build SELECT query to fetch inserted records
            placeholders = [f":id_{i}" for i in range(len(id_list))]
            select_query = f"SELECT * FROM {table_name} WHERE id IN ({', '.join(placeholders)})"
            select_params = {f"id_{i}": id_val for i, id_val in enumerate(id_list)}
            
            select_result = await session.execute(text(select_query), select_params)
            rows = select_result.fetchall()
            inserted_records = [dict(row._mapping) for row in rows]
            
        return inserted_records

    async def create_sql(
            self, table_name: str, data: dict[str, Any] | list[dict[str, Any]], session: AsyncSession = None
    ) -> list[dict[str, Any]]:
        """Create multiple records in a single transaction for better performance.
        
        This method handles both single and bulk inserts with database-specific
        optimizations for PostgreSQL and MySQL.

        Args:
            table_name: Name of the table to insert into
            data: Single record (dict) or list of records to insert
            session: Optional external AsyncSession for transaction management

        Returns:
            List of created records
        """
        # Validate table name to prevent SQL injection
        self._validate_identifier(table_name, "table name")
        
        # Validate and clean input data
        cleaned_data_list = self._validate_create_data(data)
        if not cleaned_data_list:
            return []

        # Helper function to execute the actual SQL operations
        async def _execute_insert(active_session: AsyncSession) -> list[dict[str, Any]]:
            # Branch based on database type for optimal performance
            # Each database has different capabilities and syntax
            if self._is_postgresql:
                # PostgreSQL supports RETURNING clause for efficient retrieval
                # This allows us to get inserted records in a single query
                return await self._execute_postgresql_insert(
                    table_name, cleaned_data_list, active_session
                )
            else:
                # MySQL doesn't support RETURNING, so we need a follow-up SELECT
                # This requires tracking inserted IDs and fetching them separately
                return await self._execute_mysql_insert(
                    table_name, cleaned_data_list, active_session
                )

        # Use external session if provided, otherwise create and manage our own
        if session:
            # Use provided session without commit/rollback
            return await _execute_insert(session)
        else:
            # Traditional behavior with auto-managed session
            async with self.create_external_session() as auto_session:
                try:
                    result = await _execute_insert(auto_session)
                    await auto_session.commit()
                    return result
                except Exception as e:
                    await auto_session.rollback()
                    raise e

    async def read_sql(
            self,
            table_name: str,
            filters: dict[str, Any] | None = None,
            order_by: str | None = None,
            order_direction: str = "ASC",
            limit: int | None = None,
            offset: int = 0,
            date_field: str | None = None,
            start_date: Any = None,
            end_date: Any = None,
            session: AsyncSession = None,
    ) -> list[dict[str, Any]]:
        """Read data with optional date range filtering
        
        Args:
            table_name: Name of the table to read from
            filters: Optional filters to apply
            order_by: Column to order by
            order_direction: Order direction (ASC or DESC)
            limit: Maximum number of records to return
            offset: Number of records to skip
            date_field: Optional date field for range filtering
            start_date: Optional start date for range filtering
            end_date: Optional end date for range filtering
            session: Optional external AsyncSession
            
        Returns:
            List of records as dictionaries
        """
        self._validate_identifier(table_name, "table name")

        # Validate date_field if provided
        if date_field:
            self._validate_identifier(date_field, "date field")

        # Validate order_by column if provided
        if order_by:
            self._validate_identifier(order_by, "order_by column name")

        # Validate order direction
        if order_direction.upper() not in ["ASC", "DESC"]:
            raise self.__exc(code=ErrorCode.VALIDATION_ERROR, message="order_direction must be 'ASC' or 'DESC'", context=ErrorContext(operation="validation"))

        # Validate pagination parameters
        if limit is not None and limit <= 0:
            raise self.__exc(code=ErrorCode.VALIDATION_ERROR, message="limit must be a positive integer", context=ErrorContext(operation="validation"))

        if offset < 0:
            raise self.__exc(code=ErrorCode.VALIDATION_ERROR, message="offset must be non-negative", context=ErrorContext(operation="validation"))

        # Helper function to execute the actual SQL operations
        async def _execute_read(active_session: AsyncSession) -> list[dict[str, Any]]:
            query = f"SELECT * FROM {table_name}"

            # Build WHERE clause with both filters and date range
            where_conditions = []
            params = {}

            # Add standard filters
            if filters:
                where_clause, filter_params = self._build_where_clause(filters)
                if where_clause:
                    # Extract the conditions without the WHERE keyword
                    where_conditions.append(where_clause.replace(" WHERE ", ""))
                    params.update(filter_params)

            # Add date range filters if date_field is specified
            if date_field:
                if start_date is not None:
                    where_conditions.append(f"{date_field} >= :start_date")
                    params["start_date"] = start_date

                if end_date is not None:
                    where_conditions.append(f"{date_field} <= :end_date")
                    params["end_date"] = end_date

            # Add WHERE clause if there are conditions
            if where_conditions:
                query += " WHERE " + " AND ".join(where_conditions)

            # Add ORDER BY clause
            if order_by:
                query += f" ORDER BY {order_by} {order_direction.upper()}"

            # Add LIMIT and OFFSET for pagination
            if limit is not None:
                if self._is_postgresql:
                    query += f" LIMIT {limit} OFFSET {offset}"
                else:  # MySQL
                    query += f" LIMIT {offset}, {limit}"
            elif offset > 0:
                # If only offset is provided, we need a very large limit for MySQL
                if self._is_postgresql:
                    query += f" OFFSET {offset}"
                else:  # MySQL requires LIMIT when using OFFSET
                    query += f" LIMIT {offset}, 18446744073709551615"  # Maximum value for MySQL

            result = await active_session.execute(text(query), params)
            rows = result.fetchall()
            return [dict(row._mapping) for row in rows]

        # Use external session if provided, otherwise create our own
        if session:
            # Use provided session (read operations don't need commit/rollback)
            return await _execute_read(session)
        else:
            # Traditional behavior with auto-managed session
            async with self.create_external_session() as auto_session:
                return await _execute_read(auto_session)

    async def count_sql(
        self,
        table_name: str,
        filters: dict[str, Any] | None = None,
        session: AsyncSession = None,
    ) -> int:
        """Get the total count of records in a table with optional filters"""
        self._validate_identifier(table_name, "table name")

        # Helper function to execute the actual SQL operations
        async def _execute_count(active_session: AsyncSession) -> int:
            query = f"SELECT COUNT(*) as total FROM {table_name}"

            # Add WHERE clause
            where_clause, params = self._build_where_clause(filters)
            query += where_clause

            result = await active_session.execute(text(query), params)
            row = result.fetchone()
            return row[0] if row else 0

        # Use external session if provided, otherwise create our own
        if session:
            return await _execute_count(session)
        else:
            async with self.create_external_session() as auto_session:
                return await _execute_count(auto_session)


    async def read_sql_with_conditions(
            self,
            table_name: str,
            conditions: list[str],
            params: dict[str, Any],
            order_by: str | None = None,
            order_direction: str = "DESC",
            limit: int | None = None,
            offset: int = 0,
            session: AsyncSession = None,
    ) -> list[dict[str, Any]]:
        """Read data with complex WHERE conditions"""
        self._validate_identifier(table_name, "table name")

        if order_by:
            self._validate_identifier(order_by, "order_by column name")

        if order_direction.upper() not in ["ASC", "DESC"]:
            raise self.__exc(code=ErrorCode.VALIDATION_ERROR, message="order_direction must be 'ASC' or 'DESC'", context=ErrorContext(operation="validation"))

        if limit is not None and limit <= 0:
            raise self.__exc(code=ErrorCode.VALIDATION_ERROR, message="limit must be a positive integer", context=ErrorContext(operation="validation"))

        if offset < 0:
            raise self.__exc(code=ErrorCode.VALIDATION_ERROR, message="offset must be non-negative", context=ErrorContext(operation="validation"))

        async def _execute_read_conditions(active_session: AsyncSession) -> list[dict[str, Any]]:
            query = f"SELECT * FROM {table_name}"

            # Add WHERE clause with conditions
            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            # Add ORDER BY clause - handle multiple columns
            if order_by:
                query += f" ORDER BY {order_by}"
                if order_direction.upper() in ["ASC", "DESC"]:
                    # Only add direction if it's a simple column, not complex like "priority DESC, created_at ASC"
                    if "," not in order_by and order_by.upper() not in ["ASC", "DESC"]:
                        query += f" {order_direction.upper()}"

            # Add LIMIT and OFFSET for pagination
            if limit is not None:
                if self._is_postgresql:
                    query += f" LIMIT {limit} OFFSET {offset}"
                else:  # MySQL
                    query += f" LIMIT {offset}, {limit}"
            elif offset > 0:
                if self._is_postgresql:
                    query += f" OFFSET {offset}"
                else:  # MySQL requires LIMIT when using OFFSET
                    query += f" LIMIT {offset}, 18446744073709551615"

            result = await active_session.execute(text(query), params)
            rows = result.fetchall()
            return [dict(row._mapping) for row in rows]

        if session:
            return await _execute_read_conditions(session)
        else:
            async with self.create_external_session() as auto_session:
                return await _execute_read_conditions(auto_session)

    async def get_aggregated_data(
            self,
            table_name: str,
            group_by: list[str],
            aggregations: dict[str, str] = None,
            filters: dict[str, Any] | None = None,
            having_conditions: list[str] | None = None,
            session: AsyncSession = None,
    ) -> list[dict[str, Any]]:
        """Get aggregated data with GROUP BY"""
        self._validate_identifier(table_name, "table name")

        # Validate group by fields
        for field in group_by:
            self._validate_identifier(field, "group by field")

        if not aggregations:
            aggregations = {"count": "*"}

        async def _execute_aggregation(active_session: AsyncSession) -> list[dict[str, Any]]:
            # Build SELECT clause
            select_fields = ", ".join(group_by)

            # Add aggregation fields
            agg_fields = []
            for alias, expression in aggregations.items():
                if expression == "*":
                    agg_fields.append(f"COUNT(*) as {alias}")
                else:
                    # Validate aggregation field
                    self._validate_identifier(expression, "aggregation field")
                    agg_fields.append(f"COUNT({expression}) as {alias}")

            if agg_fields:
                select_fields += ", " + ", ".join(agg_fields)

            query = f"SELECT {select_fields} FROM {table_name}"

            # Add WHERE clause
            where_clause, params = self._build_where_clause(filters)
            query += where_clause

            # Add GROUP BY clause
            query += f" GROUP BY {', '.join(group_by)}"

            # Add HAVING clause
            if having_conditions:
                query += " HAVING " + " AND ".join(having_conditions)

            result = await active_session.execute(text(query), params)
            rows = result.fetchall()
            return [dict(row._mapping) for row in rows]

        if session:
            return await _execute_aggregation(session)
        else:
            async with self.create_external_session() as auto_session:
                return await _execute_aggregation(auto_session)

    async def execute_raw_query(
            self,
            query: str,
            params: dict[str, Any] = None,
            fetch_mode: str = "all",
            session: AsyncSession = None,
    ) -> list[dict[str, Any]] | dict[str, Any] | None:
        """Execute raw SQL query safely"""
        if params is None:
            params = {}

        if fetch_mode not in ["all", "one", "none"]:
            raise self.__exc(
                code=ErrorCode.VALIDATION_ERROR,
                message="fetch_mode must be 'all', 'one', or 'none'",
                context=ErrorContext(operation="execute_raw_query", details={"fetch_mode": fetch_mode})
            )

        async def _execute_raw(active_session: AsyncSession):
            result = await active_session.execute(text(query), params)

            if fetch_mode == "none":
                return None
            elif fetch_mode == "one":
                row = result.fetchone()
                return dict(row._mapping) if row else None
            else:  # fetch_mode == "all"
                rows = result.fetchall()
                return [dict(row._mapping) for row in rows]

        if session:
            return await _execute_raw(session)
        else:
            async with self.create_external_session() as auto_session:
                return await _execute_raw(auto_session)

    async def read_one(
        self, table_name: str, id_value: Any, id_column: str = "id", session: AsyncSession = None
    ) -> dict[str, Any] | None:
        self._validate_identifier(table_name, "table name")
        self._validate_identifier(id_column, "id column name")

        # Helper function to execute the actual SQL operations
        async def _execute_read_one(active_session: AsyncSession) -> dict[str, Any] | None:
            query = f"SELECT * FROM {table_name} WHERE {id_column} = :id_value"
            result = await active_session.execute(text(query), {"id_value": id_value})
            row = result.fetchone()
            return dict(row._mapping) if row else None

        # Use external session if provided, otherwise create our own
        if session:
            return await _execute_read_one(session)
        else:
            async with self.create_external_session() as auto_session:
                return await _execute_read_one(auto_session)

    async def update_sql(
        self,
        table_name: str,
        update_data: list[dict[str, Any]],
        where_field: str = "id",
        session: AsyncSession = None,
    ) -> list[dict[str, Any]]:
        """Update multiple records in a single transaction for better performance

        Args:
            table_name: Name of the table to update
            update_data: List format: [{"where_field": where_value, "data": {"field": "new_value", ...}}, ...]
            where_field: Field name to use in WHERE clause (default: "id")
            session: Optional external AsyncSession for transaction management

        Returns:
            List of updated records
        """
        self._validate_identifier(table_name, "table name")
        self._validate_identifier(where_field, "where field name")

        if isinstance(update_data, list):
            # Validate list format
            update_list = []
            for idx, item in enumerate(update_data):
                if not isinstance(item, dict) or where_field not in item or "data" not in item:
                    raise self.__exc(code=ErrorCode.VALIDATION_ERROR, message=f"Item at index {idx} must have '{where_field}' and 'data' fields", context=ErrorContext(operation="validation"))
                update_list.append(item)
        else:
            raise self.__exc(code=ErrorCode.VALIDATION_ERROR, message="Update data must be a list", context=ErrorContext(operation="validation"))

        if not update_list:
            return []

        # Validate all update data
        validated_updates = []
        for idx, item in enumerate(update_list):
            try:
                where_value = item[where_field]
                update_fields = self._validate_data_dict(
                    item["data"], f"update operation (item {idx})"
                )
                if update_fields:  # Only include if there are valid fields to update
                    validated_updates.append({where_field: where_value, "data": update_fields})
            except Exception as e:
                raise self.__exc(code=ErrorCode.VALIDATION_ERROR, message=f"Invalid update data at index {idx}: {str(e)}", context=ErrorContext(operation="validation"))

        if not validated_updates:
            return []

        # Helper function to execute the actual SQL operations
        async def _execute_updates(active_session: AsyncSession) -> list[dict[str, Any]]:
            updated_records = []

            # Use individual updates within a transaction for better control
            # This ensures each update can have different fields and proper error handling
            for update_item in validated_updates:
                where_value = update_item[where_field]
                update_fields = update_item["data"]

                # Build SET clause
                set_clauses = []
                params = {"where_value": where_value}

                for key, value in update_fields.items():
                    set_clauses.append(f"{key} = :{key}")
                    params[key] = value

                if self._is_postgresql:
                    # PostgreSQL supports RETURNING
                    query = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {where_field} = :where_value RETURNING *"
                    result = await active_session.execute(text(query), params)
                    row = result.fetchone()
                    if row:
                        updated_records.append(dict(row._mapping))
                else:
                    # MySQL doesn't support RETURNING, update then select
                    query = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {where_field} = :where_value"
                    result = await active_session.execute(text(query), params)

                    if result.rowcount > 0:
                        # Fetch the updated record
                        select_query = (
                            f"SELECT * FROM {table_name} WHERE {where_field} = :where_value"
                        )
                        select_result = await active_session.execute(
                            text(select_query), {"where_value": where_value}
                        )
                        row = select_result.fetchone()
                        if row:
                            updated_records.append(dict(row._mapping))

            return updated_records

        # Use external session if provided, otherwise create and manage our own
        if session:
            # Use provided session without commit/rollback
            return await _execute_updates(session)
        else:
            # Traditional behavior with auto-managed session
            async with self.create_external_session() as auto_session:
                try:
                    result = await _execute_updates(auto_session)
                    await auto_session.commit()
                    return result
                except Exception as e:
                    await auto_session.rollback()
                    raise e

    async def delete_sql(
        self, table_name: str, id_values: Any | list[Any], id_column: str = "id", session: AsyncSession = None
    ) -> list[Any]:
        """Delete single or multiple records by ID for better performance

        Args:
            table_name: Name of the table to delete from
            id_values: Single ID or list of IDs to delete
            id_column: Name of the ID column (default: "id")
            session: Optional external AsyncSession for transaction management

        Returns:
            List of successfully deleted IDs
        """
        self._validate_identifier(table_name, "table name")
        self._validate_identifier(id_column, "id column name")

        # Handle single ID input by wrapping in list
        if isinstance(id_values, list):
            id_list = id_values
        else:
            id_list = [id_values]

        if not id_list:
            return []

        # Helper function to execute the actual SQL operations
        async def _execute_deletes(active_session: AsyncSession) -> list[Any]:
            successfully_deleted = []

            if len(id_list) == 1:
                # Single delete for better error reporting
                query = f"DELETE FROM {table_name} WHERE {id_column} = :id_value"
                result = await active_session.execute(
                    text(query), {"id_value": id_list[0]}
                )
                if result.rowcount > 0:
                    successfully_deleted.append(id_list[0])
            else:
                # Bulk delete with IN clause
                if self._is_postgresql:
                    query = f"DELETE FROM {table_name} WHERE {id_column} = ANY(:id_list)"
                    result = await active_session.execute(
                        text(query), {"id_list": id_list}
                    )
                else:
                    # MySQL: expand parameters
                    placeholders = [f":id_{i}" for i in range(len(id_list))]
                    query = f"DELETE FROM {table_name} WHERE {id_column} IN ({', '.join(placeholders)})"
                    params = {f"id_{i}": id_val for i, id_val in enumerate(id_list)}
                    result = await active_session.execute(
                        text(query), params
                    )

                # For bulk delete, we assume all IDs were successfully deleted
                # if rowcount matches the input count
                if result.rowcount > 0:
                    if result.rowcount == len(id_list):
                        successfully_deleted = id_list.copy()
                    else:
                        # Some records were not found - need to check which ones were deleted
                        # This is a limitation of bulk delete - we can't know exactly which ones failed
                        # For now, we'll return all requested IDs if any were deleted
                        successfully_deleted = id_list.copy()

            return successfully_deleted

        # Use external session if provided, otherwise create and manage our own
        if session:
            # Use provided session without commit/rollback
            return await _execute_deletes(session)
        else:
            # Traditional behavior with auto-managed session
            async with self.create_external_session() as auto_session:
                try:
                    result = await _execute_deletes(auto_session)
                    await auto_session.commit()
                    return result
                except Exception as e:
                    await auto_session.rollback()
                    raise e


    async def delete_filter(self, table_name: str, filters: dict[str, Any], session: AsyncSession = None) -> list[Any]:
        """Delete multiple records based on filter conditions return ids"""
        self._validate_identifier(table_name, "table name")

        if not filters:
            raise self.__exc(code=ErrorCode.VALIDATION_ERROR, message="Filters are required for delete_many operation", context=ErrorContext(operation="validation"))

        # Helper function to execute the actual SQL operations
        async def _execute_delete_filter(active_session: AsyncSession) -> list[Any]:
            where_clause, params = self._build_where_clause(filters)
            if not where_clause:
                raise self.__exc(code=ErrorCode.VALIDATION_ERROR, message="No valid filters provided for delete_many operation", context=ErrorContext(operation="validation"))

            # First, select the IDs that will be deleted
            select_query = f"SELECT id FROM {table_name}{where_clause}"
            select_result = await active_session.execute(text(select_query), params)
            ids_to_delete = [row[0] for row in select_result.fetchall()]

            if not ids_to_delete:
                return []

            # Then delete the records
            delete_query = f"DELETE FROM {table_name}{where_clause}"
            await active_session.execute(text(delete_query), params)

            return ids_to_delete

        # Use external session if provided, otherwise create and manage our own
        if session:
            # Use provided session without commit/rollback
            return await _execute_delete_filter(session)
        else:
            # Traditional behavior with auto-managed session
            async with self.create_external_session() as auto_session:
                try:
                    result = await _execute_delete_filter(auto_session)
                    await auto_session.commit()
                    return result
                except Exception as e:
                    await auto_session.rollback()
                    raise e

    async def upsert_sql(
        self,
        table_name: str,
        data: list[dict[str, Any]],
        conflict_fields: list[str],
        update_fields: list[str] = None,
        session: AsyncSession = None
    ) -> list[dict[str, Any]]:
        """Insert records or update if they already exist (UPSERT)
        
        Uses ON CONFLICT for PostgreSQL and ON DUPLICATE KEY UPDATE for MySQL
        
        Args:
            table_name: Name of the table to upsert into
            data: List of records to insert or update
            conflict_fields: Fields that determine uniqueness (e.g., ["id"] or ["user_id", "item_id"])
            update_fields: Fields to update on conflict (if None, updates all fields except conflict_fields)
            session: Optional external AsyncSession for transaction management
            
        Returns:
            List of upserted records
        """
        self._validate_identifier(table_name, "table name")
        
        # Validate conflict fields
        for field in conflict_fields:
            self._validate_identifier(field, "conflict field")
        
        # Handle single record input by wrapping in list
        if isinstance(data, dict):
            data_list = [data]
        elif isinstance(data, list):
            data_list = data
        else:
            raise self.__exc(code=ErrorCode.VALIDATION_ERROR, message="Data must be a dictionary or list of dictionaries", context=ErrorContext(operation="validation"))
        
        if not data_list:
            return []
        
        # Validate all data first
        cleaned_data_list = []
        for idx, item in enumerate(data_list):
            if not isinstance(item, dict):
                raise self.__exc(code=ErrorCode.VALIDATION_ERROR, message=f"Item at index {idx} must be a dictionary", context=ErrorContext(operation="validation"))
            
            try:
                cleaned_data = self._validate_data_dict(item, f"upsert operation (item {idx})")
                cleaned_data_list.append(cleaned_data)
            except Exception as e:
                raise self.__exc(code=ErrorCode.VALIDATION_ERROR, message=f"Invalid data at index {idx}: {str(e)}", context=ErrorContext(operation="validation"))
        
        if not cleaned_data_list:
            return []
        
        # Get columns from first item
        all_columns = list(cleaned_data_list[0].keys())
        
        # Determine fields to update
        if update_fields is None:
            # Update all fields except conflict fields
            update_fields = [col for col in all_columns if col not in conflict_fields]
        else:
            # Validate update fields
            for field in update_fields:
                self._validate_identifier(field, "update field")
        
        # Helper function to execute the actual SQL operations
        async def _execute_upsert(active_session: AsyncSession) -> list[dict[str, Any]]:
            if self._is_postgresql:
                # PostgreSQL: Use INSERT ... ON CONFLICT ... DO UPDATE
                columns_str = ", ".join(all_columns)
                
                # Build VALUES clause with numbered parameters
                values_clauses = []
                params = {}
                for i, data_item in enumerate(cleaned_data_list):
                    value_placeholders = []
                    for col in all_columns:
                        param_name = f"{col}_{i}"
                        value_placeholders.append(f":{param_name}")
                        params[param_name] = data_item.get(col)
                    values_clauses.append(f"({', '.join(value_placeholders)})")
                
                # Build ON CONFLICT clause
                conflict_str = ", ".join(conflict_fields)
                
                # Build DO UPDATE SET clause
                update_clauses = []
                for field in update_fields:
                    update_clauses.append(f"{field} = EXCLUDED.{field}")
                update_str = ", ".join(update_clauses)
                
                query = f"""
                    INSERT INTO {table_name} ({columns_str})
                    VALUES {', '.join(values_clauses)}
                    ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str}
                    RETURNING *
                """
                
                result = await active_session.execute(text(query), params)
                rows = result.fetchall()
                return [dict(row._mapping) for row in rows]
                
            else:
                # MySQL: Use INSERT ... ON DUPLICATE KEY UPDATE
                columns_str = ", ".join(all_columns)
                
                # Build VALUES clause
                values_clauses = []
                params = {}
                for i, data_item in enumerate(cleaned_data_list):
                    value_placeholders = []
                    for col in all_columns:
                        param_name = f"{col}_{i}"
                        value_placeholders.append(f":{param_name}")
                        params[param_name] = data_item.get(col)
                    values_clauses.append(f"({', '.join(value_placeholders)})")
                
                # Build ON DUPLICATE KEY UPDATE clause
                update_clauses = []
                for field in update_fields:
                    update_clauses.append(f"{field} = VALUES({field})")
                update_str = ", ".join(update_clauses)
                
                query = f"""
                    INSERT INTO {table_name} ({columns_str})
                    VALUES {', '.join(values_clauses)}
                    ON DUPLICATE KEY UPDATE {update_str}
                """
                
                result = await active_session.execute(text(query), params)
                
                # MySQL doesn't support RETURNING, need to fetch the records
                # Build a query to fetch all affected records
                where_conditions = []
                fetch_params = {}
                
                for i, data_item in enumerate(cleaned_data_list):
                    field_conditions = []
                    for field in conflict_fields:
                        param_name = f"fetch_{field}_{i}"
                        field_conditions.append(f"{field} = :{param_name}")
                        fetch_params[param_name] = data_item.get(field)
                    where_conditions.append(f"({' AND '.join(field_conditions)})")
                
                fetch_query = f"SELECT * FROM {table_name} WHERE {' OR '.join(where_conditions)}"
                fetch_result = await active_session.execute(text(fetch_query), fetch_params)
                rows = fetch_result.fetchall()
                return [dict(row._mapping) for row in rows]
        
        # Use external session if provided, otherwise create and manage our own
        if session:
            return await _execute_upsert(session)
        else:
            async with self.create_external_session() as auto_session:
                try:
                    result = await _execute_upsert(auto_session)
                    await auto_session.commit()
                    return result
                except Exception as e:
                    await auto_session.rollback()
                    raise e

    async def exists_sql(
        self,
        table_name: str,
        id_values: list[Any],
        id_column: str = "id",
        session: AsyncSession = None
    ) -> dict[Any, bool]:
        """Check which records exist in the database
        
        Returns a dictionary mapping each ID to its existence status
        
        Args:
            table_name: Name of the table to check
            id_values: List of IDs to check
            id_column: Name of the ID column (default: "id")
            session: Optional external AsyncSession
            
        Returns:
            Dictionary mapping each ID to True (exists) or False (doesn't exist)
        """
        self._validate_identifier(table_name, "table name")
        self._validate_identifier(id_column, "id column name")
        
        if not id_values:
            return {}
        
        # Initialize result with all IDs as False
        result = {id_val: False for id_val in id_values}
        
        # Helper function to execute the actual SQL operations
        async def _execute_exists(active_session: AsyncSession) -> dict[Any, bool]:
            if len(id_values) == 1:
                # Single ID check
                query = f"SELECT {id_column} FROM {table_name} WHERE {id_column} = :id_value"
                sql_result = await active_session.execute(text(query), {"id_value": id_values[0]})
                row = sql_result.fetchone()
                if row:
                    result[id_values[0]] = True
            else:
                # Multiple IDs check
                if self._is_postgresql:
                    query = f"SELECT {id_column} FROM {table_name} WHERE {id_column} = ANY(:id_list)"
                    sql_result = await active_session.execute(text(query), {"id_list": id_values})
                else:
                    # MySQL: expand parameters
                    placeholders = [f":id_{i}" for i in range(len(id_values))]
                    query = f"SELECT {id_column} FROM {table_name} WHERE {id_column} IN ({', '.join(placeholders)})"
                    params = {f"id_{i}": id_val for i, id_val in enumerate(id_values)}
                    sql_result = await active_session.execute(text(query), params)
                
                # Mark existing IDs as True
                rows = sql_result.fetchall()
                for row in rows:
                    id_val = row[0]
                    if id_val in result:
                        result[id_val] = True
            
            return result
        
        # Use external session if provided, otherwise create our own
        if session:
            return await _execute_exists(session)
        else:
            async with self.create_external_session() as auto_session:
                return await _execute_exists(auto_session)

    async def get_distinct_values(
        self,
        table_name: str,
        field: str,
        filters: dict[str, Any] = None,
        limit: int = None,
        session: AsyncSession = None
    ) -> list[Any]:
        """Get distinct values from a specific column
        
        Args:
            table_name: Name of the table
            field: Column name to get distinct values from
            filters: Optional filters to apply
            limit: Maximum number of distinct values to return
            session: Optional external AsyncSession
            
        Returns:
            List of distinct values from the specified column
        """
        self._validate_identifier(table_name, "table name")
        self._validate_identifier(field, "field name")
        
        if limit is not None and limit <= 0:
            raise self.__exc(code=ErrorCode.VALIDATION_ERROR, message="limit must be a positive integer", context=ErrorContext(operation="validation"))
        
        # Helper function to execute the actual SQL operations
        async def _execute_distinct(active_session: AsyncSession) -> list[Any]:
            query = f"SELECT DISTINCT {field} FROM {table_name}"
            
            # Add WHERE clause
            where_clause, params = self._build_where_clause(filters)
            query += where_clause
            
            # Add ORDER BY for consistent results
            query += f" ORDER BY {field}"
            
            # Add LIMIT if specified
            if limit is not None:
                query += f" LIMIT {limit}"
            
            result = await active_session.execute(text(query), params)
            rows = result.fetchall()
            
            # Extract values from rows
            return [row[0] for row in rows if row[0] is not None]
        
        # Use external session if provided, otherwise create our own
        if session:
            return await _execute_distinct(session)
        else:
            async with self.create_external_session() as auto_session:
                return await _execute_distinct(auto_session)

    async def execute_query(
            self, query: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]] | dict[str, Any]:
        async with self.create_external_session() as session:
            result = await session.execute(text(query), params or {})

            if (
                    query.strip()
                            .upper()
                            .startswith(("SELECT", "SHOW", "DESCRIBE", "EXPLAIN"))
            ):
                rows = result.fetchall()
                return [dict(row._mapping) for row in rows]
            else:
                await session.commit()
                return {
                    "affected_rows": result.rowcount
                    if hasattr(result, "rowcount")
                    else 0
                }

    async def health_check(self) -> bool:
        """Check if database connection is healthy"""
        operation_context = f"health_check(engine_type={getattr(self.__sqlClient, 'engine_type', 'unknown')})"

        try:
            async with self.create_external_session() as session:
                # Use a simple query that works on both PostgreSQL and MySQL
                result = await session.execute(text("SELECT 1 as health_check"))
                row = result.fetchone()
                is_healthy = row is not None and row[0] == 1

                if not is_healthy:
                    import structlog
                    logger = structlog.get_logger()
                    logger.warning(f"Database health check returned unexpected result: {row}")

                return is_healthy

        except (DBAPIError, asyncpg.PostgresError, pymysql.Error) as db_err:
            # Log specific database errors but don't raise - health check should return boolean
            import structlog
            logger = structlog.get_logger()
            logger.warning(f"Database health check failed in {operation_context}: {type(db_err).__name__}: {db_err}")
            return False
        except (ConnectionError, TimeoutError, OSError) as conn_err:
            # Network/connection related errors
            import structlog
            logger = structlog.get_logger()
            logger.warning(f"Connection error in {operation_context}: {type(conn_err).__name__}: {conn_err}")
            return False
        except Exception as e:
            # Log unexpected errors
            import structlog
            logger = structlog.get_logger()
            logger.error(f"Unexpected error in {operation_context}: {type(e).__name__}: {e}")
            return False


if __name__ == "__main__":
    pass
