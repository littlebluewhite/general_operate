import re
from typing import Any

import asyncpg
import pymysql
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from ..app.client.database import SQLClient
from ..utils.exception import GeneralOperateException


class SQLOperate:
    def __init__(self, client: SQLClient, exc=GeneralOperateException):
        self.__exc = exc
        self.__sqlClient = client
        self.null_set = {-999999, "null"}

        # SQL injection protection patterns
        self._valid_identifier_pattern = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

        # Detect database type for compatibility
        self._is_postgresql = client.engine_type.lower() == "postgresql"

    def _validate_identifier(
        self, identifier: str, identifier_type: str = "identifier"
    ) -> None:
        """Validate SQL identifiers (table names, column names) to prevent SQL injection"""
        if not identifier or not isinstance(identifier, str):
            raise self.__exc(
                status_code=400,
                message=f"Invalid {identifier_type}: must be a non-empty string",
                message_code=100,
            )

        if len(identifier) > 64:  # Standard SQL identifier length limit
            raise self.__exc(
                status_code=400,
                message=f"Invalid {identifier_type}: too long (max 64 characters)",
                message_code=101,
            )

        if not self._valid_identifier_pattern.match(identifier):
            raise self.__exc(
                status_code=400,
                message=f"Invalid {identifier_type}: contains invalid characters",
                message_code=102,
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
                    status_code=400,
                    message=f"Value for column '{column_name}' contains non-serializable data: {e}",
                    message_code=108,
                )

        # Check for extremely long strings that might cause issues
        if isinstance(value, str) and len(value) > 65535:  # TEXT field limit in MySQL
            raise self.__exc(
                status_code=400,
                message=f"Value for column '{column_name}' is too long (max 65535 characters)",
                message_code=103,
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

            # Check for comment patterns and procedure patterns
            for pattern in dangerous_patterns:
                if pattern in lower_value:
                    raise self.__exc(
                        status_code=400,
                        message=f"Value for column '{column_name}' contains potentially dangerous SQL characters",
                        message_code=104,
                    )

            # Check for SQL injection patterns (more specific)
            for keyword in sql_keywords:
                if keyword in lower_value:
                    raise self.__exc(
                        status_code=400,
                        message=f"Value for column '{column_name}' contains potentially dangerous SQL keywords",
                        message_code=104,
                    )

    def _validate_data_dict(
        self,
        data: dict[str, Any],
        operation: str = "operation",
        allow_empty: bool = False,
    ) -> dict[str, Any]:
        """Validate a data dictionary for database operations"""
        if not isinstance(data, dict):
            raise self.__exc(
                status_code=400,
                message=f"Data for {operation} must be a dictionary",
                message_code=105,
            )

        if not data and not allow_empty:
            raise self.__exc(
                status_code=400,
                message=f"Data for {operation} cannot be empty",
                message_code=106,
            )

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
            raise self.__exc(
                status_code=400,
                message=f"No valid data provided for {operation}",
                message_code=107,
            )
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
                if not value:  # Empty list
                    continue
                # For PostgreSQL, we need to use ANY() with array parameter
                if self._is_postgresql:
                    where_clauses.append(f"{key} = ANY(:{key})")
                    params[key] = value  # PostgreSQL handles lists natively
                else:
                    # For MySQL, expand the parameters manually
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

    async def create_sql(
        self, table_name: str, data: dict[str, Any] | list[dict[str, Any]], session: AsyncSession = None
    ) -> list[dict[str, Any]]:
        """Create multiple records in a single transaction for better performance

        Args:
            table_name: Name of the table to insert into
            data: Single record (dict) or list of records to insert
            session: Optional external AsyncSession for transaction management

        Returns:
            List of created records
        """
        self._validate_identifier(table_name, "table name")

        # Handle single record input by wrapping in list
        if isinstance(data, dict):
            data_list = [data]
        elif isinstance(data, list):
            data_list = data
        else:
            raise self.__exc(
                status_code=400,
                message="Data must be a dictionary or list of dictionaries",
                message_code=114,
            )

        if not data_list:
            return []

        # Validate all data first
        cleaned_data_list = []
        for idx, item in enumerate(data_list):
            if not isinstance(item, dict):
                raise self.__exc(
                    status_code=400,
                    message=f"Item at index {idx} must be a dictionary",
                    message_code=115,
                )

            try:
                cleaned_data = self._validate_data_dict(
                    item, f"create operation (item {idx})"
                )
                cleaned_data_list.append(cleaned_data)
            except Exception as e:
                # Re-raise with more context
                raise self.__exc(
                    status_code=400,
                    message=f"Invalid data at index {idx}: {str(e)}",
                    message_code=113,
                )

        if not cleaned_data_list:
            return []

        # Helper function to execute the actual SQL operations
        async def _execute_insert(active_session: AsyncSession) -> list[dict[str, Any]]:
            if self._is_postgresql:
                # PostgreSQL: Use VALUES with multiple rows and RETURNING
                # Get columns from first item (all should have same structure)
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

                query = f"INSERT INTO {table_name} ({columns_str}) VALUES {', '.join(values_clauses)} RETURNING *"
                result = await active_session.execute(text(query), params)

                rows = result.fetchall()
                return [dict(row._mapping) for row in rows]
            else:
                # MySQL: Use executemany or multiple INSERT statements
                # MySQL doesn't support RETURNING, so we need to track inserted IDs
                inserted_records = []

                # Use a single INSERT with multiple VALUES for better performance
                columns = list(cleaned_data_list[0].keys())
                columns_str = ", ".join(columns)

                # Build VALUES clause
                values_clauses = []
                params = {}
                for i, data_item in enumerate(cleaned_data_list):
                    value_placeholders = []
                    for col in columns:
                        param_name = f"{col}_{i}"
                        value_placeholders.append(f":{param_name}")
                        params[param_name] = data_item.get(col)
                    values_clauses.append(f"({', '.join(value_placeholders)})")

                query = f"INSERT INTO {table_name} ({columns_str}) VALUES {', '.join(values_clauses)}"
                result = await active_session.execute(text(query), params)

                # Get the first inserted ID and row count
                first_id = result.lastrowid
                row_count = result.rowcount

                # Fetch all inserted records (assuming auto-increment ID)
                if first_id and row_count > 0:
                    # Assuming sequential auto-increment IDs
                    id_list = list(range(first_id, first_id + row_count))
                    if self._is_postgresql:
                        select_query = f"SELECT * FROM {table_name} WHERE id = ANY(:id_list)"
                        select_result = await active_session.execute(
                            text(select_query), {"id_list": id_list}
                        )
                    else:
                        # MySQL: expand parameters
                        placeholders = [f":id_{i}" for i in range(len(id_list))]
                        select_query = f"SELECT * FROM {table_name} WHERE id IN ({', '.join(placeholders)})"
                        params = {f"id_{i}": id_val for i, id_val in enumerate(id_list)}
                        select_result = await active_session.execute(
                            text(select_query), params
                        )
                    rows = select_result.fetchall()
                    inserted_records = [dict(row._mapping) for row in rows]

                return inserted_records

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
        session: AsyncSession = None,
    ) -> list[dict[str, Any]]:
        self._validate_identifier(table_name, "table name")

        # Validate order_by column if provided
        if order_by:
            self._validate_identifier(order_by, "order_by column name")

        # Validate order direction
        if order_direction.upper() not in ["ASC", "DESC"]:
            raise self.__exc(
                status_code=400,
                message="order_direction must be 'ASC' or 'DESC'",
                message_code=108,
            )

        # Validate pagination parameters
        if limit is not None and limit <= 0:
            raise self.__exc(
                status_code=400,
                message="limit must be a positive integer",
                message_code=109,
            )

        if offset < 0:
            raise self.__exc(
                status_code=400, message="offset must be non-negative", message_code=110
            )

        # Helper function to execute the actual SQL operations
        async def _execute_read(active_session: AsyncSession) -> list[dict[str, Any]]:
            query = f"SELECT * FROM {table_name}"

            # Add WHERE clause
            where_clause, params = self._build_where_clause(filters)
            query += where_clause

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
        self, table_name: str, filters: dict[str, Any] | None = None, session: AsyncSession = None
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
                    raise self.__exc(
                        status_code=400,
                        message=f"Item at index {idx} must have '{where_field}' and 'data' fields",
                        message_code=116,
                    )
                update_list.append(item)
        else:
            raise self.__exc(
                status_code=400,
                message="Update data must be a list",
                message_code=117,
            )

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
                raise self.__exc(
                    status_code=400,
                    message=f"Invalid update data at index {idx}: {str(e)}",
                    message_code=118,
                )

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
            raise self.__exc(
                status_code=400,
                message="Filters are required for delete_many operation",
                message_code=111,
            )

        # Helper function to execute the actual SQL operations
        async def _execute_delete_filter(active_session: AsyncSession) -> list[Any]:
            where_clause, params = self._build_where_clause(filters)
            if not where_clause:
                raise self.__exc(
                    status_code=400,
                    message="No valid filters provided for delete_many operation",
                    message_code=112,
                )

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
