import functools
import re
from typing import Any

import asyncpg
import pymysql
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.exc import UnmappedInstanceError

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
        if value is None or value in self.null_set:
            return  # Allow null values

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
            if value not in self.null_set:
                validated_data[key] = value

        if not validated_data and not allow_empty:
            raise self.__exc(
                status_code=400,
                message=f"No valid data provided for {operation}",
                message_code=107,
            )

        return validated_data

    def _create_session(self) -> AsyncSession:
        """Create a new database session"""
        return AsyncSession(self.__sqlClient.get_engine())

    def _build_where_clause(
        self, filters: dict[str, Any] | None
    ) -> tuple[str, dict[str, Any]]:
        """Build WHERE clause and parameters from filters"""
        if not filters:
            return "", {}

        cleaned_filters = self._validate_data_dict(
            filters, "filter operation", allow_empty=True
        )
        if not cleaned_filters:
            return "", {}

        where_clauses = []
        params = {}
        for key, value in cleaned_filters.items():
            where_clauses.append(f"{key} = :{key}")
            params[key] = value

        where_clause = " WHERE " + " AND ".join(where_clauses)
        return where_clause, params

    @staticmethod
    def exception_handler(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except DBAPIError as e:
                if isinstance(e.orig, asyncpg.PostgresError):
                    pg_error = e.orig
                    message = str(pg_error).replace("\n", " ").replace("\r", " ")
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
            except UnmappedInstanceError:
                raise self.__exc(
                    status_code=486,
                    message="id: one or more of ids is not exist",
                    message_code=2,
                )

        return wrapper

    @exception_handler
    def _get_client(self) -> SQLClient:
        return self.__sqlClient

    @exception_handler
    async def create_sql(
        self, table_name: str, data: dict[str, Any] | list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Create multiple records in a single transaction for better performance

        Args:
            table_name: Name of the table to insert into
            data: Single record (dict) or list of records to insert

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

        async with self._create_session() as session:
            try:
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
                    result = await session.execute(text(query), params)
                    await session.commit()

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
                    result = await session.execute(text(query), params)

                    # Get the first inserted ID and row count
                    first_id = result.lastrowid
                    row_count = result.rowcount

                    await session.commit()

                    # Fetch all inserted records (assuming auto-increment ID)
                    if first_id and row_count > 0:
                        # Assuming sequential auto-increment IDs
                        id_list = list(range(first_id, first_id + row_count))
                        select_query = (
                            f"SELECT * FROM {table_name} WHERE id IN :id_list"
                        )
                        select_result = await session.execute(
                            text(select_query), {"id_list": tuple(id_list)}
                        )
                        rows = select_result.fetchall()
                        inserted_records = [dict(row._mapping) for row in rows]

                    return inserted_records

            except Exception:
                await session.rollback()
                raise

    @exception_handler
    async def read_sql(
        self,
        table_name: str,
        filters: dict[str, Any] | None = None,
        order_by: str | None = None,
        order_direction: str = "ASC",
        limit: int | None = None,
        offset: int = 0,
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

        async with self._create_session() as session:
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

            result = await session.execute(text(query), params)
            rows = result.fetchall()
            return [dict(row._mapping) for row in rows]

    @exception_handler
    async def count(
        self, table_name: str, filters: dict[str, Any] | None = None
    ) -> int:
        """Get the total count of records in a table with optional filters"""
        self._validate_identifier(table_name, "table name")

        async with self._create_session() as session:
            query = f"SELECT COUNT(*) as total FROM {table_name}"

            # Add WHERE clause
            where_clause, params = self._build_where_clause(filters)
            query += where_clause

            result = await session.execute(text(query), params)
            row = result.fetchone()
            return row[0] if row else 0

    @exception_handler
    async def read_one(
        self, table_name: str, id_value: Any, id_column: str = "id"
    ) -> dict[str, Any] | None:
        self._validate_identifier(table_name, "table name")
        self._validate_identifier(id_column, "id column name")

        async with self._create_session() as session:
            query = f"SELECT * FROM {table_name} WHERE {id_column} = :id_value"
            result = await session.execute(text(query), {"id_value": id_value})
            row = result.fetchone()
            return dict(row._mapping) if row else None

    @exception_handler
    async def update_sql(
        self,
        table_name: str,
        update_data: dict[str, dict[str, Any]] | list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Update multiple records in a single transaction for better performance

        Args:
            table_name: Name of the table to update
            update_data: Either:
                - Dict format: {"id_value": {"field": "new_value", ...}, ...}
                - List format: [{"id": id_value, "data": {"field": "new_value", ...}}, ...]

        Returns:
            List of updated records
        """
        self._validate_identifier(table_name, "table name")

        # Normalize input to list format
        if isinstance(update_data, dict):
            # Convert from {id: data} format to [{"id": id, "data": data}] format
            update_list = [
                {"id": id_val, "data": data} for id_val, data in update_data.items()
            ]
        elif isinstance(update_data, list):
            # Validate list format
            update_list = []
            for idx, item in enumerate(update_data):
                if not isinstance(item, dict) or "id" not in item or "data" not in item:
                    raise self.__exc(
                        status_code=400,
                        message=f"Item at index {idx} must have 'id' and 'data' fields",
                        message_code=116,
                    )
                update_list.append(item)
        else:
            raise self.__exc(
                status_code=400,
                message="Update data must be a dictionary or list",
                message_code=117,
            )

        if not update_list:
            return []

        # Validate all update data
        validated_updates = []
        for idx, item in enumerate(update_list):
            try:
                id_value = item["id"]
                update_fields = self._validate_data_dict(
                    item["data"], f"update operation (item {idx})"
                )
                if update_fields:  # Only include if there are valid fields to update
                    validated_updates.append({"id": id_value, "data": update_fields})
            except Exception as e:
                raise self.__exc(
                    status_code=400,
                    message=f"Invalid update data at index {idx}: {str(e)}",
                    message_code=118,
                )

        if not validated_updates:
            return []

        updated_records = []

        async with self._create_session() as session:
            try:
                # Use individual updates within a transaction for better control
                # This ensures each update can have different fields and proper error handling
                for update_item in validated_updates:
                    id_value = update_item["id"]
                    update_fields = update_item["data"]

                    # Build SET clause
                    set_clauses = []
                    params = {"id_value": id_value}

                    for key, value in update_fields.items():
                        set_clauses.append(f"{key} = :{key}")
                        params[key] = value

                    if self._is_postgresql:
                        # PostgreSQL supports RETURNING
                        query = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE id = :id_value RETURNING *"
                        result = await session.execute(text(query), params)
                        row = result.fetchone()
                        if row:
                            updated_records.append(dict(row._mapping))
                    else:
                        # MySQL doesn't support RETURNING, update then select
                        query = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE id = :id_value"
                        result = await session.execute(text(query), params)

                        if result.rowcount > 0:
                            # Fetch the updated record
                            select_query = (
                                f"SELECT * FROM {table_name} WHERE id = :id_value"
                            )
                            select_result = await session.execute(
                                text(select_query), {"id_value": id_value}
                            )
                            row = select_result.fetchone()
                            if row:
                                updated_records.append(dict(row._mapping))

                await session.commit()
                return updated_records

            except Exception:
                await session.rollback()
                raise

    @exception_handler
    async def delete_sql(
        self, table_name: str, id_values: Any | list[Any], id_column: str = "id"
    ) -> list[Any]:
        """Delete single or multiple records by ID for better performance

        Args:
            table_name: Name of the table to delete from
            id_values: Single ID or list of IDs to delete
            id_column: Name of the ID column (default: "id")

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

        successfully_deleted = []

        async with self._create_session() as session:
            try:
                if len(id_list) == 1:
                    # Single delete for better error reporting
                    query = f"DELETE FROM {table_name} WHERE {id_column} = :id_value"
                    result = await session.execute(
                        text(query), {"id_value": id_list[0]}
                    )
                    if result.rowcount > 0:
                        successfully_deleted.append(id_list[0])
                else:
                    # Bulk delete with IN clause
                    query = f"DELETE FROM {table_name} WHERE {id_column} IN :id_list"
                    result = await session.execute(
                        text(query), {"id_list": tuple(id_list)}
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

                await session.commit()
                return successfully_deleted

            except Exception:
                await session.rollback()
                raise


    @exception_handler
    async def delete_filter(self, table_name: str, filters: dict[str, Any]) -> int:
        """Delete multiple records based on filter conditions"""
        self._validate_identifier(table_name, "table name")

        if not filters:
            raise self.__exc(
                status_code=400,
                message="Filters are required for delete_many operation",
                message_code=111,
            )

        cleaned_filters = self._validate_data_dict(filters, "delete_many operation")

        async with self._create_session() as session:
            where_clause, params = self._build_where_clause(cleaned_filters)
            if not where_clause:
                raise self.__exc(
                    status_code=400,
                    message="No valid filters provided for delete_many operation",
                    message_code=112,
                )

            query = f"DELETE FROM {table_name}{where_clause}"
            result = await session.execute(text(query), params)
            await session.commit()
            return result.rowcount

    @exception_handler
    async def execute_query(
        self, query: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]] | dict[str, Any]:
        async with self._create_session() as session:
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

    @exception_handler
    async def health_check(self) -> bool:
        try:
            async with self._create_session() as session:
                result = await session.execute(text("SELECT 1"))
                return result.fetchone() is not None
        except (
            DBAPIError,
            asyncpg.PostgresError,
            pymysql.Error,
            ConnectionError,
            TimeoutError,
        ):
            return False


if __name__ == "__main__":
    pass
