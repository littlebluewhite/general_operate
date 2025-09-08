# New Database Operation Methods Documentation

## Overview
This document describes the new database operation methods added to the `general_operate` project. These methods provide enhanced functionality for database operations including UPSERT operations, existence checks, and efficient data retrieval with caching support.

## Table of Contents
1. [SQL Operate Methods](#sql-operate-methods)
   - [upsert_sql](#upsert_sql)
   - [exists_sql](#exists_sql)
   - [get_distinct_values](#get_distinct_values-sql)
2. [General Operate Methods](#general-operate-methods)
   - [upsert_data](#upsert_data)
   - [exists_check](#exists_check)
   - [batch_exists](#batch_exists)
   - [refresh_cache](#refresh_cache)
   - [get_distinct_values](#get_distinct_values-general)

---

## SQL Operate Methods

These are low-level methods in `sql_operate.py` that directly interact with the database.

### upsert_sql

Insert records or update if they already exist (UPSERT operation).

```python
async def upsert_sql(
    self,
    table_name: str,
    data: list[dict[str, Any]],
    conflict_fields: list[str],
    update_fields: list[str] = None,
    session: AsyncSession = None
) -> list[dict[str, Any]]
```

**Parameters:**
- `table_name`: Name of the table to upsert into
- `data`: List of records to insert or update
- `conflict_fields`: Fields that determine uniqueness (e.g., `["id"]` or `["user_id", "item_id"]`)
- `update_fields`: Fields to update on conflict (if None, updates all fields except conflict_fields)
- `session`: Optional external AsyncSession for transaction management

**Returns:**
- List of upserted records

**Database Compatibility:**
- PostgreSQL: Uses `ON CONFLICT ... DO UPDATE`
- MySQL: Uses `ON DUPLICATE KEY UPDATE`

**Example:**
```python
data = [
    {"id": 1, "name": "Product A", "price": 100},
    {"id": 2, "name": "Product B", "price": 200}
]

result = await sql_op.upsert_sql(
    "products",
    data,
    conflict_fields=["id"],
    update_fields=["price"]  # Only update price on conflict
)
```

### exists_sql

Check which records exist in the database.

```python
async def exists_sql(
    self,
    table_name: str,
    id_values: list[Any],
    id_column: str = "id",
    session: AsyncSession = None
) -> dict[Any, bool]
```

**Parameters:**
- `table_name`: Name of the table to check
- `id_values`: List of IDs to check
- `id_column`: Name of the ID column (default: "id")
- `session`: Optional external AsyncSession

**Returns:**
- Dictionary mapping each ID to True (exists) or False (doesn't exist)

**Example:**
```python
ids = [1, 2, 3, 100, 200]
existence = await sql_op.exists_sql("products", ids)
# Result: {1: True, 2: True, 3: False, 100: False, 200: False}
```

### get_distinct_values (SQL)

Get distinct values from a specific column.

```python
async def get_distinct_values(
    self,
    table_name: str,
    field: str,
    filters: dict[str, Any] = None,
    limit: int = None,
    session: AsyncSession = None
) -> list[Any]
```

**Parameters:**
- `table_name`: Name of the table
- `field`: Column name to get distinct values from
- `filters`: Optional filters to apply
- `limit`: Maximum number of distinct values to return
- `session`: Optional external AsyncSession

**Returns:**
- List of distinct values from the specified column (excludes NULL values)

**Example:**
```python
categories = await sql_op.get_distinct_values(
    "products",
    "category",
    filters={"active": True},
    limit=10
)
```

---

## General Operate Methods

These are high-level methods in `general_operate.py` that include schema validation and cache management.

### upsert_data

Insert or update data with schema validation and cache management.

```python
@handle_errors(operation="upsert_data")
async def upsert_data(
    self,
    data: list[dict],
    conflict_fields: list[str],
    update_fields: list[str] = None,
    session=None
) -> list[T]
```

**Parameters:**
- `data`: List of dictionaries containing data to upsert
- `conflict_fields`: Fields that determine uniqueness
- `update_fields`: Fields to update on conflict (if None, updates all except conflict_fields)
- `session`: Optional AsyncSession for transaction management

**Returns:**
- List of upserted records as schema objects

**Features:**
- Validates data against create/update schemas
- Clears cache for affected records before and after operation
- Removes null markers from cache
- Returns properly typed schema objects

**Example:**
```python
products = [
    {"id": 1, "name": "Updated Product", "price": 150},
    {"id": 5, "name": "New Product", "price": 300}
]

result = await product_op.upsert_data(
    products,
    conflict_fields=["id"],
    update_fields=["name", "price"]
)
```

### exists_check

Quick check if a single record exists (cache-first strategy).

```python
@handle_errors(operation="exists_check")
async def exists_check(self, id_value: Any, session=None) -> bool
```

**Parameters:**
- `id_value`: The ID value to check
- `session`: Optional AsyncSession

**Returns:**
- True if record exists, False otherwise

**Features:**
- Checks cache first (including null markers)
- Falls back to database if not in cache
- Sets null marker for non-existent records (5 minutes TTL)

**Example:**
```python
exists = await product_op.exists_check(123)
if exists:
    print("Product exists")
```

### batch_exists

Check existence of multiple records efficiently.

```python
@handle_errors(operation="batch_exists")
async def batch_exists(self, id_values: set, session=None) -> dict[Any, bool]
```

**Parameters:**
- `id_values`: Set of ID values to check
- `session`: Optional AsyncSession

**Returns:**
- Dictionary mapping each ID to its existence status

**Features:**
- Checks cache first for all IDs
- Batch queries database for uncached IDs
- Sets null markers for non-existent records

**Example:**
```python
ids = {1, 2, 3, 100, 200}
existence = await product_op.batch_exists(ids)
for id_val, exists in existence.items():
    print(f"ID {id_val}: {'exists' if exists else 'not found'}")
```

### refresh_cache

Reload specific records from database to cache.

```python
@handle_errors(operation="refresh_cache")
async def refresh_cache(self, id_values: set) -> dict
```

**Parameters:**
- `id_values`: Set of ID values to refresh in cache

**Returns:**
- Dictionary with refresh statistics:
  - `refreshed`: Number of records successfully refreshed
  - `not_found`: Number of records not found in database
  - `errors`: Number of errors encountered

**Features:**
- Clears existing cache entries and null markers
- Fetches fresh data from database
- Updates cache with current data
- Sets null markers for missing records

**Example:**
```python
stats = await product_op.refresh_cache({1, 2, 3, 999})
print(f"Refreshed: {stats['refreshed']}")
print(f"Not found: {stats['not_found']}")
```

### get_distinct_values (General)

Get distinct values from a field with optional caching.

```python
@handle_errors(operation="get_distinct_values")
async def get_distinct_values(
    self,
    field: str,
    filters: dict[str, Any] = None,
    cache_ttl: int = 300,
    session=None
) -> list[Any]
```

**Parameters:**
- `field`: The field name to get distinct values from
- `filters`: Optional filters to apply
- `cache_ttl`: Cache TTL in seconds (default: 300, set to 0 to disable caching)
- `session`: Optional AsyncSession

**Returns:**
- List of distinct values from the specified field

**Features:**
- Caches results with configurable TTL
- Generates unique cache key based on field and filters
- Returns cached results when available
- Falls back to database query on cache miss

**Example:**
```python
# First call - fetches from database and caches
categories = await product_op.get_distinct_values(
    "category",
    filters={"active": True},
    cache_ttl=600  # Cache for 10 minutes
)

# Second call - returns from cache
categories_cached = await product_op.get_distinct_values(
    "category",
    filters={"active": True},
    cache_ttl=600
)
```

---

## Best Practices

### 1. Use Transactions for Related Operations
```python
async with operator.transaction() as session:
    await operator.upsert_data(main_data, ["id"], session=session)
    await operator.upsert_data(related_data, ["id"], session=session)
```

### 2. Batch Operations for Performance
```python
# Check existence for many IDs at once
existence = await operator.batch_exists({1, 2, 3, 4, 5})

# Upsert multiple records in one operation
await operator.upsert_data(large_dataset, ["id"])
```

### 3. Cache Management
```python
# Refresh cache after external updates
await operator.refresh_cache(updated_ids)

# Use appropriate cache TTL for distinct values
await operator.get_distinct_values("category", cache_ttl=3600)  # 1 hour
```

### 4. Handle Conflicts Properly
```python
# Be specific about conflict and update fields
await operator.upsert_data(
    data,
    conflict_fields=["user_id", "product_id"],  # Composite unique key
    update_fields=["quantity", "updated_at"]     # Only update these
)
```

---

## Performance Considerations

1. **Bulk Operations**: All methods support bulk operations for better performance
2. **Cache-First Strategy**: High-level methods check cache before database
3. **Null Markers**: Prevent repeated database queries for non-existent records
4. **Transaction Support**: All methods support external sessions for ACID compliance
5. **Database Compatibility**: Methods work with both PostgreSQL and MySQL

---

## Error Handling

All methods use the `@handle_errors` decorator and follow consistent error handling:

- `DatabaseException` for database-related errors
- `ValidationError` for data validation issues
- Proper logging at each level
- Graceful fallback for cache failures

---

## Migration Guide

If you're upgrading from older methods:

1. Replace manual INSERT/UPDATE logic with `upsert_data`
2. Replace existence queries with `exists_check` or `batch_exists`
3. Use `get_distinct_values` instead of custom DISTINCT queries
4. Leverage `refresh_cache` after bulk external updates

---

## Testing

All methods have comprehensive test coverage in `/tests/test_new_methods.py`:

```bash
# Run tests for new methods
pytest tests/test_new_methods.py -xvs
```

---

## Version History

- **Version 1.0** (2025-09-08): Initial implementation of all methods
  - Added upsert operations with PostgreSQL/MySQL compatibility
  - Implemented existence checking with cache support
  - Added distinct value retrieval with caching
  - Comprehensive cache management methods