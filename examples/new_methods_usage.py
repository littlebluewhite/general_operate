"""
Example usage of the new database operation methods added to general_operate

This file demonstrates:
1. upsert_sql - Insert or update records (UPSERT)
2. exists_sql - Check if records exist
3. get_distinct_values - Get unique values from a column
4. upsert_data - High-level upsert with cache management
5. exists_check - Single record existence check
6. batch_exists - Batch existence check
7. refresh_cache - Refresh specific cache entries
8. get_distinct_values (with caching) - Get unique values with caching
"""

import asyncio
from typing import Any
from general_operate import GeneralOperate
from general_operate.app.sql_operate import SQLOperate


# Example 1: Using SQLOperate methods directly
async def example_sql_operate_methods():
    """Demonstrate low-level SQL operations"""
    
    # Assume we have a SQLOperate instance
    sql_op = SQLOperate(database_client)
    
    # 1. UPSERT operation - Insert or update records
    print("\n=== UPSERT Example ===")
    data_to_upsert = [
        {"id": 1, "name": "Product A", "price": 100, "stock": 50},
        {"id": 2, "name": "Product B", "price": 200, "stock": 30},
        {"id": 3, "name": "Product C", "price": 150, "stock": 0}
    ]
    
    # Upsert based on ID - if ID exists, update price and stock
    result = await sql_op.upsert_sql(
        table_name="products",
        data=data_to_upsert,
        conflict_fields=["id"],  # Unique constraint on ID
        update_fields=["price", "stock"]  # Only update these fields on conflict
    )
    print(f"Upserted {len(result)} records")
    
    # 2. Check which records exist
    print("\n=== EXISTS Check Example ===")
    ids_to_check = [1, 2, 5, 10, 15]
    existence_map = await sql_op.exists_sql(
        table_name="products",
        id_values=ids_to_check,
        id_column="id"
    )
    for id_val, exists in existence_map.items():
        print(f"ID {id_val}: {'Exists' if exists else 'Does not exist'}")
    
    # 3. Get distinct values from a column
    print("\n=== DISTINCT Values Example ===")
    categories = await sql_op.get_distinct_values(
        table_name="products",
        field="category",
        filters={"active": True},  # Only from active products
        limit=10
    )
    print(f"Distinct categories: {categories}")


# Example 2: Using GeneralOperate high-level methods
class ProductOperator(GeneralOperate):
    """Example operator for products table"""
    
    def get_module(self):
        """Return the product schema module"""
        # This would normally return your actual schema module
        # For example: return product_module
        pass


async def example_general_operate_methods():
    """Demonstrate high-level operations with cache management"""
    
    # Initialize operator with database and cache
    product_op = ProductOperator(
        database_client=database_client,
        redis_client=redis_client
    )
    
    # 1. Upsert with cache management
    print("\n=== High-Level UPSERT with Cache ===")
    products_to_upsert = [
        {"id": 1, "name": "Updated Product A", "price": 110},
        {"id": 4, "name": "New Product D", "price": 300}
    ]
    
    # This will validate schemas, clear cache, and upsert
    result = await product_op.upsert_data(
        data=products_to_upsert,
        conflict_fields=["id"],
        update_fields=["name", "price"]
    )
    print(f"Upserted {len(result)} products with cache sync")
    
    # 2. Quick existence check (uses cache first)
    print("\n=== Single Existence Check ===")
    exists = await product_op.exists_check(id_value=1)
    print(f"Product ID 1 exists: {exists}")
    
    # 3. Batch existence check
    print("\n=== Batch Existence Check ===")
    ids_to_check = {1, 2, 3, 100, 200}
    existence_results = await product_op.batch_exists(ids_to_check)
    for id_val, exists in existence_results.items():
        status = "cached/exists" if exists else "not found"
        print(f"Product ID {id_val}: {status}")
    
    # 4. Refresh cache for specific records
    print("\n=== Cache Refresh ===")
    ids_to_refresh = {1, 2, 3, 999}  # 999 doesn't exist
    refresh_stats = await product_op.refresh_cache(ids_to_refresh)
    print(f"Refresh results: {refresh_stats}")
    # Output: {'refreshed': 3, 'not_found': 1, 'errors': 0}
    
    # 5. Get distinct values with caching
    print("\n=== Distinct Values with Cache ===")
    # First call - fetches from database and caches
    brands = await product_op.get_distinct_values(
        field="brand",
        filters={"category": "electronics"},
        cache_ttl=300  # Cache for 5 minutes
    )
    print(f"Distinct brands (from DB): {brands}")
    
    # Second call - returns from cache
    brands_cached = await product_op.get_distinct_values(
        field="brand",
        filters={"category": "electronics"},
        cache_ttl=300
    )
    print(f"Distinct brands (from cache): {brands_cached}")


# Example 3: Using transactions for complex operations
async def example_transaction_with_upsert():
    """Demonstrate upsert within a transaction"""
    
    product_op = ProductOperator(
        database_client=database_client,
        redis_client=redis_client
    )
    
    print("\n=== Transaction with UPSERT ===")
    
    # Use transaction for atomic operations
    async with product_op.transaction() as session:
        # Upsert main product
        main_product = [{"id": 1, "name": "Bundle Product", "price": 500}]
        await product_op.upsert_data(
            data=main_product,
            conflict_fields=["id"],
            session=session  # Use the same transaction
        )
        
        # Check if related products exist
        related_ids = [10, 11, 12]
        existence = await product_op.batch_exists(
            id_values=set(related_ids),
            session=session
        )
        
        # Only create bundle if all related products exist
        if all(existence.values()):
            print("All related products exist, creating bundle...")
            # Create bundle entries...
        else:
            print("Some related products missing, rolling back...")
            raise ValueError("Cannot create bundle - missing products")


# Example 4: Bulk operations with existence checks
async def example_bulk_import():
    """Demonstrate efficient bulk import with existence checking"""
    
    product_op = ProductOperator(
        database_client=database_client,
        redis_client=redis_client
    )
    
    print("\n=== Bulk Import with Existence Check ===")
    
    # Large dataset to import
    import_data = [
        {"id": i, "name": f"Product {i}", "price": i * 10}
        for i in range(1, 101)  # 100 products
    ]
    
    # Step 1: Check which products already exist
    all_ids = {item["id"] for item in import_data}
    existence = await product_op.batch_exists(all_ids)
    
    existing_ids = {id_val for id_val, exists in existence.items() if exists}
    new_ids = all_ids - existing_ids
    
    print(f"Found {len(existing_ids)} existing products")
    print(f"Will create {len(new_ids)} new products")
    
    # Step 2: Prepare data for upsert
    updates = [item for item in import_data if item["id"] in existing_ids]
    inserts = [item for item in import_data if item["id"] in new_ids]
    
    # Step 3: Perform upsert (will handle both insert and update)
    if import_data:
        result = await product_op.upsert_data(
            data=import_data,
            conflict_fields=["id"],
            update_fields=["name", "price"]  # Update these fields for existing records
        )
        print(f"Successfully imported {len(result)} products")
    
    # Step 4: Refresh cache for updated records
    if existing_ids:
        refresh_stats = await product_op.refresh_cache(existing_ids)
        print(f"Cache refresh: {refresh_stats}")


# Example 5: Using distinct values for filtering
async def example_dynamic_filters():
    """Demonstrate using distinct values for dynamic filter options"""
    
    product_op = ProductOperator(
        database_client=database_client,
        redis_client=redis_client
    )
    
    print("\n=== Dynamic Filter Options ===")
    
    # Get all unique categories
    categories = await product_op.get_distinct_values(
        field="category",
        cache_ttl=600  # Cache for 10 minutes
    )
    print(f"Available categories: {categories}")
    
    # Get brands for a specific category
    if categories:
        selected_category = categories[0]
        brands = await product_op.get_distinct_values(
            field="brand",
            filters={"category": selected_category},
            cache_ttl=300
        )
        print(f"Brands in {selected_category}: {brands}")
    
    # Get price ranges
    price_ranges = await product_op.get_distinct_values(
        field="price_range",
        filters={"active": True, "in_stock": True},
        cache_ttl=300
    )
    print(f"Available price ranges: {price_ranges}")


# Main execution
async def main():
    """Run all examples"""
    
    print("=" * 60)
    print("NEW DATABASE OPERATION METHODS - USAGE EXAMPLES")
    print("=" * 60)
    
    # Note: You would need to initialize these with actual connections
    # global database_client, redis_client
    # database_client = await create_database_client()
    # redis_client = await create_redis_client()
    
    # Run examples (uncomment when you have actual connections)
    # await example_sql_operate_methods()
    # await example_general_operate_methods()
    # await example_transaction_with_upsert()
    # await example_bulk_import()
    # await example_dynamic_filters()
    
    print("\n" + "=" * 60)
    print("Examples completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())