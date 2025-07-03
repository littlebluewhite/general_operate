<!-- encoding: utf-8 -->
# General Operate

[![Python](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115.0+-green.svg)](https://fastapi.tiangolo.com/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A high-performance Python library providing a unified interface for database operations across SQL databases, Redis cache, and InfluxDB time-series data. Features intelligent caching strategies, ACID-compliant transactions, bulk operations, and comprehensive error handling.

## 🚀 Key Features

- **Multi-Database Support**: PostgreSQL, MySQL, Redis, InfluxDB
- **Transaction Support**: ACID-compliant transactions with automatic rollback
- **Intelligent Caching**: Cache-first read strategy with automatic fallback
- **Bulk Operations**: High-performance batch processing for CRUD operations
- **Flexible WHERE Conditions**: Support for complex filtering with custom WHERE clauses
- **Type Safety**: Full Pydantic schema validation and type hints
- **Error Handling**: Comprehensive exception management with custom error codes
- **Health Monitoring**: Built-in health checks for all database connections
- **FastAPI Integration**: Ready-to-use tutorial application demonstrating best practices

## 🏗️ Architecture

### Core Components

```
general_operate/
├── general_operate.py          # Main GeneralOperate class
├── app/
│   ├── sql_operate.py         # SQL database operations
│   ├── cache_operate.py       # Redis cache operations
│   ├── influxdb_operate.py    # InfluxDB time-series operations
│   └── client/
│       ├── database.py        # SQL client adapter
│       ├── redis_db.py        # Redis client adapter
│       └── influxdb.py        # InfluxDB client adapter
├── dependencies/              # Dependency injection utilities
└── utils/                     # Common utilities and exceptions
```

### Data Flow

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Application   │───▶│ GeneralOperate  │───▶│   SQL Database  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                               │
                               ▼
                       ┌─────────────────┐
                       │  Redis Cache    │
                       └─────────────────┘
                               │
                               ▼
                       ┌─────────────────┐
                       │    InfluxDB     │
                       └─────────────────┘
```

**Cache-First Strategy**: Read operations first check Redis cache, falling back to SQL on cache miss. Write operations update SQL first, then invalidate cache for consistency.

## 📦 Installation

### Requirements

- Python 3.13+
- PostgreSQL or MySQL (for SQL operations)
- Redis (for caching)
- InfluxDB (optional, for time-series data)

### Install Dependencies

Using `uv` (recommended):
```bash
uv add general-operate
```

Using `pip`:
```bash
pip install general-operate
```

For development:
```bash
uv add --group dev general-operate
# or
pip install general-operate[dev]
```

### Database Setup

#### PostgreSQL
```bash
# Install PostgreSQL
brew install postgresql  # macOS
# or
apt-get install postgresql  # Ubuntu

# Create database
createdb generaloperate
```

#### Redis
```bash
# Install Redis
brew install redis  # macOS
# or
apt-get install redis-server  # Ubuntu

# Start Redis
redis-server
```

#### InfluxDB (Optional)
```bash
# Install InfluxDB
brew install influxdb  # macOS
# or follow official installation guide

# Start InfluxDB
influxd
```

## 🚀 Quick Start

### Basic Usage

```python
import asyncio
from general_operate import GeneralOperate
from general_operate.app.client.database import SQLClient
from general_operate.app.client.redis_db import RedisDB

# Database configurations
sql_config = {
    "host": "127.0.0.1",
    "port": 5432,
    "user": "your_user",
    "password": "your_password",
    "db": "generaloperate",
    "engine": "postgresql"  # or "mysql"
}

redis_config = {
    "host": "127.0.0.1:6379",
    "db": 0,
    "user": None,
    "password": None
}

# Define your data schemas
from pydantic import BaseModel

class CreateSchema(BaseModel):
    name: str
    title: str
    tags: list = []
    enable: bool = True

class UpdateSchema(BaseModel):
    id: int
    name: str = None
    title: str = None
    tags: list = None
    enable: bool = None

class MainSchema(BaseModel):
    id: int
    name: str
    title: str
    tags: list
    enable: bool

# Create module object
class TutorialModule:
    table_name = "tutorial"
    main_schemas = MainSchema
    create_schemas = CreateSchema
    update_schemas = UpdateSchema

async def main():
    # Initialize clients
    db = SQLClient(sql_config)
    redis = RedisDB(redis_config).redis_client()
    
    # Create GeneralOperate instance
    module = TutorialModule()
    operate = GeneralOperate(
        module=module,
        database_client=db,
        redis_client=redis
    )
    
    # Create data
    new_data = [
        {"name": "tutorial1", "title": "My First Tutorial", "tags": ["python", "async"]}
    ]
    created = await operate.create_data(new_data)
    print(f"Created: {created}")
    
    # Read data (cache-first)
    data = await operate.read_data_by_id({1})
    print(f"Read: {data}")
    
    # Update data with flexible WHERE conditions
    update_data = [
        {"id": 1, "title": "Updated Tutorial Title"}
    ]
    updated = await operate.update_data(update_data)
    print(f"Updated: {updated}")
    
    # Delete data
    deleted_ids = await operate.delete_data({1})
    print(f"Deleted IDs: {deleted_ids}")

if __name__ == "__main__":
    asyncio.run(main())
```

## 🔧 API Reference

### Core Methods

#### `read_data_by_id(id_value: set) -> list`
Read data using cache-first strategy with automatic fallback.
```python
# Read single record
data = await operate.read_data_by_id({1})

# Read multiple records
data = await operate.read_data_by_id({1, 2, 3})
```

#### `read_data_by_filter(filters: dict, limit: int = None, offset: int = 0) -> list`
Read data using filter conditions with pagination support.
```python
# Filter by single field
data = await operate.read_data_by_filter({"name": "tutorial1"})

# Complex filters with pagination
data = await operate.read_data_by_filter(
    filters={"enable": True, "tags": ["python"]},
    limit=10,
    offset=0
)
```

#### `create_data(data: list, session=None) -> list`
Create new records with bulk insert optimization and optional transaction support.
```python
new_records = [
    {"name": "record1", "title": "Title 1"},
    {"name": "record2", "title": "Title 2"}
]

# Simple creation
created = await operate.create_data(new_records)

# With transaction context
async with session_manager() as session:
    created = await operate.create_data(new_records, session=session)
```

#### `update_data(data: list[dict], where_field: str = "id", session=None) -> list`
Update existing records with flexible WHERE conditions and bulk operations.
```python
# Update by ID (default)
updates = [
    {"id": 1, "title": "New Title 1"},
    {"id": 2, "title": "New Title 2"}
]
updated = await operate.update_data(updates)

# Update by custom field
updates = [
    {"name": "record1", "title": "Updated Title"}
]
updated = await operate.update_data(updates, where_field="name")
```

#### `delete_data(id_value: set, session=None) -> list`
Delete records and return successfully deleted IDs.
```python
deleted_ids = await operate.delete_data({1, 2, 3})
```

#### `create_by_foreign_key(foreign_key_field: str, foreign_key_value: Any, data: list, session=None) -> list`
Create related records with automatic foreign key assignment.
```python
# Create subtables for a tutorial
subtables = [
    {"description": "Step 1", "author": "John"},
    {"description": "Step 2", "author": "Jane"}
]
created = await operate.create_by_foreign_key(
    foreign_key_field="tutorial_id",
    foreign_key_value=1,
    data=subtables
)
```

#### `update_by_foreign_key(foreign_key_field: str, foreign_key_value: Any, data: list, session=None)`
Update related records with comprehensive CRUD operations based on ID rules.
```python
# Mixed CRUD operations based on ID:
# - id == 0 or None: Create new records
# - id > 0: Update existing records
# - id < 0: Delete records (using abs(id))
mixed_operations = [
    {"id": 1, "description": "Updated step"},  # Update
    {"id": 0, "description": "New step"},      # Create
    {"id": -2}                                 # Delete record with ID 2
]
await operate.update_by_foreign_key(
    foreign_key_field="tutorial_id",
    foreign_key_value=1,
    data=mixed_operations
)
```

### Transaction Support

#### `create_external_session() -> AsyncSession`
Create an external session for transaction management.
```python
session = await operate.create_external_session()
try:
    async with session.begin():
        # Perform multiple operations within transaction
        await operate.create_data(data1, session=session)
        await operate.update_data(data2, session=session)
        # Transaction commits automatically on success
finally:
    await session.close()
```

#### Transaction Context Manager Example
For complex multi-table operations with ACID compliance:
```python
from tutorial.routers.operator.API_tutorial import APITutorialOperator

# APITutorialOperator provides built-in transaction context
async with api_operator.transaction() as session:
    # All operations within this block are transactional
    await api_operator.tutorial_operate.create_data(tutorial_data, session=session)
    await api_operator.subtable_operate.create_by_foreign_key(
        "tutorial_id", tutorial_id, subtable_data, session=session
    )
    # Automatic rollback on any exception
```

### Cache Operations

#### `cache_warming(limit: int = 1000, offset: int = 0)`
Pre-load data into cache for better performance.
```python
result = await operate.cache_warming(limit=500)
```

#### `cache_clear()`
Clear all cached data for the table.
```python
await operate.cache_clear()
```

### Health Monitoring

#### `health_check() -> bool`
Check health of all database connections.
```python
is_healthy = await operate.health_check()
```

## 🌐 Tutorial Application

The project includes a FastAPI tutorial application demonstrating real-world usage.

### Running the Tutorial Server

```bash
cd tutorial
python main.py
```

The server will start at `http://0.0.0.0:8000`

### API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/tutorial` | Get all tutorials with their subtables |
| GET | `/tutorial/id?id_set=1,2,3` | Get tutorials by IDs |
| POST | `/tutorial` | Create new tutorials with subtables (transactional) |
| PATCH | `/tutorial?where_field=id` | Update existing tutorials with flexible WHERE conditions |
| DELETE | `/tutorial?id_set=1,2,3` | Delete tutorials by IDs (with cascade delete of subtables) |

### Example API Usage

```bash
# Get all tutorials
curl http://localhost:8000/tutorial

# Create new tutorial with subtables
curl -X POST http://localhost:8000/tutorial \
  -H "Content-Type: application/json" \
  -d '[{
    "name": "test", 
    "title": "Test Tutorial", 
    "tags": ["example"],
    "subtables": [
      {"description": "Step 1", "author": "John"},
      {"description": "Step 2", "author": "Jane"}
    ]
  }]'

# Update tutorial by ID (default)
curl -X PATCH "http://localhost:8000/tutorial?where_field=id" \
  -H "Content-Type: application/json" \
  -d '[{"id": 1, "title": "Updated Title"}]'

# Update tutorial by name
curl -X PATCH "http://localhost:8000/tutorial?where_field=name" \
  -H "Content-Type: application/json" \
  -d '[{"name": "test", "title": "Updated by Name"}]'

# Delete tutorial
curl -X DELETE "http://localhost:8000/tutorial?id_set=1"
```

## 🔨 Development

### Setup Development Environment

```bash
# Clone repository
git clone <repository-url>
cd general_operate_new

# Install development dependencies
uv sync --group dev

# Install pre-commit hooks
pre-commit install
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=general_operate

# Run specific test file
pytest tests/test_general_operate.py

# Run API tutorial operator tests (includes transaction tests)
pytest tests/test_api_tutorial_operator.py

# Run integration tests
pytest tests/integration/
```

### Code Quality

```bash
# Format code
ruff format .

# Lint code
ruff check .

# Type checking
mypy general_operate/
```

### Testing Framework

The project uses pytest with async support:
- **Unit Tests**: `tests/unit/`
- **Integration Tests**: `tests/integration/`
- **Performance Tests**: `tests/performance/`

## ⚙️ Configuration

### Environment Variables

```bash
# Database Configuration
DATABASE_HOST=127.0.0.1
DATABASE_PORT=5432
DATABASE_USER=your_user
DATABASE_PASSWORD=your_password
DATABASE_NAME=generaloperate

# Redis Configuration
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
REDIS_DB=0
REDIS_PASSWORD=

# InfluxDB Configuration (Optional)
INFLUXDB_HOST=127.0.0.1
INFLUXDB_PORT=8086
INFLUXDB_ORG=my-org
INFLUXDB_TOKEN=your-token
INFLUXDB_BUCKET=general_operator
```

### Database Connection Examples

#### PostgreSQL
```python
sql_config = {
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "password",
    "db": "mydb",
    "engine": "postgresql"
}
```

#### MySQL
```python
sql_config = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "password",
    "db": "mydb",
    "engine": "mysql"
}
```

#### Redis with Authentication
```python
redis_config = {
    "host": "localhost:6379",
    "db": 0,
    "user": "default",
    "password": "your_redis_password"
}
```

## 🚀 Performance Features

### Transaction Management
- **ACID Compliance**: All multi-table operations are fully transactional
- **Automatic Rollback**: Failed operations automatically rollback all changes
- **Session Injection**: External transaction control for complex workflows
- **Context Managers**: Built-in transaction context for operator classes

### Bulk Operations
All CRUD operations support bulk processing for optimal performance:
- **Bulk Insert**: Process multiple records in single transaction
- **Bulk Update**: Update multiple records with flexible WHERE conditions
- **Bulk Delete**: Delete multiple records efficiently
- **Foreign Key Operations**: Bulk CRUD operations for related data

### Cache Strategy
- **Cache-First Reads**: Always check cache before SQL
- **Write-Through**: Update SQL first, then invalidate cache
- **Null Protection**: Cache negative results to prevent repeated SQL queries
- **Batch Cache Operations**: Reduce Redis round-trips
- **Smart Invalidation**: Precise cache invalidation on updates

### Memory Optimization
- **Streaming Results**: Handle large datasets without loading all into memory
- **Connection Pooling**: Reuse database connections efficiently
- **Lazy Loading**: Load related data only when needed
- **Session Management**: Efficient async session lifecycle management

## 🛠️ Troubleshooting

### Common Issues

#### Database Connection Errors
```python
# Verify database configuration
result = await operate.health_check()
if not result:
    print("Database connection failed")
```

#### Cache Connection Issues
```python
# Test Redis connection
try:
    await redis.ping()
    print("Redis connected")
except Exception as e:
    print(f"Redis connection failed: {e}")
```

#### Schema Validation Errors
```python
# Ensure your data matches the schema
try:
    schema = CreateSchema(**your_data)
except ValidationError as e:
    print(f"Schema validation failed: {e}")
```

## 📚 Additional Resources

- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Pydantic Documentation](https://docs.pydantic.dev/)
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)
- [Redis Python Documentation](https://redis-py.readthedocs.io/)

---

For questions or support, please open an issue on GitHub.