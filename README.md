# General Operate

[![Python](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115.0+-green.svg)](https://fastapi.tiangolo.com/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A high-performance Python library providing a unified interface for database operations across SQL databases, Redis cache, and InfluxDB time-series data. Features intelligent caching strategies, bulk operations, and comprehensive error handling.

## 🚀 Key Features

- **Multi-Database Support**: PostgreSQL, MySQL, Redis, InfluxDB
- **Intelligent Caching**: Cache-first read strategy with automatic fallback
- **Bulk Operations**: High-performance batch processing for CRUD operations
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
    data = await operate.read_data({1})
    print(f"Read: {data}")
    
    # Update data
    update_data = {1: {"title": "Updated Tutorial Title"}}
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

#### `read_data(id_value: set) -> list`
Read data using cache-first strategy with automatic fallback.
```python
# Read single record
data = await operate.read_data({1})

# Read multiple records
data = await operate.read_data({1, 2, 3})
```

#### `create_data(data: list) -> list`
Create new records with bulk insert optimization.
```python
new_records = [
    {"name": "record1", "title": "Title 1"},
    {"name": "record2", "title": "Title 2"}
]
created = await operate.create_data(new_records)
```

#### `update_data(data: dict) -> list`
Update existing records with bulk operations and cache invalidation.
```python
updates = {
    1: {"title": "New Title 1"},
    2: {"title": "New Title 2"}
}
updated = await operate.update_data(updates)
```

#### `delete_data(id_value: set) -> list`
Delete records and return successfully deleted IDs.
```python
deleted_ids = await operate.delete_data({1, 2, 3})
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
| GET | `/tutorial` | Get all tutorials |
| GET | `/tutorial/id?id_set=1,2,3` | Get tutorials by IDs |
| POST | `/tutorial` | Create new tutorials |
| PATCH | `/tutorial` | Update existing tutorials |
| DELETE | `/tutorial?id_set=1,2,3` | Delete tutorials by IDs |

### Example API Usage

```bash
# Get all tutorials
curl http://localhost:8000/tutorial

# Create new tutorial
curl -X POST http://localhost:8000/tutorial \
  -H "Content-Type: application/json" \
  -d '[{"name": "test", "title": "Test Tutorial", "tags": ["example"]}]'

# Update tutorial
curl -X PATCH http://localhost:8000/tutorial \
  -H "Content-Type: application/json" \
  -d '[{"id": 1, "title": "Updated Title"}]'

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

### Bulk Operations
All CRUD operations support bulk processing for optimal performance:
- **Bulk Insert**: Process multiple records in single transaction
- **Bulk Update**: Update multiple records with single query
- **Bulk Delete**: Delete multiple records efficiently

### Cache Strategy
- **Cache-First Reads**: Always check cache before SQL
- **Write-Through**: Update SQL first, then invalidate cache
- **Null Protection**: Cache negative results to prevent repeated SQL queries
- **Batch Cache Operations**: Reduce Redis round-trips

### Memory Optimization
- **Streaming Results**: Handle large datasets without loading all into memory
- **Connection Pooling**: Reuse database connections efficiently
- **Lazy Loading**: Load related data only when needed

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

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📚 Additional Resources

- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Pydantic Documentation](https://docs.pydantic.dev/)
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)
- [Redis Python Documentation](https://redis-py.readthedocs.io/)

---

For questions or support, please open an issue on GitHub.