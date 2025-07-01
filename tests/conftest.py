"""
Global test configuration and fixtures for APITutorialOperator testing.
"""
import asyncio
import os
import sys
from collections.abc import Generator
from unittest.mock import AsyncMock

import pytest
import redis.asyncio as redis

from tutorial.routers.operator import api_schemas

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from general_operate.general_operate import GeneralOperate
from tutorial.routers.operator.API_tutorial import APITutorialOperator
from tutorial.schemas import subtable, tutorial


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop]:
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def mock_db() -> AsyncMock:
    """Mock database client for testing."""
    db = AsyncMock()
    db.engine_type = "postgresql"  # Default to PostgreSQL for testing
    return db


@pytest.fixture
async def mock_redis() -> AsyncMock:
    """Mock Redis client for testing."""
    redis_client = AsyncMock(spec=redis.Redis)
    return redis_client


@pytest.fixture
async def mock_influxdb() -> AsyncMock:
    """Mock InfluxDB client for testing."""
    influx_client = AsyncMock()
    influx_client.bucket = "test_bucket"
    return influx_client


@pytest.fixture
async def mock_tutorial_operate(mock_db, mock_redis) -> AsyncMock:
    """Mock GeneralOperate instance for tutorial operations."""
    tutorial_op = AsyncMock(spec=GeneralOperate)
    tutorial_op.module = tutorial
    tutorial_op.table_name = tutorial.table_name
    return tutorial_op


@pytest.fixture
async def mock_subtable_operate(mock_db, mock_redis) -> AsyncMock:
    """Mock GeneralOperate instance for subtable operations."""
    subtable_op = AsyncMock(spec=GeneralOperate)
    subtable_op.module = subtable
    subtable_op.table_name = subtable.table_name
    return subtable_op


@pytest.fixture
async def api_tutorial_operator(mock_db, mock_redis, mock_influxdb) -> APITutorialOperator:
    """Create APITutorialOperator instance with mocked dependencies."""
    operator = APITutorialOperator(api_schemas, mock_db, mock_redis, mock_influxdb)
    return operator


@pytest.fixture
async def api_tutorial_operator_with_mocks(
    api_tutorial_operator, mock_tutorial_operate, mock_subtable_operate
) -> APITutorialOperator:
    """APITutorialOperator with mocked GeneralOperate instances."""
    api_tutorial_operator.tutorial_operate = mock_tutorial_operate
    api_tutorial_operator.subtable_operate = mock_subtable_operate
    return api_tutorial_operator


# Sample test data
@pytest.fixture
def sample_tutorial_data():
    """Sample tutorial data for testing."""
    return {
        "id": 1,
        "name": "Python Basics",
        "title": "Introduction to Python Programming",
        "tags": ["python", "programming", "beginner"],
        "enable": True
    }


@pytest.fixture
def sample_subtable_data():
    """Sample subtable data for testing."""
    return {
        "id": 1,
        "tutorial_id": 1,
        "description": "Variables and Data Types",
        "author": "John Doe"
    }


@pytest.fixture
def sample_tutorial_create_data():
    """Sample APITutorialCreate data for testing."""
    return {
        "name": "Python Basics",
        "title": "Introduction to Python Programming",
        "tags": ["python", "programming", "beginner"],
        "enable": True,
        "subtables": [
            {
                "description": "Variables and Data Types",
                "author": "John Doe"
            },
            {
                "description": "Control Flow",
                "author": "Jane Smith"
            }
        ]
    }


@pytest.fixture
def sample_tutorial_update_data():
    """Sample APITutorialUpdate data for testing."""
    return {
        "id": 1,
        "name": "Advanced Python",
        "title": "Advanced Python Programming",
        "tags": ["python", "programming", "advanced"],
        "enable": True
    }
