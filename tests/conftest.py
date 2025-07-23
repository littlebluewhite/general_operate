"""
Global test configuration and fixtures for general_operate testing.
"""
import asyncio
from collections.abc import Generator
from unittest.mock import AsyncMock, MagicMock

import pytest
import redis.asyncio as redis


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop]:
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def mock_db() -> AsyncMock:
    """Mock database client for testing."""
    db = AsyncMock()
    db.engine_type = "postgresql"  # Default to PostgreSQL for testing
    db.get_engine = MagicMock()
    return db


@pytest.fixture
def mock_redis() -> AsyncMock:
    """Mock Redis client for testing."""
    redis_client = AsyncMock(spec=redis.Redis)
    return redis_client


@pytest.fixture
def mock_influxdb() -> AsyncMock:
    """Mock InfluxDB client for testing."""
    influx_client = AsyncMock()
    influx_client.bucket = "test_bucket"
    return influx_client