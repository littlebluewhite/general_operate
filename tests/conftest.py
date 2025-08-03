"""
Shared test fixtures for Kafka module testing.

This module provides common fixtures, mocks, and utilities for testing
the Kafka implementation including EventBus, DLQ, and ServiceEventManager.
"""

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Callable, Dict, List
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError, KafkaTimeoutError

from general_operate.kafka.kafka_client import (
    CircuitBreaker,
    EventMessage,
    KafkaAsyncConsumer,
    KafkaAsyncProducer,
    KafkaEventBus,
    MessageStatus,
    ProcessingResult,
    RetryConfig,
)
from general_operate.kafka.manager_config import (
    ConsumerGroupConfig,
    ServiceConfig,
    TopicConfig,
)
from general_operate.kafka.dlq_handler import (
    DLQEvent,
    DLQEventType,
    DLQHandler,
    DLQMetrics,
    TimeBasedRecoveryStrategy,
    ValidationBasedRecoveryStrategy,
)
from general_operate.kafka.service_event_manager import ServiceEventManager


# Test Configuration Constants
TEST_KAFKA_CONFIG = {
    "bootstrap_servers": "localhost:9092",
    "security_protocol": "PLAINTEXT",
    "enable_idempotence": True,
    "max_in_flight_requests_per_connection": 1,
    "retries": 3,
    "request_timeout_ms": 30000,
    "api_version": "auto",
    "acks": "all",  # For producer
    "auto_offset_reset": "earliest",  # For consumer
}

TEST_SSL_KAFKA_CONFIG = {
    **TEST_KAFKA_CONFIG,
    "security_protocol": "SSL",
    "ssl_check_hostname": True,
    "ssl_cafile": "/path/to/ca.pem",
    "ssl_certfile": "/path/to/cert.pem",
    "ssl_keyfile": "/path/to/key.pem",
}

TEST_SASL_KAFKA_CONFIG = {
    **TEST_KAFKA_CONFIG,
    "security_protocol": "SASL_SSL",
    "sasl_mechanism": "PLAIN",
    "sasl_plain_username": "test_user",
    "sasl_plain_password": "test_password",
}


# Pytest Configuration
@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def kafka_config() -> Dict[str, Any]:
    """Basic Kafka configuration for testing."""
    return TEST_KAFKA_CONFIG.copy()


@pytest.fixture
def ssl_kafka_config() -> Dict[str, Any]:
    """SSL-enabled Kafka configuration for testing."""
    return TEST_SSL_KAFKA_CONFIG.copy()


@pytest.fixture
def sasl_kafka_config() -> Dict[str, Any]:
    """SASL-enabled Kafka configuration for testing."""
    return TEST_SASL_KAFKA_CONFIG.copy()


@pytest.fixture
def retry_config() -> RetryConfig:
    """Standard retry configuration for testing."""
    return RetryConfig(
        max_retries=3,
        base_delay=0.1,  # Faster for tests
        max_delay=1.0,   # Shorter for tests
        exponential_base=2.0,
        jitter=False,    # Deterministic for tests
    )


@pytest.fixture
def aggressive_retry_config() -> RetryConfig:
    """Aggressive retry configuration for stress testing."""
    return RetryConfig(
        max_retries=10,
        base_delay=0.01,
        max_delay=0.5,
        exponential_base=1.5,
        jitter=True,
    )


@pytest.fixture
def minimal_retry_config() -> RetryConfig:
    """Minimal retry configuration for quick failure testing."""
    return RetryConfig(
        max_retries=1,
        base_delay=0.01,
        max_delay=0.1,
        exponential_base=2.0,
        jitter=False,
    )


# Event Message Fixtures
@pytest.fixture
def sample_event() -> EventMessage:
    """Sample event message for testing."""
    return EventMessage(
        event_type="test.event",
        tenant_id="test-tenant",
        user_id="test-user",
        data={"key": "value", "number": 42},
        metadata={"source": "test", "version": "1.0"},
        correlation_id=str(uuid.uuid4()),
    )


@pytest.fixture
def invalid_event() -> EventMessage:
    """Invalid event message for error testing."""
    return EventMessage(
        event_type="",  # Invalid empty event type
        tenant_id="",   # Invalid empty tenant
        user_id=None,
        data={},
        metadata=None,
        correlation_id=None,
    )


@pytest.fixture
def large_event() -> EventMessage:
    """Large event message for size testing."""
    large_data = {"items": [{"id": i, "data": "x" * 1000} for i in range(100)]}
    return EventMessage(
        event_type="test.large_event",
        tenant_id="test-tenant",
        user_id="test-user",
        data=large_data,
        correlation_id=str(uuid.uuid4()),
    )


# Processing Result Fixtures
@pytest.fixture
def success_result() -> ProcessingResult:
    """Successful processing result."""
    return ProcessingResult(status=MessageStatus.SUCCESS)


@pytest.fixture
def retry_result() -> ProcessingResult:
    """Retry processing result."""
    return ProcessingResult(
        status=MessageStatus.RETRY,
        error_message="Temporary failure",
    )


@pytest.fixture
def failed_result() -> ProcessingResult:
    """Failed processing result."""
    return ProcessingResult(
        status=MessageStatus.FAILED,
        error_message="Permanent failure",
    )


# Mock Fixtures
@pytest.fixture
def mock_aiokafka_producer():
    """Mock AIOKafkaProducer."""
    producer = AsyncMock(spec=AIOKafkaProducer)
    producer.start = AsyncMock()
    producer.stop = AsyncMock()
    producer.send_and_wait = AsyncMock()
    producer.send = AsyncMock()
    return producer


@pytest.fixture
def mock_aiokafka_consumer():
    """Mock AIOKafkaConsumer."""
    consumer = AsyncMock(spec=AIOKafkaConsumer)
    consumer.start = AsyncMock()
    consumer.stop = AsyncMock()
    consumer.subscribe = AsyncMock()
    consumer.__aiter__ = AsyncMock(return_value=iter([]))
    consumer.__anext__ = AsyncMock()
    return consumer


@pytest.fixture
def mock_circuit_breaker():
    """Mock CircuitBreaker."""
    circuit_breaker = Mock(spec=CircuitBreaker)
    circuit_breaker.is_closed = True
    circuit_breaker.call = AsyncMock()
    circuit_breaker.open_circuit = Mock()
    circuit_breaker.close_circuit = Mock()
    return circuit_breaker


# Handler Fixtures
@pytest.fixture
def success_handler() -> Callable:
    """Handler that always succeeds."""
    async def handler(event: EventMessage) -> ProcessingResult:
        return ProcessingResult(status=MessageStatus.SUCCESS)
    return handler


@pytest.fixture
def failure_handler() -> Callable:
    """Handler that always fails."""
    async def handler(event: EventMessage) -> ProcessingResult:
        return ProcessingResult(
            status=MessageStatus.FAILED,
            error_message="Handler always fails",
        )
    return handler


@pytest.fixture
def retry_handler() -> Callable:
    """Handler that requires retry."""
    async def handler(event: EventMessage) -> ProcessingResult:
        return ProcessingResult(
            status=MessageStatus.RETRY,
            error_message="Handler needs retry",
        )
    return handler


@pytest.fixture
def slow_handler() -> Callable:
    """Handler that is slow (for timeout testing)."""
    async def handler(event: EventMessage) -> ProcessingResult:
        await asyncio.sleep(2.0)  # Longer than typical timeout
        return ProcessingResult(status=MessageStatus.SUCCESS)
    return handler


@pytest.fixture
def exception_handler() -> Callable:
    """Handler that raises exceptions."""
    async def handler(event: EventMessage) -> ProcessingResult:
        raise ValueError("Handler exception")
    return handler


# DLQ Fixtures
@pytest.fixture
def dlq_event() -> DLQEvent:
    """Sample DLQ event for testing."""
    # Create a sample EventMessage first
    sample_event = EventMessage(
        event_type="test.event",
        tenant_id="test-tenant",
        user_id="test-user",
        data={"test": "data"},
        metadata={"error_code": "TEST_001"},
        correlation_id=str(uuid.uuid4()),
    )
    
    return DLQEvent(
        correlation_id=sample_event.correlation_id,
        original_topic="test-topic",
        service_name="test-service",
        dlq_type=DLQEventType.PROCESSING_FAILURE,
        error_message="Test error message",
        retry_count=2,
        event_data=sample_event.data,
        metadata=sample_event.metadata,
        created_at=datetime.now(timezone.utc),
        recovery_attempts=0,
        tenant_id=sample_event.tenant_id,
        original_event=sample_event,
    )


@pytest.fixture
def dlq_metrics() -> DLQMetrics:
    """Sample DLQ metrics for testing."""
    return DLQMetrics(
        total_dlq_events=10,
        recovered_events=3,
        permanently_failed_events=2,
        events_by_type={"PROCESSING_ERROR": 5, "TIMEOUT": 3, "VALIDATION_ERROR": 2},
        events_by_service={"service-a": 6, "service-b": 4},
        last_processed_at=datetime.now(timezone.utc).isoformat(),
    )


# Service Configuration Fixtures
@pytest.fixture
def service_config() -> ServiceConfig:
    """Standard service configuration."""
    return ServiceConfig(
        service_name="test-service",
        retry_config=RetryConfig(),
        enable_dlq=True,
        dlq_topic_suffix=".dlq",
        circuit_breaker_threshold=5,
        circuit_breaker_timeout=30.0,
        topics=["test-topic"],
        consumer_groups=[
            ConsumerGroupConfig(
                group_id="test-group",
                topics=["test-topic"],
            )
        ],
    )


@pytest.fixture
def topic_config() -> TopicConfig:
    """Standard topic configuration."""
    return TopicConfig(
        name="test-topic",
        partitions=3,
        replication_factor=1,
        config={"cleanup.policy": "delete", "retention.ms": "86400000"},
    )


@pytest.fixture
def consumer_group_config() -> ConsumerGroupConfig:
    """Standard consumer group configuration."""
    return ConsumerGroupConfig(
        group_id="test-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        session_timeout_ms=30000,
        max_poll_records=500,
    )


# Async Context Managers for Testing
@pytest.fixture
async def kafka_event_bus(kafka_config: Dict[str, Any]) -> AsyncGenerator[KafkaEventBus, None]:
    """Kafka event bus instance with proper lifecycle management."""
    event_bus = KafkaEventBus(kafka_config, "test-service")
    try:
        # Mock the underlying components to avoid real Kafka dependency
        event_bus.producer = AsyncMock()
        event_bus.producer.start = AsyncMock()
        event_bus.producer.stop = AsyncMock()
        event_bus.producer.send_event = AsyncMock()
        
        await event_bus.start()
        yield event_bus
    finally:
        await event_bus.stop()


@pytest.fixture
async def dlq_handler(kafka_event_bus: KafkaEventBus) -> AsyncGenerator[DLQHandler, None]:
    """DLQ handler instance with proper lifecycle management."""
    handler = DLQHandler(
        event_bus=kafka_event_bus,
        dlq_topics=["test-topic.dlq"],
        enable_periodic_reporting=False,  # Disable for tests
    )
    try:
        await handler.start()
        yield handler
    finally:
        await handler.stop()


@pytest.fixture
async def service_event_manager(kafka_config: Dict[str, Any], service_config: ServiceConfig) -> AsyncGenerator[ServiceEventManager, None]:
    """Service event manager instance with proper lifecycle management."""
    manager = ServiceEventManager(kafka_config, service_config)
    try:
        # Mock underlying components
        manager.event_bus = AsyncMock()
        manager.event_bus.start = AsyncMock()
        manager.event_bus.stop = AsyncMock()
        manager.dlq_handler = AsyncMock()
        manager.dlq_handler.start = AsyncMock()
        manager.dlq_handler.stop = AsyncMock()
        
        await manager.start()
        yield manager
    finally:
        await manager.stop()


# Performance Testing Utilities
@pytest.fixture
def performance_timer():
    """Utility for measuring execution time in tests."""
    import time
    
    class Timer:
        def __init__(self):
            self.start_time = None
            self.end_time = None
        
        def start(self):
            self.start_time = time.perf_counter()
        
        def stop(self):
            self.end_time = time.perf_counter()
        
        @property
        def elapsed(self) -> float:
            if self.start_time and self.end_time:
                return self.end_time - self.start_time
            return 0.0
    
    return Timer()


# Test Data Generators
def generate_test_events(count: int) -> List[EventMessage]:
    """Generate a list of test events."""
    events = []
    for i in range(count):
        event = EventMessage(
            event_type=f"test.event.{i}",
            tenant_id=f"tenant-{i % 3}",  # Distribute across 3 tenants
            user_id=f"user-{i}",
            data={"sequence": i, "batch": count},
            correlation_id=str(uuid.uuid4()),
        )
        events.append(event)
    return events


def generate_dlq_events(count: int) -> List[DLQEvent]:
    """Generate a list of test DLQ events."""
    events = []
    for i in range(count):
        dlq_event = DLQEvent(
            correlation_id=str(uuid.uuid4()),
            original_topic=f"topic-{i % 5}",
            service_name=f"service-{i % 3}",
            dlq_type=list(DLQEventType)[i % len(DLQEventType)],
            error_message=f"Error {i}",
            retry_count=i % 4,
            event_data={"sequence": i},
            created_at=datetime.now(timezone.utc),
        )
        events.append(dlq_event)
    return events


# Kafka Error Simulation
class KafkaErrorSimulator:
    """Utility class for simulating various Kafka errors in tests."""
    
    @staticmethod
    def simulate_connection_error():
        """Simulate Kafka connection error."""
        raise KafkaError("Connection failed")
    
    @staticmethod
    def simulate_timeout_error():
        """Simulate Kafka timeout error."""
        raise KafkaTimeoutError("Request timed out")
    
    @staticmethod
    async def simulate_intermittent_error(success_rate: float = 0.7):
        """Simulate intermittent errors based on success rate."""
        import random
        if random.random() > success_rate:
            raise KafkaError("Intermittent failure")


@pytest.fixture
def kafka_error_simulator():
    """Kafka error simulator for testing error scenarios."""
    return KafkaErrorSimulator()


# Test Markers
pytest_plugins = ["pytest_asyncio"]


# Custom Assertions
def assert_event_message_equal(actual: EventMessage, expected: EventMessage):
    """Assert that two EventMessage instances are equal."""
    assert actual.event_type == expected.event_type
    assert actual.tenant_id == expected.tenant_id
    assert actual.user_id == expected.user_id
    assert actual.data == expected.data
    assert actual.metadata == expected.metadata
    if expected.correlation_id:
        assert actual.correlation_id == expected.correlation_id


def assert_processing_result_equal(actual: ProcessingResult, expected: ProcessingResult):
    """Assert that two ProcessingResult instances are equal."""
    assert actual.status == expected.status
    assert actual.error_message == expected.error_message


# Async Test Utilities
async def wait_for_condition(condition_func: Callable[[], bool], timeout: float = 5.0, interval: float = 0.1):
    """Wait for a condition to become true with timeout."""
    import time
    start_time = time.time()
    while time.time() - start_time < timeout:
        if condition_func():
            return True
        await asyncio.sleep(interval)
    return False


# Test Resource Cleanup
@pytest.fixture(autouse=True)
async def cleanup_async_tasks():
    """Automatically cleanup any remaining async tasks after each test."""
    yield
    
    # Cancel any remaining tasks
    tasks = [task for task in asyncio.all_tasks() if not task.done()]
    if tasks:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)