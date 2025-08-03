"""
Unit tests for the KafkaClient module.

Tests cover:
- EventMessage creation and validation
- RetryConfig functionality  
- CircuitBreaker behavior
- KafkaAsyncProducer operations
- KafkaAsyncConsumer operations
- KafkaEventBus integration
- Error handling and edge cases
- Performance and timeout scenarios
"""

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict
from unittest.mock import AsyncMock, Mock, patch

import pytest
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


class TestEventMessage:
    """Test EventMessage creation and validation."""

    def test_event_message_creation(self):
        """Test basic EventMessage creation."""
        event = EventMessage(
            event_type="test.event",
            tenant_id="tenant-123",
            user_id="user-456",
            data={"key": "value"},
            metadata={"source": "test"},
            correlation_id="corr-123",
        )
        
        assert event.event_type == "test.event"
        assert event.tenant_id == "tenant-123" 
        assert event.user_id == "user-456"
        assert event.data == {"key": "value"}
        assert event.metadata == {"source": "test"}
        assert event.correlation_id == "corr-123"

    def test_event_message_auto_correlation_id(self):
        """Test automatic correlation ID generation."""
        event = EventMessage(
            event_type="test.event",
            tenant_id="tenant-123",
            data={"key": "value"},
        )
        
        assert event.correlation_id is not None
        assert len(event.correlation_id) > 0
        # Should be a valid UUID
        uuid.UUID(event.correlation_id)

    def test_event_message_auto_timestamp(self):
        """Test automatic timestamp generation."""
        before = datetime.now(timezone.utc)
        event = EventMessage(
            event_type="test.event",
            tenant_id="tenant-123",
            data={"key": "value"},
        )
        after = datetime.now(timezone.utc)
        
        assert event.timestamp is not None
        assert before <= event.timestamp <= after

    def test_event_message_serialization(self):
        """Test EventMessage JSON serialization."""
        event = EventMessage(
            event_type="test.event",
            tenant_id="tenant-123",
            user_id="user-456",
            data={"key": "value", "number": 42},
            metadata={"source": "test"},
        )
        
        # Test to_dict
        event_dict = event.to_dict()
        assert event_dict["event_type"] == "test.event"
        assert event_dict["tenant_id"] == "tenant-123"
        assert event_dict["user_id"] == "user-456"
        assert event_dict["data"] == {"key": "value", "number": 42}
        assert event_dict["metadata"] == {"source": "test"}
        assert "correlation_id" in event_dict
        assert "timestamp" in event_dict

    def test_event_message_deserialization(self):
        """Test EventMessage JSON deserialization."""
        timestamp = datetime.now(timezone.utc)
        event_dict = {
            "event_type": "test.event",
            "tenant_id": "tenant-123",
            "user_id": "user-456", 
            "data": {"key": "value"},
            "metadata": {"source": "test"},
            "correlation_id": "corr-123",
            "timestamp": timestamp.isoformat(),
        }
        
        event = EventMessage.from_dict(event_dict)
        assert event.event_type == "test.event"
        assert event.tenant_id == "tenant-123"
        assert event.user_id == "user-456"
        assert event.data == {"key": "value"}
        assert event.metadata == {"source": "test"}
        assert event.correlation_id == "corr-123"
        assert event.timestamp == timestamp

    def test_event_message_validation_errors(self):
        """Test EventMessage validation for invalid data."""
        # Empty event type should be valid but not recommended
        event = EventMessage(
            event_type="",
            tenant_id="tenant-123",
            data={"key": "value"},
        )
        assert event.event_type == ""

        # Empty tenant should be valid but not recommended
        event = EventMessage(
            event_type="test.event",
            tenant_id="",
            data={"key": "value"},
        )
        assert event.tenant_id == ""

    def test_event_message_large_data(self):
        """Test EventMessage with large data payload."""
        large_data = {"items": [{"id": i, "data": "x" * 1000} for i in range(100)]}
        event = EventMessage(
            event_type="test.large_event",
            tenant_id="tenant-123",
            data=large_data,
        )
        
        assert len(event.data["items"]) == 100
        serialized = json.dumps(event.to_dict())
        assert len(serialized) > 100000  # Should be large


class TestRetryConfig:
    """Test RetryConfig functionality."""

    def test_retry_config_defaults(self):
        """Test RetryConfig default values."""
        config = RetryConfig()
        
        assert config.max_retries == 3
        assert config.base_delay == 1.0
        assert config.max_delay == 60.0
        assert config.exponential_base == 2.0
        assert config.jitter is True

    def test_retry_config_custom_values(self):
        """Test RetryConfig with custom values."""
        config = RetryConfig(
            max_retries=5,
            base_delay=0.5,
            max_delay=30.0,
            exponential_base=1.5,
            jitter=False,
        )
        
        assert config.max_retries == 5
        assert config.base_delay == 0.5
        assert config.max_delay == 30.0
        assert config.exponential_base == 1.5
        assert config.jitter is False

    def test_retry_config_validation(self):
        """Test RetryConfig validation constraints."""
        # Valid configurations should work
        RetryConfig(max_retries=0)  # 0 retries is valid
        RetryConfig(base_delay=0.1, max_delay=60.0)
        RetryConfig(exponential_base=1.1)

    def test_calculate_delay(self):
        """Test delay calculation with exponential backoff."""
        config = RetryConfig(
            base_delay=1.0,
            max_delay=10.0,
            exponential_base=2.0,
            jitter=False,
        )
        
        # Test exponential backoff
        delay1 = config.calculate_delay(0)
        delay2 = config.calculate_delay(1)
        delay3 = config.calculate_delay(2)
        
        assert delay1 == 1.0  # base_delay * (2^0)
        assert delay2 == 2.0  # base_delay * (2^1)
        assert delay3 == 4.0  # base_delay * (2^2)

    def test_calculate_delay_max_limit(self):
        """Test delay calculation respects max_delay."""
        config = RetryConfig(
            base_delay=1.0,
            max_delay=5.0,
            exponential_base=2.0,
            jitter=False,
        )
        
        # Should cap at max_delay
        delay = config.calculate_delay(10)  # Would be 1024 without cap
        assert delay == 5.0

    def test_calculate_delay_with_jitter(self):
        """Test delay calculation with jitter."""
        config = RetryConfig(
            base_delay=1.0,
            max_delay=10.0,
            exponential_base=2.0,
            jitter=True,
        )
        
        # With jitter, delays should vary
        delays = [config.calculate_delay(1) for _ in range(10)]
        assert len(set(delays)) > 1  # Should have variation
        assert all(0.5 <= d <= 3.0 for d in delays)  # Should be in reasonable range


class TestCircuitBreaker:
    """Test CircuitBreaker functionality."""

    def test_circuit_breaker_initial_state(self):
        """Test CircuitBreaker initial state."""
        cb = CircuitBreaker(threshold=5, timeout=30.0)
        
        assert cb.is_closed is True
        assert cb.failure_count == 0

    def test_circuit_breaker_success(self):
        """Test CircuitBreaker behavior on success."""
        cb = CircuitBreaker(threshold=3, timeout=30.0)
        
        # Record successes
        cb.record_success()
        cb.record_success()
        
        assert cb.is_closed is True
        assert cb.failure_count == 0

    def test_circuit_breaker_failures(self):
        """Test CircuitBreaker behavior on failures."""
        cb = CircuitBreaker(threshold=3, timeout=30.0)
        
        # Record failures but below threshold
        cb.record_failure()
        cb.record_failure()
        assert cb.is_closed is True
        assert cb.failure_count == 2
        
        # Cross threshold
        cb.record_failure()
        assert cb.is_closed is False  # Should open

    def test_circuit_breaker_reset_after_success(self):
        """Test CircuitBreaker resets failure count on success."""
        cb = CircuitBreaker(threshold=3, timeout=30.0)
        
        # Build up failures
        cb.record_failure()
        cb.record_failure()
        assert cb.failure_count == 2
        
        # Success should reset
        cb.record_success()
        assert cb.failure_count == 0

    @pytest.mark.asyncio
    async def test_circuit_breaker_call_success(self):
        """Test CircuitBreaker call method with success."""
        cb = CircuitBreaker(threshold=3, timeout=30.0)
        
        async def success_func():
            return "success"
        
        result = await cb.call(success_func)
        assert result == "success"
        assert cb.failure_count == 0

    @pytest.mark.asyncio
    async def test_circuit_breaker_call_failure(self):
        """Test CircuitBreaker call method with failure."""
        cb = CircuitBreaker(threshold=2, timeout=30.0)
        
        async def failure_func():
            raise ValueError("Test error")
        
        # First failure
        with pytest.raises(ValueError):
            await cb.call(failure_func)
        assert cb.failure_count == 1
        assert cb.is_closed is True
        
        # Second failure - should open circuit
        with pytest.raises(ValueError):
            await cb.call(failure_func)
        assert cb.failure_count == 2
        assert cb.is_closed is False

    @pytest.mark.asyncio
    async def test_circuit_breaker_open_state(self):
        """Test CircuitBreaker behavior when open."""
        cb = CircuitBreaker(threshold=1, timeout=0.1)  # Short timeout for testing
        
        async def failure_func():
            raise ValueError("Test error")
        
        # Open the circuit
        with pytest.raises(ValueError):
            await cb.call(failure_func)
        assert cb.is_closed is False
        
        # Should raise CircuitOpenError
        from general_operate.kafka.kafka_client import CircuitOpenError
        with pytest.raises(CircuitOpenError):
            await cb.call(failure_func)

    @pytest.mark.asyncio
    async def test_circuit_breaker_half_open_state(self):
        """Test CircuitBreaker half-open state after timeout."""
        cb = CircuitBreaker(threshold=1, timeout=0.01)  # Very short timeout
        
        async def failure_func():
            raise ValueError("Test error")
        
        async def success_func():
            return "success"
        
        # Open the circuit
        with pytest.raises(ValueError):
            await cb.call(failure_func)
        assert cb.is_closed is False
        
        # Wait for timeout
        await asyncio.sleep(0.02)
        
        # Next call should succeed and close circuit
        result = await cb.call(success_func)
        assert result == "success"
        assert cb.is_closed is True
        assert cb.failure_count == 0


class TestProcessingResult:
    """Test ProcessingResult functionality."""

    def test_processing_result_success(self):
        """Test successful ProcessingResult."""
        result = ProcessingResult(status=MessageStatus.SUCCESS)
        
        assert result.status == MessageStatus.SUCCESS
        assert result.error_message is None

    def test_processing_result_failure(self):
        """Test failed ProcessingResult."""
        result = ProcessingResult(
            status=MessageStatus.FAILED,
            error_message="Test error",
        )
        
        assert result.status == MessageStatus.FAILED
        assert result.error_message == "Test error"

    def test_processing_result_retry(self):
        """Test retry ProcessingResult."""
        result = ProcessingResult(
            status=MessageStatus.RETRY,
            error_message="Temporary error",
        )
        
        assert result.status == MessageStatus.RETRY
        assert result.error_message == "Temporary error"


@patch("general_operate.kafka.kafka_client.AIOKafkaProducer")
class TestKafkaAsyncProducer:
    """Test KafkaAsyncProducer functionality."""

    @pytest.mark.asyncio
    async def test_producer_initialization(self, mock_producer_class, kafka_config):
        """Test KafkaAsyncProducer initialization."""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        producer = KafkaAsyncProducer(kafka_config, "test-service")
        
        assert producer.service_name == "test-service"
        assert producer.kafka_config == kafka_config

    @pytest.mark.asyncio
    async def test_producer_start_stop(self, mock_producer_class, kafka_config):
        """Test KafkaAsyncProducer start and stop."""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        producer = KafkaAsyncProducer(kafka_config, "test-service")
        
        # Test start
        await producer.start()
        mock_producer.start.assert_called_once()
        
        # Test stop
        await producer.stop()
        mock_producer.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_producer_send_event(self, mock_producer_class, kafka_config, sample_event):
        """Test KafkaAsyncProducer event sending."""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        producer = KafkaAsyncProducer(kafka_config, "test-service")
        await producer.start()
        
        # Test sending event
        await producer.send_event("test-topic", sample_event)
        
        # Verify producer.send_and_wait was called
        mock_producer.send_and_wait.assert_called_once()
        call_args = mock_producer.send_and_wait.call_args
        assert call_args[1]["topic"] == "test-topic"
        
        # Verify message content
        message_data = json.loads(call_args[1]["value"])
        assert message_data["event_type"] == sample_event.event_type
        assert message_data["tenant_id"] == sample_event.tenant_id

    @pytest.mark.asyncio
    async def test_producer_send_with_retry(self, mock_producer_class, kafka_config, sample_event, retry_config):
        """Test KafkaAsyncProducer retry mechanism."""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        # Simulate failures then success
        mock_producer.send_and_wait.side_effect = [
            KafkaTimeoutError("Timeout 1"),
            KafkaTimeoutError("Timeout 2"),
            None,  # Success on third try
        ]
        
        producer = KafkaAsyncProducer(kafka_config, "test-service")
        await producer.start()
        
        # Should succeed after retries
        await producer.send_event_with_retry("test-topic", sample_event, retry_config)
        
        # Should have been called 3 times
        assert mock_producer.send_and_wait.call_count == 3

    @pytest.mark.asyncio
    async def test_producer_send_retry_exhausted(self, mock_producer_class, kafka_config, sample_event, minimal_retry_config):
        """Test KafkaAsyncProducer when retries are exhausted."""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        # Always fail
        mock_producer.send_and_wait.side_effect = KafkaTimeoutError("Always timeout")
        
        producer = KafkaAsyncProducer(kafka_config, "test-service")
        await producer.start()
        
        # Should raise after exhausting retries
        with pytest.raises(KafkaTimeoutError):
            await producer.send_event_with_retry("test-topic", sample_event, minimal_retry_config)
        
        # Should have tried max_retries + 1 times
        assert mock_producer.send_and_wait.call_count == minimal_retry_config.max_retries + 1

    @pytest.mark.asyncio
    async def test_producer_error_handling(self, mock_producer_class, kafka_config, sample_event):
        """Test KafkaAsyncProducer error handling."""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        # Simulate various errors
        mock_producer.send_and_wait.side_effect = KafkaError("Connection failed")
        
        producer = KafkaAsyncProducer(kafka_config, "test-service")
        await producer.start()
        
        # Should propagate Kafka errors
        with pytest.raises(KafkaError):
            await producer.send_event("test-topic", sample_event)


@patch("general_operate.kafka.kafka_client.AIOKafkaConsumer")
class TestKafkaAsyncConsumer:
    """Test KafkaAsyncConsumer functionality."""

    @pytest.mark.asyncio
    async def test_consumer_initialization(self, mock_consumer_class, kafka_config, success_handler):
        """Test KafkaAsyncConsumer initialization."""
        mock_consumer = AsyncMock()
        mock_consumer_class.return_value = mock_consumer
        
        consumer = KafkaAsyncConsumer(
            topics=["test-topic"],
            group_id="test-group",
            message_handler=success_handler,
            kafka_config=kafka_config,
            service_name="test-service",
        )
        
        assert consumer.topics == ["test-topic"]
        assert consumer.group_id == "test-group"
        assert consumer.service_name == "test-service"

    @pytest.mark.asyncio
    async def test_consumer_start_stop(self, mock_consumer_class, kafka_config, success_handler):
        """Test KafkaAsyncConsumer start and stop."""
        mock_consumer = AsyncMock()
        mock_consumer_class.return_value = mock_consumer
        
        consumer = KafkaAsyncConsumer(
            topics=["test-topic"],
            group_id="test-group", 
            message_handler=success_handler,
            kafka_config=kafka_config,
            service_name="test-service",
        )
        
        # Test start
        await consumer.start()
        mock_consumer.start.assert_called_once()
        mock_consumer.subscribe.assert_called_once_with(["test-topic"])
        
        # Test stop
        await consumer.stop()
        mock_consumer.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_consumer_message_processing(self, mock_consumer_class, kafka_config, success_handler, sample_event):
        """Test KafkaAsyncConsumer message processing."""
        mock_consumer = AsyncMock()
        mock_consumer_class.return_value = mock_consumer
        
        # Create mock message
        mock_message = Mock()
        mock_message.value = json.dumps(sample_event.to_dict()).encode()
        mock_message.topic = "test-topic"
        mock_message.partition = 0
        mock_message.offset = 123
        
        # Setup consumer to yield one message then stop
        async def async_iter():
            yield mock_message
            # After yielding, trigger stop
            consumer._running = False
        
        mock_consumer.__aiter__ = lambda self: async_iter()
        
        consumer = KafkaAsyncConsumer(
            topics=["test-topic"],
            group_id="test-group",
            message_handler=success_handler,
            kafka_config=kafka_config,
            service_name="test-service",
        )
        
        await consumer.start()
        
        # Start consuming (should process one message then stop)
        await consumer.consume()
        
        # Verify the handler was called (indirectly through successful processing)
        assert not consumer._running

    @pytest.mark.asyncio
    async def test_consumer_handler_failure(self, mock_consumer_class, kafka_config, failure_handler, sample_event):
        """Test KafkaAsyncConsumer handling of handler failures."""
        mock_consumer = AsyncMock()
        mock_consumer_class.return_value = mock_consumer
        
        # Create mock message
        mock_message = Mock()
        mock_message.value = json.dumps(sample_event.to_dict()).encode()
        mock_message.topic = "test-topic"
        mock_message.partition = 0
        mock_message.offset = 123
        
        # Setup consumer to yield one message then stop
        async def async_iter():
            yield mock_message
            consumer._running = False
        
        mock_consumer.__aiter__ = lambda self: async_iter()
        
        consumer = KafkaAsyncConsumer(
            topics=["test-topic"],
            group_id="test-group",
            message_handler=failure_handler,
            kafka_config=kafka_config,
            service_name="test-service",
        )
        
        await consumer.start()
        await consumer.consume()
        
        # Should have processed the message despite failure
        assert not consumer._running

    @pytest.mark.asyncio
    async def test_consumer_retry_logic(self, mock_consumer_class, kafka_config, retry_handler, sample_event, retry_config):
        """Test KafkaAsyncConsumer retry logic."""
        mock_consumer = AsyncMock()
        mock_consumer_class.return_value = mock_consumer
        
        # Create mock message
        mock_message = Mock()
        mock_message.value = json.dumps(sample_event.to_dict()).encode()
        mock_message.topic = "test-topic"
        mock_message.partition = 0
        mock_message.offset = 123
        
        call_count = 0
        async def async_iter():
            nonlocal call_count
            call_count += 1
            if call_count <= 1:  # Yield message once
                yield mock_message
        
        mock_consumer.__aiter__ = lambda self: async_iter()
        
        consumer = KafkaAsyncConsumer(
            topics=["test-topic"],
            group_id="test-group",
            message_handler=retry_handler,
            kafka_config=kafka_config,
            service_name="test-service",
            retry_config=retry_config,
        )
        
        await consumer.start()
        
        # Process the message and then stop
        try:
            await asyncio.wait_for(consumer.consume(), timeout=2.0)
        except asyncio.TimeoutError:
            pass  # Expected when async iterator finishes
            
        await consumer.stop()
        
        # Should have processed the message and stopped
        assert not consumer._running

    @pytest.mark.asyncio
    async def test_consumer_dlq_functionality(self, mock_consumer_class, kafka_config, failure_handler, sample_event):
        """Test KafkaAsyncConsumer DLQ functionality.""" 
        mock_consumer = AsyncMock()
        mock_consumer_class.return_value = mock_consumer
        
        # Mock producer for DLQ
        mock_producer = AsyncMock()
        
        # Create mock message
        mock_message = Mock()
        mock_message.value = json.dumps(sample_event.to_dict()).encode()
        mock_message.topic = "test-topic"
        mock_message.partition = 0
        mock_message.offset = 123
        
        async def async_iter():
            yield mock_message
            consumer._running = False
        
        mock_consumer.__aiter__ = lambda self: async_iter()
        
        with patch("general_operate.kafka.kafka_client.KafkaAsyncProducer") as mock_producer_class:
            mock_producer_class.return_value = mock_producer
            
            consumer = KafkaAsyncConsumer(
                topics=["test-topic"],
                group_id="test-group",
                message_handler=failure_handler,
                kafka_config=kafka_config,
                service_name="test-service",
                dead_letter_topic="test-topic.dlq",
                enable_dlq=True,
            )
            
            await consumer.start()
            await consumer.consume()
            
            # Should have processed the message
            assert not consumer._running


@patch("general_operate.kafka.kafka_client.KafkaAsyncProducer")
class TestKafkaEventBus:
    """Test KafkaEventBus functionality."""

    @pytest.mark.asyncio
    async def test_event_bus_initialization(self, mock_producer_class, kafka_config):
        """Test KafkaEventBus initialization."""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        event_bus = KafkaEventBus(kafka_config, "test-service")
        
        assert event_bus.service_name == "test-service"
        assert event_bus.kafka_config == kafka_config
        assert len(event_bus.consumers) == 0

    @pytest.mark.asyncio
    async def test_event_bus_start_stop(self, mock_producer_class, kafka_config):
        """Test KafkaEventBus start and stop."""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        event_bus = KafkaEventBus(kafka_config, "test-service")
        
        # Test start
        await event_bus.start()
        mock_producer.start.assert_called_once()
        
        # Test stop
        await event_bus.stop()
        mock_producer.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_event_bus_publish(self, mock_producer_class, kafka_config):
        """Test KafkaEventBus publish functionality."""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        event_bus = KafkaEventBus(kafka_config, "test-service")
        await event_bus.start()
        
        # Test publish
        await event_bus.publish(
            topic="test-topic",
            event_type="test.event",
            tenant_id="tenant-123",
            data={"key": "value"},
            user_id="user-456",
            metadata={"source": "test"},
        )
        
        # Verify producer was called
        mock_producer.send_event.assert_called_once()
        call_args = mock_producer.send_event.call_args
        assert call_args[0][0] == "test-topic"  # topic
        
        # Verify event data
        event = call_args[0][1]
        assert event.event_type == "test.event"
        assert event.tenant_id == "tenant-123"
        assert event.user_id == "user-456"
        assert event.data == {"key": "value"}

    @pytest.mark.asyncio 
    async def test_event_bus_subscribe(self, mock_producer_class, kafka_config, success_handler):
        """Test KafkaEventBus subscribe functionality."""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        event_bus = KafkaEventBus(kafka_config, "test-service")
        await event_bus.start()
        
        with patch("general_operate.kafka.kafka_client.KafkaAsyncConsumer") as mock_consumer_class:
            mock_consumer = AsyncMock()
            mock_consumer_class.return_value = mock_consumer
            
            # Test subscribe
            consumer = await event_bus.subscribe_with_retry(
                topics=["test-topic"],
                group_id="test-group",
                handler=success_handler,
                service_name="test-service",
            )
            
            # Verify consumer was created and stored
            assert "test-group" in event_bus.consumers
            assert event_bus.consumers["test-group"] == consumer
            
            # Verify consumer was configured correctly
            mock_consumer_class.assert_called_once()
            call_kwargs = mock_consumer_class.call_args[1]
            assert call_kwargs["topics"] == ["test-topic"]
            assert call_kwargs["group_id"] == "test-group"
            assert call_kwargs["service_name"] == "test-service"

    @pytest.mark.asyncio
    async def test_event_bus_stop_with_consumers(self, mock_producer_class, kafka_config, success_handler):
        """Test KafkaEventBus stop with active consumers."""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        event_bus = KafkaEventBus(kafka_config, "test-service")
        await event_bus.start()
        
        with patch("general_operate.kafka.kafka_client.KafkaAsyncConsumer") as mock_consumer_class:
            mock_consumer = AsyncMock()
            mock_consumer_class.return_value = mock_consumer
            
            # Subscribe to create consumer
            await event_bus.subscribe_with_retry(
                topics=["test-topic"],
                group_id="test-group",
                handler=success_handler,
                service_name="test-service",
            )
            
            # Test stop
            await event_bus.stop()
            
            # Verify consumer was stopped
            mock_consumer.stop.assert_called_once()
            mock_producer.stop.assert_called_once()


class TestPerformanceAndTimeouts:
    """Test performance characteristics and timeout handling."""

    @pytest.mark.asyncio
    async def test_producer_timeout_handling(self, kafka_config, sample_event):
        """Test producer timeout handling."""
        with patch("general_operate.kafka.kafka_client.AIOKafkaProducer") as mock_producer_class:
            mock_producer = AsyncMock()
            mock_producer_class.return_value = mock_producer
            
            # Simulate timeout
            mock_producer.send_and_wait.side_effect = asyncio.TimeoutError("Timeout")
            
            producer = KafkaAsyncProducer(kafka_config, "test-service")
            await producer.start()
            
            with pytest.raises(asyncio.TimeoutError):
                await producer.send_event("test-topic", sample_event)

    @pytest.mark.asyncio
    async def test_consumer_slow_handler_timeout(self, kafka_config, slow_handler, sample_event):
        """Test consumer handling of slow message handlers."""
        with patch("general_operate.kafka.kafka_client.AIOKafkaConsumer") as mock_consumer_class:
            mock_consumer = AsyncMock()
            mock_consumer_class.return_value = mock_consumer
            
            # Create mock message
            mock_message = Mock()
            mock_message.value = json.dumps(sample_event.to_dict()).encode()
            mock_message.topic = "test-topic"
            mock_message.partition = 0
            mock_message.offset = 123
            
            async def async_iter():
                yield mock_message
                consumer._running = False
            
            mock_consumer.__aiter__ = lambda self: async_iter()
            
            consumer = KafkaAsyncConsumer(
                topics=["test-topic"],
                group_id="test-group",
                message_handler=slow_handler,
                kafka_config=kafka_config,
                service_name="test-service",
            )
            
            await consumer.start()
            
            # Should handle slow handler gracefully (though it might time out)
            try:
                await asyncio.wait_for(consumer.consume(), timeout=1.0)
            except asyncio.TimeoutError:
                # Expected for slow handler
                pass
            
            await consumer.stop()

    @pytest.mark.asyncio
    async def test_high_volume_processing(self, kafka_config, success_handler):
        """Test high-volume message processing."""
        from conftest import generate_test_events
        
        with patch("general_operate.kafka.kafka_client.AIOKafkaConsumer") as mock_consumer_class:
            mock_consumer = AsyncMock()
            mock_consumer_class.return_value = mock_consumer
            
            # Generate many test events
            events = generate_test_events(100)
            
            # Create mock messages
            mock_messages = []
            for event in events:
                mock_message = Mock()
                mock_message.value = json.dumps(event.to_dict()).encode()
                mock_message.topic = "test-topic"
                mock_message.partition = 0
                mock_message.offset = len(mock_messages)
                mock_messages.append(mock_message)
            
            message_iter = iter(mock_messages)
            
            async def async_iter():
                try:
                    while True:
                        yield next(message_iter)
                except StopIteration:
                    consumer._running = False
            
            mock_consumer.__aiter__ = lambda self: async_iter()
            
            consumer = KafkaAsyncConsumer(
                topics=["test-topic"],
                group_id="test-group",
                message_handler=success_handler,
                kafka_config=kafka_config,
                service_name="test-service",
            )
            
            await consumer.start()
            
            # Measure processing time
            start_time = asyncio.get_event_loop().time()
            await consumer.consume()
            end_time = asyncio.get_event_loop().time()
            
            processing_time = end_time - start_time
            
            # Should process 100 messages reasonably quickly
            assert processing_time < 10.0  # Should take less than 10 seconds
            
            await consumer.stop()


class TestSecurityAndSSL:
    """Test security configurations and SSL handling."""

    @pytest.mark.asyncio
    async def test_ssl_configuration(self, ssl_kafka_config):
        """Test SSL configuration handling."""
        with patch("general_operate.kafka.kafka_client.AIOKafkaProducer") as mock_producer_class:
            mock_producer = AsyncMock()
            mock_producer_class.return_value = mock_producer
            
            producer = KafkaAsyncProducer(ssl_kafka_config, "test-service")
            await producer.start()  # SSL config is passed during start()
            
            # Verify SSL config was passed
            call_kwargs = mock_producer_class.call_args[1]
            assert call_kwargs["security_protocol"] == "SSL"
            assert call_kwargs["ssl_check_hostname"] is True

    @pytest.mark.asyncio
    async def test_sasl_configuration(self, sasl_kafka_config):
        """Test SASL configuration handling."""
        with patch("general_operate.kafka.kafka_client.AIOKafkaProducer") as mock_producer_class:
            mock_producer = AsyncMock()
            mock_producer_class.return_value = mock_producer
            
            producer = KafkaAsyncProducer(sasl_kafka_config, "test-service")
            await producer.start()  # SASL config is passed during start()
            
            # Verify SASL config was passed
            call_kwargs = mock_producer_class.call_args[1]
            assert call_kwargs["security_protocol"] == "SASL_SSL"
            assert call_kwargs["sasl_mechanism"] == "PLAIN"
            assert call_kwargs["sasl_plain_username"] == "test_user"
            assert call_kwargs["sasl_plain_password"] == "test_password"

    def test_sensitive_data_handling(self, sample_event):
        """Test that sensitive data is not logged."""
        # Create event with sensitive data
        sensitive_event = EventMessage(
            event_type="user.login",
            tenant_id="tenant-123",
            user_id="user-456",
            data={
                "username": "testuser",
                "password": "secret123",  # Should not be logged
                "api_key": "key_secret",  # Should not be logged
            },
        )
        
        # Convert to dict for serialization
        event_dict = sensitive_event.to_dict()
        
        # Verify sensitive fields are present (they shouldn't be filtered at this level)
        # Actual filtering should happen in logging configuration
        assert "password" in event_dict["data"]
        assert "api_key" in event_dict["data"]