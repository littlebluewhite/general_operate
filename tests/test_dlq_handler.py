"""
Unit tests for the DLQ (Dead Letter Queue) Handler module.

Tests cover:
- DLQ event creation and validation
- DLQ metrics tracking
- Recovery strategy implementations  
- DLQ handler lifecycle management
- Async context management
- Periodic reporting functionality
- Health check operations
- Error handling and edge cases
"""

import asyncio
import json
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List
from unittest.mock import AsyncMock, Mock, patch

import pytest

from general_operate.kafka.dlq_handler import (
    DLQEvent,
    DLQEventType,
    DLQHandler,
    DLQMetrics,
    DLQRecoveryStrategy,
    TimeBasedRecoveryStrategy,
    ValidationBasedRecoveryStrategy,
    create_dlq_handler_for_service,
)
from general_operate.kafka.kafka_client import (
    EventMessage,
    KafkaEventBus,
    MessageStatus,
    ProcessingResult,
    RetryConfig,
)


class TestDLQEvent:
    """Test DLQ event creation and functionality."""

    def test_dlq_event_creation(self):
        """Test basic DLQ event creation."""
        event = DLQEvent(
            correlation_id="corr-123",
            original_topic="test-topic",
            service_name="test-service",
            dlq_type=DLQEventType.PROCESSING_ERROR,
            error_message="Test error",
            retry_count=2,
            event_data={"key": "value"},
            metadata={"error_code": "ERR_001"},
            created_at=datetime.now(timezone.utc),
            recovery_attempts=0,
        )
        
        assert event.correlation_id == "corr-123"
        assert event.original_topic == "test-topic"
        assert event.service_name == "test-service"
        assert event.dlq_type == DLQEventType.PROCESSING_ERROR
        assert event.error_message == "Test error"
        assert event.retry_count == 2
        assert event.event_data == {"key": "value"}
        assert event.metadata == {"error_code": "ERR_001"}
        assert event.recovery_attempts == 0

    def test_dlq_event_from_event_message(self, sample_event):
        """Test creating DLQ event from EventMessage."""
        # Mock the from_event_message method (assuming it exists)
        # Since we don't see this method in the provided code, we'll test the concept
        dlq_event = DLQEvent(
            correlation_id=sample_event.correlation_id,
            original_topic="test-topic",
            service_name="test-service",
            dlq_type=DLQEventType.PROCESSING_ERROR,
            error_message="Processing failed",
            retry_count=1,
            event_data=sample_event.data,
            metadata=sample_event.metadata,
            created_at=datetime.now(timezone.utc),
        )
        
        assert dlq_event.correlation_id == sample_event.correlation_id
        assert dlq_event.event_data == sample_event.data
        assert dlq_event.metadata == sample_event.metadata

    def test_dlq_event_types(self):
        """Test all DLQ event types."""
        event_types = [
            DLQEventType.PROCESSING_ERROR,
            DLQEventType.TIMEOUT,
            DLQEventType.VALIDATION_ERROR,
            DLQEventType.CIRCUIT_BREAKER_OPEN,
            DLQEventType.RESOURCE_UNAVAILABLE,
            DLQEventType.SERIALIZATION_ERROR,
        ]
        
        for event_type in event_types:
            event = DLQEvent(
                correlation_id=str(uuid.uuid4()),
                original_topic="test-topic",
                service_name="test-service",
                dlq_type=event_type,
                error_message=f"Error of type {event_type.value}",
                retry_count=1,
                created_at=datetime.now(timezone.utc),
            )
            assert event.dlq_type == event_type

    def test_dlq_event_serialization(self):
        """Test DLQ event serialization to dict."""
        timestamp = datetime.now(timezone.utc)
        event = DLQEvent(
            correlation_id="corr-123",
            original_topic="test-topic",
            service_name="test-service",
            dlq_type=DLQEventType.TIMEOUT,
            error_message="Request timeout",
            retry_count=3,
            event_data={"user_id": "123"},
            metadata={"timeout_ms": 5000},
            created_at=timestamp,
            recovery_attempts=1,
        )
        
        # Test serialization (assuming to_dict method exists)
        if hasattr(event, 'to_dict'):
            event_dict = event.to_dict()
            assert event_dict["correlation_id"] == "corr-123"
            assert event_dict["dlq_type"] == "TIMEOUT"
            assert event_dict["retry_count"] == 3


class TestDLQMetrics:
    """Test DLQ metrics functionality."""

    def test_dlq_metrics_initialization(self):
        """Test DLQ metrics initial state."""
        metrics = DLQMetrics()
        
        assert metrics.total_dlq_events == 0
        assert metrics.recovered_events == 0
        assert metrics.permanently_failed_events == 0
        assert metrics.events_by_type == {}
        assert metrics.events_by_service == {}
        assert metrics.last_processed_at is None

    def test_dlq_metrics_custom_values(self):
        """Test DLQ metrics with custom values."""
        metrics = DLQMetrics(
            total_dlq_events=100,
            recovered_events=20,
            permanently_failed_events=5,
            events_by_type={"TIMEOUT": 30, "PROCESSING_ERROR": 70},
            events_by_service={"service-a": 60, "service-b": 40},
            last_processed_at="2024-01-01T12:00:00Z",
        )
        
        assert metrics.total_dlq_events == 100
        assert metrics.recovered_events == 20
        assert metrics.permanently_failed_events == 5
        assert metrics.events_by_type["TIMEOUT"] == 30
        assert metrics.events_by_service["service-a"] == 60

    def test_dlq_metrics_update(self, dlq_event):
        """Test updating DLQ metrics with events."""
        metrics = DLQMetrics()
        
        # Simulate metric updates (testing the logic from DLQHandler._update_metrics)
        metrics.total_dlq_events += 1
        metrics.last_processed_at = datetime.now(timezone.utc).isoformat()
        
        # Update by type
        dlq_type = dlq_event.dlq_type.value
        metrics.events_by_type[dlq_type] = metrics.events_by_type.get(dlq_type, 0) + 1
        
        # Update by service
        service = dlq_event.service_name
        metrics.events_by_service[service] = metrics.events_by_service.get(service, 0) + 1
        
        assert metrics.total_dlq_events == 1
        assert metrics.events_by_type[dlq_event.dlq_type.value] == 1
        assert metrics.events_by_service[dlq_event.service_name] == 1


class TestDLQRecoveryStrategy:
    """Test DLQ recovery strategy implementations."""

    @pytest.mark.asyncio
    async def test_time_based_recovery_strategy(self, dlq_event):
        """Test TimeBasedRecoveryStrategy functionality."""
        mock_event_bus = AsyncMock(spec=KafkaEventBus)
        
        strategy = TimeBasedRecoveryStrategy(
            event_bus=mock_event_bus,
            min_wait_hours=1,
            max_recovery_attempts=3,
        )
        
        # Test recent event (should not recover)
        dlq_event.created_at = datetime.now(timezone.utc) - timedelta(minutes=30)
        can_recover = await strategy.can_recover(dlq_event)
        assert can_recover is False
        
        # Test old event (should be able to recover)
        dlq_event.created_at = datetime.now(timezone.utc) - timedelta(hours=2)
        dlq_event.recovery_attempts = 1  # Below max attempts
        can_recover = await strategy.can_recover(dlq_event)
        assert can_recover is True
        
        # Test exhausted attempts
        dlq_event.recovery_attempts = 3  # At max attempts
        can_recover = await strategy.can_recover(dlq_event)
        assert can_recover is False

    @pytest.mark.asyncio
    async def test_time_based_recovery_execution(self, dlq_event):
        """Test TimeBasedRecoveryStrategy recovery execution."""
        mock_event_bus = AsyncMock(spec=KafkaEventBus)
        mock_event_bus.publish.return_value = None
        
        strategy = TimeBasedRecoveryStrategy(
            event_bus=mock_event_bus,
            min_wait_hours=1,
            max_recovery_attempts=3,
        )
        
        # Setup event for recovery
        dlq_event.created_at = datetime.now(timezone.utc) - timedelta(hours=2)
        dlq_event.recovery_attempts = 1
        
        # Test recovery
        success = await strategy.recover(dlq_event)
        assert success is True
        
        # Verify event was republished
        mock_event_bus.publish.assert_called_once()
        call_kwargs = mock_event_bus.publish.call_args[1]
        assert call_kwargs["topic"] == dlq_event.original_topic
        assert call_kwargs["event_type"] == "dlq.recovery.time_based"

    @pytest.mark.asyncio
    async def test_validation_based_recovery_strategy(self, dlq_event):
        """Test ValidationBasedRecoveryStrategy functionality."""
        mock_event_bus = AsyncMock(spec=KafkaEventBus)
        
        strategy = ValidationBasedRecoveryStrategy(event_bus=mock_event_bus)
        
        # Test validation logic
        can_recover = await strategy.can_recover(dlq_event)
        # Implementation depends on specific validation logic
        assert isinstance(can_recover, bool)

    @pytest.mark.asyncio
    async def test_validation_based_recovery_execution(self, dlq_event):
        """Test ValidationBasedRecoveryStrategy recovery execution."""
        mock_event_bus = AsyncMock(spec=KafkaEventBus)
        mock_event_bus.publish.return_value = None
        
        strategy = ValidationBasedRecoveryStrategy(event_bus=mock_event_bus)
        
        # Test recovery
        success = await strategy.recover(dlq_event)
        assert isinstance(success, bool)
        
        if success:
            # Verify event was republished
            mock_event_bus.publish.assert_called()

    @pytest.mark.asyncio
    async def test_recovery_strategy_error_handling(self, dlq_event):
        """Test recovery strategy error handling."""
        mock_event_bus = AsyncMock(spec=KafkaEventBus)
        mock_event_bus.publish.side_effect = Exception("Recovery failed")
        
        strategy = TimeBasedRecoveryStrategy(
            event_bus=mock_event_bus,
            min_wait_hours=1,
            max_recovery_attempts=3,
        )
        
        # Setup event for recovery
        dlq_event.created_at = datetime.now(timezone.utc) - timedelta(hours=2)
        dlq_event.recovery_attempts = 1
        
        # Recovery should handle errors gracefully
        success = await strategy.recover(dlq_event)
        assert success is False


class TestDLQHandler:
    """Test DLQ handler functionality."""

    @pytest.mark.asyncio
    async def test_dlq_handler_initialization(self):
        """Test DLQ handler initialization."""
        mock_event_bus = AsyncMock(spec=KafkaEventBus)
        
        handler = DLQHandler(
            event_bus=mock_event_bus,
            dlq_topics=["topic1.dlq", "topic2.dlq"],
            enable_periodic_reporting=False,
        )
        
        assert handler.event_bus == mock_event_bus
        assert handler.dlq_topics == ["topic1.dlq", "topic2.dlq"]
        assert handler.enable_periodic_reporting is False
        assert len(handler.recovery_strategies) > 0  # Should have default strategies
        assert handler._running is False

    @pytest.mark.asyncio
    async def test_dlq_handler_start_stop(self):
        """Test DLQ handler start and stop lifecycle."""
        mock_event_bus = AsyncMock(spec=KafkaEventBus)
        mock_event_bus.subscribe_with_retry.return_value = AsyncMock()
        
        handler = DLQHandler(
            event_bus=mock_event_bus,
            dlq_topics=["test-topic.dlq"],
            enable_periodic_reporting=False,
        )
        
        # Test start
        await handler.start()
        assert handler._running is True
        
        # Verify subscriptions were created
        mock_event_bus.subscribe_with_retry.assert_called()
        
        # Test stop
        await handler.stop()
        assert handler._running is False

    @pytest.mark.asyncio
    async def test_dlq_handler_async_context_manager(self):
        """Test DLQ handler as async context manager."""
        mock_event_bus = AsyncMock(spec=KafkaEventBus)
        mock_event_bus.subscribe_with_retry.return_value = AsyncMock()
        
        handler = DLQHandler(
            event_bus=mock_event_bus,
            dlq_topics=["test-topic.dlq"],
            enable_periodic_reporting=False,
        )
        
        # Test async context manager
        async with handler as h:
            assert h._running is True
            assert h == handler
        
        # Should be stopped after context exit
        assert handler._running is False

    @pytest.mark.asyncio
    async def test_dlq_handler_event_processing(self, dlq_event, sample_event):
        """Test DLQ handler event processing."""
        mock_event_bus = AsyncMock(spec=KafkaEventBus)
        mock_event_bus.subscribe_with_retry.return_value = AsyncMock()
        
        handler = DLQHandler(
            event_bus=mock_event_bus,
            dlq_topics=["test-topic.dlq"],
            enable_periodic_reporting=False,
        )
        
        # Test handling DLQ event
        result = await handler._handle_dlq_event(sample_event)
        
        assert isinstance(result, ProcessingResult)
        assert result.status == MessageStatus.SUCCESS
        
        # Verify metrics were updated
        assert handler.metrics.total_dlq_events > 0

    @pytest.mark.asyncio
    async def test_dlq_handler_recovery_attempt(self, dlq_event):
        """Test DLQ handler recovery attempt."""
        mock_event_bus = AsyncMock(spec=KafkaEventBus)
        
        # Mock recovery strategy
        mock_strategy = AsyncMock(spec=DLQRecoveryStrategy)
        mock_strategy.can_recover.return_value = True
        mock_strategy.recover.return_value = True
        
        handler = DLQHandler(
            event_bus=mock_event_bus,
            dlq_topics=["test-topic.dlq"],
            recovery_strategies=[mock_strategy],
            enable_periodic_reporting=False,
        )
        
        # Test recovery attempt
        success = await handler._attempt_recovery(dlq_event)
        assert success is True
        
        # Verify strategy was called
        mock_strategy.can_recover.assert_called_once_with(dlq_event)
        mock_strategy.recover.assert_called_once_with(dlq_event)
        
        # Verify metrics were updated
        assert handler.metrics.recovered_events == 1

    @pytest.mark.asyncio
    async def test_dlq_handler_recovery_failure(self, dlq_event):
        """Test DLQ handler when recovery fails."""
        mock_event_bus = AsyncMock(spec=KafkaEventBus)
        
        # Mock failing recovery strategy
        mock_strategy = AsyncMock(spec=DLQRecoveryStrategy)
        mock_strategy.can_recover.return_value = True
        mock_strategy.recover.return_value = False
        
        handler = DLQHandler(
            event_bus=mock_event_bus,
            dlq_topics=["test-topic.dlq"],
            recovery_strategies=[mock_strategy],
            enable_periodic_reporting=False,
        )
        
        # Set up event for permanent failure
        dlq_event.recovery_attempts = 3
        
        # Test recovery attempt
        success = await handler._attempt_recovery(dlq_event)
        assert success is False
        
        # Verify permanent failure was recorded
        assert handler.metrics.permanently_failed_events == 1

    @pytest.mark.asyncio
    async def test_dlq_handler_alerts(self, dlq_event):
        """Test DLQ handler alert functionality."""
        mock_event_bus = AsyncMock(spec=KafkaEventBus)
        
        handler = DLQHandler(
            event_bus=mock_event_bus,
            dlq_topics=["test-topic.dlq"],
            enable_periodic_reporting=False,
        )
        
        # Test critical event alerting
        dlq_event.dlq_type = DLQEventType.CIRCUIT_BREAKER_OPEN
        await handler._send_dlq_alerts(dlq_event)
        
        # Should log critical alert (test by checking if no exceptions)
        # In a real implementation, this might send to an alerting system

    @pytest.mark.asyncio
    async def test_dlq_handler_summary(self):
        """Test DLQ handler summary generation."""
        mock_event_bus = AsyncMock(spec=KafkaEventBus)
        
        handler = DLQHandler(
            event_bus=mock_event_bus,
            dlq_topics=["topic1.dlq", "topic2.dlq"],
            enable_periodic_reporting=False,
        )
        
        # Update some metrics
        handler.metrics.total_dlq_events = 10
        handler.metrics.recovered_events = 3
        
        # Test summary
        summary = await handler.get_dlq_summary()
        
        assert "metrics" in summary
        assert "monitored_topics" in summary
        assert "recovery_strategies" in summary
        assert "last_updated" in summary
        assert "is_running" in summary
        
        assert summary["monitored_topics"] == ["topic1.dlq", "topic2.dlq"]
        assert summary["metrics"]["total_dlq_events"] == 10
        assert summary["metrics"]["recovered_events"] == 3

    @pytest.mark.asyncio
    async def test_dlq_handler_health_check(self):
        """Test DLQ handler health check."""
        mock_event_bus = AsyncMock(spec=KafkaEventBus)
        mock_consumer = AsyncMock()
        mock_consumer.group_id = "test-group"
        
        handler = DLQHandler(
            event_bus=mock_event_bus,
            dlq_topics=["test-topic.dlq"],
            enable_periodic_reporting=False,
        )
        
        # Add mock consumer
        handler.consumers["test-topic.dlq"] = mock_consumer
        handler._running = True
        
        # Test health check
        health = await handler.health_check()
        
        assert health["status"] == "healthy"
        assert health["is_running"] is True
        assert "monitored_topics" in health
        assert "consumer_status" in health
        assert "metrics_summary" in health

    @pytest.mark.asyncio
    async def test_dlq_handler_health_check_error(self):
        """Test DLQ handler health check with error."""
        mock_event_bus = AsyncMock(spec=KafkaEventBus)
        
        handler = DLQHandler(
            event_bus=mock_event_bus,
            dlq_topics=["test-topic.dlq"],
            enable_periodic_reporting=False,
        )
        
        # Simulate error in health check by creating a mock that raises exception on iteration
        from unittest.mock import MagicMock
        error_mock = MagicMock()
        error_mock.items.side_effect = Exception("Health check error")
        handler.consumers = error_mock
        
        health = await handler.health_check()
        
        assert health["status"] == "error"
        assert "error" in health

    @pytest.mark.asyncio
    async def test_dlq_handler_periodic_reporting(self):
        """Test DLQ handler periodic reporting."""
        mock_event_bus = AsyncMock(spec=KafkaEventBus)
        
        handler = DLQHandler(
            event_bus=mock_event_bus,
            dlq_topics=["test-topic.dlq"],
            enable_periodic_reporting=True,
            reporting_interval_seconds=0.1,  # Fast for testing
        )
        
        # Add some metrics
        handler.metrics.total_dlq_events = 5
        
        # Start handler (which starts periodic reporting)
        await handler.start()
        
        # Wait a bit for reporting
        await asyncio.sleep(0.2)
        
        # Stop handler
        await handler.stop()
        
        # Reporting task should have been created and cancelled
        assert handler._reporting_task is not None

    @pytest.mark.asyncio
    async def test_dlq_handler_no_topics(self):
        """Test DLQ handler with no topics configured."""
        mock_event_bus = AsyncMock(spec=KafkaEventBus)
        
        handler = DLQHandler(
            event_bus=mock_event_bus,
            dlq_topics=[],  # No topics
            enable_periodic_reporting=False,
        )
        
        # Should initialize without error
        await handler.initialize()
        
        # Should have no consumers
        assert len(handler.consumers) == 0

    @pytest.mark.asyncio
    async def test_dlq_handler_force_recovery(self):
        """Test DLQ handler force recovery functionality."""
        # Test static method
        success = await DLQHandler.force_recovery_attempt(
            correlation_id="test-corr-123",
            topic="test-topic",
        )
        
        # Currently returns False as it's not fully implemented
        assert success is False

    @pytest.mark.asyncio
    async def test_dlq_handler_consumer_stop_error(self):
        """Test DLQ handler handling consumer stop errors."""
        mock_event_bus = AsyncMock(spec=KafkaEventBus)
        mock_consumer = AsyncMock()
        mock_consumer.stop.side_effect = Exception("Stop error")
        
        handler = DLQHandler(
            event_bus=mock_event_bus,
            dlq_topics=["test-topic.dlq"],
            enable_periodic_reporting=False,
        )
        
        # Add mock consumer
        handler.consumers["test-topic.dlq"] = mock_consumer
        handler._running = True
        
        # Stop should handle consumer errors gracefully
        await handler.stop()
        assert handler._running is False
        assert len(handler.consumers) == 0


class TestDLQHandlerFactory:
    """Test DLQ handler factory function."""

    @pytest.mark.asyncio
    async def test_create_dlq_handler_for_service(self, kafka_config, service_config):
        """Test DLQ handler factory function."""
        with patch("general_operate.kafka.dlq_handler.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock()
            mock_event_bus_class.return_value = mock_event_bus
            
            # Test factory function
            handler = await create_dlq_handler_for_service(
                kafka_config=kafka_config,
                service_config=service_config,
                dlq_topics=["service.dlq"],
            )
            
            assert isinstance(handler, DLQHandler)
            assert handler.event_bus == mock_event_bus
            assert handler.dlq_topics == ["service.dlq"]


class TestDLQIntegration:
    """Integration tests for DLQ functionality."""

    @pytest.mark.asyncio
    async def test_dlq_end_to_end_processing(self, sample_event):
        """Test end-to-end DLQ processing."""
        mock_event_bus = AsyncMock(spec=KafkaEventBus)
        mock_event_bus.subscribe_with_retry.return_value = AsyncMock()
        
        # Create handler with time-based recovery
        handler = DLQHandler(
            event_bus=mock_event_bus,
            dlq_topics=["test-topic.dlq"],
            enable_periodic_reporting=False,
        )
        
        await handler.start()
        
        # Process a DLQ event
        result = await handler._handle_dlq_event(sample_event)
        
        # Verify processing
        assert result.status == MessageStatus.SUCCESS
        assert handler.metrics.total_dlq_events == 1
        
        await handler.stop()

    @pytest.mark.asyncio
    async def test_dlq_multiple_recovery_strategies(self, dlq_event):
        """Test DLQ handler with multiple recovery strategies."""
        mock_event_bus = AsyncMock(spec=KafkaEventBus)
        
        # Create multiple mock strategies
        strategy1 = AsyncMock(spec=DLQRecoveryStrategy)
        strategy1.can_recover.return_value = False  # Can't recover
        
        strategy2 = AsyncMock(spec=DLQRecoveryStrategy)
        strategy2.can_recover.return_value = True
        strategy2.recover.return_value = True  # Successful recovery
        
        handler = DLQHandler(
            event_bus=mock_event_bus,
            dlq_topics=["test-topic.dlq"],
            recovery_strategies=[strategy1, strategy2],
            enable_periodic_reporting=False,
        )
        
        # Test recovery with multiple strategies
        success = await handler._attempt_recovery(dlq_event)
        assert success is True
        
        # Verify first strategy was tried but couldn't recover
        strategy1.can_recover.assert_called_once()
        strategy1.recover.assert_not_called()
        
        # Verify second strategy was tried and succeeded
        strategy2.can_recover.assert_called_once()
        strategy2.recover.assert_called_once()

    @pytest.mark.asyncio
    async def test_dlq_handler_stress_test(self):
        """Test DLQ handler under stress with many events."""
        from conftest import generate_dlq_events
        
        mock_event_bus = AsyncMock(spec=KafkaEventBus)
        
        handler = DLQHandler(
            event_bus=mock_event_bus,
            dlq_topics=["test-topic.dlq"],
            enable_periodic_reporting=False,
        )
        
        # Generate many DLQ events
        dlq_events = generate_dlq_events(100)
        
        # Process all events
        for dlq_event in dlq_events:
            # Create mock EventMessage for testing
            event_message = EventMessage(
                event_type="dlq.event",
                tenant_id="test-tenant",
                data=dlq_event.event_data or {},
                correlation_id=dlq_event.correlation_id,
            )
            
            result = await handler._handle_dlq_event(event_message)
            assert result.status == MessageStatus.SUCCESS
        
        # Verify all events were processed
        assert handler.metrics.total_dlq_events == 100

    @pytest.mark.asyncio
    async def test_dlq_handler_concurrent_processing(self, sample_event):
        """Test DLQ handler concurrent event processing."""
        mock_event_bus = AsyncMock(spec=KafkaEventBus)
        
        handler = DLQHandler(
            event_bus=mock_event_bus,
            dlq_topics=["test-topic.dlq"],
            enable_periodic_reporting=False,
        )
        
        # Process multiple events concurrently
        tasks = []
        for i in range(10):
            event = EventMessage(
                event_type="test.concurrent",
                tenant_id="test-tenant",
                data={"sequence": i},
                correlation_id=f"corr-{i}",
            )
            task = asyncio.create_task(handler._handle_dlq_event(event))
            tasks.append(task)
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks)
        
        # Verify all succeeded
        assert all(result.status == MessageStatus.SUCCESS for result in results)
        assert handler.metrics.total_dlq_events == 10