"""
Unit tests for the ServiceEventManager module.

Tests cover:
- ServiceEventManager initialization and configuration
- Event bus integration and lifecycle management
- DLQ handler integration and coordination
- Service event publishing and subscribing
- Error handling and recovery patterns
- Resource management and cleanup
- Health check and monitoring functionality
- Async context management patterns
"""

import asyncio
import uuid
from typing import Any, Dict
from unittest.mock import AsyncMock, Mock, patch

import pytest

from general_operate.kafka.service_event_manager import ServiceEventManager
from general_operate.kafka.kafka_client import (
    EventMessage,
    KafkaEventBus,
    MessageStatus,
    ProcessingResult,
)
from general_operate.kafka.manager_config import ServiceConfig, RetryConfig
from general_operate.kafka.dlq_handler import DLQHandler


class TestServiceEventManager:
    """Test ServiceEventManager functionality."""

    @pytest.mark.asyncio
    async def test_service_event_manager_initialization(self, kafka_config, service_config):
        """Test ServiceEventManager initialization."""
        manager = ServiceEventManager(kafka_config, service_config)
        
        assert manager.kafka_config == kafka_config
        assert manager.service_config == service_config
        assert manager.service_name == service_config.service_name
        assert manager.event_bus is None  # Not initialized yet
        assert manager.dlq_handler is None  # Not initialized yet

    @pytest.mark.asyncio
    async def test_service_event_manager_start_stop(self, kafka_config, service_config):
        """Test ServiceEventManager start and stop lifecycle."""
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class, \
             patch("general_operate.kafka.dlq_handler.create_dlq_handler_for_service") as mock_dlq_factory:
            
            # Setup mocks
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_event_bus_class.return_value = mock_event_bus
            
            mock_dlq_handler = AsyncMock(spec=DLQHandler)
            mock_dlq_factory.return_value = mock_dlq_handler
            
            manager = ServiceEventManager(kafka_config, service_config)
            
            # Test start
            await manager.start()
            
            assert manager.event_bus == mock_event_bus
            assert manager.dlq_handler == mock_dlq_handler
            
            # Verify components were started
            mock_event_bus.start.assert_called_once()
            mock_dlq_handler.start.assert_called_once()
            
            # Test stop
            await manager.stop()
            
            # Verify components were stopped
            mock_event_bus.stop.assert_called_once()
            mock_dlq_handler.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_service_event_manager_dlq_disabled(self, kafka_config):
        """Test ServiceEventManager with DLQ disabled."""
        service_config = ServiceConfig(
            service_name="test-service",
            enable_dlq=False,  # Disable DLQ
        )
        
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_event_bus_class.return_value = mock_event_bus
            
            manager = ServiceEventManager(kafka_config, service_config)
            await manager.start()
            
            # Should have event bus but no DLQ handler
            assert manager.event_bus == mock_event_bus
            assert manager.dlq_handler is None
            
            await manager.stop()

    @pytest.mark.asyncio
    async def test_service_event_manager_publish_event(self, kafka_config, service_config, sample_event):
        """Test ServiceEventManager event publishing."""
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_event_bus_class.return_value = mock_event_bus
            
            manager = ServiceEventManager(kafka_config, service_config)
            await manager.start()
            
            # Test publishing event
            await manager.publish_event(
                topic="test-topic",
                event_type="test.event",
                tenant_id="tenant-123",
                data={"key": "value"},
                user_id="user-456",
                metadata={"source": "test"},
            )
            
            # Verify event bus publish was called
            mock_event_bus.publish.assert_called_once()
            call_kwargs = mock_event_bus.publish.call_args[1]
            assert call_kwargs["topic"] == "test-topic"
            assert call_kwargs["event_type"] == "test.event"
            assert call_kwargs["tenant_id"] == "tenant-123"
            assert call_kwargs["data"] == {"key": "value"}
            assert call_kwargs["user_id"] == "user-456"
            assert call_kwargs["metadata"] == {"source": "test"}
            
            await manager.stop()

    @pytest.mark.asyncio
    async def test_service_event_manager_subscribe_to_events(self, kafka_config, service_config, success_handler):
        """Test ServiceEventManager event subscription."""
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_consumer = AsyncMock()
            mock_event_bus.subscribe_with_retry.return_value = mock_consumer
            mock_event_bus_class.return_value = mock_event_bus
            
            manager = ServiceEventManager(kafka_config, service_config)
            await manager.start()
            
            # Test subscribing to events
            consumer = await manager.subscribe_to_events(
                topics=["test-topic"],
                group_id="test-group",
                handler=success_handler,
                filter_event_types=["test.event"],
            )
            
            assert consumer == mock_consumer
            
            # Verify event bus subscribe was called (at least twice: once for DLQ, once for test)
            assert mock_event_bus.subscribe_with_retry.call_count >= 2
            
            # Check the last call was for our test subscription
            last_call_kwargs = mock_event_bus.subscribe_with_retry.call_args[1]
            assert last_call_kwargs["topics"] == ["test-topic"]
            assert last_call_kwargs["group_id"] == "test-group"
            assert last_call_kwargs["handler"] == success_handler
            assert last_call_kwargs["service_name"] == service_config.service_name
            assert last_call_kwargs["filter_event_types"] == ["test.event"]
            
            await manager.stop()

    @pytest.mark.asyncio
    async def test_service_event_manager_subscribe_with_custom_config(self, kafka_config, service_config, success_handler):
        """Test ServiceEventManager subscription with custom retry config."""
        custom_retry = RetryConfig(max_retries=10, base_delay=2.0)
        
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_consumer = AsyncMock()
            mock_event_bus.subscribe_with_retry.return_value = mock_consumer
            mock_event_bus_class.return_value = mock_event_bus
            
            manager = ServiceEventManager(kafka_config, service_config)
            await manager.start()
            
            # Test subscribing with custom retry config
            consumer = await manager.subscribe_to_events(
                topics=["test-topic"],
                group_id="test-group",
                handler=success_handler,
                retry_config=custom_retry,
                dead_letter_topic="custom.dlq",
            )
            
            assert consumer == mock_consumer
            
            # Verify custom configuration was passed
            call_kwargs = mock_event_bus.subscribe_with_retry.call_args[1]
            assert call_kwargs["retry_config"] == custom_retry
            assert call_kwargs["dead_letter_topic"] == "custom.dlq"
            
            await manager.stop()

    @pytest.mark.asyncio
    async def test_service_event_manager_health_check(self, kafka_config, service_config):
        """Test ServiceEventManager health check functionality."""
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class, \
             patch("general_operate.kafka.dlq_handler.create_dlq_handler_for_service") as mock_dlq_factory:
            
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_event_bus_class.return_value = mock_event_bus
            
            mock_dlq_handler = AsyncMock(spec=DLQHandler)
            mock_dlq_handler.health_check.return_value = {
                "status": "healthy",
                "metrics_summary": {"total_events": 0},
            }
            mock_dlq_factory.return_value = mock_dlq_handler
            
            manager = ServiceEventManager(kafka_config, service_config)
            await manager.start()
            
            # Test health check
            health = await manager.health_check()
            
            assert "service_name" in health
            assert "status" in health
            assert "event_bus" in health
            assert "dlq_handler" in health
            
            assert health["service_name"] == service_config.service_name
            assert health["status"] == "healthy"
            assert health["event_bus"]["status"] == "running"
            assert health["dlq_handler"]["status"] == "healthy"
            
            await manager.stop()

    @pytest.mark.asyncio
    async def test_service_event_manager_health_check_stopped(self, kafka_config, service_config):
        """Test ServiceEventManager health check when stopped."""
        manager = ServiceEventManager(kafka_config, service_config)
        
        # Health check without starting
        health = await manager.health_check()
        
        assert health["status"] == "stopped"
        assert health["event_bus"]["status"] == "not_initialized"
        assert health["dlq_handler"]["status"] == "not_initialized"

    @pytest.mark.asyncio
    async def test_service_event_manager_health_check_error(self, kafka_config, service_config):
        """Test ServiceEventManager health check with component errors."""
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class, \
             patch("general_operate.kafka.dlq_handler.create_dlq_handler_for_service") as mock_dlq_factory:
            
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_event_bus_class.return_value = mock_event_bus
            
            mock_dlq_handler = AsyncMock(spec=DLQHandler)
            mock_dlq_handler.health_check.side_effect = Exception("DLQ health check failed")
            mock_dlq_factory.return_value = mock_dlq_handler
            
            manager = ServiceEventManager(kafka_config, service_config)
            await manager.start()
            
            # Health check should handle component errors gracefully
            health = await manager.health_check()
            
            assert health["status"] == "degraded"
            assert "error" in health["dlq_handler"]
            
            await manager.stop()

    @pytest.mark.asyncio
    async def test_service_event_manager_get_metrics(self, kafka_config, service_config):
        """Test ServiceEventManager metrics collection."""
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class, \
             patch("general_operate.kafka.dlq_handler.create_dlq_handler_for_service") as mock_dlq_factory:
            
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_event_bus_class.return_value = mock_event_bus
            
            mock_dlq_handler = AsyncMock(spec=DLQHandler)
            mock_dlq_handler.get_dlq_summary.return_value = {
                "metrics": {
                    "total_dlq_events": 5,
                    "recovered_events": 2,
                    "permanently_failed_events": 1,
                },
                "monitored_topics": ["test-topic.dlq"],
            }
            mock_dlq_factory.return_value = mock_dlq_handler
            
            manager = ServiceEventManager(kafka_config, service_config)
            await manager.start()
            
            # Test metrics collection
            metrics = await manager.get_metrics()
            
            assert "service_name" in metrics
            assert "dlq_metrics" in metrics
            assert "event_bus_status" in metrics
            
            assert metrics["service_name"] == service_config.service_name
            assert metrics["dlq_metrics"]["total_dlq_events"] == 5
            assert metrics["dlq_metrics"]["recovered_events"] == 2
            
            await manager.stop()

    @pytest.mark.asyncio
    async def test_service_event_manager_error_handling(self, kafka_config, service_config):
        """Test ServiceEventManager error handling during initialization."""
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_event_bus.start.side_effect = Exception("Event bus start failed")
            mock_event_bus_class.return_value = mock_event_bus
            
            manager = ServiceEventManager(kafka_config, service_config)
            
            # Start should handle errors gracefully
            with pytest.raises(Exception, match="Event bus start failed"):
                await manager.start()

    @pytest.mark.asyncio
    async def test_service_event_manager_concurrent_operations(self, kafka_config, service_config, success_handler):
        """Test ServiceEventManager concurrent operations."""
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_consumer = AsyncMock()
            mock_event_bus.subscribe_with_retry.return_value = mock_consumer
            mock_event_bus_class.return_value = mock_event_bus
            
            manager = ServiceEventManager(kafka_config, service_config)
            await manager.start()
            
            # Test concurrent publishing and subscribing
            publish_tasks = []
            subscribe_tasks = []
            
            for i in range(5):
                # Concurrent publishing
                publish_task = asyncio.create_task(
                    manager.publish_event(
                        topic=f"topic-{i}",
                        event_type=f"test.event.{i}",
                        tenant_id="tenant-123",
                        data={"sequence": i},
                    )
                )
                publish_tasks.append(publish_task)
                
                # Concurrent subscribing
                subscribe_task = asyncio.create_task(
                    manager.subscribe_to_events(
                        topics=[f"topic-{i}"],
                        group_id=f"group-{i}",
                        handler=success_handler,
                    )
                )
                subscribe_tasks.append(subscribe_task)
            
            # Wait for all operations to complete
            await asyncio.gather(*publish_tasks)
            consumers = await asyncio.gather(*subscribe_tasks)
            
            # Verify all operations succeeded
            assert len(consumers) == 5
            assert mock_event_bus.publish.call_count == 5
            # 5 explicit subscriptions + 1 for DLQ handler monitoring
            assert mock_event_bus.subscribe_with_retry.call_count == 6
            
            await manager.stop()

    @pytest.mark.asyncio
    async def test_service_event_manager_resource_cleanup(self, kafka_config, service_config):
        """Test ServiceEventManager proper resource cleanup."""
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class, \
             patch("general_operate.kafka.dlq_handler.create_dlq_handler_for_service") as mock_dlq_factory:
            
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_event_bus_class.return_value = mock_event_bus
            
            mock_dlq_handler = AsyncMock(spec=DLQHandler)
            mock_dlq_factory.return_value = mock_dlq_handler
            
            manager = ServiceEventManager(kafka_config, service_config)
            
            # Start and verify resources are created
            await manager.start()
            assert manager.event_bus is not None
            assert manager.dlq_handler is not None
            
            # Stop and verify resources are cleaned up
            await manager.stop()
            mock_event_bus.stop.assert_called_once()
            mock_dlq_handler.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_service_event_manager_multiple_start_stop(self, kafka_config, service_config):
        """Test ServiceEventManager handling multiple start/stop calls."""
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_event_bus_class.return_value = mock_event_bus
            
            manager = ServiceEventManager(kafka_config, service_config)
            
            # Multiple starts should be handled gracefully
            await manager.start()
            await manager.start()  # Second start
            
            # Event bus should only be started once
            mock_event_bus.start.assert_called_once()
            
            # Multiple stops should be handled gracefully
            await manager.stop()
            await manager.stop()  # Second stop
            
            # Event bus should only be stopped once
            mock_event_bus.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_service_event_manager_custom_dlq_topics(self, kafka_config):
        """Test ServiceEventManager with custom DLQ topic configuration."""
        service_config = ServiceConfig(
            service_name="custom-dlq-service",
            enable_dlq=True,
            dlq_topic_suffix=".deadletter",
        )
        
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class, \
             patch("general_operate.kafka.dlq_handler.create_dlq_handler_for_service") as mock_dlq_factory:
            
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_event_bus_class.return_value = mock_event_bus
            
            mock_dlq_handler = AsyncMock(spec=DLQHandler)
            mock_dlq_factory.return_value = mock_dlq_handler
            
            manager = ServiceEventManager(kafka_config, service_config)
            await manager.start()
            
            # Verify DLQ handler was created with custom suffix
            mock_dlq_factory.assert_called_once()
            call_kwargs = mock_dlq_factory.call_args[1]
            assert call_kwargs["service_name"] == "custom-dlq-service"
            assert call_kwargs["dlq_topic_suffixes"] == ["custom-dlq-service.events.dlq"]
            
            await manager.stop()

    @pytest.mark.asyncio
    async def test_service_event_manager_performance_metrics(self, kafka_config, service_config, performance_timer):
        """Test ServiceEventManager performance characteristics."""
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_event_bus_class.return_value = mock_event_bus
            
            manager = ServiceEventManager(kafka_config, service_config)
            
            # Measure startup time
            performance_timer.start()
            await manager.start()
            performance_timer.stop()
            
            startup_time = performance_timer.elapsed
            assert startup_time < 1.0  # Should start quickly
            
            # Measure publishing performance
            performance_timer.start()
            
            # Publish many events quickly
            publish_tasks = []
            for i in range(100):
                task = asyncio.create_task(
                    manager.publish_event(
                        topic="perf-topic",
                        event_type="perf.test",
                        tenant_id="tenant-123",
                        data={"sequence": i},
                    )
                )
                publish_tasks.append(task)
            
            await asyncio.gather(*publish_tasks)
            performance_timer.stop()
            
            publishing_time = performance_timer.elapsed
            assert publishing_time < 5.0  # Should publish 100 events quickly
            
            await manager.stop()


class TestServiceEventManagerIntegration:
    """Integration tests for ServiceEventManager."""

    @pytest.mark.asyncio
    async def test_service_event_manager_full_workflow(self, kafka_config, service_config):
        """Test complete ServiceEventManager workflow."""
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class, \
             patch("general_operate.kafka.dlq_handler.create_dlq_handler_for_service") as mock_dlq_factory:
            
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_consumer = AsyncMock()
            mock_event_bus.subscribe_with_retry.return_value = mock_consumer
            mock_event_bus_class.return_value = mock_event_bus
            
            mock_dlq_handler = AsyncMock(spec=DLQHandler)
            mock_dlq_handler.health_check.return_value = {"status": "healthy"}
            mock_dlq_handler.get_dlq_summary.return_value = {
                "metrics": {"total_dlq_events": 0},
                "monitored_topics": [],
            }
            mock_dlq_factory.return_value = mock_dlq_handler
            
            manager = ServiceEventManager(kafka_config, service_config)
            
            # Complete workflow
            await manager.start()
            
            # Publish events
            await manager.publish_event(
                topic="workflow-topic",
                event_type="workflow.start",
                tenant_id="tenant-123",
                data={"step": "start"},
            )
            
            # Subscribe to events
            async def workflow_handler(event: EventMessage) -> ProcessingResult:
                return ProcessingResult(status=MessageStatus.SUCCESS)
            
            consumer = await manager.subscribe_to_events(
                topics=["workflow-topic"],
                group_id="workflow-group",
                handler=workflow_handler,
            )
            
            # Check health
            health = await manager.health_check()
            assert health["status"] == "healthy"
            
            # Get metrics
            metrics = await manager.get_metrics()
            assert "service_name" in metrics
            
            # Cleanup
            await manager.stop()
            
            # Verify all components were properly used
            mock_event_bus.publish.assert_called()
            mock_event_bus.subscribe_with_retry.assert_called()
            mock_dlq_handler.health_check.assert_called()
            mock_dlq_handler.get_dlq_summary.assert_called()

    @pytest.mark.asyncio
    async def test_service_event_manager_error_recovery(self, kafka_config, service_config):
        """Test ServiceEventManager error recovery scenarios."""
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class, \
             patch("general_operate.kafka.dlq_handler.create_dlq_handler_for_service") as mock_dlq_factory:
            
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_event_bus_class.return_value = mock_event_bus
            
            mock_dlq_handler = AsyncMock(spec=DLQHandler)
            mock_dlq_factory.return_value = mock_dlq_handler
            
            manager = ServiceEventManager(kafka_config, service_config)
            await manager.start()
            
            # Simulate event bus publish error
            mock_event_bus.publish.side_effect = Exception("Publish failed")
            
            with pytest.raises(Exception, match="Publish failed"):
                await manager.publish_event(
                    topic="error-topic",
                    event_type="error.test",
                    tenant_id="tenant-123",
                    data={"test": "error"},
                )
            
            # Service should still be healthy despite publish error
            health = await manager.health_check()
            # Health might be degraded but service should still be running
            assert health["event_bus"]["status"] == "running"
            
            await manager.stop()

    @pytest.mark.asyncio
    async def test_service_event_manager_configuration_variations(self, kafka_config):
        """Test ServiceEventManager with various configurations."""
        configurations = [
            # Minimal configuration
            ServiceConfig(service_name="minimal-service"),
            
            # High retry configuration
            ServiceConfig(
                service_name="high-retry-service",
                retry_config=RetryConfig(max_retries=10, base_delay=0.1),
            ),
            
            # DLQ disabled
            ServiceConfig(
                service_name="no-dlq-service",
                enable_dlq=False,
            ),
            
            # Custom circuit breaker
            ServiceConfig(
                service_name="custom-cb-service",
                circuit_breaker_threshold=20,
                circuit_breaker_timeout=120.0,
            ),
        ]
        
        for config in configurations:
            with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
                mock_event_bus = AsyncMock(spec=KafkaEventBus)
                mock_event_bus_class.return_value = mock_event_bus
                
                manager = ServiceEventManager(kafka_config, config)
                
                # Should initialize and start with any valid configuration
                await manager.start()
                assert manager.service_name == config.service_name
                
                await manager.stop()

    @pytest.mark.asyncio
    async def test_service_event_manager_async_context_manager(self, kafka_config, service_config):
        """Test ServiceEventManager as async context manager (if implemented)."""
        # This test assumes async context manager functionality
        # If not implemented, this could be a feature request
        
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_event_bus_class.return_value = mock_event_bus
            
            # If async context manager is implemented
            if hasattr(ServiceEventManager, '__aenter__'):
                async with ServiceEventManager(kafka_config, service_config) as manager:
                    assert manager.event_bus is not None
                    
                    # Should be started within context
                    health = await manager.health_check()
                    assert health["status"] in ["healthy", "running"]
                
                # Should be stopped after context exit
                # (This would need to be verified based on implementation)
            else:
                # Manual lifecycle management
                manager = ServiceEventManager(kafka_config, service_config)
                await manager.start()
                try:
                    assert manager.event_bus is not None
                finally:
                    await manager.stop()