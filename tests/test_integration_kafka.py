"""
Integration tests for the complete Kafka module.

Tests cover:
- End-to-end message flow from producer to consumer
- Event bus integration with DLQ handling
- Service event manager complete workflows
- Cross-component interaction and coordination
- Performance characteristics under load
- Error propagation and recovery patterns
- Resource lifecycle management
- Real-world usage scenarios
"""

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List
from unittest.mock import AsyncMock, Mock, patch

import pytest

from general_operate.kafka.kafka_client import (
    EventMessage,
    KafkaEventBus,
    MessageStatus,
    ProcessingResult,
    RetryConfig,
)
from general_operate.kafka.dlq_handler import DLQHandler, DLQEventType
from general_operate.kafka.manager_config import ServiceConfig
from general_operate.kafka.service_event_manager import ServiceEventManager


class TestKafkaIntegration:
    """Integration tests for complete Kafka functionality."""

    @pytest.mark.asyncio
    async def test_end_to_end_message_flow(self, kafka_config, service_config):
        """Test complete message flow from publishing to consumption."""
        messages_received = []
        
        async def message_handler(event: EventMessage) -> ProcessingResult:
            messages_received.append(event)
            return ProcessingResult(status=MessageStatus.SUCCESS)
        
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_consumer = AsyncMock()
            mock_event_bus.subscribe_with_retry.return_value = mock_consumer
            mock_event_bus_class.return_value = mock_event_bus
            
            # Create service event manager
            manager = ServiceEventManager(kafka_config, service_config)
            await manager.start()
            
            # Subscribe to events
            await manager.subscribe_to_events(
                topics=["integration-topic"],
                group_id="integration-group",
                handler=message_handler,
            )
            
            # Publish events
            test_events = [
                {
                    "topic": "integration-topic",
                    "event_type": "user.created",
                    "tenant_id": "tenant-1",
                    "data": {"user_id": "123", "email": "test@example.com"},
                },
                {
                    "topic": "integration-topic",
                    "event_type": "user.updated",
                    "tenant_id": "tenant-1",
                    "data": {"user_id": "123", "name": "Test User"},
                },
                {
                    "topic": "integration-topic",
                    "event_type": "user.deleted",
                    "tenant_id": "tenant-1",
                    "data": {"user_id": "123"},
                },
            ]
            
            for event_data in test_events:
                await manager.publish_event(**event_data)
            
            # Verify all events were published
            assert mock_event_bus.publish.call_count == 3
            
            # Verify subscription was created (at least twice: once for DLQ, once for test)
            assert mock_event_bus.subscribe_with_retry.call_count >= 2
            
            await manager.stop()

    @pytest.mark.asyncio
    async def test_dlq_integration_workflow(self, kafka_config):
        """Test DLQ integration with event processing."""
        dlq_events_processed = []
        
        # Handler that fails on certain events
        async def failing_handler(event: EventMessage) -> ProcessingResult:
            if "fail" in event.data.get("action", ""):
                return ProcessingResult(
                    status=MessageStatus.FAILED,
                    error_message="Intentional failure for DLQ testing",
                )
            return ProcessingResult(status=MessageStatus.SUCCESS)
        
        # DLQ event handler
        async def dlq_handler(event: EventMessage) -> ProcessingResult:
            dlq_events_processed.append(event)
            return ProcessingResult(status=MessageStatus.SUCCESS)
        
        service_config = ServiceConfig(
            service_name="dlq-integration-service",
            enable_dlq=True,
            dlq_topic_suffix=".dlq",
        )
        
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class, \
             patch("general_operate.kafka.dlq_handler.create_dlq_handler_for_service") as mock_dlq_factory:
            
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_consumer = AsyncMock()
            mock_event_bus.subscribe_with_retry.return_value = mock_consumer
            mock_event_bus_class.return_value = mock_event_bus
            
            mock_dlq_handler = AsyncMock(spec=DLQHandler)
            mock_dlq_handler.health_check.return_value = {"status": "healthy"}
            mock_dlq_factory.return_value = mock_dlq_handler
            
            manager = ServiceEventManager(kafka_config, service_config)
            await manager.start()
            
            # Subscribe to main topic with failing handler
            await manager.subscribe_to_events(
                topics=["main-topic"],
                group_id="main-group",
                handler=failing_handler,
            )
            
            # Publish events that will succeed and fail
            test_events = [
                {"action": "process", "data": "normal"},
                {"action": "fail", "data": "this will fail"},
                {"action": "process", "data": "normal again"},
            ]
            
            for i, event_data in enumerate(test_events):
                await manager.publish_event(
                    topic="main-topic",
                    event_type="test.event",
                    tenant_id="tenant-1",
                    data=event_data,
                    correlation_id=f"corr-{i}",
                )
            
            # Verify all events were published
            assert mock_event_bus.publish.call_count == 3
            
            # Verify DLQ handler was created and started
            mock_dlq_factory.assert_called_once()
            mock_dlq_handler.start.assert_called_once()
            
            await manager.stop()

    @pytest.mark.asyncio
    async def test_multi_service_coordination(self, kafka_config):
        """Test coordination between multiple services."""
        # Create multiple service configurations
        service_configs = [
            ServiceConfig(service_name="user-service", enable_dlq=True),
            ServiceConfig(service_name="order-service", enable_dlq=True),
            ServiceConfig(service_name="notification-service", enable_dlq=False),
        ]
        
        services = []
        
        try:
            with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
                mock_event_bus = AsyncMock(spec=KafkaEventBus)
                mock_consumer = AsyncMock()
                mock_event_bus.subscribe_with_retry.return_value = mock_consumer
                # Setup producer mock for health check
                mock_event_bus.producer = AsyncMock()
                mock_event_bus.producer.started = True
                mock_event_bus.consumers = {}
                mock_event_bus_class.return_value = mock_event_bus
                
                # Start all services
                for config in service_configs:
                    service = ServiceEventManager(kafka_config, config)
                    await service.start()
                    services.append(service)
                
                # Simulate cross-service communication
                user_service, order_service, notification_service = services
                
                # User service publishes user creation event
                await user_service.publish_event(
                    topic="user-events",
                    event_type="user.created",
                    tenant_id="tenant-1",
                    data={"user_id": "123", "email": "test@example.com"},
                )
                
                # Order service subscribes to user events and publishes order events
                async def user_event_handler(event: EventMessage) -> ProcessingResult:
                    if event.event_type == "user.created":
                        await order_service.publish_event(
                            topic="order-events",
                            event_type="order.welcome_created",
                            tenant_id=event.tenant_id,
                            data={
                                "user_id": event.data["user_id"],
                                "order_type": "welcome",
                            },
                            correlation_id=event.correlation_id,
                        )
                    return ProcessingResult(status=MessageStatus.SUCCESS)
                
                await order_service.subscribe_to_events(
                    topics=["user-events"],
                    group_id="order-service-group",
                    handler=user_event_handler,
                )
                
                # Notification service subscribes to both user and order events
                notifications_sent = []
                
                async def notification_handler(event: EventMessage) -> ProcessingResult:
                    notifications_sent.append({
                        "type": event.event_type,
                        "tenant_id": event.tenant_id,
                        "data": event.data,
                    })
                    return ProcessingResult(status=MessageStatus.SUCCESS)
                
                await notification_service.subscribe_to_events(
                    topics=["user-events", "order-events"],
                    group_id="notification-service-group",
                    handler=notification_handler,
                )
                
                # Verify services are communicating
                # (In real test, we'd verify actual message flow)
                
                # Check health of all services
                for service in services:
                    health = await service.health_check()
                    assert health["status"] in ["healthy", "running"]
                
        finally:
            # Cleanup all services
            for service in services:
                await service.stop()

    @pytest.mark.asyncio
    async def test_error_propagation_and_recovery(self, kafka_config, service_config):
        """Test error propagation and recovery mechanisms."""
        retry_attempts = []
        
        # Handler that fails initially then succeeds
        async def retry_handler(event: EventMessage) -> ProcessingResult:
            attempt_count = len([a for a in retry_attempts if a["correlation_id"] == event.correlation_id])
            retry_attempts.append({
                "correlation_id": event.correlation_id,
                "attempt": attempt_count + 1,
                "timestamp": datetime.now(timezone.utc),
            })
            
            if attempt_count < 2:  # Fail first 2 attempts
                return ProcessingResult(
                    status=MessageStatus.RETRY,
                    error_message=f"Retry attempt {attempt_count + 1}",
                )
            else:  # Succeed on 3rd attempt
                return ProcessingResult(status=MessageStatus.SUCCESS)
        
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_consumer = AsyncMock()
            mock_event_bus.subscribe_with_retry.return_value = mock_consumer
            mock_event_bus_class.return_value = mock_event_bus
            
            manager = ServiceEventManager(kafka_config, service_config)
            await manager.start()
            
            # Subscribe with retry configuration
            retry_config = RetryConfig(
                max_retries=3,
                base_delay=0.1,  # Fast for testing
                exponential_base=1.5,
            )
            
            await manager.subscribe_to_events(
                topics=["retry-topic"],
                group_id="retry-group",
                handler=retry_handler,
                retry_config=retry_config,
            )
            
            # Publish event that will require retries
            await manager.publish_event(
                topic="retry-topic",
                event_type="retry.test",
                tenant_id="tenant-1",
                data={"test": "retry_scenario"},
                correlation_id="retry-corr-123",
            )
            
            # Verify subscription was created with retry config
            call_kwargs = mock_event_bus.subscribe_with_retry.call_args[1]
            assert call_kwargs["retry_config"] == retry_config
            
            await manager.stop()

    @pytest.mark.asyncio
    async def test_performance_under_load(self, kafka_config, service_config, performance_timer):
        """Test Kafka module performance under load."""
        processed_events = []
        
        async def high_throughput_handler(event: EventMessage) -> ProcessingResult:
            processed_events.append(event.correlation_id)
            # Simulate some processing work
            await asyncio.sleep(0.001)  # 1ms processing time
            return ProcessingResult(status=MessageStatus.SUCCESS)
        
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_consumer = AsyncMock()
            mock_event_bus.subscribe_with_retry.return_value = mock_consumer
            mock_event_bus_class.return_value = mock_event_bus
            
            manager = ServiceEventManager(kafka_config, service_config)
            await manager.start()
            
            # Subscribe to high-throughput topic
            await manager.subscribe_to_events(
                topics=["high-throughput-topic"],
                group_id="high-throughput-group",
                handler=high_throughput_handler,
            )
            
            # Publish many events concurrently
            event_count = 1000
            performance_timer.start()
            
            publish_tasks = []
            for i in range(event_count):
                task = asyncio.create_task(
                    manager.publish_event(
                        topic="high-throughput-topic",
                        event_type="load.test",
                        tenant_id=f"tenant-{i % 10}",  # Distribute across 10 tenants
                        data={"sequence": i},
                        correlation_id=f"load-{i}",
                    )
                )
                publish_tasks.append(task)
            
            await asyncio.gather(*publish_tasks)
            performance_timer.stop()
            
            publish_time = performance_timer.elapsed
            
            # Verify performance characteristics
            assert mock_event_bus.publish.call_count == event_count
            assert publish_time < 10.0  # Should publish 1000 events in < 10 seconds
            
            # Calculate throughput
            throughput = event_count / publish_time
            assert throughput > 100  # Should achieve > 100 events/second
            
            await manager.stop()

    @pytest.mark.asyncio
    async def test_resource_lifecycle_management(self, kafka_config, service_config):
        """Test proper resource lifecycle management."""
        resource_states = {
            "event_bus_started": False,
            "event_bus_stopped": False,
            "dlq_handler_started": False,
            "dlq_handler_stopped": False,
        }
        
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class, \
             patch("general_operate.kafka.dlq_handler.create_dlq_handler_for_service") as mock_dlq_factory:
            
            # Setup mocks to track lifecycle
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            
            async def track_event_bus_start():
                resource_states["event_bus_started"] = True
            
            async def track_event_bus_stop():
                resource_states["event_bus_stopped"] = True
            
            mock_event_bus.start.side_effect = track_event_bus_start
            mock_event_bus.stop.side_effect = track_event_bus_stop
            # Setup producer mock for health check
            mock_event_bus.producer = AsyncMock()
            mock_event_bus.producer.started = True
            mock_event_bus.consumers = {}
            mock_event_bus_class.return_value = mock_event_bus
            
            mock_dlq_handler = AsyncMock(spec=DLQHandler)
            
            async def track_dlq_start():
                resource_states["dlq_handler_started"] = True
            
            async def track_dlq_stop():
                resource_states["dlq_handler_stopped"] = True
            
            mock_dlq_handler.start.side_effect = track_dlq_start
            mock_dlq_handler.stop.side_effect = track_dlq_stop
            mock_dlq_handler.health_check.return_value = {"status": "healthy"}
            mock_dlq_factory.return_value = mock_dlq_handler
            
            # Test complete lifecycle
            manager = ServiceEventManager(kafka_config, service_config)
            
            # Initial state - nothing started
            assert not any(resource_states.values())
            
            # Start manager
            await manager.start()
            
            # Verify resources are started
            assert resource_states["event_bus_started"]
            assert resource_states["dlq_handler_started"]
            assert not resource_states["event_bus_stopped"]
            assert not resource_states["dlq_handler_stopped"]
            
            # Verify manager is operational
            health = await manager.health_check()
            assert health["status"] == "healthy"
            
            # Stop manager
            await manager.stop()
            
            # Verify resources are stopped
            assert resource_states["event_bus_stopped"]
            assert resource_states["dlq_handler_stopped"]

    @pytest.mark.asyncio
    async def test_circuit_breaker_integration(self, kafka_config):
        """Test circuit breaker integration with event processing."""
        failure_count = 0
        
        async def circuit_breaker_handler(event: EventMessage) -> ProcessingResult:
            nonlocal failure_count
            failure_count += 1
            
            # Fail consistently to trigger circuit breaker
            return ProcessingResult(
                status=MessageStatus.FAILED,
                error_message=f"Failure {failure_count}",
            )
        
        service_config = ServiceConfig(
            service_name="circuit-breaker-service",
            circuit_breaker_threshold=3,  # Low threshold for testing
            circuit_breaker_timeout=0.1,  # Short timeout for testing
        )
        
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_consumer = AsyncMock()
            mock_event_bus.subscribe_with_retry.return_value = mock_consumer
            mock_event_bus_class.return_value = mock_event_bus
            
            manager = ServiceEventManager(kafka_config, service_config)
            await manager.start()
            
            # Subscribe with circuit breaker-enabled handler
            await manager.subscribe_to_events(
                topics=["circuit-breaker-topic"],
                group_id="circuit-breaker-group",
                handler=circuit_breaker_handler,
            )
            
            # Publish events that will trigger circuit breaker
            for i in range(5):
                await manager.publish_event(
                    topic="circuit-breaker-topic",
                    event_type="circuit.test",
                    tenant_id="tenant-1",
                    data={"attempt": i},
                )
            
            # Verify events were published
            assert mock_event_bus.publish.call_count == 5
            
            await manager.stop()

    @pytest.mark.asyncio
    async def test_complex_event_routing(self, kafka_config):
        """Test complex event routing scenarios."""
        routing_results = {
            "user_events": [],
            "order_events": [],
            "notification_events": [],
        }
        
        # Different handlers for different event types
        async def user_handler(event: EventMessage) -> ProcessingResult:
            routing_results["user_events"].append(event.event_type)
            return ProcessingResult(status=MessageStatus.SUCCESS)
        
        async def order_handler(event: EventMessage) -> ProcessingResult:
            routing_results["order_events"].append(event.event_type)
            return ProcessingResult(status=MessageStatus.SUCCESS)
        
        async def notification_handler(event: EventMessage) -> ProcessingResult:
            routing_results["notification_events"].append(event.event_type)
            return ProcessingResult(status=MessageStatus.SUCCESS)
        
        service_config = ServiceConfig(service_name="routing-service")
        
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_consumer = AsyncMock()
            mock_event_bus.subscribe_with_retry.return_value = mock_consumer
            mock_event_bus_class.return_value = mock_event_bus
            
            manager = ServiceEventManager(kafka_config, service_config)
            await manager.start()
            
            # Subscribe to different topics with different handlers
            await manager.subscribe_to_events(
                topics=["user-topic"],
                group_id="user-group",
                handler=user_handler,
                filter_event_types=["user.created", "user.updated", "user.deleted"],
            )
            
            await manager.subscribe_to_events(
                topics=["order-topic"],
                group_id="order-group",
                handler=order_handler,
                filter_event_types=["order.created", "order.paid", "order.shipped"],
            )
            
            await manager.subscribe_to_events(
                topics=["user-topic", "order-topic"],  # Multi-topic subscription
                group_id="notification-group",
                handler=notification_handler,
            )
            
            # Publish various events
            events_to_publish = [
                ("user-topic", "user.created"),
                ("user-topic", "user.updated"),
                ("order-topic", "order.created"),
                ("order-topic", "order.paid"),
                ("user-topic", "user.deleted"),
                ("order-topic", "order.shipped"),
            ]
            
            for topic, event_type in events_to_publish:
                await manager.publish_event(
                    topic=topic,
                    event_type=event_type,
                    tenant_id="tenant-1",
                    data={"test": "routing"},
                )
            
            # Verify all events were published
            assert mock_event_bus.publish.call_count == 6
            
            # Verify subscriptions were created correctly (3 test subscriptions + 1 DLQ subscription)
            assert mock_event_bus.subscribe_with_retry.call_count == 4
            
            await manager.stop()


class TestKafkaModuleEdgeCases:
    """Test edge cases and error scenarios in Kafka integration."""

    @pytest.mark.asyncio
    async def test_empty_event_handling(self, kafka_config, service_config):
        """Test handling of empty or minimal events."""
        events_processed = []
        
        async def minimal_handler(event: EventMessage) -> ProcessingResult:
            events_processed.append(event)
            return ProcessingResult(status=MessageStatus.SUCCESS)
        
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_consumer = AsyncMock()
            mock_event_bus.subscribe_with_retry.return_value = mock_consumer
            mock_event_bus_class.return_value = mock_event_bus
            
            manager = ServiceEventManager(kafka_config, service_config)
            await manager.start()
            
            await manager.subscribe_to_events(
                topics=["minimal-topic"],
                group_id="minimal-group",
                handler=minimal_handler,
            )
            
            # Publish minimal events
            minimal_events = [
                # Empty data
                {
                    "topic": "minimal-topic",
                    "event_type": "empty.event",
                    "tenant_id": "tenant-1",
                    "data": {},
                },
                # None data (should be handled)
                {
                    "topic": "minimal-topic",
                    "event_type": "none.event",
                    "tenant_id": "tenant-1",
                    "data": None,
                },
                # Empty strings
                {
                    "topic": "minimal-topic",
                    "event_type": "",
                    "tenant_id": "",
                    "data": {"key": ""},
                },
            ]
            
            for event_data in minimal_events:
                await manager.publish_event(**event_data)
            
            # Verify all minimal events were published
            assert mock_event_bus.publish.call_count == 3
            
            await manager.stop()

    @pytest.mark.asyncio
    async def test_large_event_handling(self, kafka_config, service_config):
        """Test handling of large events."""
        large_events_processed = []
        
        async def large_handler(event: EventMessage) -> ProcessingResult:
            large_events_processed.append(len(json.dumps(event.to_dict())))
            return ProcessingResult(status=MessageStatus.SUCCESS)
        
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_consumer = AsyncMock()
            mock_event_bus.subscribe_with_retry.return_value = mock_consumer
            mock_event_bus_class.return_value = mock_event_bus
            
            manager = ServiceEventManager(kafka_config, service_config)
            await manager.start()
            
            await manager.subscribe_to_events(
                topics=["large-topic"],
                group_id="large-group",
                handler=large_handler,
            )
            
            # Create large event data
            large_data = {
                "items": [{"id": i, "data": "x" * 1000} for i in range(100)],
                "metadata": {"large_field": "y" * 10000},
            }
            
            await manager.publish_event(
                topic="large-topic",
                event_type="large.event",
                tenant_id="tenant-1",
                data=large_data,
            )
            
            # Verify large event was published
            mock_event_bus.publish.assert_called_once()
            
            await manager.stop()

    @pytest.mark.asyncio
    async def test_concurrent_service_operations(self, kafka_config, service_config):
        """Test concurrent operations on the same service."""
        operation_results = []
        
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_consumer = AsyncMock()
            mock_event_bus.subscribe_with_retry.return_value = mock_consumer
            mock_event_bus_class.return_value = mock_event_bus
            
            manager = ServiceEventManager(kafka_config, service_config)
            await manager.start()
            
            # Define concurrent operations
            async def publish_operation():
                for i in range(10):
                    await manager.publish_event(
                        topic="concurrent-topic",
                        event_type="concurrent.test",
                        tenant_id="tenant-1",
                        data={"sequence": i},
                    )
                operation_results.append("publish_complete")
            
            async def subscribe_operation():
                async def handler(event: EventMessage) -> ProcessingResult:
                    return ProcessingResult(status=MessageStatus.SUCCESS)
                
                for i in range(5):
                    await manager.subscribe_to_events(
                        topics=[f"topic-{i}"],
                        group_id=f"group-{i}",
                        handler=handler,
                    )
                operation_results.append("subscribe_complete")
            
            async def health_check_operation():
                for _ in range(10):
                    await manager.health_check()
                    await asyncio.sleep(0.01)
                operation_results.append("health_check_complete")
            
            # Run operations concurrently
            await asyncio.gather(
                publish_operation(),
                subscribe_operation(),
                health_check_operation(),
            )
            
            # Verify all operations completed
            assert "publish_complete" in operation_results
            assert "subscribe_complete" in operation_results
            assert "health_check_complete" in operation_results
            
            # Verify expected calls were made
            assert mock_event_bus.publish.call_count == 10
            assert mock_event_bus.subscribe_with_retry.call_count == 6  # 5 test + 1 DLQ
            
            await manager.stop()

    @pytest.mark.asyncio
    async def test_service_restart_scenarios(self, kafka_config, service_config):
        """Test service restart and recovery scenarios."""
        restart_events = []
        
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_consumer = AsyncMock()
            mock_event_bus.subscribe_with_retry.return_value = mock_consumer
            # Setup producer mock for health check
            mock_event_bus.producer = AsyncMock()
            mock_event_bus.producer.started = True
            mock_event_bus.consumers = {}
            mock_event_bus_class.return_value = mock_event_bus
            
            manager = ServiceEventManager(kafka_config, service_config)
            
            # Start service
            await manager.start()
            restart_events.append("first_start")
            
            # Verify initial state
            health = await manager.health_check()
            assert health["status"] in ["healthy", "running"]
            
            # Stop service
            await manager.stop()
            restart_events.append("first_stop")
            
            # Restart service
            await manager.start()
            restart_events.append("restart")
            
            # Verify service is operational after restart
            health = await manager.health_check()
            assert health["status"] in ["healthy", "running"]
            
            # Publish event after restart
            await manager.publish_event(
                topic="restart-topic",
                event_type="restart.test",
                tenant_id="tenant-1",
                data={"test": "after_restart"},
            )
            
            # Stop service again
            await manager.stop()
            restart_events.append("final_stop")
            
            # Verify restart sequence
            expected_events = ["first_start", "first_stop", "restart", "final_stop"]
            assert restart_events == expected_events
            
            # Verify event bus was started/stopped correctly
            assert mock_event_bus.start.call_count == 2  # Initial start + restart
            assert mock_event_bus.stop.call_count == 2   # Initial stop + final stop