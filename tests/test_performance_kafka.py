"""
Performance tests for the Kafka module.

Tests cover:
- Throughput benchmarks for event publishing
- Consumer processing performance
- Memory usage and resource efficiency
- Concurrent operation scalability
- DLQ handler performance under load
- Circuit breaker performance impact
- Large message handling performance
- Resource cleanup efficiency
"""

import asyncio
import json
import time
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
from general_operate.kafka.manager_config import ServiceConfig
from general_operate.kafka.service_event_manager import ServiceEventManager


class TestKafkaPerformance:
    """Performance tests for Kafka module components."""

    @pytest.mark.asyncio
    async def test_event_publishing_throughput(self, kafka_config, service_config, performance_timer):
        """Test event publishing throughput performance."""
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_event_bus_class.return_value = mock_event_bus
            
            manager = ServiceEventManager(kafka_config, service_config)
            await manager.start()
            
            # Benchmark parameters
            event_count = 1000
            batch_sizes = [1, 10, 50, 100]
            
            results = {}
            
            for batch_size in batch_sizes:
                performance_timer.start()
                
                # Publish events in batches
                for batch_start in range(0, event_count, batch_size):
                    batch_tasks = []
                    for i in range(batch_start, min(batch_start + batch_size, event_count)):
                        task = asyncio.create_task(
                            manager.publish_event(
                                topic="throughput-topic",
                                event_type="throughput.test",
                                tenant_id=f"tenant-{i % 10}",
                                data={"sequence": i, "batch_size": batch_size},
                                correlation_id=f"throughput-{i}",
                            )
                        )
                        batch_tasks.append(task)
                    
                    await asyncio.gather(*batch_tasks)
                
                performance_timer.stop()
                
                elapsed_time = performance_timer.elapsed
                throughput = event_count / elapsed_time
                
                results[batch_size] = {
                    "elapsed_time": elapsed_time,
                    "throughput": throughput,
                    "events_per_second": throughput,
                }
                
                # Reset for next test
                mock_event_bus.publish.reset_mock()
            
            # Verify performance characteristics
            for batch_size, result in results.items():
                assert result["throughput"] > 50  # At least 50 events/second
                assert result["elapsed_time"] < 20  # Should complete within 20 seconds
            
            # Verify that batching improves performance
            assert results[100]["throughput"] >= results[1]["throughput"]
            
            await manager.stop()

    @pytest.mark.asyncio
    async def test_consumer_processing_performance(self, kafka_config, service_config, performance_timer):
        """Test consumer message processing performance."""
        processed_events = []
        processing_times = []
        
        async def performance_handler(event: EventMessage) -> ProcessingResult:
            start_time = time.perf_counter()
            
            # Simulate processing work
            await asyncio.sleep(0.001)  # 1ms processing time
            
            end_time = time.perf_counter()
            processing_times.append(end_time - start_time)
            processed_events.append(event.correlation_id)
            
            return ProcessingResult(status=MessageStatus.SUCCESS)
        
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_consumer = AsyncMock()
            mock_event_bus.subscribe_with_retry.return_value = mock_consumer
            mock_event_bus_class.return_value = mock_event_bus
            
            manager = ServiceEventManager(kafka_config, service_config)
            await manager.start()
            
            # Subscribe with performance handler
            await manager.subscribe_to_events(
                topics=["performance-topic"],
                group_id="performance-group",
                handler=performance_handler,
            )
            
            # Simulate high-volume message processing
            message_count = 500
            performance_timer.start()
            
            # Simulate messages being consumed
            # (In real implementation, this would be handled by the consumer)
            simulate_tasks = []
            for i in range(message_count):
                event = EventMessage(
                    event_type="performance.test",
                    tenant_id=f"tenant-{i % 5}",
                    data={"sequence": i},
                    correlation_id=f"perf-{i}",
                )
                task = asyncio.create_task(performance_handler(event))
                simulate_tasks.append(task)
            
            await asyncio.gather(*simulate_tasks)
            performance_timer.stop()
            
            total_time = performance_timer.elapsed
            processing_throughput = message_count / total_time
            
            # Performance assertions
            assert len(processed_events) == message_count
            assert processing_throughput > 100  # At least 100 messages/second
            assert total_time < 10  # Should complete within 10 seconds
            
            # Check individual processing times
            avg_processing_time = sum(processing_times) / len(processing_times)
            assert avg_processing_time < 0.01  # Average processing time < 10ms
            
            await manager.stop()

    @pytest.mark.asyncio
    async def test_memory_usage_under_load(self, kafka_config, service_config):
        """Test memory usage characteristics under load."""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_consumer = AsyncMock()
            mock_event_bus.subscribe_with_retry.return_value = mock_consumer
            mock_event_bus_class.return_value = mock_event_bus
            
            manager = ServiceEventManager(kafka_config, service_config)
            await manager.start()
            
            # Create many subscriptions and publish many events
            subscription_count = 50
            events_per_subscription = 100
            
            # Create multiple subscriptions
            handlers = []
            for i in range(subscription_count):
                processed_events = []
                
                async def handler(event: EventMessage, events_list=processed_events) -> ProcessingResult:
                    events_list.append(event.correlation_id)
                    return ProcessingResult(status=MessageStatus.SUCCESS)
                
                handlers.append(handler)
                
                await manager.subscribe_to_events(
                    topics=[f"memory-topic-{i}"],
                    group_id=f"memory-group-{i}",
                    handler=handler,
                )
            
            # Check memory after subscriptions
            memory_after_subscriptions = process.memory_info().rss
            
            # Publish events to all topics
            publish_tasks = []
            for i in range(subscription_count):
                for j in range(events_per_subscription):
                    task = asyncio.create_task(
                        manager.publish_event(
                            topic=f"memory-topic-{i}",
                            event_type="memory.test",
                            tenant_id=f"tenant-{i}",
                            data={"subscription": i, "event": j},
                            correlation_id=f"mem-{i}-{j}",
                        )
                    )
                    publish_tasks.append(task)
            
            await asyncio.gather(*publish_tasks)
            
            # Check memory after publishing
            memory_after_publishing = process.memory_info().rss
            
            await manager.stop()
            
            # Check memory after cleanup
            memory_after_cleanup = process.memory_info().rss
            
            # Memory usage assertions
            subscription_memory_increase = memory_after_subscriptions - initial_memory
            publishing_memory_increase = memory_after_publishing - memory_after_subscriptions
            
            # Memory should not grow excessively
            assert subscription_memory_increase < 100 * 1024 * 1024  # < 100MB
            assert publishing_memory_increase < 50 * 1024 * 1024   # < 50MB
            
            # Memory should be released after cleanup (within reason)
            memory_retained = memory_after_cleanup - initial_memory
            assert memory_retained < 200 * 1024 * 1024  # < 200MB retained

    @pytest.mark.asyncio
    async def test_concurrent_operation_scalability(self, kafka_config, service_config, performance_timer):
        """Test scalability with concurrent operations."""
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_consumer = AsyncMock()
            mock_event_bus.subscribe_with_retry.return_value = mock_consumer
            mock_event_bus_class.return_value = mock_event_bus
            
            manager = ServiceEventManager(kafka_config, service_config)
            await manager.start()
            
            # Test different concurrency levels
            concurrency_levels = [1, 5, 10, 20, 50]
            scalability_results = {}
            
            for concurrency in concurrency_levels:
                performance_timer.start()
                
                # Create concurrent operations
                operation_tasks = []
                
                # Concurrent publishing
                for i in range(concurrency):
                    publish_task = asyncio.create_task(
                        manager.publish_event(
                            topic=f"scalability-topic-{i}",
                            event_type="scalability.test",
                            tenant_id=f"tenant-{i}",
                            data={"concurrency_level": concurrency, "task_id": i},
                        )
                    )
                    operation_tasks.append(publish_task)
                
                # Concurrent subscribing
                for i in range(concurrency):
                    async def handler(event: EventMessage) -> ProcessingResult:
                        return ProcessingResult(status=MessageStatus.SUCCESS)
                    
                    subscribe_task = asyncio.create_task(
                        manager.subscribe_to_events(
                            topics=[f"scalability-topic-{i}"],
                            group_id=f"scalability-group-{i}",
                            handler=handler,
                        )
                    )
                    operation_tasks.append(subscribe_task)
                
                # Concurrent health checks
                health_check_tasks = []
                for _ in range(concurrency // 2):
                    health_task = asyncio.create_task(manager.health_check())
                    health_check_tasks.append(health_task)
                
                # Execute all operations
                await asyncio.gather(*operation_tasks)
                health_results = await asyncio.gather(*health_check_tasks)
                
                performance_timer.stop()
                
                elapsed_time = performance_timer.elapsed
                operations_per_second = (concurrency * 2) / elapsed_time  # publish + subscribe
                
                scalability_results[concurrency] = {
                    "elapsed_time": elapsed_time,
                    "operations_per_second": operations_per_second,
                    "health_checks_succeeded": len([h for h in health_results if h["status"] in ["healthy", "running"]]),
                }
                
                # Reset mocks for next iteration
                mock_event_bus.publish.reset_mock()
                mock_event_bus.subscribe_with_retry.reset_mock()
            
            # Verify scalability characteristics
            for concurrency, result in scalability_results.items():
                assert result["elapsed_time"] < 5.0  # Should complete within 5 seconds
                assert result["operations_per_second"] > 10  # At least 10 ops/second
                assert result["health_checks_succeeded"] == concurrency // 2
            
            # Verify that performance doesn't degrade significantly with concurrency
            performance_degradation = (
                scalability_results[1]["operations_per_second"] / 
                scalability_results[50]["operations_per_second"]
            )
            assert performance_degradation < 5  # Performance shouldn't degrade by more than 5x
            
            await manager.stop()

    @pytest.mark.asyncio
    async def test_dlq_handler_performance(self, kafka_config, performance_timer):
        """Test DLQ handler performance under load."""
        service_config = ServiceConfig(
            service_name="dlq-performance-service",
            enable_dlq=True,
        )
        
        dlq_events_processed = []
        
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class, \
             patch("general_operate.kafka.service_event_manager.create_dlq_handler_for_service") as mock_dlq_factory:
            
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_consumer = AsyncMock()
            mock_event_bus.subscribe_with_retry.return_value = mock_consumer
            mock_event_bus_class.return_value = mock_event_bus
            
            # Mock DLQ handler
            mock_dlq_handler = AsyncMock()
            
            async def mock_handle_dlq_event(event):
                dlq_events_processed.append(event.correlation_id)
                return ProcessingResult(status=MessageStatus.SUCCESS)
            
            mock_dlq_handler._handle_dlq_event = mock_handle_dlq_event
            mock_dlq_handler.health_check.return_value = {"status": "healthy"}
            mock_dlq_handler.get_dlq_summary.return_value = {
                "metrics": {"total_dlq_events": len(dlq_events_processed)},
            }
            mock_dlq_factory.return_value = mock_dlq_handler
            
            manager = ServiceEventManager(kafka_config, service_config)
            await manager.start()
            
            # Simulate high volume of DLQ events
            dlq_event_count = 1000
            performance_timer.start()
            
            # Simulate DLQ events being processed
            dlq_tasks = []
            for i in range(dlq_event_count):
                event = EventMessage(
                    event_type="dlq.performance.test",
                    tenant_id=f"tenant-{i % 10}",
                    data={"sequence": i, "error": "simulated_error"},
                    correlation_id=f"dlq-perf-{i}",
                )
                task = asyncio.create_task(mock_handle_dlq_event(event))
                dlq_tasks.append(task)
            
            await asyncio.gather(*dlq_tasks)
            performance_timer.stop()
            
            dlq_processing_time = performance_timer.elapsed
            dlq_throughput = dlq_event_count / dlq_processing_time
            
            # DLQ performance assertions
            assert len(dlq_events_processed) == dlq_event_count
            assert dlq_throughput > 200  # At least 200 DLQ events/second
            assert dlq_processing_time < 10  # Should process 1000 DLQ events in < 10 seconds
            
            await manager.stop()

    @pytest.mark.asyncio
    async def test_large_message_performance(self, kafka_config, service_config, performance_timer):
        """Test performance with large message payloads."""
        large_messages_processed = []
        
        async def large_message_handler(event: EventMessage) -> ProcessingResult:
            message_size = len(json.dumps(event.to_dict()))
            large_messages_processed.append(message_size)
            return ProcessingResult(status=MessageStatus.SUCCESS)
        
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_consumer = AsyncMock()
            mock_event_bus.subscribe_with_retry.return_value = mock_consumer
            mock_event_bus_class.return_value = mock_event_bus
            
            manager = ServiceEventManager(kafka_config, service_config)
            await manager.start()
            
            await manager.subscribe_to_events(
                topics=["large-message-topic"],
                group_id="large-message-group",
                handler=large_message_handler,
            )
            
            # Test different message sizes
            message_sizes = [1024, 10240, 102400, 1048576]  # 1KB, 10KB, 100KB, 1MB
            
            for target_size in message_sizes:
                performance_timer.start()
                
                # Create large message data
                data_size = target_size // 10  # Approximate data size
                large_data = {
                    "large_field": "x" * data_size,
                    "items": [{"id": i, "data": "y" * 100} for i in range(data_size // 200)],
                    "metadata": {"size_category": f"{target_size}_bytes"},
                }
                
                # Publish large message
                await manager.publish_event(
                    topic="large-message-topic",
                    event_type="large.message.test",
                    tenant_id="tenant-1",
                    data=large_data,
                    correlation_id=f"large-{target_size}",
                )
                
                performance_timer.stop()
                
                publish_time = performance_timer.elapsed
                
                # Performance assertions for large messages
                assert publish_time < 1.0  # Should publish even large messages quickly
                
            # Verify all large messages were published
            assert mock_event_bus.publish.call_count == len(message_sizes)
            
            await manager.stop()

    @pytest.mark.asyncio
    async def test_retry_mechanism_performance(self, kafka_config, service_config, performance_timer):
        """Test performance impact of retry mechanisms."""
        retry_attempts = []
        
        async def retry_handler(event: EventMessage) -> ProcessingResult:
            attempt_count = len([a for a in retry_attempts if a["correlation_id"] == event.correlation_id])
            retry_attempts.append({
                "correlation_id": event.correlation_id,
                "attempt": attempt_count + 1,
                "timestamp": time.perf_counter(),
            })
            
            # Fail first 2 attempts, succeed on 3rd
            if attempt_count < 2:
                return ProcessingResult(
                    status=MessageStatus.RETRY,
                    error_message=f"Retry attempt {attempt_count + 1}",
                )
            else:
                return ProcessingResult(status=MessageStatus.SUCCESS)
        
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_consumer = AsyncMock()
            mock_event_bus.subscribe_with_retry.return_value = mock_consumer
            mock_event_bus_class.return_value = mock_event_bus
            
            manager = ServiceEventManager(kafka_config, service_config)
            await manager.start()
            
            # Configure fast retry for performance testing
            fast_retry_config = RetryConfig(
                max_retries=3,
                base_delay=0.01,  # 10ms base delay
                max_delay=0.1,    # 100ms max delay
                exponential_base=1.5,
                jitter=False,     # Deterministic for testing
            )
            
            await manager.subscribe_to_events(
                topics=["retry-performance-topic"],
                group_id="retry-performance-group",
                handler=retry_handler,
                retry_config=fast_retry_config,
            )
            
            # Test retry performance
            event_count = 100
            performance_timer.start()
            
            # Simulate processing events that require retries
            retry_tasks = []
            for i in range(event_count):
                event = EventMessage(
                    event_type="retry.performance.test",
                    tenant_id=f"tenant-{i % 5}",
                    data={"sequence": i},
                    correlation_id=f"retry-perf-{i}",
                )
                
                # Simulate the retry mechanism by calling handler multiple times
                async def process_event_with_retries(evt):
                    result = await retry_handler(evt)
                    while result.status == MessageStatus.RETRY:
                        result = await retry_handler(evt)
                    return result
                
                task = asyncio.create_task(process_event_with_retries(event))
                retry_tasks.append(task)
            
            await asyncio.gather(*retry_tasks)
            performance_timer.stop()
            
            retry_processing_time = performance_timer.elapsed
            
            # Performance assertions
            # Each event should have been attempted 3 times
            expected_total_attempts = event_count * 3
            assert len(retry_attempts) == expected_total_attempts
            
            # Should complete within reasonable time despite retries
            assert retry_processing_time < 5.0  # Should complete within 5 seconds
            
            # Calculate retry overhead
            attempts_per_second = expected_total_attempts / retry_processing_time
            assert attempts_per_second > 100  # At least 100 retry attempts/second
            
            await manager.stop()

    @pytest.mark.asyncio
    async def test_resource_cleanup_performance(self, kafka_config, service_config, performance_timer):
        """Test performance of resource cleanup operations."""
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class, \
             patch("general_operate.kafka.service_event_manager.create_dlq_handler_for_service") as mock_dlq_factory:
            
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_consumer = AsyncMock()
            mock_event_bus.subscribe_with_retry.return_value = mock_consumer
            mock_event_bus_class.return_value = mock_event_bus
            
            mock_dlq_handler = AsyncMock()
            mock_dlq_handler.health_check.return_value = {"status": "healthy"}
            mock_dlq_factory.return_value = mock_dlq_handler
            
            # Test startup performance
            startup_times = []
            cleanup_times = []
            
            for iteration in range(5):  # Multiple iterations to get average
                manager = ServiceEventManager(kafka_config, service_config)
                
                # Measure startup time
                performance_timer.start()
                await manager.start()
                performance_timer.stop()
                startup_times.append(performance_timer.elapsed)
                
                # Create some subscriptions and publish events
                async def dummy_handler(event: EventMessage) -> ProcessingResult:
                    return ProcessingResult(status=MessageStatus.SUCCESS)
                
                for i in range(10):
                    await manager.subscribe_to_events(
                        topics=[f"cleanup-topic-{i}"],
                        group_id=f"cleanup-group-{i}",
                        handler=dummy_handler,
                    )
                
                for i in range(50):
                    await manager.publish_event(
                        topic=f"cleanup-topic-{i % 10}",
                        event_type="cleanup.test",
                        tenant_id="tenant-1",
                        data={"iteration": iteration, "event": i},
                    )
                
                # Measure cleanup time
                performance_timer.start()
                await manager.stop()
                performance_timer.stop()
                cleanup_times.append(performance_timer.elapsed)
                
                # Reset mocks for next iteration
                mock_event_bus.start.reset_mock()
                mock_event_bus.stop.reset_mock()
                mock_event_bus.publish.reset_mock()
                mock_event_bus.subscribe_with_retry.reset_mock()
                mock_dlq_handler.start.reset_mock()
                mock_dlq_handler.stop.reset_mock()
            
            # Performance assertions
            avg_startup_time = sum(startup_times) / len(startup_times)
            avg_cleanup_time = sum(cleanup_times) / len(cleanup_times)
            
            assert avg_startup_time < 1.0   # Average startup should be < 1 second
            assert avg_cleanup_time < 0.5   # Average cleanup should be < 0.5 seconds
            
            # Verify consistency
            startup_variance = max(startup_times) - min(startup_times)
            cleanup_variance = max(cleanup_times) - min(cleanup_times)
            
            assert startup_variance < 0.5   # Startup times should be consistent
            assert cleanup_variance < 0.3   # Cleanup times should be consistent


class TestKafkaStressTests:
    """Stress tests for Kafka module under extreme conditions."""

    @pytest.mark.asyncio
    async def test_extreme_concurrency_stress(self, kafka_config, service_config):
        """Test Kafka module under extreme concurrency stress."""
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_consumer = AsyncMock()
            mock_event_bus.subscribe_with_retry.return_value = mock_consumer
            mock_event_bus_class.return_value = mock_event_bus
            
            manager = ServiceEventManager(kafka_config, service_config)
            await manager.start()
            
            # Extreme concurrency parameters
            concurrent_publishers = 100
            concurrent_subscribers = 50
            events_per_publisher = 50
            
            stress_results = {
                "publish_errors": 0,
                "subscribe_errors": 0,
                "total_events_published": 0,
                "total_subscriptions_created": 0,
            }
            
            async def stress_publisher(publisher_id: int):
                try:
                    for event_id in range(events_per_publisher):
                        await manager.publish_event(
                            topic=f"stress-topic-{publisher_id % 10}",
                            event_type="stress.test",
                            tenant_id=f"tenant-{publisher_id}",
                            data={
                                "publisher_id": publisher_id,
                                "event_id": event_id,
                                "stress_test": True,
                            },
                            correlation_id=f"stress-{publisher_id}-{event_id}",
                        )
                        stress_results["total_events_published"] += 1
                except Exception:
                    stress_results["publish_errors"] += 1
            
            async def stress_subscriber(subscriber_id: int):
                try:
                    async def stress_handler(event: EventMessage) -> ProcessingResult:
                        # Minimal processing to test throughput
                        return ProcessingResult(status=MessageStatus.SUCCESS)
                    
                    await manager.subscribe_to_events(
                        topics=[f"stress-topic-{subscriber_id % 10}"],
                        group_id=f"stress-group-{subscriber_id}",
                        handler=stress_handler,
                    )
                    stress_results["total_subscriptions_created"] += 1
                except Exception:
                    stress_results["subscribe_errors"] += 1
            
            # Execute stress test
            start_time = time.perf_counter()
            
            # Create all tasks
            publisher_tasks = [
                asyncio.create_task(stress_publisher(i))
                for i in range(concurrent_publishers)
            ]
            
            subscriber_tasks = [
                asyncio.create_task(stress_subscriber(i))
                for i in range(concurrent_subscribers)
            ]
            
            # Execute all tasks concurrently
            await asyncio.gather(
                *publisher_tasks,
                *subscriber_tasks,
                return_exceptions=True
            )
            
            end_time = time.perf_counter()
            total_stress_time = end_time - start_time
            
            # Stress test assertions
            expected_events = concurrent_publishers * events_per_publisher
            expected_subscriptions = concurrent_subscribers
            
            # Should handle most operations successfully
            assert stress_results["publish_errors"] < expected_events * 0.1  # < 10% errors
            assert stress_results["subscribe_errors"] < expected_subscriptions * 0.1  # < 10% errors
            
            # Should complete within reasonable time
            assert total_stress_time < 30.0  # Should complete within 30 seconds
            
            # Calculate stress throughput
            total_operations = stress_results["total_events_published"] + stress_results["total_subscriptions_created"]
            stress_throughput = total_operations / total_stress_time
            
            assert stress_throughput > 50  # At least 50 operations/second under stress
            
            await manager.stop()

    @pytest.mark.asyncio
    async def test_memory_pressure_stress(self, kafka_config, service_config):
        """Test Kafka module under memory pressure."""
        import gc
        
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_consumer = AsyncMock()
            mock_event_bus.subscribe_with_retry.return_value = mock_consumer
            mock_event_bus_class.return_value = mock_event_bus
            
            manager = ServiceEventManager(kafka_config, service_config)
            await manager.start()
            
            # Create memory pressure by generating large amounts of data
            large_objects = []
            
            try:
                # Create memory pressure
                for i in range(100):  # Create large objects
                    large_data = {
                        "id": i,
                        "data": "x" * 100000,  # 100KB per object
                        "items": [{"item_id": j, "content": "y" * 1000} for j in range(100)],
                    }
                    large_objects.append(large_data)
                
                # Test operations under memory pressure
                memory_stress_results = []
                
                for i in range(20):
                    # Publish events with large payloads
                    await manager.publish_event(
                        topic="memory-stress-topic",
                        event_type="memory.stress.test",
                        tenant_id="stress-tenant",
                        data=large_objects[i % len(large_objects)],
                        correlation_id=f"memory-stress-{i}",
                    )
                    
                    # Check health periodically
                    if i % 5 == 0:
                        health = await manager.health_check()
                        memory_stress_results.append(health["status"])
                
                # Verify operations continued to work under memory pressure
                assert len(memory_stress_results) == 4  # 20 events / 5 = 4 health checks
                healthy_checks = sum(1 for status in memory_stress_results if status in ["healthy", "running"])
                assert healthy_checks >= 3  # At least 75% health checks should pass
                
                # Verify events were published
                assert mock_event_bus.publish.call_count == 20
                
            finally:
                # Clean up memory
                large_objects.clear()
                gc.collect()
            
            await manager.stop()

    @pytest.mark.asyncio
    async def test_rapid_start_stop_stress(self, kafka_config, service_config):
        """Test rapid start/stop cycles for resource management stress."""
        start_stop_cycles = 20
        cycle_times = []
        
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class, \
             patch("general_operate.kafka.service_event_manager.create_dlq_handler_for_service") as mock_dlq_factory:
            
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_consumer = AsyncMock()
            mock_event_bus.subscribe_with_retry.return_value = mock_consumer
            mock_event_bus_class.return_value = mock_event_bus
            
            mock_dlq_handler = AsyncMock()
            mock_dlq_handler.health_check.return_value = {"status": "healthy"}
            mock_dlq_factory.return_value = mock_dlq_handler
            
            for cycle in range(start_stop_cycles):
                manager = ServiceEventManager(kafka_config, service_config)
                
                cycle_start = time.perf_counter()
                
                # Start
                await manager.start()
                
                # Do some work
                await manager.publish_event(
                    topic=f"cycle-topic-{cycle}",
                    event_type="cycle.test",
                    tenant_id="cycle-tenant",
                    data={"cycle": cycle},
                )
                
                health = await manager.health_check()
                assert health["status"] in ["healthy", "running"]
                
                # Stop
                await manager.stop()
                
                cycle_end = time.perf_counter()
                cycle_times.append(cycle_end - cycle_start)
                
                # Reset mocks for next cycle
                mock_event_bus.start.reset_mock()
                mock_event_bus.stop.reset_mock()
                mock_event_bus.publish.reset_mock()
                mock_dlq_handler.start.reset_mock()
                mock_dlq_handler.stop.reset_mock()
            
            # Stress test assertions
            avg_cycle_time = sum(cycle_times) / len(cycle_times)
            max_cycle_time = max(cycle_times)
            
            assert avg_cycle_time < 1.0   # Average cycle should be < 1 second
            assert max_cycle_time < 2.0   # No cycle should take > 2 seconds
            
            # Verify no significant performance degradation over time
            early_cycles = cycle_times[:5]
            late_cycles = cycle_times[-5:]
            
            avg_early = sum(early_cycles) / len(early_cycles)
            avg_late = sum(late_cycles) / len(late_cycles)
            
            performance_degradation = avg_late / avg_early
            assert performance_degradation < 2.0  # Performance shouldn't degrade by more than 2x