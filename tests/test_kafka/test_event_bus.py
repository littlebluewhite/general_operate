"""
Unit tests for Kafka event bus - no external dependencies required
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from dataclasses import dataclass
from typing import Any
from datetime import datetime, UTC

from general_operate.kafka.event_bus import KafkaEventBus, EventSubscription
from general_operate.kafka.models.event_message import EventMessage
from general_operate.kafka.exceptions import (
    KafkaProducerException,
    KafkaConsumerException,
    KafkaConfigurationException
)


class TestEventSubscription:
    """Test EventSubscription dataclass"""
    
    def test_event_subscription_creation(self):
        """Test creating EventSubscription"""
        async def handler(event):
            pass
            
        def filter_func(event):
            return True
            
        subscription = EventSubscription(
            subscription_id="test-id",
            event_type="test-event",
            handler=handler,
            filter_func=filter_func
        )
        
        assert subscription.subscription_id == "test-id"
        assert subscription.event_type == "test-event"
        assert subscription.handler == handler
        assert subscription.filter_func == filter_func
    
    def test_event_subscription_without_filter(self):
        """Test creating EventSubscription without filter"""
        async def handler(event):
            pass
            
        subscription = EventSubscription(
            subscription_id="test-id",
            event_type="test-event",
            handler=handler
        )
        
        assert subscription.filter_func is None


class TestKafkaEventBusInitialization:
    """Test KafkaEventBus initialization"""
    
    @patch('general_operate.kafka.event_bus.KafkaProducerOperate')
    def test_initialization_minimal_config(self, mock_producer_class):
        """Test initialization with minimal configuration"""
        event_bus = KafkaEventBus(
            bootstrap_servers="localhost:9092"
        )
        
        assert event_bus.bootstrap_servers == "localhost:9092"
        assert event_bus.client_id == "event-bus"
        assert event_bus.default_topic == "events"
        assert event_bus.config == {}
        assert event_bus._started is False
        assert len(event_bus._consumers) == 0
        assert len(event_bus._subscriptions) == 0
        assert len(event_bus._subscription_tasks) == 0
        
        # Check producer initialization
        mock_producer_class.assert_called_once()
    
    @patch('general_operate.kafka.event_bus.KafkaProducerOperate')
    def test_initialization_full_config(self, mock_producer_class):
        """Test initialization with full configuration"""
        config = {
            "enable_transactions": True,
            "custom_param": "value"
        }
        
        event_bus = KafkaEventBus(
            bootstrap_servers=["broker1:9092", "broker2:9092"],
            client_id="custom-bus",
            default_topic="custom-events",
            config=config
        )
        
        assert event_bus.bootstrap_servers == ["broker1:9092", "broker2:9092"]
        assert event_bus.client_id == "custom-bus"
        assert event_bus.default_topic == "custom-events"
        assert event_bus.config == config
        
        # Check producer was created with correct config
        mock_producer_class.assert_called_once()
        call_args = mock_producer_class.call_args
        assert call_args[1]["bootstrap_servers"] == ["broker1:9092", "broker2:9092"]
        assert call_args[1]["client_id"] == "custom-bus-producer"
        assert call_args[1]["config"]["enable_transactions"] is True
        assert call_args[1]["config"]["transactional_id"] == "custom-bus-tx"


class TestKafkaEventBusLifecycle:
    """Test event bus lifecycle management"""
    
    @patch('general_operate.kafka.event_bus.KafkaProducerOperate')
    @pytest.mark.asyncio
    async def test_start_success(self, mock_producer_class):
        """Test successful event bus start"""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        event_bus = KafkaEventBus(bootstrap_servers="localhost:9092")
        
        await event_bus.start()
        
        assert event_bus._started is True
        mock_producer.start.assert_called_once()
    
    @patch('general_operate.kafka.event_bus.KafkaProducerOperate')
    @pytest.mark.asyncio
    async def test_start_idempotent(self, mock_producer_class):
        """Test that multiple start calls are idempotent"""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        event_bus = KafkaEventBus(bootstrap_servers="localhost:9092")
        
        await event_bus.start()
        await event_bus.start()  # Second call should be no-op
        
        assert event_bus._started is True
        mock_producer.start.assert_called_once()  # Only called once
    
    @patch('general_operate.kafka.event_bus.KafkaProducerOperate')
    @pytest.mark.asyncio
    async def test_stop_without_start(self, mock_producer_class):
        """Test stop when event bus was never started"""
        event_bus = KafkaEventBus(bootstrap_servers="localhost:9092")
        
        await event_bus.stop()  # Should not raise an exception
        
        assert event_bus._started is False
    
    @patch('general_operate.kafka.event_bus.KafkaProducerOperate')
    @patch('general_operate.kafka.event_bus.asyncio.gather')
    @pytest.mark.asyncio
    async def test_stop_with_consumers_and_tasks(self, mock_gather, mock_producer_class):
        """Test stop with active consumers and subscription tasks"""
        # Mock gather to return an awaitable
        async def mock_gather_func(*args, **kwargs):
            return []
        mock_gather.side_effect = mock_gather_func
        
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        # Mock consumer
        mock_consumer = AsyncMock()
        
        # Mock running task
        mock_task = MagicMock()
        mock_task.cancel = MagicMock()
        
        event_bus = KafkaEventBus(bootstrap_servers="localhost:9092")
        event_bus._started = True
        event_bus._consumers["test-topic"] = mock_consumer
        event_bus._subscription_tasks["test-topic"] = mock_task
        
        await event_bus.stop()
        
        assert event_bus._started is False
        mock_task.cancel.assert_called_once()
        mock_consumer.stop.assert_called_once()
        mock_producer.stop.assert_called_once()


class TestKafkaEventBusPublish:
    """Test event publishing functionality"""
    
    @patch('general_operate.kafka.event_bus.KafkaProducerOperate')
    @patch('general_operate.kafka.event_bus.uuid.uuid4')
    @patch('general_operate.kafka.event_bus.datetime')
    @pytest.mark.asyncio
    async def test_publish_basic_event(self, mock_datetime, mock_uuid, mock_producer_class):
        """Test basic event publishing"""
        # Mock dependencies
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        mock_uuid.return_value.hex = "test-correlation-id"
        mock_datetime.now.return_value.isoformat.return_value = "2023-12-01T10:00:00Z"
        
        event_bus = KafkaEventBus(bootstrap_servers="localhost:9092")
        
        event = await event_bus.publish(
            event_type="user.login",
            data={"user_id": "123", "ip": "192.168.1.1"},
            tenant_id="tenant-1",
            user_id="user-123"
        )
        
        # Check that producer was started and event was sent
        mock_producer.start.assert_called_once()
        mock_producer.send_event.assert_called_once()
        
        # Verify send_event call arguments
        call_args = mock_producer.send_event.call_args
        assert call_args[1]["topic"] == "events"  # default topic
        assert call_args[1]["key"] == "tenant-1"
        assert call_args[1]["headers"]["event_type"] == b"user.login"
        
        # Check returned event
        assert event.event_type == "user.login"
        assert event.tenant_id == "tenant-1"
        assert event.user_id == "user-123"
        assert event.data == {"user_id": "123", "ip": "192.168.1.1"}
    
    @patch('general_operate.kafka.event_bus.KafkaProducerOperate')
    @patch('general_operate.kafka.event_bus.uuid.uuid4')
    @patch('general_operate.kafka.event_bus.datetime')
    @pytest.mark.asyncio
    async def test_publish_with_custom_topic_and_key(self, mock_datetime, mock_uuid, mock_producer_class):
        """Test publishing with custom topic and key"""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        mock_uuid.return_value.hex = "test-correlation-id"
        mock_datetime.now.return_value.isoformat.return_value = "2023-12-01T10:00:00Z"
        
        event_bus = KafkaEventBus(bootstrap_servers="localhost:9092")
        
        await event_bus.publish(
            event_type="order.created",
            data={"order_id": "order-123"},
            topic="orders",
            key="custom-key",
            metadata={"source": "api"}
        )
        
        # Verify send_event call arguments
        call_args = mock_producer.send_event.call_args
        assert call_args[1]["topic"] == "orders"
        assert call_args[1]["key"] == "custom-key"
    
    @patch('general_operate.kafka.event_bus.KafkaProducerOperate')
    @pytest.mark.asyncio
    async def test_publish_when_not_started(self, mock_producer_class):
        """Test publishing when event bus is not started (should auto-start)"""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        event_bus = KafkaEventBus(bootstrap_servers="localhost:9092")
        
        await event_bus.publish(
            event_type="test.event",
            data={"test": "data"}
        )
        
        # Should have auto-started the producer
        mock_producer.start.assert_called_once()
        mock_producer.send_event.assert_called_once()


class TestKafkaEventBusSubscribe:
    """Test event subscription functionality"""
    
    @patch('general_operate.kafka.event_bus.KafkaProducerOperate')
    @patch('general_operate.kafka.event_bus.KafkaConsumerOperate')
    @patch('general_operate.kafka.event_bus.asyncio.create_task')
    @patch('general_operate.kafka.event_bus.uuid.uuid4')
    @pytest.mark.asyncio
    async def test_subscribe_basic(self, mock_uuid, mock_create_task, mock_consumer_class, mock_producer_class):
        """Test basic event subscription"""
        mock_uuid.return_value = MagicMock()
        mock_uuid.return_value.__str__ = MagicMock(return_value="test-subscription-id")
        mock_consumer = AsyncMock()
        mock_consumer_class.return_value = mock_consumer
        mock_task = AsyncMock()
        mock_create_task.return_value = mock_task
        
        event_bus = KafkaEventBus(bootstrap_servers="localhost:9092")
        
        async def handler(event):
            pass
        
        subscription_id = await event_bus.subscribe(
            event_type="user.login",
            handler=handler
        )
        
        assert subscription_id == "test-subscription-id"
        
        # Check that consumer was created
        mock_consumer_class.assert_called_once()
        call_args = mock_consumer_class.call_args
        assert call_args[1]["topics"] == ["events"]
        assert call_args[1]["group_id"] == "event-bus-user.login"
        
        # Check that consumption task was created
        mock_create_task.assert_called_once()
        
        # Check subscription was stored
        assert len(event_bus._subscriptions["events"]) == 1
        subscription = event_bus._subscriptions["events"][0]
        assert subscription.event_type == "user.login"
        assert subscription.handler == handler
    
    @patch('general_operate.kafka.event_bus.KafkaProducerOperate')
    @patch('general_operate.kafka.event_bus.KafkaConsumerOperate')
    @patch('general_operate.kafka.event_bus.asyncio.create_task')
    @patch('general_operate.kafka.event_bus.uuid.uuid4')
    @pytest.mark.asyncio
    async def test_subscribe_with_filter(self, mock_uuid, mock_create_task, mock_consumer_class, mock_producer_class):
        """Test subscription with filter function"""
        mock_uuid.return_value.hex = "test-subscription-id"
        
        event_bus = KafkaEventBus(bootstrap_servers="localhost:9092")
        
        async def handler(event):
            pass
        
        def filter_func(event):
            return event.tenant_id == "tenant-1"
        
        await event_bus.subscribe(
            event_type="user.*",
            handler=handler,
            topic="custom-topic",
            group_id="custom-group",
            filter_func=filter_func
        )
        
        # Check subscription was stored with filter
        subscription = event_bus._subscriptions["custom-topic"][0]
        assert subscription.filter_func == filter_func
    
    @patch('general_operate.kafka.event_bus.KafkaProducerOperate')
    @patch('general_operate.kafka.event_bus.KafkaConsumerOperate')
    @patch('general_operate.kafka.event_bus.asyncio.create_task')
    @pytest.mark.asyncio
    async def test_subscribe_multiple_to_same_topic(self, mock_create_task, mock_consumer_class, mock_producer_class):
        """Test multiple subscriptions to the same topic"""
        event_bus = KafkaEventBus(bootstrap_servers="localhost:9092")
        
        async def handler1(event):
            pass
        
        async def handler2(event):
            pass
        
        # First subscription creates consumer
        await event_bus.subscribe("user.login", handler1)
        
        # Second subscription reuses existing consumer
        await event_bus.subscribe("user.logout", handler2)
        
        # Only one consumer should be created
        mock_consumer_class.assert_called_once()
        
        # But two subscriptions should exist
        assert len(event_bus._subscriptions["events"]) == 2


class TestKafkaEventBusUnsubscribe:
    """Test event unsubscription functionality"""
    
    @patch('general_operate.kafka.event_bus.KafkaProducerOperate')
    @pytest.mark.asyncio
    async def test_unsubscribe_success(self, mock_producer_class):
        """Test successful unsubscription"""
        event_bus = KafkaEventBus(bootstrap_servers="localhost:9092")
        
        # Manually add a subscription
        async def handler(event):
            pass
        
        subscription = EventSubscription(
            subscription_id="test-id",
            event_type="test.event",
            handler=handler
        )
        event_bus._subscriptions["events"].append(subscription)
        
        # Unsubscribe
        await event_bus.unsubscribe("test-id")
        
        # Check subscription was removed
        assert len(event_bus._subscriptions["events"]) == 0
    
    @patch('general_operate.kafka.event_bus.KafkaProducerOperate')
    @pytest.mark.asyncio
    async def test_unsubscribe_nonexistent(self, mock_producer_class):
        """Test unsubscribing from non-existent subscription"""
        event_bus = KafkaEventBus(bootstrap_servers="localhost:9092")
        
        # Should not raise an exception
        await event_bus.unsubscribe("non-existent-id")


class TestKafkaEventBusEventMatching:
    """Test event type matching logic"""
    
    def test_match_exact_event_type(self):
        """Test exact event type matching"""
        assert KafkaEventBus._match_event_type("user.login", "user.login") is True
        assert KafkaEventBus._match_event_type("user.login", "user.logout") is False
    
    def test_match_wildcard_all(self):
        """Test wildcard * matching"""
        assert KafkaEventBus._match_event_type("user.login", "*") is True
        assert KafkaEventBus._match_event_type("order.created", "*") is True
    
    def test_match_prefix_wildcard(self):
        """Test prefix wildcard matching"""
        assert KafkaEventBus._match_event_type("user.login", "user.*") is True
        assert KafkaEventBus._match_event_type("user.logout", "user.*") is True
        assert KafkaEventBus._match_event_type("order.created", "user.*") is False


class TestKafkaEventBusMessageConsumption:
    """Test message consumption and distribution"""
    
    @patch('general_operate.kafka.event_bus.KafkaProducerOperate')
    @pytest.mark.asyncio
    async def test_consume_topic_success(self, mock_producer_class):
        """Test successful message consumption and distribution"""
        event_bus = KafkaEventBus(bootstrap_servers="localhost:9092")
        
        # Mock consumer
        mock_consumer = AsyncMock()
        
        # Track handler calls
        handler_calls = []
        
        async def handler(event):
            handler_calls.append(event)
        
        # Add subscription
        subscription = EventSubscription(
            subscription_id="test-id",
            event_type="user.login",
            handler=handler
        )
        event_bus._subscriptions["events"].append(subscription)
        
        # Test message data
        message_data = {
            "event_type": "user.login",
            "tenant_id": "tenant-1",
            "user_id": "user-123",
            "data": {"ip": "192.168.1.1"},
            "metadata": {},
            "timestamp": "2023-12-01T10:00:00Z",
            "correlation_id": "test-correlation"
        }
        
        # Call the consume method directly
        await event_bus._consume_topic("events", mock_consumer)
        
        # Mock the handler function that would be passed to consume_events
        # We need to simulate the consumer calling our handler
        handle_message = mock_consumer.consume_events.call_args[0][0]
        await handle_message(message_data)
        
        # Check that handler was called with correct event
        assert len(handler_calls) == 1
        event = handler_calls[0]
        assert event.event_type == "user.login"
        assert event.tenant_id == "tenant-1"
    
    @patch('general_operate.kafka.event_bus.KafkaProducerOperate')
    @pytest.mark.asyncio
    async def test_consume_topic_with_filter(self, mock_producer_class):
        """Test message consumption with filter function"""
        event_bus = KafkaEventBus(bootstrap_servers="localhost:9092")
        
        mock_consumer = AsyncMock()
        handler_calls = []
        
        async def handler(event):
            handler_calls.append(event)
        
        def filter_func(event):
            return event.tenant_id == "tenant-1"
        
        # Add subscription with filter
        subscription = EventSubscription(
            subscription_id="test-id",
            event_type="user.login",
            handler=handler,
            filter_func=filter_func
        )
        event_bus._subscriptions["events"].append(subscription)
        
        # Test message that should pass filter
        passing_message = {
            "event_type": "user.login",
            "tenant_id": "tenant-1",
            "user_id": "user-123",
            "data": {},
            "metadata": {},
            "timestamp": "2023-12-01T10:00:00Z",
            "correlation_id": "test-correlation"
        }
        
        # Test message that should be filtered out
        filtered_message = {
            "event_type": "user.login",
            "tenant_id": "tenant-2",
            "user_id": "user-456",
            "data": {},
            "metadata": {},
            "timestamp": "2023-12-01T10:00:00Z",
            "correlation_id": "test-correlation-2"
        }
        
        await event_bus._consume_topic("events", mock_consumer)
        handle_message = mock_consumer.consume_events.call_args[0][0]
        
        # Process both messages
        await handle_message(passing_message)
        await handle_message(filtered_message)
        
        # Only one message should have passed the filter
        assert len(handler_calls) == 1
        assert handler_calls[0].tenant_id == "tenant-1"
    
    @patch('general_operate.kafka.event_bus.KafkaProducerOperate')
    @pytest.mark.asyncio
    async def test_consume_topic_invalid_message(self, mock_producer_class):
        """Test handling of invalid message format"""
        event_bus = KafkaEventBus(bootstrap_servers="localhost:9092")
        
        mock_consumer = AsyncMock()
        handler_calls = []
        
        async def handler(event):
            handler_calls.append(event)
        
        subscription = EventSubscription(
            subscription_id="test-id",
            event_type="user.login",
            handler=handler
        )
        event_bus._subscriptions["events"].append(subscription)
        
        # Invalid message data (missing required fields)
        invalid_message = {"invalid": "data"}
        
        await event_bus._consume_topic("events", mock_consumer)
        handle_message = mock_consumer.consume_events.call_args[0][0]
        
        # Should not raise exception, just log error
        await handle_message(invalid_message)
        
        # Handler should not have been called
        assert len(handler_calls) == 0
    
    @patch('general_operate.kafka.event_bus.KafkaProducerOperate')
    @pytest.mark.asyncio
    async def test_consume_topic_handler_error(self, mock_producer_class):
        """Test handling of handler errors"""
        event_bus = KafkaEventBus(bootstrap_servers="localhost:9092")
        
        mock_consumer = AsyncMock()
        
        async def failing_handler(event):
            raise ValueError("Handler error")
        
        subscription = EventSubscription(
            subscription_id="test-id",
            event_type="user.login",
            handler=failing_handler
        )
        event_bus._subscriptions["events"].append(subscription)
        
        message_data = {
            "event_type": "user.login",
            "tenant_id": "tenant-1",
            "user_id": "user-123",
            "data": {},
            "metadata": {},
            "timestamp": "2023-12-01T10:00:00Z",
            "correlation_id": "test-correlation"
        }
        
        await event_bus._consume_topic("events", mock_consumer)
        handle_message = mock_consumer.consume_events.call_args[0][0]
        
        # Should not raise exception, just log error
        await handle_message(message_data)
        
        # Error should be logged (we can't easily test this without mocking logger)


class TestKafkaEventBusHealthCheck:
    """Test health check functionality"""
    
    @patch('general_operate.kafka.event_bus.KafkaProducerOperate')
    @pytest.mark.asyncio
    async def test_health_check_not_started(self, mock_producer_class):
        """Test health check when event bus is not started"""
        mock_producer = AsyncMock()
        mock_producer.is_started = False
        mock_producer_class.return_value = mock_producer
        
        event_bus = KafkaEventBus(bootstrap_servers="localhost:9092")
        
        health = await event_bus.health_check()
        
        assert health["event_bus"] is False
        assert health["producer"] is False
        assert health["consumers"] == {}
    
    @patch('general_operate.kafka.event_bus.KafkaProducerOperate')
    @pytest.mark.asyncio
    async def test_health_check_with_consumers(self, mock_producer_class):
        """Test health check with active consumers"""
        mock_producer = AsyncMock()
        mock_producer.is_started = True
        mock_producer_class.return_value = mock_producer
        
        mock_consumer1 = AsyncMock()
        mock_consumer1.is_started = True
        mock_consumer2 = AsyncMock()
        mock_consumer2.is_started = False
        
        event_bus = KafkaEventBus(bootstrap_servers="localhost:9092")
        event_bus._started = True
        event_bus._consumers["topic1"] = mock_consumer1
        event_bus._consumers["topic2"] = mock_consumer2
        
        health = await event_bus.health_check()
        
        assert health["event_bus"] is True
        assert health["producer"] is True
        assert health["consumers"]["topic1"] is True
        assert health["consumers"]["topic2"] is False


if __name__ == "__main__":
    pass