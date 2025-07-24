"""
Tests for Kafka integration with GeneralOperate
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from dataclasses import dataclass
from typing import Any

# Try to import aiokafka, skip tests if not available
try:
    import aiokafka
    aiokafka_available = True
except ImportError:
    aiokafka_available = False

# Import the classes to test
from general_operate.kafka.kafka_operate import KafkaOperate
from general_operate.kafka.producer_operate import KafkaProducerOperate
from general_operate.kafka.consumer_operate import KafkaConsumerOperate
from general_operate.kafka.event_bus import KafkaEventBus
from general_operate.kafka.models.event_message import EventMessage
from general_operate.kafka.exceptions import KafkaOperateException
from general_operate import GeneralOperate

# Skip all tests in this module if aiokafka is not available
pytestmark = pytest.mark.skipif(not aiokafka_available, reason="aiokafka not installed")


@dataclass
class MockTestModule:
    """Test module for testing"""
    table_name = "test_table"
    topic_name = "test-topic"
    
    @dataclass
    class TestSchema:
        id: int
        name: str
        value: int
    
    main_schemas = TestSchema
    create_schemas = TestSchema
    update_schemas = TestSchema


class MockKafkaOperate(GeneralOperate[MockTestModule.TestSchema]):
    """Test class extending GeneralOperate"""
    
    def get_module(self):
        return MockTestModule


@pytest.fixture
def kafka_config():
    """Kafka configuration for testing"""
    return {
        "bootstrap_servers": "localhost:9092",
        "client_id": "test-client",
        "producer": {
            "acks": "all",
            "enable_idempotence": True
        },
        "consumer": {
            "topics": ["test-topic"],
            "group_id": "test-group"
        },
        "event_bus": {
            "default_topic": "test-events"
        }
    }


class TestEventMessage:
    """Test EventMessage class"""
    
    def test_event_message_creation(self):
        """Test EventMessage creation with default values"""
        event = EventMessage(
            event_type="test.event",
            tenant_id="tenant1",
            data={"key": "value"}
        )
        
        assert event.event_type == "test.event"
        assert event.tenant_id == "tenant1"
        assert event.data == {"key": "value"}
        assert event.timestamp is not None
        assert event.correlation_id is not None
        assert event.metadata == {}
    
    def test_event_message_serialization(self):
        """Test EventMessage JSON serialization"""
        event = EventMessage(
            event_type="test.event",
            tenant_id="tenant1",
            user_id="user1",
            data={"test": "data"},
            metadata={"meta": "info"}
        )
        
        # Test to_json and from_json
        json_str = event.to_json()
        restored_event = EventMessage.from_json(json_str)
        
        assert restored_event.event_type == event.event_type
        assert restored_event.tenant_id == event.tenant_id
        assert restored_event.user_id == event.user_id
        assert restored_event.data == event.data
        assert restored_event.metadata == event.metadata
    
    def test_event_message_metadata_operations(self):
        """Test metadata operations"""
        event = EventMessage(event_type="test", tenant_id="tenant1")
        
        # Add metadata
        event.add_metadata("key1", "value1")
        event.add_metadata("key2", {"nested": "object"})
        
        # Get metadata
        assert event.get_metadata("key1") == "value1"
        assert event.get_metadata("key2") == {"nested": "object"}
        assert event.get_metadata("nonexistent", "default") == "default"


class TestKafkaProducerOperate:
    """Test KafkaProducerOperate class"""
    
    @pytest.mark.asyncio
    async def test_producer_initialization(self):
        """Test producer initialization"""
        producer = KafkaProducerOperate(
            bootstrap_servers="localhost:9092",
            client_id="test-producer"
        )
        
        assert producer.bootstrap_servers == ["localhost:9092"]
        assert producer.client_id == "test-producer"
        assert not producer.is_started
    
    @pytest.mark.asyncio
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    async def test_producer_start_stop(self, mock_producer_class):
        """Test producer start and stop"""
        # Mock AIOKafkaProducer
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        producer = KafkaProducerOperate(
            bootstrap_servers="localhost:9092",
            client_id="test-producer"
        )
        
        # Test start
        await producer.start()
        assert producer.is_started
        mock_producer.start.assert_called_once()
        
        # Test stop
        await producer.stop()
        assert not producer.is_started
        mock_producer.stop.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    async def test_send_event(self, mock_producer_class):
        """Test sending single event"""
        # Mock AIOKafkaProducer
        mock_producer = AsyncMock()
        mock_metadata = MagicMock()
        mock_metadata.partition = 0
        mock_metadata.offset = 123
        mock_producer.send_and_wait.return_value = mock_metadata
        mock_producer_class.return_value = mock_producer
        
        producer = KafkaProducerOperate(
            bootstrap_servers="localhost:9092",
            client_id="test-producer"
        )
        
        # Send event
        result = await producer.send_event(
            topic="test-topic",
            value={"test": "data"},
            key="test-key"
        )
        
        # Verify
        assert result == mock_metadata
        mock_producer.send_and_wait.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    async def test_send_batch(self, mock_producer_class):
        """Test sending batch of events"""
        # Mock AIOKafkaProducer
        mock_producer = AsyncMock()
        mock_metadata = MagicMock()
        mock_producer.send_and_wait.return_value = mock_metadata
        mock_producer_class.return_value = mock_producer
        
        producer = KafkaProducerOperate(
            bootstrap_servers="localhost:9092",
            client_id="test-producer"
        )
        
        # Send batch
        messages = [
            ("key1", {"data": 1}, None),
            ("key2", {"data": 2}, None),
            ("key3", {"data": 3}, None)
        ]
        
        results = await producer.send_batch(
            topic="test-topic",
            messages=messages
        )
        
        # Verify
        assert len(results) == 3
        assert mock_producer.send_and_wait.call_count == 3


class TestKafkaConsumerOperate:
    """Test KafkaConsumerOperate class"""
    
    @pytest.mark.asyncio
    async def test_consumer_initialization(self):
        """Test consumer initialization"""
        consumer = KafkaConsumerOperate(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"],
            group_id="test-group",
            client_id="test-consumer"
        )
        
        assert consumer.bootstrap_servers == ["localhost:9092"]
        assert consumer.topics == ["test-topic"]
        assert consumer.group_id == "test-group"
        assert not consumer.is_started
    
    @pytest.mark.asyncio
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    async def test_consumer_start_stop(self, mock_consumer_class):
        """Test consumer start and stop"""
        # Mock AIOKafkaConsumer
        mock_consumer = AsyncMock()
        mock_consumer_class.return_value = mock_consumer
        
        consumer = KafkaConsumerOperate(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"],
            group_id="test-group"
        )
        
        # Test start
        await consumer.start()
        assert consumer.is_started
        mock_consumer.start.assert_called_once()
        
        # Test stop
        await consumer.stop()
        assert not consumer.is_started
        mock_consumer.stop.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    async def test_consume_events(self, mock_consumer_class):
        """Test consuming events"""
        # Mock AIOKafkaConsumer
        mock_consumer = AsyncMock()
        mock_message = MagicMock()
        mock_message.value = {"test": "data"}
        mock_message.topic = "test-topic"
        mock_message.partition = 0
        mock_message.offset = 100
        
        # Mock async iteration
        mock_consumer.__aiter__.return_value = [mock_message]
        mock_consumer_class.return_value = mock_consumer
        
        consumer = KafkaConsumerOperate(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"],
            group_id="test-group"
        )
        
        # Mock handler
        handler = AsyncMock()
        
        # Consume events (with timeout to prevent infinite loop)
        consumer._running = True
        async def stop_after_delay():
            await asyncio.sleep(0.1)
            consumer._running = False
        
        # Start stop task
        stop_task = asyncio.create_task(stop_after_delay())
        
        # Start consuming
        await consumer.consume_events(handler)
        
        # Wait for stop task
        await stop_task
        
        # Verify handler was called
        handler.assert_called_once_with({"test": "data"})


class TestKafkaEventBus:
    """Test KafkaEventBus class"""
    
    @pytest.mark.asyncio
    @patch('general_operate.kafka.event_bus.KafkaProducerOperate')
    async def test_event_bus_initialization(self, mock_producer_class):
        """Test event bus initialization"""
        event_bus = KafkaEventBus(
            bootstrap_servers="localhost:9092",
            client_id="test-bus",
            default_topic="test-events",
            config={}
        )
        
        assert event_bus.bootstrap_servers == "localhost:9092"
        assert event_bus.client_id == "test-bus"
        assert event_bus.default_topic == "test-events"
        mock_producer_class.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('general_operate.kafka.event_bus.KafkaProducerOperate')
    async def test_publish_event(self, mock_producer_class):
        """Test publishing event"""
        # Mock producer
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        event_bus = KafkaEventBus(
            bootstrap_servers="localhost:9092",
            default_topic="test-events",
            config={}
        )
        
        # Publish event
        event = await event_bus.publish(
            event_type="test.event",
            data={"key": "value"},
            tenant_id="tenant1",
            user_id="user1"
        )
        
        # Verify
        assert isinstance(event, EventMessage)
        assert event.event_type == "test.event"
        assert event.tenant_id == "tenant1"
        assert event.user_id == "user1"
        mock_producer.send_event.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('general_operate.kafka.event_bus.KafkaProducerOperate')
    @patch('general_operate.kafka.event_bus.KafkaConsumerOperate')
    async def test_subscribe_event(self, mock_consumer_class, mock_producer_class):
        """Test subscribing to events"""
        # Mock producer and consumer
        mock_producer = AsyncMock()
        mock_consumer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        mock_consumer_class.return_value = mock_consumer
        
        event_bus = KafkaEventBus(
            bootstrap_servers="localhost:9092",
            default_topic="test-events",
            config={}
        )
        
        # Mock handler
        handler = AsyncMock()
        
        # Subscribe
        subscription_id = await event_bus.subscribe(
            event_type="test.*",
            handler=handler,
            topic="test-events"
        )
        
        # Verify
        assert subscription_id is not None
        assert len(event_bus._subscriptions["test-events"]) == 1
        mock_consumer_class.assert_called_once()


class TestGeneralOperateKafkaIntegration:
    """Test GeneralOperate Kafka integration"""
    
    @pytest.mark.asyncio
    @patch('general_operate.general_operate.SQLOperate.__init__')
    async def test_general_operate_kafka_initialization(self, mock_sql_init, kafka_config):
        """Test GeneralOperate initialization with Kafka config"""
        mock_sql_init.return_value = None
        operate = MockKafkaOperate(kafka_config=kafka_config)
        
        assert operate._kafka_producer is not None
        assert operate._kafka_consumer is not None
        assert operate._kafka_event_bus is not None
    
    @pytest.mark.asyncio
    @patch('general_operate.general_operate.SQLOperate.__init__')
    async def test_general_operate_without_kafka(self, mock_sql_init):
        """Test GeneralOperate without Kafka config"""
        mock_sql_init.return_value = None
        operate = MockKafkaOperate()
        
        assert operate._kafka_producer is None
        assert operate._kafka_consumer is None
        assert operate._kafka_event_bus is None
    
    @pytest.mark.asyncio
    @patch('general_operate.general_operate.SQLOperate.__init__')
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    async def test_send_event_integration(self, mock_consumer_class, mock_producer_class, mock_sql_init, kafka_config):
        """Test send_event method integration"""
        # Mock SQLOperate and Kafka classes
        mock_sql_init.return_value = None
        mock_producer = AsyncMock()
        mock_consumer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        mock_consumer_class.return_value = mock_consumer
        
        operate = MockKafkaOperate(kafka_config=kafka_config)
        
        # Mock the producer's send_event method directly
        with patch.object(operate._kafka_producer, 'send_event') as mock_send_event:
            async with operate.kafka_lifespan():
                await operate.send_event(
                    event_type="test.event",
                    data={"test": "data"},
                    key="test-key"
                )
            
            # Verify producer was called
            mock_send_event.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('general_operate.general_operate.SQLOperate.__init__')
    async def test_kafka_health_check(self, mock_sql_init, kafka_config):
        """Test Kafka health check"""
        mock_sql_init.return_value = None
        operate = MockKafkaOperate(kafka_config=kafka_config)
        
        health = await operate.kafka_health_check()
        
        assert "producer" in health
        assert "consumer" in health
        assert "event_bus" in health
    
    @pytest.mark.asyncio
    @patch('general_operate.general_operate.SQLOperate.__init__')
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    async def test_kafka_lifespan_context_manager(self, mock_consumer_class, mock_producer_class, mock_sql_init, kafka_config):
        """Test Kafka lifespan context manager"""
        # Mock SQLOperate and Kafka classes
        mock_sql_init.return_value = None
        mock_producer = AsyncMock()
        mock_consumer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        mock_consumer_class.return_value = mock_consumer
        
        operate = MockKafkaOperate(kafka_config=kafka_config)
        
        # Mock the start/stop methods
        with patch.object(operate._kafka_producer, 'start') as mock_start, \
             patch.object(operate._kafka_producer, 'stop') as mock_stop:
            
            async with operate.kafka_lifespan():
                # Verify components started
                mock_start.assert_called_once()
            
            # Verify components stopped
            mock_stop.assert_called_once()


@pytest.mark.integration
class TestKafkaIntegrationReal:
    """Integration tests with real Kafka (requires running Kafka)"""
    
    @pytest.mark.skip(reason="Requires running Kafka cluster")
    @pytest.mark.asyncio
    async def test_end_to_end_flow(self):
        """End-to-end test with real Kafka"""
        kafka_config = {
            "bootstrap_servers": "localhost:9092",
            "producer": {"acks": "all"},
            "consumer": {
                "topics": ["integration-test"],
                "group_id": "integration-test-group"
            }
        }
        
        operate = MockKafkaOperate(kafka_config=kafka_config)
        received_events = []
        
        async def event_handler(event_data):
            received_events.append(event_data)
        
        async with operate.kafka_lifespan():
            # Start consuming
            consume_task = asyncio.create_task(
                operate.start_consuming(event_handler)
            )
            
            # Send event
            await operate.send_event(
                event_type="integration.test",
                data={"message": "Hello Kafka!"},
                topic="integration-test"
            )
            
            # Wait for event to be received
            await asyncio.sleep(2)
            
            # Stop consuming
            operate._kafka_consumer._running = False
            consume_task.cancel()
        
        # Verify event was received
        assert len(received_events) == 1
        assert received_events[0]["event_type"] == "integration.test"
        assert received_events[0]["data"]["message"] == "Hello Kafka!"


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])