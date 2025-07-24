"""
Unit tests for Kafka producer operations - no external dependencies required
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Any

from general_operate.kafka.producer_operate import KafkaProducerOperate
from general_operate.kafka.exceptions import (
    KafkaProducerException,
    KafkaConfigurationException
)


class TestKafkaProducerOperateInitialization:
    """Test KafkaProducerOperate initialization"""
    
    @patch('general_operate.kafka.producer_operate.aiokafka', None)
    def test_initialization_without_aiokafka(self):
        """Test initialization fails when aiokafka is not available"""
        with pytest.raises(KafkaConfigurationException) as exc_info:
            KafkaProducerOperate(
                bootstrap_servers="localhost:9092"
            )
        
        assert exc_info.value.status_code == 500
        assert "aiokafka is not installed" in exc_info.value.message
        assert exc_info.value.message_code == 4101
    
    @patch('general_operate.kafka.producer_operate.aiokafka')
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    def test_initialization_with_minimal_config(self, mock_producer_class, mock_aiokafka):
        """Test initialization with minimal configuration"""
        producer = KafkaProducerOperate(
            bootstrap_servers="localhost:9092"
        )
        
        assert producer.bootstrap_servers == ["localhost:9092"]
        assert producer._started is False
        assert producer.producer is None
        
        # Check producer config defaults
        assert producer.producer_config["acks"] == "all"
        assert producer.producer_config["enable_idempotence"] is True
        assert producer.producer_config["compression_type"] == "gzip"
        assert producer.producer_config["max_batch_size"] == 16384
        assert producer.producer_config["linger_ms"] == 10
    
    @patch('general_operate.kafka.producer_operate.aiokafka')
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    def test_initialization_with_full_config(self, mock_producer_class, mock_aiokafka):
        """Test initialization with comprehensive configuration"""
        config = {
            "acks": "1",
            "enable_idempotence": False,
            "compression_type": "lz4",
            "max_batch_size": 32768,
            "linger_ms": 50,
            "producer_config": {
                "max_request_size": 1048576,
                "request_timeout_ms": 30000
            }
        }
        
        producer = KafkaProducerOperate(
            bootstrap_servers=["broker1:9092", "broker2:9092"],
            client_id="custom-producer",
            config=config
        )
        
        assert producer.bootstrap_servers == ["broker1:9092", "broker2:9092"]
        assert producer.client_id == "custom-producer"
        
        # Check custom config was applied
        assert producer.producer_config["acks"] == "1"
        assert producer.producer_config["enable_idempotence"] is False
        assert producer.producer_config["compression_type"] == "lz4"
        assert producer.producer_config["max_batch_size"] == 32768
        assert producer.producer_config["linger_ms"] == 50
        assert producer.producer_config["max_request_size"] == 1048576
        assert producer.producer_config["request_timeout_ms"] == 30000
    
    @patch('general_operate.kafka.producer_operate.aiokafka')
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    def test_initialization_with_custom_serializers(self, mock_producer_class, mock_aiokafka):
        """Test initialization with custom serializers"""
        def custom_serializer(data):
            return b"custom"
        
        def custom_key_serializer(key):
            return b"key"
        
        producer = KafkaProducerOperate(
            bootstrap_servers="localhost:9092",
            serializer=custom_serializer,
            key_serializer=custom_key_serializer
        )
        
        assert producer.serializer == custom_serializer
        assert producer.key_serializer == custom_key_serializer


class TestKafkaProducerOperateLifecycle:
    """Test producer lifecycle management"""
    
    @patch('general_operate.kafka.producer_operate.aiokafka')
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    @pytest.mark.asyncio
    async def test_start_success(self, mock_producer_class, mock_aiokafka):
        """Test successful producer start"""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        producer = KafkaProducerOperate(
            bootstrap_servers="localhost:9092"
        )
        
        await producer.start()
        
        assert producer._started is True
        assert producer.producer == mock_producer
        mock_producer.start.assert_called_once()
    
    @patch('general_operate.kafka.producer_operate.aiokafka')
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    @pytest.mark.asyncio
    async def test_start_idempotent(self, mock_producer_class, mock_aiokafka):
        """Test that multiple start calls are idempotent"""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        producer = KafkaProducerOperate(
            bootstrap_servers="localhost:9092"
        )
        
        await producer.start()
        await producer.start()  # Second call should be no-op
        
        assert producer._started is True
        mock_producer.start.assert_called_once()  # Only called once
    
    @patch('general_operate.kafka.producer_operate.aiokafka')
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    @pytest.mark.asyncio
    async def test_start_failure(self, mock_producer_class, mock_aiokafka):
        """Test producer start failure handling"""
        mock_producer = AsyncMock()
        mock_producer.start.side_effect = Exception("Connection failed")
        mock_producer_class.return_value = mock_producer
        
        producer = KafkaProducerOperate(
            bootstrap_servers="localhost:9092"
        )
        
        with pytest.raises(KafkaProducerException):
            await producer.start()
        
        assert producer._started is False
    
    @patch('general_operate.kafka.producer_operate.aiokafka')
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    @pytest.mark.asyncio
    async def test_stop_success(self, mock_producer_class, mock_aiokafka):
        """Test successful producer stop"""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        producer = KafkaProducerOperate(
            bootstrap_servers="localhost:9092"
        )
        
        await producer.start()
        await producer.stop()
        
        assert producer._started is False
        mock_producer.stop.assert_called_once()
    
    @patch('general_operate.kafka.producer_operate.aiokafka')
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    @pytest.mark.asyncio
    async def test_stop_when_not_started(self, mock_producer_class, mock_aiokafka):
        """Test stop when producer was never started"""
        producer = KafkaProducerOperate(
            bootstrap_servers="localhost:9092"
        )
        
        await producer.stop()  # Should not raise an exception
        
        assert producer._started is False
    
    @patch('general_operate.kafka.producer_operate.aiokafka')
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    @pytest.mark.asyncio
    async def test_stop_error_handling(self, mock_producer_class, mock_aiokafka):
        """Test stop error handling"""
        mock_producer = AsyncMock()
        mock_producer.stop.side_effect = Exception("Stop failed")
        mock_producer_class.return_value = mock_producer
        
        producer = KafkaProducerOperate(
            bootstrap_servers="localhost:9092"
        )
        
        await producer.start()
        
        # Stop should handle the exception gracefully (logs warning but continues)
        await producer.stop()
        
        # Producer should still be marked as started since stop() failed
        assert producer._started is True


class TestKafkaProducerOperateSendEvent:
    """Test single event sending functionality"""
    
    @patch('general_operate.kafka.producer_operate.aiokafka')
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    @pytest.mark.asyncio
    async def test_send_event_basic(self, mock_producer_class, mock_aiokafka):
        """Test basic event sending"""
        # Mock record metadata
        mock_metadata = MagicMock()
        mock_metadata.partition = 0
        mock_metadata.offset = 123
        
        mock_producer = AsyncMock()
        mock_producer.send_and_wait.return_value = mock_metadata
        mock_producer_class.return_value = mock_producer
        
        producer = KafkaProducerOperate(
            bootstrap_servers="localhost:9092"
        )
        
        result = await producer.send_event(
            topic="test-topic",
            value={"test": "data"},
            key="test-key"
        )
        
        assert result == mock_metadata
        mock_producer.start.assert_called_once()
        mock_producer.send_and_wait.assert_called_once()
        
        # Check send_and_wait call arguments
        call_args = mock_producer.send_and_wait.call_args
        assert call_args[1]["topic"] == "test-topic"
        assert call_args[1]["value"] == {"test": "data"}
        assert call_args[1]["key"] == "test-key"
        assert call_args[1]["headers"] is None
    
    @patch('general_operate.kafka.producer_operate.aiokafka')
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    @pytest.mark.asyncio
    async def test_send_event_with_headers(self, mock_producer_class, mock_aiokafka):
        """Test sending event with headers"""
        mock_metadata = MagicMock()
        mock_producer = AsyncMock()
        mock_producer.send_and_wait.return_value = mock_metadata
        mock_producer_class.return_value = mock_producer
        
        producer = KafkaProducerOperate(
            bootstrap_servers="localhost:9092"
        )
        
        headers = {"event_type": b"user.login", "correlation_id": b"test-id"}
        
        await producer.send_event(
            topic="events",
            value={"user_id": "123"},
            headers=headers
        )
        
        # Check headers were converted to list of tuples
        call_args = mock_producer.send_and_wait.call_args
        expected_headers = [("event_type", b"user.login"), ("correlation_id", b"test-id")]
        assert call_args[1]["headers"] == expected_headers
    
    @patch('general_operate.kafka.producer_operate.aiokafka')
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    @pytest.mark.asyncio
    async def test_send_event_with_partition_and_timestamp(self, mock_producer_class, mock_aiokafka):
        """Test sending event with specific partition and timestamp"""
        mock_metadata = MagicMock()
        mock_producer = AsyncMock()
        mock_producer.send_and_wait.return_value = mock_metadata
        mock_producer_class.return_value = mock_producer
        
        producer = KafkaProducerOperate(
            bootstrap_servers="localhost:9092"
        )
        
        await producer.send_event(
            topic="events",
            value={"data": "test"},
            partition=2,
            timestamp_ms=1640995200000
        )
        
        call_args = mock_producer.send_and_wait.call_args
        assert call_args[1]["partition"] == 2
        assert call_args[1]["timestamp_ms"] == 1640995200000
    
    @patch('general_operate.kafka.producer_operate.aiokafka')
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    @pytest.mark.asyncio
    async def test_send_event_when_not_started(self, mock_producer_class, mock_aiokafka):
        """Test sending event when producer is not started (should auto-start)"""
        mock_metadata = MagicMock()
        mock_producer = AsyncMock()
        mock_producer.send_and_wait.return_value = mock_metadata
        mock_producer_class.return_value = mock_producer
        
        producer = KafkaProducerOperate(
            bootstrap_servers="localhost:9092"
        )
        
        await producer.send_event(
            topic="test-topic",
            value={"test": "data"}
        )
        
        # Should have auto-started the producer
        mock_producer.start.assert_called_once()
        mock_producer.send_and_wait.assert_called_once()
    
    @patch('general_operate.kafka.producer_operate.aiokafka')
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    @pytest.mark.asyncio
    async def test_send_event_failure(self, mock_producer_class, mock_aiokafka):
        """Test event sending failure handling"""
        mock_producer = AsyncMock()
        mock_producer.send_and_wait.side_effect = Exception("Send failed")
        mock_producer_class.return_value = mock_producer
        
        producer = KafkaProducerOperate(
            bootstrap_servers="localhost:9092"
        )
        
        with pytest.raises(KafkaProducerException):
            await producer.send_event(
                topic="test-topic",
                value={"test": "data"}
            )


class TestKafkaProducerOperateSendBatch:
    """Test batch sending functionality"""
    
    @patch('general_operate.kafka.producer_operate.aiokafka')
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    @patch('general_operate.kafka.producer_operate.asyncio.gather')
    @pytest.mark.asyncio
    async def test_send_batch_basic(self, mock_gather, mock_producer_class, mock_aiokafka):
        """Test basic batch sending"""
        # Mock metadata results
        mock_metadata1 = MagicMock()
        mock_metadata2 = MagicMock()
        mock_gather.return_value = asyncio.Future()
        mock_gather.return_value.set_result([mock_metadata1, mock_metadata2])
        
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        producer = KafkaProducerOperate(
            bootstrap_servers="localhost:9092"
        )
        
        messages = [
            ("key1", {"data": "message1"}, {"header1": b"value1"}),
            ("key2", {"data": "message2"}, None)
        ]
        
        result = await producer.send_batch(
            topic="test-topic",
            messages=messages
        )
        
        assert result == [mock_metadata1, mock_metadata2]
        mock_producer.start.assert_called_once()
        
        # Check that gather was called
        mock_gather.assert_called_once()
    
    @patch('general_operate.kafka.producer_operate.aiokafka')
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    @pytest.mark.asyncio
    async def test_send_batch_with_partition(self, mock_producer_class, mock_aiokafka):
        """Test batch sending with specific partition"""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        producer = KafkaProducerOperate(
            bootstrap_servers="localhost:9092"
        )
        
        messages = [
            ("key1", {"data": "message1"}, None)
        ]
        
        await producer.send_batch(
            topic="test-topic",
            messages=messages,
            partition=1
        )
        
        # Check that send_and_wait was called with correct partition
        mock_producer.send_and_wait.assert_called_once()
        call_args = mock_producer.send_and_wait.call_args
        assert call_args[1]["partition"] == 1
    
    @patch('general_operate.kafka.producer_operate.aiokafka')
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    @pytest.mark.asyncio
    async def test_send_batch_failure(self, mock_producer_class, mock_aiokafka):
        """Test batch sending failure handling"""
        mock_producer = AsyncMock()
        mock_producer.send_and_wait.side_effect = Exception("Batch send failed")
        mock_producer_class.return_value = mock_producer
        
        producer = KafkaProducerOperate(
            bootstrap_servers="localhost:9092"
        )
        
        messages = [("key1", {"data": "message1"}, None)]
        
        with pytest.raises(KafkaProducerException):
            await producer.send_batch(
                topic="test-topic",
                messages=messages
            )


class TestKafkaProducerOperateSendAsync:
    """Test asynchronous sending functionality"""
    
    @patch('general_operate.kafka.producer_operate.aiokafka')
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    @pytest.mark.asyncio
    async def test_send_event_async_basic(self, mock_producer_class, mock_aiokafka):
        """Test basic asynchronous event sending"""
        mock_future = AsyncMock()
        mock_producer = AsyncMock()
        mock_producer.send.return_value = mock_future
        mock_producer_class.return_value = mock_producer
        
        producer = KafkaProducerOperate(
            bootstrap_servers="localhost:9092"
        )
        
        result = await producer.send_event_async(
            topic="test-topic",
            value={"test": "data"},
            key="test-key"
        )
        
        assert result == mock_future
        mock_producer.start.assert_called_once()
        mock_producer.send.assert_called_once()
        
        # Check send call arguments
        call_args = mock_producer.send.call_args
        assert call_args[1]["topic"] == "test-topic"
        assert call_args[1]["value"] == {"test": "data"}
        assert call_args[1]["key"] == "test-key"
    
    @patch('general_operate.kafka.producer_operate.aiokafka')
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    @pytest.mark.asyncio
    async def test_send_event_async_with_headers(self, mock_producer_class, mock_aiokafka):
        """Test asynchronous sending with headers"""
        mock_future = AsyncMock()
        mock_producer = AsyncMock()
        mock_producer.send.return_value = mock_future
        mock_producer_class.return_value = mock_producer
        
        producer = KafkaProducerOperate(
            bootstrap_servers="localhost:9092"
        )
        
        headers = {"event_type": b"test.event"}
        
        await producer.send_event_async(
            topic="events",
            value={"data": "test"},
            headers=headers
        )
        
        # Check headers were converted correctly
        call_args = mock_producer.send.call_args
        expected_headers = [("event_type", b"test.event")]
        assert call_args[1]["headers"] == expected_headers
    
    @patch('general_operate.kafka.producer_operate.aiokafka')
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    @pytest.mark.asyncio
    async def test_send_event_async_failure(self, mock_producer_class, mock_aiokafka):
        """Test asynchronous sending failure handling"""
        mock_producer = AsyncMock()
        mock_producer.send.side_effect = Exception("Async send failed")
        mock_producer_class.return_value = mock_producer
        
        producer = KafkaProducerOperate(
            bootstrap_servers="localhost:9092"
        )
        
        with pytest.raises(KafkaProducerException):
            await producer.send_event_async(
                topic="test-topic",
                value={"test": "data"}
            )


class TestKafkaProducerOperateFlush:
    """Test flush functionality"""
    
    @patch('general_operate.kafka.producer_operate.aiokafka')
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    @pytest.mark.asyncio
    async def test_flush_success(self, mock_producer_class, mock_aiokafka):
        """Test successful flush"""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        producer = KafkaProducerOperate(
            bootstrap_servers="localhost:9092"
        )
        
        await producer.start()
        await producer.flush()
        
        mock_producer.flush.assert_called_once_with(None)
    
    @patch('general_operate.kafka.producer_operate.aiokafka')
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    @pytest.mark.asyncio
    async def test_flush_with_timeout(self, mock_producer_class, mock_aiokafka):
        """Test flush with timeout"""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        producer = KafkaProducerOperate(
            bootstrap_servers="localhost:9092"
        )
        
        await producer.start()
        await producer.flush(timeout=5.0)
        
        mock_producer.flush.assert_called_once_with(5.0)
    
    @patch('general_operate.kafka.producer_operate.aiokafka')
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    @pytest.mark.asyncio
    async def test_flush_when_not_started(self, mock_producer_class, mock_aiokafka):
        """Test flush when producer is not started"""
        producer = KafkaProducerOperate(
            bootstrap_servers="localhost:9092"
        )
        
        # Should not raise an exception and should be a no-op
        await producer.flush()
        
        # flush() should have been called on the mock producer but won't do anything since not started
        # The producer instance was created during __init__ but never started
    
    @patch('general_operate.kafka.producer_operate.aiokafka')
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    @pytest.mark.asyncio
    async def test_flush_failure(self, mock_producer_class, mock_aiokafka):
        """Test flush failure handling"""
        mock_producer = AsyncMock()
        mock_producer.flush.side_effect = Exception("Flush failed")
        mock_producer_class.return_value = mock_producer
        
        producer = KafkaProducerOperate(
            bootstrap_servers="localhost:9092"
        )
        
        await producer.start()
        
        with pytest.raises(KafkaProducerException):
            await producer.flush()


class TestKafkaProducerOperateEdgeCases:
    """Test edge cases and error scenarios"""
    
    @patch('general_operate.kafka.producer_operate.aiokafka')
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    @pytest.mark.asyncio
    async def test_multiple_starts_and_stops(self, mock_producer_class, mock_aiokafka):
        """Test multiple start/stop cycles"""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        producer = KafkaProducerOperate(
            bootstrap_servers="localhost:9092"
        )
        
        # First cycle
        await producer.start()
        await producer.stop()
        
        # Second cycle
        await producer.start()
        await producer.stop()
        
        assert producer._started is False
        assert mock_producer.start.call_count == 2
        assert mock_producer.stop.call_count == 2
    
    @patch('general_operate.kafka.producer_operate.aiokafka')
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    def test_config_inheritance(self, mock_producer_class, mock_aiokafka):
        """Test that producer config inherits from base config"""
        producer = KafkaProducerOperate(
            bootstrap_servers="localhost:9092",
            client_id="test-producer"
        )
        
        # Check that base config is inherited
        assert producer.producer_config["bootstrap_servers"] == ["localhost:9092"]
        assert producer.producer_config["client_id"] == "test-producer"
        
        # Check producer-specific defaults
        assert producer.producer_config["acks"] == "all"
        assert producer.producer_config["enable_idempotence"] is True
        assert producer.producer_config["compression_type"] == "gzip"
    
    @patch('general_operate.kafka.producer_operate.aiokafka')
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    @pytest.mark.asyncio
    async def test_send_empty_batch(self, mock_producer_class, mock_aiokafka):
        """Test sending empty batch"""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        producer = KafkaProducerOperate(
            bootstrap_servers="localhost:9092"
        )
        
        result = await producer.send_batch(
            topic="test-topic",
            messages=[]
        )
        
        assert result == []
        mock_producer.start.assert_called_once()


class TestKafkaProducerOperateIntegration:
    """Test realistic integration scenarios"""
    
    @patch('general_operate.kafka.producer_operate.aiokafka')
    @patch('general_operate.kafka.producer_operate.AIOKafkaProducer')
    @pytest.mark.asyncio
    async def test_event_publishing_workflow(self, mock_producer_class, mock_aiokafka):
        """Test realistic event publishing workflow"""
        # Mock metadata responses
        mock_metadata1 = MagicMock()
        mock_metadata1.partition = 0
        mock_metadata1.offset = 100
        
        mock_metadata2 = MagicMock()
        mock_metadata2.partition = 1
        mock_metadata2.offset = 101
        
        mock_producer = AsyncMock()
        mock_producer.send_and_wait.side_effect = [mock_metadata1, mock_metadata2]
        mock_producer_class.return_value = mock_producer
        
        producer = KafkaProducerOperate(
            bootstrap_servers="localhost:9092",
            client_id="event-publisher",
            config={
                "acks": "all",
                "enable_idempotence": True
            }
        )
        
        # Send individual events
        event1_result = await producer.send_event(
            topic="user-events",
            value={"event_type": "user.login", "user_id": "123"},
            key="user-123",
            headers={"event_type": b"user.login"}
        )
        
        event2_result = await producer.send_event(
            topic="user-events",
            value={"event_type": "user.logout", "user_id": "123"},
            key="user-123",
            headers={"event_type": b"user.logout"}
        )
        
        # Verify results
        assert event1_result.partition == 0
        assert event1_result.offset == 100
        assert event2_result.partition == 1
        assert event2_result.offset == 101
        
        # Verify producer was called correctly
        assert mock_producer.send_and_wait.call_count == 2
        
        # Clean shutdown
        await producer.stop()
        assert producer._started is False


if __name__ == "__main__":
    pass