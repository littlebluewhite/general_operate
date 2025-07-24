"""
Unit tests for Kafka consumer operations - no external dependencies required
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from dataclasses import dataclass
from typing import Any

from general_operate.kafka.consumer_operate import KafkaConsumerOperate
from general_operate.kafka.exceptions import (
    KafkaConsumerException,
    KafkaConfigurationException
)


class TestKafkaConsumerOperateInitialization:
    """Test KafkaConsumerOperate initialization"""
    
    @patch('general_operate.kafka.consumer_operate.aiokafka', None)
    def test_initialization_without_aiokafka(self):
        """Test initialization fails when aiokafka is not available"""
        with pytest.raises(KafkaConfigurationException) as exc_info:
            KafkaConsumerOperate(
                bootstrap_servers="localhost:9092",
                topics=["test-topic"],
                group_id="test-group"
            )
        
        assert exc_info.value.status_code == 500
        assert "aiokafka is not installed" in exc_info.value.message
        assert exc_info.value.message_code == 4101
    
    @patch('general_operate.kafka.consumer_operate.aiokafka')
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    def test_initialization_with_minimal_config(self, mock_consumer_class, mock_aiokafka):
        """Test initialization with minimal configuration"""
        consumer = KafkaConsumerOperate(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"],
            group_id="test-group"
        )
        
        assert consumer.topics == ["test-topic"]
        assert consumer.group_id == "test-group"
        assert consumer.bootstrap_servers == ["localhost:9092"]
        assert consumer._started is False
        assert consumer._running is False
        assert consumer.consumer is None
    
    @patch('general_operate.kafka.consumer_operate.aiokafka')
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    def test_initialization_with_full_config(self, mock_consumer_class, mock_aiokafka):
        """Test initialization with comprehensive configuration"""
        config = {
            "auto_offset_reset": "latest",
            "enable_auto_commit": False,
            "auto_commit_interval_ms": 10000,
            "isolation_level": "read_uncommitted",
            "consumer_config": {
                "max_poll_records": 1000,
                "fetch_max_wait_ms": 500
            }
        }
        
        consumer = KafkaConsumerOperate(
            bootstrap_servers=["broker1:9092", "broker2:9092"],
            topics=["topic1", "topic2"],
            group_id="advanced-group",
            client_id="advanced-consumer",
            config=config
        )
        
        assert consumer.topics == ["topic1", "topic2"]
        assert consumer.group_id == "advanced-group"
        assert consumer.bootstrap_servers == ["broker1:9092", "broker2:9092"]
        assert consumer.client_id == "advanced-consumer"
        
        # Check consumer config
        assert consumer.consumer_config["auto_offset_reset"] == "latest"
        assert consumer.consumer_config["enable_auto_commit"] is False
        assert consumer.consumer_config["auto_commit_interval_ms"] == 10000
        assert consumer.consumer_config["isolation_level"] == "read_uncommitted"
        assert consumer.consumer_config["max_poll_records"] == 1000
        assert consumer.consumer_config["fetch_max_wait_ms"] == 500
    
    @patch('general_operate.kafka.consumer_operate.aiokafka')
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    def test_custom_deserializer(self, mock_consumer_class, mock_aiokafka):
        """Test initialization with custom deserializer"""
        def custom_deserializer(data: bytes) -> dict:
            return {"custom": "data"}
        
        consumer = KafkaConsumerOperate(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"],
            group_id="test-group",
            deserializer=custom_deserializer
        )
        
        assert consumer.deserializer == custom_deserializer


class TestKafkaConsumerOperateLifecycle:
    """Test consumer lifecycle management"""
    
    @patch('general_operate.kafka.consumer_operate.aiokafka')
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    @pytest.mark.asyncio
    async def test_start_success(self, mock_consumer_class, mock_aiokafka):
        """Test successful consumer start"""
        mock_consumer = AsyncMock()
        mock_consumer_class.return_value = mock_consumer
        
        consumer = KafkaConsumerOperate(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"],
            group_id="test-group"
        )
        
        await consumer.start()
        
        assert consumer._started is True
        assert consumer.consumer == mock_consumer
        mock_consumer.start.assert_called_once()
    
    @patch('general_operate.kafka.consumer_operate.aiokafka')
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    @pytest.mark.asyncio
    async def test_start_idempotent(self, mock_consumer_class, mock_aiokafka):
        """Test that multiple start calls are idempotent"""
        mock_consumer = AsyncMock()
        mock_consumer_class.return_value = mock_consumer
        
        consumer = KafkaConsumerOperate(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"],
            group_id="test-group"
        )
        
        await consumer.start()
        await consumer.start()  # Second call should be no-op
        
        assert consumer._started is True
        mock_consumer.start.assert_called_once()  # Only called once
    
    @patch('general_operate.kafka.consumer_operate.aiokafka')
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    @pytest.mark.asyncio
    async def test_start_failure(self, mock_consumer_class, mock_aiokafka):
        """Test consumer start failure handling"""
        mock_consumer = AsyncMock()
        mock_consumer.start.side_effect = Exception("Connection failed")
        mock_consumer_class.return_value = mock_consumer
        
        consumer = KafkaConsumerOperate(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"],
            group_id="test-group"
        )
        
        with pytest.raises(KafkaConsumerException):
            await consumer.start()
        
        assert consumer._started is False
    
    @patch('general_operate.kafka.consumer_operate.aiokafka')
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    @pytest.mark.asyncio
    async def test_stop_success(self, mock_consumer_class, mock_aiokafka):
        """Test successful consumer stop"""
        mock_consumer = AsyncMock()
        mock_consumer_class.return_value = mock_consumer
        
        consumer = KafkaConsumerOperate(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"],
            group_id="test-group"
        )
        
        await consumer.start()
        await consumer.stop()
        
        assert consumer._started is False
        assert consumer._running is False
        mock_consumer.stop.assert_called_once()
    
    @patch('general_operate.kafka.consumer_operate.aiokafka')
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    @pytest.mark.asyncio
    async def test_stop_when_not_started(self, mock_consumer_class, mock_aiokafka):
        """Test stop when consumer was never started"""
        consumer = KafkaConsumerOperate(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"],
            group_id="test-group"
        )
        
        await consumer.stop()  # Should not raise an exception
        
        assert consumer._running is False


class TestKafkaConsumerOperateConsumeEvents:
    """Test event consumption functionality"""
    
    @patch('general_operate.kafka.consumer_operate.aiokafka')
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    @pytest.mark.asyncio
    async def test_consume_events_basic(self, mock_consumer_class, mock_aiokafka):
        """Test basic event consumption"""
        # Mock message
        mock_message = MagicMock()
        mock_message.value = {"test": "data"}
        mock_message.topic = "test-topic"
        mock_message.partition = 0
        mock_message.offset = 123
        
        # Mock consumer
        mock_consumer = AsyncMock()
        mock_consumer.__aiter__.return_value = [mock_message]
        mock_consumer_class.return_value = mock_consumer
        
        # Handler
        handler_calls = []
        async def test_handler(event):
            handler_calls.append(event)
            # Stop consumption after first message
            consumer._running = False
        
        consumer = KafkaConsumerOperate(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"],
            group_id="test-group"
        )
        
        await consumer.consume_events(test_handler)
        
        assert len(handler_calls) == 1
        assert handler_calls[0] == {"test": "data"}
        mock_consumer.start.assert_called_once()
    
    @patch('general_operate.kafka.consumer_operate.aiokafka')
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    @pytest.mark.asyncio
    async def test_consume_events_with_manual_commit(self, mock_consumer_class, mock_aiokafka):
        """Test event consumption with manual commit"""
        # Mock message
        mock_message = MagicMock()
        mock_message.value = {"test": "data"}
        mock_message.topic = "test-topic"
        mock_message.partition = 0
        mock_message.offset = 123
        
        # Mock consumer
        mock_consumer = AsyncMock()
        mock_consumer.__aiter__.return_value = [mock_message]
        mock_consumer_class.return_value = mock_consumer
        
        # Handler
        async def test_handler(event):
            consumer._running = False
        
        consumer = KafkaConsumerOperate(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"],
            group_id="test-group",
            config={"enable_auto_commit": False}
        )
        
        await consumer.consume_events(test_handler)
        
        # Manual commit should be called
        mock_consumer.commit.assert_called_once()
    
    @patch('general_operate.kafka.consumer_operate.aiokafka')
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    @pytest.mark.asyncio
    async def test_consume_events_handler_error(self, mock_consumer_class, mock_aiokafka):
        """Test event consumption with handler error"""
        # Mock message
        mock_message = MagicMock()
        mock_message.value = {"test": "data"}
        mock_message.topic = "test-topic"
        mock_message.partition = 0
        mock_message.offset = 123
        
        # Mock consumer
        mock_consumer = AsyncMock()
        mock_consumer.__aiter__.return_value = [mock_message]
        mock_consumer_class.return_value = mock_consumer
        
        # Handler that raises exception
        async def failing_handler(event):
            consumer._running = False
            raise ValueError("Handler error")
        
        # Error handler
        error_calls = []
        async def error_handler(error, message):
            error_calls.append((error, message))
        
        consumer = KafkaConsumerOperate(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"],
            group_id="test-group"
        )
        
        await consumer.consume_events(failing_handler, error_handler)
        
        assert len(error_calls) == 1
        assert isinstance(error_calls[0][0], ValueError)
        assert error_calls[0][1] == mock_message
    
    @patch('general_operate.kafka.consumer_operate.aiokafka')
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    @pytest.mark.asyncio
    async def test_consume_events_consumer_error(self, mock_consumer_class, mock_aiokafka):
        """Test event consumption with consumer error"""
        # Mock consumer that raises exception
        mock_consumer = AsyncMock()
        mock_consumer.__aiter__.side_effect = Exception("Consumer error")
        mock_consumer_class.return_value = mock_consumer
        
        async def test_handler(event):
            pass
        
        consumer = KafkaConsumerOperate(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"],
            group_id="test-group"
        )
        
        with pytest.raises(KafkaConsumerException):
            await consumer.consume_events(test_handler)


class TestKafkaConsumerOperateBatchConsume:
    """Test batch consumption functionality"""
    
    @patch('general_operate.kafka.consumer_operate.aiokafka')
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    @pytest.mark.asyncio
    async def test_consume_batch_basic(self, mock_consumer_class, mock_aiokafka):
        """Test basic batch consumption"""
        # Mock messages
        mock_messages = []
        for i in range(3):
            msg = MagicMock()
            msg.value = {"id": i, "data": f"message_{i}"}
            mock_messages.append(msg)
        
        # Mock topic partition and data
        from aiokafka import TopicPartition
        tp = TopicPartition("test-topic", 0)
        
        # Mock consumer getmany responses
        mock_consumer = AsyncMock()
        
        # First call returns batch, second call returns empty to stop
        call_count = 0
        async def mock_getmany(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {tp: mock_messages}
            else:
                consumer._running = False
                return {}
        
        mock_consumer.getmany.side_effect = mock_getmany
        mock_consumer_class.return_value = mock_consumer
        
        # Batch handler
        batch_calls = []
        async def batch_handler(batch):
            batch_calls.append(batch)
        
        consumer = KafkaConsumerOperate(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"],
            group_id="test-group"
        )
        
        await consumer.consume_batch(batch_handler, batch_size=5, timeout_ms=500)
        
        assert len(batch_calls) == 1
        assert len(batch_calls[0]) == 3
        assert batch_calls[0][0] == {"id": 0, "data": "message_0"}
        mock_consumer.start.assert_called_once()
    
    @patch('general_operate.kafka.consumer_operate.aiokafka')
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    @pytest.mark.asyncio
    async def test_consume_batch_with_manual_commit(self, mock_consumer_class, mock_aiokafka):
        """Test batch consumption with manual commit"""
        # Mock consumer
        mock_consumer = AsyncMock()
        
        call_count = 0
        async def mock_getmany(**kwargs):
            nonlocal call_count
            call_count += 1
            consumer._running = False
            return {}
        
        mock_consumer.getmany.side_effect = mock_getmany
        mock_consumer_class.return_value = mock_consumer
        
        async def batch_handler(batch):
            pass
        
        consumer = KafkaConsumerOperate(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"],
            group_id="test-group",
            config={"enable_auto_commit": False}
        )
        
        await consumer.consume_batch(batch_handler)
        
        # Should not commit since no batches were processed
        mock_consumer.commit.assert_not_called()
    
    @patch('general_operate.kafka.consumer_operate.aiokafka')
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    @pytest.mark.asyncio
    async def test_consume_batch_error(self, mock_consumer_class, mock_aiokafka):
        """Test batch consumption error handling"""
        # Mock consumer that raises exception
        mock_consumer = AsyncMock()
        mock_consumer.getmany.side_effect = Exception("Batch error")
        mock_consumer_class.return_value = mock_consumer
        
        async def batch_handler(batch):
            pass
        
        consumer = KafkaConsumerOperate(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"],
            group_id="test-group"
        )
        
        with pytest.raises(KafkaConsumerException):
            await consumer.consume_batch(batch_handler)


class TestKafkaConsumerOperateSeekAndPosition:
    """Test seek and position functionality"""
    
    @patch('general_operate.kafka.consumer_operate.aiokafka')
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    @pytest.mark.asyncio
    async def test_seek_to_beginning_all_partitions(self, mock_consumer_class, mock_aiokafka):
        """Test seeking to beginning of all partitions"""
        mock_consumer = AsyncMock()
        mock_consumer_class.return_value = mock_consumer
        
        consumer = KafkaConsumerOperate(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"],
            group_id="test-group"
        )
        
        await consumer.seek_to_beginning()
        
        mock_consumer.start.assert_called_once()
        mock_consumer.seek_to_beginning.assert_called_once_with()
    
    @patch('general_operate.kafka.consumer_operate.aiokafka')
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    @pytest.mark.asyncio
    async def test_seek_to_beginning_specific_partitions(self, mock_consumer_class, mock_aiokafka):
        """Test seeking to beginning of specific partitions"""
        mock_consumer = AsyncMock()
        mock_consumer_class.return_value = mock_consumer
        
        from aiokafka import TopicPartition
        partitions = [TopicPartition("test-topic", 0), TopicPartition("test-topic", 1)]
        
        consumer = KafkaConsumerOperate(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"],
            group_id="test-group"
        )
        
        await consumer.seek_to_beginning(partitions)
        
        mock_consumer.seek_to_beginning.assert_called_once_with(*partitions)
    
    @patch('general_operate.kafka.consumer_operate.aiokafka')
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    @pytest.mark.asyncio
    async def test_seek_to_end_all_partitions(self, mock_consumer_class, mock_aiokafka):
        """Test seeking to end of all partitions"""
        mock_consumer = AsyncMock()
        mock_consumer_class.return_value = mock_consumer
        
        consumer = KafkaConsumerOperate(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"],
            group_id="test-group"
        )
        
        await consumer.seek_to_end()
        
        mock_consumer.seek_to_end.assert_called_once_with()
    
    @patch('general_operate.kafka.consumer_operate.aiokafka')
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    @pytest.mark.asyncio
    async def test_seek_to_end_specific_partitions(self, mock_consumer_class, mock_aiokafka):
        """Test seeking to end of specific partitions"""
        mock_consumer = AsyncMock()
        mock_consumer_class.return_value = mock_consumer
        
        from aiokafka import TopicPartition
        partitions = [TopicPartition("test-topic", 0)]
        
        consumer = KafkaConsumerOperate(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"],
            group_id="test-group"
        )
        
        await consumer.seek_to_end(partitions)
        
        mock_consumer.seek_to_end.assert_called_once_with(*partitions)


class TestKafkaConsumerOperateCommitAndControl:
    """Test commit and control functionality"""
    
    @patch('general_operate.kafka.consumer_operate.aiokafka')
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    @pytest.mark.asyncio
    async def test_manual_commit_no_offsets(self, mock_consumer_class, mock_aiokafka):
        """Test manual commit without specific offsets"""
        mock_consumer = AsyncMock()
        mock_consumer_class.return_value = mock_consumer
        
        consumer = KafkaConsumerOperate(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"],
            group_id="test-group"
        )
        
        await consumer.commit()
        
        mock_consumer.start.assert_called_once()
        mock_consumer.commit.assert_called_once_with(None)
    
    @patch('general_operate.kafka.consumer_operate.aiokafka')
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    @pytest.mark.asyncio
    async def test_manual_commit_with_offsets(self, mock_consumer_class, mock_aiokafka):
        """Test manual commit with specific offsets"""
        mock_consumer = AsyncMock()
        mock_consumer_class.return_value = mock_consumer
        
        from aiokafka import TopicPartition
        offsets = {TopicPartition("test-topic", 0): 100}
        
        consumer = KafkaConsumerOperate(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"],
            group_id="test-group"
        )
        
        await consumer.commit(offsets)
        
        mock_consumer.commit.assert_called_once_with(offsets)
    
    @patch('general_operate.kafka.consumer_operate.aiokafka')
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    @pytest.mark.asyncio
    async def test_pause_all_partitions(self, mock_consumer_class, mock_aiokafka):
        """Test pausing all partitions"""
        mock_consumer = AsyncMock()
        
        # Mock assigned partitions
        from aiokafka import TopicPartition
        assigned_partitions = [TopicPartition("test-topic", 0), TopicPartition("test-topic", 1)]
        # assignment() and pause() are regular methods, not async
        mock_consumer.assignment = MagicMock(return_value=assigned_partitions)
        mock_consumer.pause = MagicMock()
        mock_consumer_class.return_value = mock_consumer
        
        consumer = KafkaConsumerOperate(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"],
            group_id="test-group"
        )
        
        await consumer.pause()
        
        mock_consumer.pause.assert_called_once_with(*assigned_partitions)
    
    @patch('general_operate.kafka.consumer_operate.aiokafka')
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    @pytest.mark.asyncio
    async def test_pause_specific_partitions(self, mock_consumer_class, mock_aiokafka):
        """Test pausing specific partitions"""
        mock_consumer = AsyncMock()
        # pause() is a regular method, not async
        mock_consumer.pause = MagicMock()
        mock_consumer_class.return_value = mock_consumer
        
        from aiokafka import TopicPartition
        partitions = [TopicPartition("test-topic", 0)]
        
        consumer = KafkaConsumerOperate(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"],
            group_id="test-group"
        )
        
        await consumer.pause(partitions)
        
        mock_consumer.pause.assert_called_once_with(*partitions)
    
    @patch('general_operate.kafka.consumer_operate.aiokafka')
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    @pytest.mark.asyncio
    async def test_pause_no_assigned_partitions(self, mock_consumer_class, mock_aiokafka):
        """Test pausing when no partitions are assigned"""
        mock_consumer = AsyncMock()
        # assignment() and pause() are regular methods, not async
        mock_consumer.assignment = MagicMock(return_value=None)
        mock_consumer.pause = MagicMock()
        mock_consumer_class.return_value = mock_consumer
        
        consumer = KafkaConsumerOperate(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"],
            group_id="test-group"
        )
        
        await consumer.pause()
        
        # Should not call pause if no partitions assigned
        mock_consumer.pause.assert_not_called()
    
    @patch('general_operate.kafka.consumer_operate.aiokafka')
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    @pytest.mark.asyncio
    async def test_resume_all_partitions(self, mock_consumer_class, mock_aiokafka):
        """Test resuming all partitions"""
        mock_consumer = AsyncMock()
        
        # Mock assigned partitions
        from aiokafka import TopicPartition
        assigned_partitions = [TopicPartition("test-topic", 0), TopicPartition("test-topic", 1)]
        # assignment() and resume() are regular methods, not async
        mock_consumer.assignment = MagicMock(return_value=assigned_partitions)
        mock_consumer.resume = MagicMock()
        mock_consumer_class.return_value = mock_consumer
        
        consumer = KafkaConsumerOperate(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"],
            group_id="test-group"
        )
        
        await consumer.resume()
        
        mock_consumer.resume.assert_called_once_with(*assigned_partitions)
    
    @patch('general_operate.kafka.consumer_operate.aiokafka')
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    @pytest.mark.asyncio
    async def test_resume_specific_partitions(self, mock_consumer_class, mock_aiokafka):
        """Test resuming specific partitions"""
        mock_consumer = AsyncMock()
        # resume() is a regular method, not async
        mock_consumer.resume = MagicMock()
        mock_consumer_class.return_value = mock_consumer
        
        from aiokafka import TopicPartition
        partitions = [TopicPartition("test-topic", 1)]
        
        consumer = KafkaConsumerOperate(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"],
            group_id="test-group"
        )
        
        await consumer.resume(partitions)
        
        mock_consumer.resume.assert_called_once_with(*partitions)


class TestKafkaConsumerOperateEdgeCases:
    """Test edge cases and error scenarios"""
    
    @patch('general_operate.kafka.consumer_operate.aiokafka')
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    @pytest.mark.asyncio
    async def test_multiple_topic_support(self, mock_consumer_class, mock_aiokafka):
        """Test consumer with multiple topics"""
        mock_consumer = AsyncMock()
        mock_consumer_class.return_value = mock_consumer
        
        consumer = KafkaConsumerOperate(
            bootstrap_servers="localhost:9092",
            topics=["topic1", "topic2", "topic3"],
            group_id="test-group"
        )
        
        await consumer.start()
        
        # Verify consumer is created with all topics
        mock_consumer_class.assert_called_once()
        args, kwargs = mock_consumer_class.call_args
        assert "topic1" in args
        assert "topic2" in args
        assert "topic3" in args
    
    @patch('general_operate.kafka.consumer_operate.aiokafka')
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    @pytest.mark.asyncio
    async def test_stop_error_handling(self, mock_consumer_class, mock_aiokafka):
        """Test stop error handling"""
        mock_consumer = AsyncMock()
        mock_consumer.stop.side_effect = Exception("Stop failed")
        mock_consumer_class.return_value = mock_consumer
        
        consumer = KafkaConsumerOperate(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"],
            group_id="test-group"
        )
        
        await consumer.start()
        
        # Stop should handle the exception gracefully
        await consumer.stop()
        
        assert consumer._running is False
    
    @patch('general_operate.kafka.consumer_operate.aiokafka')
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    @pytest.mark.asyncio
    async def test_consumer_config_inheritance(self, mock_consumer_class, mock_aiokafka):
        """Test that consumer config inherits from base config"""
        consumer = KafkaConsumerOperate(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"],
            group_id="test-group",
            client_id="test-client"
        )
        
        # Check that base config is inherited
        assert consumer.consumer_config["bootstrap_servers"] == ["localhost:9092"]
        assert consumer.consumer_config["client_id"] == "test-client"
        assert consumer.consumer_config["group_id"] == "test-group"
        
        # Check consumer-specific defaults
        assert consumer.consumer_config["auto_offset_reset"] == "earliest"
        assert consumer.consumer_config["enable_auto_commit"] is True
        assert consumer.consumer_config["auto_commit_interval_ms"] == 5000
        assert consumer.consumer_config["isolation_level"] == "read_committed"


class TestKafkaConsumerOperateIntegration:
    """Test realistic integration scenarios"""
    
    @patch('general_operate.kafka.consumer_operate.aiokafka')
    @patch('general_operate.kafka.consumer_operate.AIOKafkaConsumer')
    @pytest.mark.asyncio
    async def test_consume_and_process_events_scenario(self, mock_consumer_class, mock_aiokafka):
        """Test realistic event consumption and processing"""
        # Mock messages representing user events
        events = [
            {"event_type": "user_login", "user_id": "user1", "timestamp": "2023-12-01T10:00:00Z"},
            {"event_type": "user_logout", "user_id": "user1", "timestamp": "2023-12-01T10:30:00Z"},
            {"event_type": "user_login", "user_id": "user2", "timestamp": "2023-12-01T10:15:00Z"}
        ]
        
        # Mock messages
        mock_messages = []
        for i, event in enumerate(events):
            msg = MagicMock()
            msg.value = event
            msg.topic = "user-events"
            msg.partition = 0
            msg.offset = i
            mock_messages.append(msg)
        
        # Mock consumer
        mock_consumer = AsyncMock()
        mock_consumer.__aiter__.return_value = mock_messages
        mock_consumer_class.return_value = mock_consumer
        
        # Event processor
        processed_events = []
        async def event_processor(event):
            processed_events.append(event)
            if len(processed_events) >= len(events):
                consumer._running = False
        
        consumer = KafkaConsumerOperate(
            bootstrap_servers="localhost:9092",
            topics=["user-events"],
            group_id="user-analytics-group",
            config={"auto_offset_reset": "earliest"}
        )
        
        await consumer.consume_events(event_processor)
        
        # Verify all events were processed
        assert len(processed_events) == 3
        assert processed_events[0]["event_type"] == "user_login"
        assert processed_events[1]["event_type"] == "user_logout"
        assert processed_events[2]["event_type"] == "user_login"


if __name__ == "__main__":
    pass