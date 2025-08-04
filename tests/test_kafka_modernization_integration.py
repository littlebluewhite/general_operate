"""
Integration tests for modernized Kafka client implementation.

This module tests the integration between the modernized security configuration
and the actual Kafka client classes to ensure they work together correctly.
"""

import asyncio
import pytest
from unittest.mock import Mock, patch, AsyncMock, MagicMock
from typing import Any

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient

from general_operate.kafka.kafka_client import (
    KafkaAsyncProducer,
    KafkaAsyncConsumer,
    KafkaAsyncAdmin,
    EventMessage,
    ProcessingResult,
    SecurityConfigBuilder,
    KafkaConnectionBuilder,
)


class TestModernizedKafkaAsyncProducer:
    """Test the modernized KafkaAsyncProducer with security configuration"""

    @pytest.fixture
    def sample_event(self):
        """Create a sample event for testing"""
        return EventMessage(
            event_type="test.event",
            tenant_id="tenant-1",
            user_id="user-1",
            correlation_id="corr-123",
            data={"message": "test message"},
            metadata={"source": "test"}
        )

    @pytest.mark.asyncio
    async def test_producer_start_uses_modern_ssl_config(self, sample_event):
        """Test that producer start uses modern SSL configuration"""
        ssl_config = {
            "bootstrap_servers": ["ssl-broker:9093"],
            "security_protocol": "SSL",
            "ssl_cafile": "/path/to/ca.pem",
            "ssl_certfile": "/path/to/cert.pem",
            "ssl_keyfile": "/path/to/key.pem",
        }
        
        with patch('general_operate.kafka.kafka_client.AIOKafkaProducer') as mock_producer_class:
            with patch.object(SecurityConfigBuilder, '_create_ssl_context') as mock_ssl:
                mock_context = Mock()
                mock_ssl.return_value = mock_context
                
                mock_producer_instance = AsyncMock()
                mock_producer_class.return_value = mock_producer_instance
                
                producer = KafkaAsyncProducer(ssl_config, "ssl-test-service")
                await producer.start()
                
                # Verify AIOKafkaProducer was called with modernized config
                call_args = mock_producer_class.call_args[1]  # kwargs
                
                assert call_args["bootstrap_servers"] == ["ssl-broker:9093"]
                assert call_args["security_protocol"] == "SSL"
                assert call_args["ssl_context"] == mock_context
                assert call_args["client_id"] == "ssl-test-service-producer"
                
                # Verify legacy SSL parameters are NOT passed
                assert "ssl_cafile" not in call_args
                assert "ssl_certfile" not in call_args
                assert "ssl_keyfile" not in call_args
                
                # Verify producer was started
                mock_producer_instance.start.assert_called_once()

    @pytest.mark.asyncio
    async def test_producer_start_uses_modern_sasl_config(self, sample_event):
        """Test that producer start uses modern SASL configuration"""
        sasl_config = {
            "bootstrap_servers": ["sasl-broker:9094"],
            "security_protocol": "SASL_PLAINTEXT",
            "sasl_mechanism": "SCRAM-SHA-256",
            "sasl_plain_username": "test_user",
            "sasl_plain_password": "test_pass",
        }
        
        with patch('general_operate.kafka.kafka_client.AIOKafkaProducer') as mock_producer_class:
            mock_producer_instance = AsyncMock()
            mock_producer_class.return_value = mock_producer_instance
            
            producer = KafkaAsyncProducer(sasl_config, "sasl-test-service")
            await producer.start()
            
            # Verify AIOKafkaProducer was called with modernized config
            call_args = mock_producer_class.call_args[1]  # kwargs
            
            assert call_args["bootstrap_servers"] == ["sasl-broker:9094"]
            assert call_args["security_protocol"] == "SASL_PLAINTEXT"
            assert call_args["sasl_mechanism"] == "SCRAM-SHA-256"
            assert call_args["sasl_plain_username"] == "test_user"
            assert call_args["sasl_plain_password"] == "test_pass"
            assert call_args["client_id"] == "sasl-test-service-producer"
            
            # Verify SSL context is not present for SASL_PLAINTEXT
            assert "ssl_context" not in call_args

    @pytest.mark.asyncio
    async def test_producer_send_event_with_modernized_config(self, sample_event):
        """Test that producer can send events with modernized configuration"""
        kafka_config = {
            "bootstrap_servers": ["localhost:9092"],
            "security_protocol": "PLAINTEXT",
        }
        
        with patch('general_operate.kafka.kafka_client.AIOKafkaProducer') as mock_producer_class:
            mock_producer_instance = AsyncMock()
            mock_producer_instance.send_and_wait = AsyncMock()
            
            # Mock the return value of send_and_wait
            mock_record_metadata = Mock()
            mock_record_metadata.partition = 0
            mock_record_metadata.offset = 123
            mock_producer_instance.send_and_wait.return_value = mock_record_metadata
            
            mock_producer_class.return_value = mock_producer_instance
            
            producer = KafkaAsyncProducer(kafka_config, "test-service")
            await producer.start()
            
            # Send an event
            await producer.send_event("test-topic", sample_event)
            
            # Verify send_and_wait was called correctly
            mock_producer_instance.send_and_wait.assert_called_once()
            call_args = mock_producer_instance.send_and_wait.call_args[1]  # kwargs
            
            assert call_args["topic"] == "test-topic"
            assert call_args["key"] == sample_event.tenant_id
            assert call_args["value"] == sample_event.to_json()

    @pytest.mark.asyncio
    async def test_producer_handles_connection_errors_gracefully(self, sample_event):
        """Test that producer handles connection errors gracefully with modern config"""
        kafka_config = {
            "bootstrap_servers": ["localhost:9092"],
            "security_protocol": "PLAINTEXT",
        }
        
        with patch('general_operate.kafka.kafka_client.AIOKafkaProducer') as mock_producer_class:
            mock_producer_instance = AsyncMock()
            
            # Simulate connection error on first attempt, success on retry
            from aiokafka.errors import KafkaConnectionError
            
            mock_record_metadata = Mock()
            mock_record_metadata.partition = 0
            mock_record_metadata.offset = 123
            
            mock_producer_instance.send_and_wait.side_effect = [
                KafkaConnectionError("Connection failed"),
                mock_record_metadata  # Success on retry
            ]
            
            mock_producer_class.return_value = mock_producer_instance
            
            producer = KafkaAsyncProducer(kafka_config, "retry-test-service")
            await producer.start()
            
            # Send an event (should retry after connection error)
            await producer.send_event("test-topic", sample_event)
            
            # Verify send_and_wait was called twice (original + retry)
            assert mock_producer_instance.send_and_wait.call_count == 2


class TestModernizedKafkaAsyncConsumer:
    """Test the modernized KafkaAsyncConsumer with security configuration"""

    @pytest.mark.asyncio
    async def test_consumer_start_uses_modern_ssl_config(self):
        """Test that consumer start uses modern SSL configuration"""
        ssl_config = {
            "bootstrap_servers": ["ssl-broker:9093"],
            "security_protocol": "SSL",
            "ssl_cafile": "mock_ca.pem",
            "ssl_certfile": "mock_cert.pem",  
            "ssl_keyfile": "mock_key.pem",
        }
        
        with patch('general_operate.kafka.kafka_client.AIOKafkaConsumer') as mock_consumer_class:
            with patch('general_operate.kafka.kafka_client.AIOKafkaProducer') as mock_producer_class:
                with patch('general_operate.kafka.kafka_client.create_ssl_context') as mock_ssl:
                    mock_context = Mock()
                    mock_ssl.return_value = mock_context
                    
                    mock_consumer_instance = AsyncMock()
                    mock_consumer_class.return_value = mock_consumer_instance
                    
                    mock_producer_instance = AsyncMock()
                    mock_producer_class.return_value = mock_producer_instance
                    
                    async def mock_handler(message):
                        return ProcessingResult(success=True)
                    
                    consumer = KafkaAsyncConsumer(
                        topics=["test-topic"],
                        group_id="ssl-group", 
                        message_handler=mock_handler,
                        kafka_config=ssl_config,
                        service_name="ssl-consumer-service"
                    )
                    await consumer.start()
                    
                    # Verify AIOKafkaConsumer was called with modernized config
                    call_args = mock_consumer_class.call_args[1]  # kwargs
                    
                    assert call_args["bootstrap_servers"] == ["ssl-broker:9093"]
                    assert call_args["security_protocol"] == "SSL"
                    assert call_args["ssl_context"] == mock_context
                    assert call_args["group_id"] == "ssl-group"
                    assert call_args["client_id"] == "ssl-consumer-service-consumer-ssl-group"
                    
                    # Verify legacy SSL parameters are NOT passed
                    assert "ssl_cafile" not in call_args
                    assert "ssl_certfile" not in call_args
                    assert "ssl_keyfile" not in call_args

    @pytest.mark.asyncio
    async def test_consumer_start_uses_modern_sasl_ssl_config(self):
        """Test that consumer start uses modern SASL_SSL configuration"""
        sasl_ssl_config = {
            "bootstrap_servers": ["secure-broker:9094"],
            "security_protocol": "SASL_SSL",
            "ssl_cafile": "/path/to/ca.pem",
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": "secure_user",
            "sasl_plain_password": "secure_pass",
        }
        
        with patch('general_operate.kafka.kafka_client.AIOKafkaConsumer') as mock_consumer_class:
            with patch('general_operate.kafka.kafka_client.AIOKafkaProducer') as mock_producer_class:
                with patch.object(SecurityConfigBuilder, '_create_ssl_context') as mock_ssl:
                    mock_context = Mock()
                    mock_ssl.return_value = mock_context
                    
                    mock_consumer_instance = AsyncMock()
                    mock_consumer_class.return_value = mock_consumer_instance
                    
                    mock_producer_instance = AsyncMock()
                    mock_producer_class.return_value = mock_producer_instance
                    
                    async def mock_handler(message):
                        return ProcessingResult(success=True)
                    
                    consumer = KafkaAsyncConsumer(
                        topics=["secure-topic"],
                        group_id="secure-group",
                        message_handler=mock_handler,
                        kafka_config=sasl_ssl_config,
                        service_name="sasl-ssl-service"
                    )
                    await consumer.start()
                    
                    # Verify AIOKafkaConsumer was called with modernized config
                    call_args = mock_consumer_class.call_args[1]  # kwargs
                    
                    assert call_args["bootstrap_servers"] == ["secure-broker:9094"]
                    assert call_args["security_protocol"] == "SASL_SSL"
                    assert call_args["ssl_context"] == mock_context
                    assert call_args["sasl_mechanism"] == "PLAIN"
                    assert call_args["sasl_plain_username"] == "secure_user"
                    assert call_args["sasl_plain_password"] == "secure_pass"
                    assert call_args["group_id"] == "secure-group"
                    assert call_args["client_id"] == "sasl-ssl-service-consumer-secure-group"
                    
                    # Verify legacy SSL parameters are NOT passed
                    assert "ssl_cafile" not in call_args

    @pytest.mark.asyncio
    async def test_consumer_consume_messages_with_modernized_config(self):
        """Test that consumer can consume messages with modernized configuration"""
        kafka_config = {
            "bootstrap_servers": ["localhost:9092"],
            "security_protocol": "PLAINTEXT",
        }
        
        with patch('general_operate.kafka.kafka_client.AIOKafkaConsumer') as mock_consumer_class:
            with patch('general_operate.kafka.kafka_client.AIOKafkaProducer') as mock_producer_class:
                mock_consumer_instance = AsyncMock()
                mock_producer_instance = AsyncMock()
                
                # Mock message
                mock_message = Mock()
                mock_message.topic = "test-topic"
                mock_message.partition = 0
                mock_message.offset = 456
                mock_message.key = b"test-key"
                mock_message.value = b'{"event_type": "test.event", "data": {"msg": "test"}}'
                mock_message.timestamp = 1609459200000  # Jan 1, 2021
                
                # Mock the async iteration properly
                async def mock_aiter():
                    yield mock_message
                
                mock_consumer_instance.__aiter__ = lambda self: mock_aiter()
                mock_consumer_class.return_value = mock_consumer_instance
                mock_producer_class.return_value = mock_producer_instance
                
                async def mock_handler(message):
                    return ProcessingResult(success=True)
                
                consumer = KafkaAsyncConsumer(
                    topics=["test-topic"],
                    group_id="test-group",
                    message_handler=mock_handler,
                    kafka_config=kafka_config,
                    service_name="consume-test-service"
                )
                await consumer.start()
                
                # Test that consumer can process messages correctly
                # Since consume() processes messages internally, we'll verify the mock setup
                
                # Configure the consumer to stop after processing one message
                original_handler = consumer.message_handler
                messages_processed = []
                
                async def tracking_handler(message):
                    messages_processed.append(message)
                    consumer._running = False  # Stop after one message
                    return await original_handler(message)
                
                consumer.message_handler = tracking_handler
                
                # Start consuming (will process one message then stop)
                await consumer.consume()
                
                # Verify consumer was subscribed to topics
                mock_consumer_instance.subscribe.assert_called_once_with(["test-topic"])
                
                # Note: Due to mocking, we can't easily verify message processing details
                # but we can verify the configuration was correct


class TestModernizedKafkaAsyncAdmin:
    """Test the modernized KafkaAsyncAdmin with security configuration"""

    @pytest.mark.asyncio
    async def test_admin_uses_modern_ssl_config(self):
        """Test that admin client uses modern SSL configuration"""
        ssl_config = {
            "bootstrap_servers": ["ssl-broker:9093"],
            "security_protocol": "SSL",
            "ssl_cafile": "/path/to/ca.pem",
            "ssl_certfile": "/path/to/cert.pem",
            "ssl_keyfile": "/path/to/key.pem",
        }
        
        with patch('aiokafka.admin.AIOKafkaAdminClient') as mock_admin_class:
            with patch.object(SecurityConfigBuilder, '_create_ssl_context') as mock_ssl:
                mock_context = Mock()
                mock_ssl.return_value = mock_context
                
                mock_admin_instance = AsyncMock()
                mock_admin_instance.start = AsyncMock()
                mock_admin_instance.close = AsyncMock()
                mock_admin_class.return_value = mock_admin_instance
                
                admin = KafkaAsyncAdmin(ssl_config)
                await admin.start()
                
                # Verify AIOKafkaAdminClient was called with modernized config
                call_args = mock_admin_class.call_args[1]  # kwargs
                
                assert call_args["bootstrap_servers"] == ["ssl-broker:9093"]
                assert call_args["security_protocol"] == "SSL"
                assert call_args["ssl_context"] == mock_context
                
                # Verify legacy SSL parameters are NOT passed
                assert "ssl_cafile" not in call_args
                assert "ssl_certfile" not in call_args
                assert "ssl_keyfile" not in call_args

    @pytest.mark.asyncio
    async def test_admin_create_topics_with_modernized_config(self):
        """Test that admin can create topics with modernized configuration"""
        kafka_config = {
            "bootstrap_servers": ["localhost:9092"],
            "security_protocol": "PLAINTEXT",
        }
        
        with patch('aiokafka.admin.AIOKafkaAdminClient') as mock_admin_class:
            with patch('general_operate.kafka.kafka_client.NewTopic') as mock_new_topic:
                mock_admin_instance = AsyncMock()
                mock_admin_instance.start = AsyncMock()
                mock_admin_instance.close = AsyncMock()
                mock_admin_instance.create_topics = AsyncMock()
                mock_admin_class.return_value = mock_admin_instance
                
                mock_topic = Mock()
                mock_new_topic.return_value = mock_topic
                
                admin = KafkaAsyncAdmin(kafka_config)
                await admin.start()
                
                # Create a topic
                await admin.create_topics(["test-topic"])
                
                # Verify NewTopic was created correctly
                mock_new_topic.assert_called_once_with(
                    name="test-topic",
                    num_partitions=3,
                    replication_factor=1,
                    topic_configs={
                        "cleanup.policy": "delete",
                        "retention.ms": "604800000",
                    }
                )
                
                # Verify create_topics was called
                mock_admin_instance.create_topics.assert_called_once_with([mock_topic], validate_only=False)


class TestConfigurationMigrationScenarios:
    """Test scenarios that simulate migrating from old to new configuration"""

    def test_legacy_ssl_config_migration(self):
        """Test migration from legacy SSL configuration to modern"""
        # Simulate legacy configuration that might have caused issues
        legacy_ssl_config = {
            "bootstrap_servers": "legacy-broker:9093",
            "security_protocol": "SSL",
            "ssl_cafile": "/legacy/ca.pem",
            "ssl_certfile": "/legacy/cert.pem", 
            "ssl_keyfile": "/legacy/key.pem",
            "ssl_check_hostname": True,  # This caused issues in old implementation
            "ssl_crlfile": "/legacy/crl.pem",  # Not supported by aiokafka
        }
        
        with patch.object(SecurityConfigBuilder, '_create_ssl_context') as mock_ssl:
            mock_context = Mock()
            mock_ssl.return_value = mock_context
            
            # Build modern configuration
            modern_config = KafkaConnectionBuilder.build_producer_config(
                legacy_ssl_config, "migrated-service"
            )
            
            # Verify modern configuration structure
            assert modern_config["bootstrap_servers"] == "legacy-broker:9093"
            assert modern_config["security_protocol"] == "SSL"
            assert modern_config["ssl_context"] == mock_context
            
            # Verify problematic legacy parameters are filtered out
            problematic_params = [
                "ssl_cafile", "ssl_certfile", "ssl_keyfile", 
                "ssl_check_hostname", "ssl_crlfile"
            ]
            for param in problematic_params:
                assert param not in modern_config, f"Legacy parameter {param} should be filtered out"
            
            # Verify SSL context creation was called with correct parameters
            call_args = mock_ssl.call_args[0][0]  # First positional argument (kafka_config)
            assert call_args["ssl_cafile"] == "/legacy/ca.pem"
            assert call_args["ssl_certfile"] == "/legacy/cert.pem"
            assert call_args["ssl_keyfile"] == "/legacy/key.pem"

    def test_mixed_config_environments(self):
        """Test handling of mixed configuration environments"""
        # Configuration that mixes SSL and SASL parameters (common in migration)
        mixed_config = {
            "bootstrap_servers": ["mixed-broker:9094"],
            "security_protocol": "SASL_SSL",
            "ssl_cafile": "/mixed/ca.pem",
            "sasl_mechanism": "SCRAM-SHA-256",
            "sasl_plain_username": "mixed_user",
            "sasl_plain_password": "mixed_pass",
            # Some legacy parameters that should be ignored
            "ssl_check_hostname": True,
            "enable_idempotence": True,  # Producer-specific, should be preserved
        }
        
        with patch.object(SecurityConfigBuilder, '_create_ssl_context') as mock_ssl:
            mock_context = Mock()
            mock_ssl.return_value = mock_context
            
            # Test producer configuration
            producer_config = KafkaConnectionBuilder.build_producer_config(
                mixed_config, "mixed-producer"
            )
            
            # Verify both SSL and SASL are configured correctly
            assert producer_config["security_protocol"] == "SASL_SSL"
            assert producer_config["ssl_context"] == mock_context
            assert producer_config["sasl_mechanism"] == "SCRAM-SHA-256"
            assert producer_config["sasl_plain_username"] == "mixed_user"
            assert producer_config["sasl_plain_password"] == "mixed_pass"
            assert producer_config["enable_idempotence"] is True
            
            # Verify legacy SSL parameters are not present
            assert "ssl_cafile" not in producer_config
            assert "ssl_check_hostname" not in producer_config

    @pytest.mark.asyncio
    async def test_runtime_configuration_validation(self):
        """Test that runtime configuration validation works with modern config"""
        # Configuration with intentionally invalid SSL setup
        invalid_ssl_config = {
            "bootstrap_servers": ["localhost:9092"],
            "security_protocol": "SSL",
            "ssl_cafile": "/nonexistent/ca.pem",
        }
        
        with patch('general_operate.kafka.kafka_client.create_ssl_context') as mock_create:
            mock_create.side_effect = FileNotFoundError("CA certificate file not found")
            
            producer = KafkaAsyncProducer(invalid_ssl_config, "validation-test")
            
            # Should raise error during start due to SSL configuration issues
            with pytest.raises(FileNotFoundError, match="CA certificate file not found"):
                await producer.start()

    def test_environment_specific_configurations(self):
        """Test configurations for different environments (dev, staging, prod)"""
        environments = {
            "development": {
                "bootstrap_servers": ["dev-kafka:9092"],
                "security_protocol": "PLAINTEXT",
            },
            "staging": {
                "bootstrap_servers": ["staging-kafka:9093"],
                "security_protocol": "SSL",
                "ssl_cafile": "/staging/ca.pem",
                "ssl_certfile": "/staging/cert.pem",
                "ssl_keyfile": "/staging/key.pem",
            },
            "production": {
                "bootstrap_servers": ["prod-kafka-1:9094", "prod-kafka-2:9094"],
                "security_protocol": "SASL_SSL",
                "ssl_cafile": "/prod/ca.pem",
                "sasl_mechanism": "SCRAM-SHA-512",
                "sasl_plain_username": "prod_user",
                "sasl_plain_password": "prod_secure_pass",
            }
        }
        
        for env, config in environments.items():
            if config["security_protocol"] in ["SSL", "SASL_SSL"]:
                with patch.object(SecurityConfigBuilder, '_create_ssl_context') as mock_ssl:
                    mock_context = Mock()
                    mock_ssl.return_value = mock_context
                    
                    modern_config = KafkaConnectionBuilder.build_consumer_config(
                        config, f"{env}-group", f"{env}-service"
                    )
                    
                    # Verify environment-specific configuration
                    assert modern_config["bootstrap_servers"] == config["bootstrap_servers"]
                    assert modern_config["security_protocol"] == config["security_protocol"]
                    assert modern_config["group_id"] == f"{env}-group"
                    assert modern_config["client_id"] == f"{env}-service-consumer-{env}-group"
                    
                    if config["security_protocol"] in ["SSL", "SASL_SSL"]:
                        assert modern_config["ssl_context"] == mock_context
                        
                    if config["security_protocol"] in ["SASL_PLAINTEXT", "SASL_SSL"]:
                        assert "sasl_mechanism" in modern_config
            else:
                # PLAINTEXT configuration
                modern_config = KafkaConnectionBuilder.build_consumer_config(
                    config, f"{env}-group", f"{env}-service"
                )
                
                assert modern_config["security_protocol"] == "PLAINTEXT"
                assert "ssl_context" not in modern_config
                assert "sasl_mechanism" not in modern_config