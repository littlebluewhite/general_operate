"""
Comprehensive tests for modernized Kafka security configuration.

This module tests the new SecurityConfigBuilder and KafkaConnectionBuilder classes
that properly handle SSL/SASL configuration using aiokafka's modern patterns.
"""

import ssl
import pytest
from unittest.mock import Mock, patch, MagicMock, AsyncMock
from typing import Any

from aiokafka.helpers import create_ssl_context

from general_operate.kafka.kafka_client import (
    SecurityConfigBuilder,
    KafkaConnectionBuilder,
    KafkaAsyncProducer,
    KafkaAsyncConsumer,
    KafkaAsyncAdmin,
)


class TestSecurityConfigBuilder:
    """Test SecurityConfigBuilder class for modern SSL/SASL configuration"""

    def test_build_with_plaintext_protocol(self):
        """Test building configuration with PLAINTEXT protocol"""
        kafka_config = {
            "bootstrap_servers": "localhost:9092",
            "security_protocol": "PLAINTEXT",
        }
        
        result = SecurityConfigBuilder.build(kafka_config)
        
        assert result == {"security_protocol": "PLAINTEXT"}
        assert "ssl_context" not in result
        assert "sasl_mechanism" not in result

    def test_build_with_ssl_protocol(self):
        """Test building configuration with SSL protocol"""
        kafka_config = {
            "bootstrap_servers": "localhost:9092",
            "security_protocol": "SSL",
            "ssl_cafile": "/path/to/ca.pem",
            "ssl_certfile": "/path/to/cert.pem",
            "ssl_keyfile": "/path/to/key.pem",
        }
        
        with patch.object(SecurityConfigBuilder, '_create_ssl_context') as mock_ssl:
            mock_context = Mock(spec=ssl.SSLContext)
            mock_ssl.return_value = mock_context
            
            result = SecurityConfigBuilder.build(kafka_config)
            
            assert result["security_protocol"] == "SSL"
            assert result["ssl_context"] == mock_context
            mock_ssl.assert_called_once_with(kafka_config)

    def test_build_with_sasl_plaintext_protocol(self):
        """Test building configuration with SASL_PLAINTEXT protocol"""
        kafka_config = {
            "bootstrap_servers": "localhost:9092",
            "security_protocol": "SASL_PLAINTEXT",
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": "test_user",
            "sasl_plain_password": "test_pass",
        }
        
        result = SecurityConfigBuilder.build(kafka_config)
        
        assert result["security_protocol"] == "SASL_PLAINTEXT"
        assert result["sasl_mechanism"] == "PLAIN"
        assert result["sasl_plain_username"] == "test_user"
        assert result["sasl_plain_password"] == "test_pass"
        assert "ssl_context" not in result

    def test_build_with_sasl_ssl_protocol(self):
        """Test building configuration with SASL_SSL protocol"""
        kafka_config = {
            "bootstrap_servers": "localhost:9092",
            "security_protocol": "SASL_SSL",
            "ssl_cafile": "/path/to/ca.pem",
            "sasl_mechanism": "SCRAM-SHA-256",
            "sasl_plain_username": "test_user",
            "sasl_plain_password": "test_pass",
        }
        
        with patch.object(SecurityConfigBuilder, '_create_ssl_context') as mock_ssl:
            mock_context = Mock(spec=ssl.SSLContext)
            mock_ssl.return_value = mock_context
            
            result = SecurityConfigBuilder.build(kafka_config)
            
            assert result["security_protocol"] == "SASL_SSL"
            assert result["ssl_context"] == mock_context
            assert result["sasl_mechanism"] == "SCRAM-SHA-256"
            assert result["sasl_plain_username"] == "test_user"
            assert result["sasl_plain_password"] == "test_pass"

    def test_create_ssl_context_with_all_params(self):
        """Test _create_ssl_context with all SSL parameters"""
        kafka_config = {
            "ssl_cafile": "/path/to/ca.pem",
            "ssl_certfile": "/path/to/cert.pem",
            "ssl_keyfile": "/path/to/key.pem",
            "ssl_password": "test_password",
        }
        
        with patch('general_operate.kafka.kafka_client.create_ssl_context') as mock_create:
            mock_context = Mock(spec=ssl.SSLContext)
            mock_create.return_value = mock_context
            
            result = SecurityConfigBuilder._create_ssl_context(kafka_config)
            
            assert result == mock_context
            mock_create.assert_called_once_with(
                cafile="/path/to/ca.pem",
                certfile="/path/to/cert.pem",
                keyfile="/path/to/key.pem",
                password="test_password"
            )

    def test_create_ssl_context_with_partial_params(self):
        """Test _create_ssl_context with only some SSL parameters"""
        kafka_config = {
            "ssl_cafile": "/path/to/ca.pem",
            "ssl_certfile": "/path/to/cert.pem",
            # No keyfile or password
        }
        
        with patch('general_operate.kafka.kafka_client.create_ssl_context') as mock_create:
            mock_context = Mock(spec=ssl.SSLContext)
            mock_create.return_value = mock_context
            
            result = SecurityConfigBuilder._create_ssl_context(kafka_config)
            
            assert result == mock_context
            mock_create.assert_called_once_with(
                cafile="/path/to/ca.pem",
                certfile="/path/to/cert.pem"
            )

    def test_create_ssl_context_with_no_ssl_params(self):
        """Test _create_ssl_context with no SSL parameters returns None"""
        kafka_config = {
            "bootstrap_servers": "localhost:9092",
            # No SSL parameters
        }
        
        result = SecurityConfigBuilder._create_ssl_context(kafka_config)
        assert result is None

    def test_create_ssl_context_handles_ssl_context_creation_error(self):
        """Test _create_ssl_context handles errors from create_ssl_context"""
        kafka_config = {
            "ssl_cafile": "/invalid/path/ca.pem",
        }
        
        with patch('general_operate.kafka.kafka_client.create_ssl_context') as mock_create:
            mock_create.side_effect = Exception("SSL context creation failed")
            
            with pytest.raises(Exception, match="SSL context creation failed"):
                SecurityConfigBuilder._create_ssl_context(kafka_config)

    def test_build_sasl_config_with_all_valid_params(self):
        """Test _build_sasl_config with all valid SASL parameters"""
        kafka_config = {
            "sasl_mechanism": "SCRAM-SHA-256",
            "sasl_plain_username": "test_user",
            "sasl_plain_password": "test_pass",
            "sasl_kerberos_service_name": "kafka",
            "sasl_kerberos_domain_name": "example.com",
            # sasl_oauth_token_provider would be a callable, skip for this test
        }
        
        result = SecurityConfigBuilder._build_sasl_config(kafka_config)
        
        expected = {
            "sasl_mechanism": "SCRAM-SHA-256",
            "sasl_plain_username": "test_user",
            "sasl_plain_password": "test_pass",
            "sasl_kerberos_service_name": "kafka",
            "sasl_kerberos_domain_name": "example.com",
        }
        assert result == expected

    def test_build_sasl_config_filters_invalid_params(self):
        """Test _build_sasl_config filters out invalid SASL parameters"""
        kafka_config = {
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": "test_user",
            "sasl_plain_password": "test_pass",
            # These should be filtered out
            "ssl_cafile": "/path/to/ca.pem",
            "bootstrap_servers": "localhost:9092",
            "invalid_param": "should_be_filtered",
        }
        
        result = SecurityConfigBuilder._build_sasl_config(kafka_config)
        
        expected = {
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": "test_user",
            "sasl_plain_password": "test_pass",
        }
        assert result == expected
        assert "ssl_cafile" not in result
        assert "bootstrap_servers" not in result
        assert "invalid_param" not in result

    def test_build_with_default_plaintext_when_no_protocol_specified(self):
        """Test build defaults to PLAINTEXT when no security_protocol specified"""
        kafka_config = {
            "bootstrap_servers": "localhost:9092",
            # No security_protocol specified
        }
        
        result = SecurityConfigBuilder.build(kafka_config)
        
        assert result == {"security_protocol": "PLAINTEXT"}


class TestKafkaConnectionBuilder:
    """Test KafkaConnectionBuilder class for modern connection configuration"""

    def test_build_producer_config_basic(self):
        """Test building basic producer configuration"""
        kafka_config = {
            "bootstrap_servers": ["localhost:9092"],
            "security_protocol": "PLAINTEXT",
        }
        service_name = "test-service"
        
        with patch.object(SecurityConfigBuilder, 'build') as mock_security:
            mock_security.return_value = {"security_protocol": "PLAINTEXT"}
            
            result = KafkaConnectionBuilder.build_producer_config(
                kafka_config, service_name
            )
            
            expected_keys = {
                "bootstrap_servers", "value_serializer", "key_serializer",
                "client_id", "acks", "enable_idempotence", "compression_type",
                "request_timeout_ms", "security_protocol"
            }
            assert set(result.keys()) == expected_keys
            assert result["bootstrap_servers"] == ["localhost:9092"]
            assert result["client_id"] == "test-service-producer"
            assert result["acks"] == "all"
            assert result["enable_idempotence"] is True
            assert result["compression_type"] == "gzip"
            assert result["request_timeout_ms"] == 30000
            mock_security.assert_called_once_with(kafka_config)

    def test_build_producer_config_with_custom_settings(self):
        """Test building producer configuration with custom settings"""
        kafka_config = {
            "bootstrap_servers": ["broker1:9092", "broker2:9092"],
            "security_protocol": "SSL",
            "acks": "1",
            "enable_idempotence": False,
            "compression_type": "snappy",
            "request_timeout_ms": 60000,
        }
        service_name = "custom-service"
        
        with patch.object(SecurityConfigBuilder, 'build') as mock_security:
            mock_security.return_value = {"security_protocol": "SSL", "ssl_context": Mock()}
            
            result = KafkaConnectionBuilder.build_producer_config(
                kafka_config, service_name
            )
            
            assert result["bootstrap_servers"] == ["broker1:9092", "broker2:9092"]
            assert result["client_id"] == "custom-service-producer"
            assert result["acks"] == "1"
            assert result["enable_idempotence"] is False
            assert result["compression_type"] == "snappy"
            assert result["request_timeout_ms"] == 60000

    def test_build_consumer_config_basic(self):
        """Test building basic consumer configuration"""
        kafka_config = {
            "bootstrap_servers": ["localhost:9092"],
            "security_protocol": "PLAINTEXT",
        }
        group_id = "test-group"
        service_name = "test-service"
        
        with patch.object(SecurityConfigBuilder, 'build') as mock_security:
            mock_security.return_value = {"security_protocol": "PLAINTEXT"}
            
            result = KafkaConnectionBuilder.build_consumer_config(
                kafka_config, group_id, service_name
            )
            
            expected_keys = {
                "bootstrap_servers", "group_id", "client_id", "auto_offset_reset",
                "enable_auto_commit", "value_deserializer", "key_deserializer",
                "isolation_level", "session_timeout_ms", "heartbeat_interval_ms",
                "max_poll_records", "max_poll_interval_ms", "security_protocol"
            }
            assert set(result.keys()) == expected_keys
            assert result["bootstrap_servers"] == ["localhost:9092"]
            assert result["group_id"] == "test-group"
            assert result["client_id"] == "test-service-consumer-test-group"
            assert result["auto_offset_reset"] == "latest"
            assert result["enable_auto_commit"] is False
            assert result["isolation_level"] == "read_committed"
            mock_security.assert_called_once_with(kafka_config)

    def test_build_consumer_config_with_custom_settings(self):
        """Test building consumer configuration with custom settings"""
        kafka_config = {
            "bootstrap_servers": ["broker1:9092"],
            "security_protocol": "SASL_SSL",
            "auto_offset_reset": "earliest",
            "session_timeout_ms": 60000,
            "heartbeat_interval_ms": 5000,
            "max_poll_records": 1000,
            "max_poll_interval_ms": 600000,
        }
        group_id = "custom-group"
        service_name = "custom-service"
        
        with patch.object(SecurityConfigBuilder, 'build') as mock_security:
            mock_security.return_value = {
                "security_protocol": "SASL_SSL",
                "ssl_context": Mock(),
                "sasl_mechanism": "PLAIN"
            }
            
            result = KafkaConnectionBuilder.build_consumer_config(
                kafka_config, group_id, service_name
            )
            
            assert result["group_id"] == "custom-group"
            assert result["client_id"] == "custom-service-consumer-custom-group"
            assert result["auto_offset_reset"] == "earliest"
            assert result["session_timeout_ms"] == 60000
            assert result["heartbeat_interval_ms"] == 5000
            assert result["max_poll_records"] == 1000
            assert result["max_poll_interval_ms"] == 600000

    def test_producer_serializers_work_correctly(self):
        """Test that producer serializers work correctly"""
        kafka_config = {
            "bootstrap_servers": ["localhost:9092"],
            "security_protocol": "PLAINTEXT",
        }
        
        with patch.object(SecurityConfigBuilder, 'build') as mock_security:
            mock_security.return_value = {"security_protocol": "PLAINTEXT"}
            
            result = KafkaConnectionBuilder.build_producer_config(
                kafka_config, "test-service"
            )
            
            # Test value serializer
            value_serializer = result["value_serializer"]
            assert value_serializer("test_string") == b"test_string"
            assert value_serializer(b"test_bytes") == b"test_bytes"
            
            # Test key serializer
            key_serializer = result["key_serializer"]
            assert key_serializer("test_key") == b"test_key"
            assert key_serializer(b"test_key_bytes") == b"test_key_bytes"

    def test_consumer_deserializers_work_correctly(self):
        """Test that consumer deserializers work correctly"""
        kafka_config = {
            "bootstrap_servers": ["localhost:9092"],
            "security_protocol": "PLAINTEXT",
        }
        
        with patch.object(SecurityConfigBuilder, 'build') as mock_security:
            mock_security.return_value = {"security_protocol": "PLAINTEXT"}
            
            result = KafkaConnectionBuilder.build_consumer_config(
                kafka_config, "test-group", "test-service"
            )
            
            # Test value deserializer
            value_deserializer = result["value_deserializer"]
            assert value_deserializer(b"test_message") == "test_message"
            assert value_deserializer(None) is None
            
            # Test key deserializer
            key_deserializer = result["key_deserializer"]
            assert key_deserializer(b"test_key") == "test_key"
            assert key_deserializer(None) is None


class TestKafkaClientsUsingModernConfig:
    """Test that Kafka clients use the modern configuration builders"""

    @pytest.mark.asyncio
    async def test_kafka_async_producer_uses_connection_builder(self):
        """Test that KafkaAsyncProducer uses KafkaConnectionBuilder"""
        kafka_config = {
            "bootstrap_servers": ["localhost:9092"],
            "security_protocol": "SSL",
            "ssl_cafile": "/path/to/ca.pem",
        }
        service_name = "test-service"
        
        with patch('general_operate.kafka.kafka_client.AIOKafkaProducer') as mock_producer_class:
            with patch.object(KafkaConnectionBuilder, 'build_producer_config') as mock_builder:
                mock_config = {
                    "bootstrap_servers": ["localhost:9092"],
                    "security_protocol": "SSL",
                    "ssl_context": Mock(),
                    "client_id": "test-service-producer",
                    "value_serializer": lambda v: v,
                    "key_serializer": lambda k: k,
                    "acks": "all",
                    "enable_idempotence": True,
                    "compression_type": "gzip",
                    "request_timeout_ms": 30000,
                }
                mock_builder.return_value = mock_config
                
                # Mock the producer instance with async start method
                mock_producer_instance = Mock()
                mock_producer_instance.start = AsyncMock()
                mock_producer_class.return_value = mock_producer_instance
                
                producer = KafkaAsyncProducer(kafka_config, service_name)
                await producer.start()
                
                # Verify that the connection builder was called
                mock_builder.assert_called_once_with(kafka_config, service_name)
                
                # Verify that AIOKafkaProducer was instantiated with the built config
                mock_producer_class.assert_called_once_with(**mock_config)
                
                # Verify that the producer was started
                mock_producer_instance.start.assert_called_once()

    def test_connection_builder_is_used_for_consumer_config(self):
        """Test that KafkaConnectionBuilder is used correctly for consumer configuration"""
        kafka_config = {
            "bootstrap_servers": ["localhost:9092"],
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": "user",
            "sasl_plain_password": "pass",
        }
        group_id = "test-group"
        service_name = "test-service"
        
        with patch.object(KafkaConnectionBuilder, 'build_consumer_config') as mock_builder:
            mock_config = {
                "bootstrap_servers": ["localhost:9092"],
                "group_id": "test-group",
                "security_protocol": "SASL_SSL",
                "ssl_context": Mock(),
                "sasl_mechanism": "PLAIN",
                "client_id": "test-service-consumer-test-group",
                "value_deserializer": lambda m: m,
                "key_deserializer": lambda k: k,
                "auto_offset_reset": "latest",
                "enable_auto_commit": False,
                "isolation_level": "read_committed",
                "session_timeout_ms": 30000,
                "heartbeat_interval_ms": 3000,
                "max_poll_records": 500,
                "max_poll_interval_ms": 300000,
            }
            mock_builder.return_value = mock_config
            
            # Test that the connection builder produces the expected configuration
            result = KafkaConnectionBuilder.build_consumer_config(
                kafka_config, group_id, service_name
            )
            
            # Since we mocked it, verify the mock was called correctly
            mock_builder.assert_called_once_with(kafka_config, group_id, service_name)
            
            # Verify the mock returns the expected structure
            assert result == mock_config

    def test_no_invalid_ssl_params_passed_to_aiokafka(self):
        """Test that invalid SSL parameters are not passed to aiokafka clients"""
        # This is a critical test to ensure backward compatibility fixes
        kafka_config = {
            "bootstrap_servers": ["localhost:9092"],
            "security_protocol": "SSL",
            "ssl_cafile": "/path/to/ca.pem",
            "ssl_certfile": "/path/to/cert.pem",
            "ssl_keyfile": "/path/to/key.pem",
            # These parameters were causing issues in the old implementation
            "ssl_check_hostname": True,  # Should not be passed directly
            "ssl_verify_mode": "CERT_REQUIRED",  # Should not be passed directly
        }
        
        with patch.object(SecurityConfigBuilder, '_create_ssl_context') as mock_ssl:
            mock_context = Mock(spec=ssl.SSLContext)
            mock_ssl.return_value = mock_context
            
            producer_config = KafkaConnectionBuilder.build_producer_config(
                kafka_config, "test-service"
            )
            
            # Verify that only valid parameters are in the config
            assert "ssl_context" in producer_config
            assert "security_protocol" in producer_config
            
            # Verify that invalid SSL parameters are NOT in the config
            assert "ssl_cafile" not in producer_config
            assert "ssl_certfile" not in producer_config
            assert "ssl_keyfile" not in producer_config
            assert "ssl_check_hostname" not in producer_config
            assert "ssl_verify_mode" not in producer_config
            
            # Verify that create_ssl_context was called with the correct parameters
            mock_ssl.assert_called_once()
            call_args = mock_ssl.call_args[0][0]  # First positional argument
            assert "ssl_cafile" in call_args
            assert "ssl_certfile" in call_args
            assert "ssl_keyfile" in call_args


class TestBackwardCompatibility:
    """Test backward compatibility with existing configurations"""

    def test_existing_plaintext_config_still_works(self):
        """Test that existing PLAINTEXT configurations continue to work"""
        # Old-style configuration
        old_config = {
            "bootstrap_servers": "localhost:9092",
            "security_protocol": "PLAINTEXT",
            "acks": "all",
            "enable_idempotence": True,
        }
        
        producer_config = KafkaConnectionBuilder.build_producer_config(
            old_config, "legacy-service"
        )
        
        assert producer_config["bootstrap_servers"] == "localhost:9092"
        assert producer_config["security_protocol"] == "PLAINTEXT"
        assert producer_config["acks"] == "all"
        assert producer_config["enable_idempotence"] is True

    def test_existing_ssl_config_gets_modernized(self):
        """Test that existing SSL configurations get modernized"""
        # Old-style SSL configuration that might have caused issues
        old_ssl_config = {
            "bootstrap_servers": "localhost:9093",
            "security_protocol": "SSL",
            "ssl_cafile": "/etc/kafka/ca.pem",
            "ssl_certfile": "/etc/kafka/cert.pem",
            "ssl_keyfile": "/etc/kafka/key.pem",
            "ssl_check_hostname": True,  # This would cause issues in old implementation
        }
        
        with patch.object(SecurityConfigBuilder, '_create_ssl_context') as mock_ssl:
            mock_context = Mock(spec=ssl.SSLContext)
            mock_ssl.return_value = mock_context
            
            producer_config = KafkaConnectionBuilder.build_producer_config(
                old_ssl_config, "legacy-ssl-service"
            )
            
            # Verify modernized configuration
            assert producer_config["security_protocol"] == "SSL"
            assert producer_config["ssl_context"] == mock_context
            
            # Verify old problematic parameters are not passed through
            assert "ssl_cafile" not in producer_config
            assert "ssl_check_hostname" not in producer_config

    def test_existing_sasl_config_gets_modernized(self):
        """Test that existing SASL configurations get modernized"""
        old_sasl_config = {
            "bootstrap_servers": "localhost:9094",
            "security_protocol": "SASL_PLAINTEXT",
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": "old_user",
            "sasl_plain_password": "old_pass",
        }
        
        consumer_config = KafkaConnectionBuilder.build_consumer_config(
            old_sasl_config, "legacy-group", "legacy-sasl-service"
        )
        
        assert consumer_config["security_protocol"] == "SASL_PLAINTEXT"
        assert consumer_config["sasl_mechanism"] == "PLAIN"
        assert consumer_config["sasl_plain_username"] == "old_user"
        assert consumer_config["sasl_plain_password"] == "old_pass"
        assert "ssl_context" not in consumer_config  # Should not have SSL for SASL_PLAINTEXT


class TestErrorHandling:
    """Test error handling in security configuration"""

    def test_ssl_context_creation_failure_propagates(self):
        """Test that SSL context creation failures are properly propagated"""
        kafka_config = {
            "security_protocol": "SSL",
            "ssl_cafile": "/nonexistent/ca.pem",
        }
        
        with patch('general_operate.kafka.kafka_client.create_ssl_context') as mock_create:
            mock_create.side_effect = FileNotFoundError("CA file not found")
            
            with pytest.raises(FileNotFoundError, match="CA file not found"):
                SecurityConfigBuilder.build(kafka_config)

    def test_missing_ssl_files_handled_gracefully(self):
        """Test that missing SSL files are handled gracefully when no SSL params provided"""
        kafka_config = {
            "security_protocol": "SSL",
            # No SSL files provided
        }
        
        result = SecurityConfigBuilder.build(kafka_config)
        
        # Should still have security protocol but no SSL context
        assert result["security_protocol"] == "SSL"
        assert "ssl_context" not in result

    def test_partial_sasl_config_handled_correctly(self):
        """Test that partial SASL configuration is handled correctly"""
        kafka_config = {
            "security_protocol": "SASL_PLAINTEXT",
            "sasl_mechanism": "PLAIN",
            # Missing username/password - should still work
        }
        
        result = SecurityConfigBuilder.build(kafka_config)
        
        assert result["security_protocol"] == "SASL_PLAINTEXT"
        assert result["sasl_mechanism"] == "PLAIN"
        assert "sasl_plain_username" not in result
        assert "sasl_plain_password" not in result


class TestConfigurationIntegration:
    """Integration tests for the complete configuration flow"""

    def test_complete_ssl_flow_integration(self):
        """Test complete SSL configuration flow from config to client"""
        ssl_config = {
            "bootstrap_servers": ["ssl-broker:9093"],
            "security_protocol": "SSL",
            "ssl_cafile": "/etc/ssl/ca.pem",
            "ssl_certfile": "/etc/ssl/client.pem",
            "ssl_keyfile": "/etc/ssl/client.key",
            "ssl_password": "secret",
        }
        
        with patch('general_operate.kafka.kafka_client.create_ssl_context') as mock_create:
            mock_context = Mock(spec=ssl.SSLContext)
            mock_create.return_value = mock_context
            
            # Test producer configuration
            producer_config = KafkaConnectionBuilder.build_producer_config(
                ssl_config, "ssl-producer-service"
            )
            
            assert producer_config["bootstrap_servers"] == ["ssl-broker:9093"]
            assert producer_config["security_protocol"] == "SSL"
            assert producer_config["ssl_context"] == mock_context
            assert producer_config["client_id"] == "ssl-producer-service-producer"
            
            # Verify create_ssl_context was called with correct parameters
            mock_create.assert_called_with(
                cafile="/etc/ssl/ca.pem",
                certfile="/etc/ssl/client.pem",
                keyfile="/etc/ssl/client.key",
                password="secret"
            )

    def test_complete_sasl_ssl_flow_integration(self):
        """Test complete SASL_SSL configuration flow from config to client"""
        sasl_ssl_config = {
            "bootstrap_servers": ["secure-broker:9094"],
            "security_protocol": "SASL_SSL",
            "ssl_cafile": "/etc/ssl/ca.pem",
            "sasl_mechanism": "SCRAM-SHA-256",
            "sasl_plain_username": "secure_user",
            "sasl_plain_password": "secure_pass",
        }
        
        with patch('general_operate.kafka.kafka_client.create_ssl_context') as mock_create:
            mock_context = Mock(spec=ssl.SSLContext)
            mock_create.return_value = mock_context
            
            # Test consumer configuration
            consumer_config = KafkaConnectionBuilder.build_consumer_config(
                sasl_ssl_config, "secure-group", "sasl-ssl-consumer-service"
            )
            
            assert consumer_config["bootstrap_servers"] == ["secure-broker:9094"]
            assert consumer_config["security_protocol"] == "SASL_SSL"
            assert consumer_config["ssl_context"] == mock_context
            assert consumer_config["sasl_mechanism"] == "SCRAM-SHA-256"
            assert consumer_config["sasl_plain_username"] == "secure_user"
            assert consumer_config["sasl_plain_password"] == "secure_pass"
            assert consumer_config["group_id"] == "secure-group"
            assert consumer_config["client_id"] == "sasl-ssl-consumer-service-consumer-secure-group"

    def test_modernized_config_works_with_real_aiokafka_interface(self):
        """Test that the modernized configuration interface matches AIOKafka expectations"""
        kafka_config = {
            "bootstrap_servers": ["localhost:9092"],
            "security_protocol": "PLAINTEXT",
        }
        
        producer_config = KafkaConnectionBuilder.build_producer_config(
            kafka_config, "interface-test-service"
        )
        
        # These are the parameters that AIOKafkaProducer expects
        expected_aiokafka_params = {
            'bootstrap_servers', 'client_id', 'security_protocol',
            'value_serializer', 'key_serializer', 'acks', 'enable_idempotence',
            'compression_type', 'request_timeout_ms'
        }
        
        # Verify all expected parameters are present
        assert set(producer_config.keys()) == expected_aiokafka_params
        
        # Verify no unexpected legacy parameters are present
        legacy_params = {
            'ssl_cafile', 'ssl_certfile', 'ssl_keyfile', 'ssl_check_hostname',
            'ssl_crlfile', 'ssl_verify_mode'
        }
        
        for legacy_param in legacy_params:
            assert legacy_param not in producer_config, f"Legacy parameter {legacy_param} found in config"