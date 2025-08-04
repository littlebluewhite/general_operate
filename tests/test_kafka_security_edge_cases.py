"""
Edge case and error handling tests for modernized Kafka security configuration.

This module tests various edge cases, error conditions, and boundary scenarios
for the SecurityConfigBuilder and KafkaConnectionBuilder classes.
"""

import ssl
import pytest
from unittest.mock import Mock, patch, call
from typing import Any

from general_operate.kafka.kafka_client import (
    SecurityConfigBuilder,
    KafkaConnectionBuilder,
)


class TestSecurityConfigBuilderEdgeCases:
    """Test edge cases and error conditions in SecurityConfigBuilder"""

    def test_empty_kafka_config(self):
        """Test handling of empty kafka configuration"""
        empty_config = {}
        
        result = SecurityConfigBuilder.build(empty_config)
        
        # Should default to PLAINTEXT when no protocol specified
        assert result == {"security_protocol": "PLAINTEXT"}

    def test_none_values_in_ssl_config(self):
        """Test handling of None values in SSL configuration"""
        config_with_nones = {
            "security_protocol": "SSL",
            "ssl_cafile": None,
            "ssl_certfile": "/path/to/cert.pem",
            "ssl_keyfile": None,
            "ssl_password": None,
        }
        
        with patch('general_operate.kafka.kafka_client.create_ssl_context') as mock_create:
            mock_context = Mock(spec=ssl.SSLContext)
            mock_create.return_value = mock_context
            
            result = SecurityConfigBuilder.build(config_with_nones)
            
            assert result["security_protocol"] == "SSL"
            assert result["ssl_context"] == mock_context
            
            # Verify create_ssl_context was called with only non-None values
            mock_create.assert_called_once_with(certfile="/path/to/cert.pem")

    def test_empty_string_values_in_ssl_config(self):
        """Test handling of empty string values in SSL configuration"""
        config_with_empty_strings = {
            "security_protocol": "SSL",
            "ssl_cafile": "",
            "ssl_certfile": "/path/to/cert.pem",
            "ssl_keyfile": "",
            "ssl_password": "",
        }
        
        # Temporarily override the autouse mock to test the actual implementation
        with patch('general_operate.kafka.kafka_client.create_ssl_context') as mock_create:
            # Only SSL certfile is non-empty, so SSL context should be created
            mock_context = Mock(spec=ssl.SSLContext)
            mock_create.return_value = mock_context
            
            result = SecurityConfigBuilder._create_ssl_context(config_with_empty_strings)
            
            # Should create SSL context since certfile is provided (empty strings are passed through)
            assert result == mock_context
            mock_create.assert_called_once_with(
                cafile="", 
                certfile="/path/to/cert.pem", 
                keyfile="", 
                password=""
            )

    def test_ssl_context_creation_with_various_exceptions(self):
        """Test SSL context creation with different types of exceptions"""
        ssl_config = {
            "security_protocol": "SSL",
            "ssl_cafile": "/path/to/ca.pem",
        }
        
        exceptions_to_test = [
            FileNotFoundError("Certificate file not found"),
            PermissionError("Permission denied accessing certificate"),
            ssl.SSLError("SSL configuration error"),
            ValueError("Invalid certificate format"),
            Exception("Unexpected error during SSL context creation"),
        ]
        
        for exception in exceptions_to_test:
            with patch('general_operate.kafka.kafka_client.create_ssl_context') as mock_create:
                mock_create.side_effect = exception
                
                with pytest.raises(type(exception)):
                    SecurityConfigBuilder.build(ssl_config)

    def test_case_insensitive_security_protocol(self):
        """Test that security protocol handling is case-sensitive (as it should be)"""
        configs_to_test = [
            {"security_protocol": "ssl"},  # lowercase
            {"security_protocol": "SSL"},  # uppercase
            {"security_protocol": "Ssl"},  # mixed case
        ]
        
        for config in configs_to_test:
            result = SecurityConfigBuilder.build(config)
            # Should preserve the exact case as provided
            assert result["security_protocol"] == config["security_protocol"]

    def test_invalid_security_protocol(self):
        """Test handling of invalid security protocol values"""
        invalid_config = {
            "security_protocol": "INVALID_PROTOCOL",
        }
        
        result = SecurityConfigBuilder.build(invalid_config)
        
        # Should still include the invalid protocol (let aiokafka handle validation)
        assert result["security_protocol"] == "INVALID_PROTOCOL"
        # Should not include SSL or SASL configuration
        assert "ssl_context" not in result
        assert "sasl_mechanism" not in result

    def test_sasl_config_with_partial_credentials(self):
        """Test SASL configuration with partial credentials"""
        partial_sasl_configs = [
            {
                "security_protocol": "SASL_PLAINTEXT",
                "sasl_mechanism": "PLAIN",
                "sasl_plain_username": "user",
                # Missing password
            },
            {
                "security_protocol": "SASL_PLAINTEXT", 
                "sasl_mechanism": "PLAIN",
                "sasl_plain_password": "pass",
                # Missing username
            },
            {
                "security_protocol": "SASL_PLAINTEXT",
                "sasl_plain_username": "user",
                "sasl_plain_password": "pass",
                # Missing mechanism
            },
        ]
        
        for config in partial_sasl_configs:
            result = SecurityConfigBuilder.build(config)
            
            assert result["security_protocol"] == "SASL_PLAINTEXT"
            
            # Should include whatever SASL parameters are provided
            for key, value in config.items():
                if key.startswith("sasl_"):
                    assert result[key] == value

    def test_sasl_config_with_unsupported_parameters(self):
        """Test SASL configuration with unsupported parameters"""
        config_with_unsupported = {
            "security_protocol": "SASL_PLAINTEXT",
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": "user",
            "sasl_plain_password": "pass",
            # These should be filtered out
            "sasl_unsupported_param": "value",
            "sasl_custom_setting": "another_value",
        }
        
        result = SecurityConfigBuilder.build(config_with_unsupported)
        
        # Should include valid SASL parameters
        assert result["sasl_mechanism"] == "PLAIN"
        assert result["sasl_plain_username"] == "user"
        assert result["sasl_plain_password"] == "pass"
        
        # Should exclude unsupported parameters
        assert "sasl_unsupported_param" not in result
        assert "sasl_custom_setting" not in result

    def test_ssl_and_sasl_with_conflicting_protocols(self):
        """Test configuration that specifies SSL params but SASL protocol"""
        conflicting_config = {
            "security_protocol": "SASL_PLAINTEXT",  # No SSL expected
            "ssl_cafile": "/path/to/ca.pem",  # SSL params present
            "ssl_certfile": "/path/to/cert.pem",
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": "user",
            "sasl_plain_password": "pass",
        }
        
        result = SecurityConfigBuilder.build(conflicting_config)
        
        # Should follow the protocol specification (SASL_PLAINTEXT = no SSL)
        assert result["security_protocol"] == "SASL_PLAINTEXT"
        assert "ssl_context" not in result  # No SSL for SASL_PLAINTEXT
        assert result["sasl_mechanism"] == "PLAIN"

    def test_concurrent_ssl_context_creation(self):
        """Test SSL context creation with concurrent access patterns"""
        ssl_config = {
            "security_protocol": "SSL",
            "ssl_cafile": "/path/to/ca.pem",
        }
        
        with patch('general_operate.kafka.kafka_client.create_ssl_context') as mock_create:
            mock_context1 = Mock(spec=ssl.SSLContext)
            mock_context2 = Mock(spec=ssl.SSLContext)
            mock_create.side_effect = [mock_context1, mock_context2]
            
            # Simulate concurrent calls
            result1 = SecurityConfigBuilder.build(ssl_config)
            result2 = SecurityConfigBuilder.build(ssl_config)
            
            # Should create separate SSL contexts for each call
            assert result1["ssl_context"] == mock_context1
            assert result2["ssl_context"] == mock_context2
            assert mock_create.call_count == 2


class TestKafkaConnectionBuilderEdgeCases:
    """Test edge cases and error conditions in KafkaConnectionBuilder"""

    def test_build_producer_config_with_minimal_input(self):
        """Test building producer config with minimal required input"""
        minimal_config = {
            "bootstrap_servers": "localhost:9092",
        }
        
        with patch.object(SecurityConfigBuilder, 'build') as mock_security:
            mock_security.return_value = {"security_protocol": "PLAINTEXT"}
            
            result = KafkaConnectionBuilder.build_producer_config(
                minimal_config, "minimal-service"
            )
            
            # Should have all required producer parameters with defaults
            assert "bootstrap_servers" in result
            assert "client_id" in result
            assert "acks" in result
            assert "enable_idempotence" in result
            assert "compression_type" in result
            assert "request_timeout_ms" in result
            assert "value_serializer" in result
            assert "key_serializer" in result

    def test_build_consumer_config_with_minimal_input(self):
        """Test building consumer config with minimal required input"""
        minimal_config = {
            "bootstrap_servers": "localhost:9092",
        }
        
        with patch.object(SecurityConfigBuilder, 'build') as mock_security:
            mock_security.return_value = {"security_protocol": "PLAINTEXT"}
            
            result = KafkaConnectionBuilder.build_consumer_config(
                minimal_config, "minimal-group", "minimal-service"
            )
            
            # Should have all required consumer parameters with defaults
            required_params = [
                "bootstrap_servers", "group_id", "client_id", "auto_offset_reset",
                "enable_auto_commit", "isolation_level", "session_timeout_ms",
                "heartbeat_interval_ms", "max_poll_records", "max_poll_interval_ms",
                "value_deserializer", "key_deserializer"
            ]
            
            for param in required_params:
                assert param in result, f"Required parameter {param} missing from consumer config"

    def test_serializer_deserializer_edge_cases(self):
        """Test serializer and deserializer behavior with edge cases"""
        kafka_config = {
            "bootstrap_servers": ["localhost:9092"],
            "security_protocol": "PLAINTEXT",
        }
        
        with patch.object(SecurityConfigBuilder, 'build') as mock_security:
            mock_security.return_value = {"security_protocol": "PLAINTEXT"}
            
            # Test producer serializers
            producer_config = KafkaConnectionBuilder.build_producer_config(
                kafka_config, "test-service"
            )
            
            value_serializer = producer_config["value_serializer"]
            key_serializer = producer_config["key_serializer"]
            
            # Test various input types for value serializer
            assert value_serializer("string") == b"string"
            assert value_serializer(b"bytes") == b"bytes"
            assert value_serializer("") == b""
            assert value_serializer(b"") == b""
            
            # Test various input types for key serializer  
            assert key_serializer("key") == b"key"
            assert key_serializer(b"key_bytes") == b"key_bytes"
            assert key_serializer("") == b""
            
            # Test consumer deserializers
            consumer_config = KafkaConnectionBuilder.build_consumer_config(
                kafka_config, "test-group", "test-service"
            )
            
            value_deserializer = consumer_config["value_deserializer"]
            key_deserializer = consumer_config["key_deserializer"]
            
            # Test various input types for value deserializer
            assert value_deserializer(b"message") == "message"
            assert value_deserializer(b"") is None  # Empty bytes are treated as None
            assert value_deserializer(None) is None
            
            # Test various input types for key deserializer
            assert key_deserializer(b"key") == "key"
            assert key_deserializer(b"") is None  # Empty bytes are treated as None
            assert key_deserializer(None) is None

    def test_client_id_generation_with_special_characters(self):
        """Test client ID generation with special characters in service names"""
        test_cases = [
            ("service-with-dashes", "group-with-dashes"),
            ("service_with_underscores", "group_with_underscores"),
            ("service.with.dots", "group.with.dots"),
            ("service with spaces", "group with spaces"),
            ("service@domain.com", "group@domain.com"),
            ("service123", "group456"),
        ]
        
        kafka_config = {
            "bootstrap_servers": ["localhost:9092"],
            "security_protocol": "PLAINTEXT",
        }
        
        with patch.object(SecurityConfigBuilder, 'build') as mock_security:
            mock_security.return_value = {"security_protocol": "PLAINTEXT"}
            
            for service_name, group_id in test_cases:
                producer_config = KafkaConnectionBuilder.build_producer_config(
                    kafka_config, service_name
                )
                
                consumer_config = KafkaConnectionBuilder.build_consumer_config(
                    kafka_config, group_id, service_name
                )
                
                # Verify client ID generation
                expected_producer_client_id = f"{service_name}-producer"
                expected_consumer_client_id = f"{service_name}-consumer-{group_id}"
                
                assert producer_config["client_id"] == expected_producer_client_id
                assert consumer_config["client_id"] == expected_consumer_client_id

    def test_bootstrap_servers_normalization(self):
        """Test that bootstrap_servers are handled correctly in different formats"""
        test_configs = [
            {
                "bootstrap_servers": "localhost:9092",  # Single string
                "security_protocol": "PLAINTEXT",
            },
            {
                "bootstrap_servers": ["localhost:9092"],  # Single item list
                "security_protocol": "PLAINTEXT",
            },
            {
                "bootstrap_servers": ["broker1:9092", "broker2:9092"],  # Multiple brokers
                "security_protocol": "PLAINTEXT",
            },
        ]
        
        with patch.object(SecurityConfigBuilder, 'build') as mock_security:
            mock_security.return_value = {"security_protocol": "PLAINTEXT"}
            
            for config in test_configs:
                producer_config = KafkaConnectionBuilder.build_producer_config(
                    config, "test-service"
                )
                
                consumer_config = KafkaConnectionBuilder.build_consumer_config(
                    config, "test-group", "test-service"
                )
                
                # Verify bootstrap_servers are preserved as-is
                assert producer_config["bootstrap_servers"] == config["bootstrap_servers"]
                assert consumer_config["bootstrap_servers"] == config["bootstrap_servers"]

    def test_config_parameter_override_precedence(self):
        """Test that custom configuration parameters override defaults"""
        custom_config = {
            "bootstrap_servers": ["custom-broker:9092"],
            "security_protocol": "PLAINTEXT",
            # Override default values
            "acks": "1",  # Default is "all"
            "enable_idempotence": False,  # Default is True
            "compression_type": "snappy",  # Default is "gzip"
            "request_timeout_ms": 60000,  # Default is 30000
            "auto_offset_reset": "earliest",  # Default is "latest"
            "session_timeout_ms": 60000,  # Default is 30000
            "heartbeat_interval_ms": 5000,  # Default is 3000
            "max_poll_records": 1000,  # Default is 500
            "max_poll_interval_ms": 600000,  # Default is 300000
        }
        
        with patch.object(SecurityConfigBuilder, 'build') as mock_security:
            mock_security.return_value = {"security_protocol": "PLAINTEXT"}
            
            producer_config = KafkaConnectionBuilder.build_producer_config(
                custom_config, "custom-service"
            )
            
            consumer_config = KafkaConnectionBuilder.build_consumer_config(
                custom_config, "custom-group", "custom-service"
            )
            
            # Verify custom values override defaults in producer config
            assert producer_config["acks"] == "1"
            assert producer_config["enable_idempotence"] is False
            assert producer_config["compression_type"] == "snappy"
            assert producer_config["request_timeout_ms"] == 60000
            
            # Verify custom values override defaults in consumer config
            assert consumer_config["auto_offset_reset"] == "earliest"
            assert consumer_config["session_timeout_ms"] == 60000
            assert consumer_config["heartbeat_interval_ms"] == 5000
            assert consumer_config["max_poll_records"] == 1000
            assert consumer_config["max_poll_interval_ms"] == 600000

    def test_security_config_integration_failure_handling(self):
        """Test handling of SecurityConfigBuilder failures"""
        kafka_config = {
            "bootstrap_servers": ["localhost:9092"],
            "security_protocol": "SSL",
            "ssl_cafile": "/invalid/path/ca.pem",
        }
        
        with patch.object(SecurityConfigBuilder, 'build') as mock_security:
            mock_security.side_effect = FileNotFoundError("SSL certificate not found")
            
            with pytest.raises(FileNotFoundError):
                KafkaConnectionBuilder.build_producer_config(
                    kafka_config, "failing-service"
                )
            
            with pytest.raises(FileNotFoundError):
                KafkaConnectionBuilder.build_consumer_config(
                    kafka_config, "failing-group", "failing-service"
                )

    def test_config_immutability(self):
        """Test that original config is not modified by builders"""
        original_config = {
            "bootstrap_servers": ["localhost:9092"],
            "security_protocol": "SSL",
            "ssl_cafile": "/path/to/ca.pem",
            "custom_param": "custom_value",
        }
        
        # Create a copy to verify it doesn't change
        original_config_copy = original_config.copy()
        
        with patch.object(SecurityConfigBuilder, 'build') as mock_security:
            mock_security.return_value = {
                "security_protocol": "SSL",
                "ssl_context": Mock()
            }
            
            KafkaConnectionBuilder.build_producer_config(
                original_config, "immutable-service"
            )
            
            KafkaConnectionBuilder.build_consumer_config(
                original_config, "immutable-group", "immutable-service"
            )
            
            # Verify original config was not modified
            assert original_config == original_config_copy