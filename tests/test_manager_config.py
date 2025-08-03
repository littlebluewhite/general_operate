"""
Unit tests for the Manager Configuration module.

Tests cover:
- RetryConfig validation and functionality
- ConsumerGroupConfig validation
- ServiceConfig validation  
- TopicConfig validation
- Configuration inheritance and defaults
- Validation edge cases and error handling
- Configuration serialization/deserialization
- Integration with Pydantic validation
"""

import json
from typing import Any, Dict

import pytest
from pydantic import ValidationError

from general_operate.kafka.manager_config import (
    ConsumerGroupConfig,
    ServiceConfig,
    TopicConfig,
)
from general_operate.kafka.config import RetryConfig


class TestRetryConfig:
    """Test RetryConfig validation and functionality."""

    def test_retry_config_defaults(self):
        """Test RetryConfig default values."""
        config = RetryConfig()
        
        assert config.max_retries == 3
        assert config.base_delay == 1.0
        assert config.max_delay == 60.0
        assert config.exponential_base == 2.0
        assert config.jitter is True

    def test_retry_config_custom_values(self):
        """Test RetryConfig with custom values."""
        config = RetryConfig(
            max_retries=5,
            base_delay=0.5,
            max_delay=30.0,
            exponential_base=1.5,
            jitter=False,
        )
        
        assert config.max_retries == 5
        assert config.base_delay == 0.5
        assert config.max_delay == 30.0
        assert config.exponential_base == 1.5
        assert config.jitter is False

    def test_retry_config_validation_constraints(self):
        """Test RetryConfig validation constraints."""
        # Valid edge case: zero retries
        config = RetryConfig(max_retries=0)
        assert config.max_retries == 0
        
        # Valid edge case: minimum delays
        config = RetryConfig(base_delay=0.1, max_delay=0.1)
        assert config.base_delay == 0.1
        assert config.max_delay == 0.1
        
        # Valid edge case: minimum exponential base
        config = RetryConfig(exponential_base=1.1)
        assert config.exponential_base == 1.1

    def test_retry_config_invalid_values(self):
        """Test RetryConfig validation with invalid values."""
        # Negative max_retries should fail
        with pytest.raises(ValidationError):
            RetryConfig(max_retries=-1)
        
        # Zero or negative base_delay should fail
        with pytest.raises(ValidationError):
            RetryConfig(base_delay=0.0)
        
        with pytest.raises(ValidationError):
            RetryConfig(base_delay=-1.0)
        
        # Zero or negative max_delay should fail
        with pytest.raises(ValidationError):
            RetryConfig(max_delay=0.0)
        
        with pytest.raises(ValidationError):
            RetryConfig(max_delay=-1.0)
        
        # Exponential base <= 1 should fail
        with pytest.raises(ValidationError):
            RetryConfig(exponential_base=1.0)
        
        with pytest.raises(ValidationError):
            RetryConfig(exponential_base=0.5)

    def test_retry_config_serialization(self):
        """Test RetryConfig serialization and deserialization."""
        config = RetryConfig(
            max_retries=5,
            base_delay=2.0,
            max_delay=120.0,
            exponential_base=3.0,
            jitter=False,
        )
        
        # Test dict conversion
        config_dict = config.model_dump()
        expected_dict = {
            "max_retries": 5,
            "base_delay": 2.0,
            "max_delay": 120.0,
            "exponential_base": 3.0,
            "jitter": False,
        }
        assert config_dict == expected_dict
        
        # Test reconstruction from dict
        new_config = RetryConfig(**config_dict)
        assert new_config.max_retries == config.max_retries
        assert new_config.base_delay == config.base_delay
        assert new_config.max_delay == config.max_delay
        assert new_config.exponential_base == config.exponential_base
        assert new_config.jitter == config.jitter

    def test_retry_config_json_serialization(self):
        """Test RetryConfig JSON serialization."""
        config = RetryConfig(
            max_retries=10,
            base_delay=0.5,
            max_delay=30.0,
            exponential_base=1.8,
            jitter=True,
        )
        
        # Test JSON serialization
        json_str = config.model_dump_json()
        config_dict = json.loads(json_str)
        
        assert config_dict["max_retries"] == 10
        assert config_dict["base_delay"] == 0.5
        assert config_dict["max_delay"] == 30.0
        assert config_dict["exponential_base"] == 1.8
        assert config_dict["jitter"] is True

    def test_retry_config_field_descriptions(self):
        """Test RetryConfig field descriptions are present."""
        schema = RetryConfig.model_json_schema()
        properties = schema["properties"]
        
        # Verify all fields have descriptions
        assert "description" in properties["max_retries"]
        assert "description" in properties["base_delay"]
        assert "description" in properties["max_delay"]
        assert "description" in properties["exponential_base"]
        assert "description" in properties["jitter"]
        
        # Check that descriptions contain expected keywords (in Chinese or English)
        assert "retries" in properties["max_retries"]["description"].lower() or "重試" in properties["max_retries"]["description"]
        assert "delay" in properties["base_delay"]["description"].lower() or "延遲" in properties["base_delay"]["description"]


class TestConsumerGroupConfig:
    """Test ConsumerGroupConfig validation and functionality."""

    def test_consumer_group_config_defaults(self):
        """Test ConsumerGroupConfig default values."""
        config = ConsumerGroupConfig(group_id="test-group")
        
        assert config.group_id == "test-group"
        assert config.auto_offset_reset == "latest"
        assert config.enable_auto_commit is True
        assert config.auto_commit_interval_ms == 5000
        assert config.session_timeout_ms == 30000
        assert config.max_poll_records == 500

    def test_consumer_group_config_custom_values(self):
        """Test ConsumerGroupConfig with custom values."""
        config = ConsumerGroupConfig(
            group_id="custom-group",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            auto_commit_interval_ms=1000,
            session_timeout_ms=60000,
            max_poll_records=1000,
        )
        
        assert config.group_id == "custom-group"
        assert config.auto_offset_reset == "earliest"
        assert config.enable_auto_commit is False
        assert config.auto_commit_interval_ms == 1000
        assert config.session_timeout_ms == 60000
        assert config.max_poll_records == 1000

    def test_consumer_group_config_validation(self):
        """Test ConsumerGroupConfig validation constraints."""
        # Valid auto_offset_reset values
        for reset_value in ["earliest", "latest", "none"]:
            config = ConsumerGroupConfig(
                group_id="test-group",
                auto_offset_reset=reset_value,
            )
            assert config.auto_offset_reset == reset_value

    def test_consumer_group_config_invalid_values(self):
        """Test ConsumerGroupConfig validation with invalid values."""
        # Missing required group_id
        with pytest.raises(ValidationError):
            ConsumerGroupConfig()
        
        # Empty group_id should still be valid (might be validated elsewhere)
        config = ConsumerGroupConfig(group_id="")
        assert config.group_id == ""
        
        # Negative values should fail
        with pytest.raises(ValidationError):
            ConsumerGroupConfig(
                group_id="test-group",
                auto_commit_interval_ms=-1,
            )
        
        with pytest.raises(ValidationError):
            ConsumerGroupConfig(
                group_id="test-group",
                session_timeout_ms=-1,
            )
        
        with pytest.raises(ValidationError):
            ConsumerGroupConfig(
                group_id="test-group",
                max_poll_records=-1,
            )

    def test_consumer_group_config_serialization(self):
        """Test ConsumerGroupConfig serialization."""
        config = ConsumerGroupConfig(
            group_id="serialization-test",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            auto_commit_interval_ms=2000,
            session_timeout_ms=45000,
            max_poll_records=750,
        )
        
        # Test dict conversion
        config_dict = config.model_dump()
        assert config_dict["group_id"] == "serialization-test"
        assert config_dict["auto_offset_reset"] == "earliest"
        assert config_dict["enable_auto_commit"] is False
        
        # Test reconstruction
        new_config = ConsumerGroupConfig(**config_dict)
        assert new_config.group_id == config.group_id
        assert new_config.auto_offset_reset == config.auto_offset_reset
        assert new_config.enable_auto_commit == config.enable_auto_commit


class TestServiceConfig:
    """Test ServiceConfig validation and functionality."""

    def test_service_config_defaults(self):
        """Test ServiceConfig default values."""
        config = ServiceConfig(service_name="test-service")
        
        assert config.service_name == "test-service"
        assert isinstance(config.retry_config, RetryConfig)
        assert config.enable_dlq is True
        assert config.dlq_topic_suffix == ".dlq"
        assert config.circuit_breaker_threshold == 5
        assert config.circuit_breaker_timeout == 30.0

    def test_service_config_custom_values(self):
        """Test ServiceConfig with custom values."""
        custom_retry = RetryConfig(max_retries=10, base_delay=2.0)
        
        config = ServiceConfig(
            service_name="custom-service",
            retry_config=custom_retry,
            enable_dlq=False,
            dlq_topic_suffix=".dead_letter",
            circuit_breaker_threshold=10,
            circuit_breaker_timeout=60.0,
        )
        
        assert config.service_name == "custom-service"
        assert config.retry_config == custom_retry
        assert config.enable_dlq is False
        assert config.dlq_topic_suffix == ".dead_letter"
        assert config.circuit_breaker_threshold == 10
        assert config.circuit_breaker_timeout == 60.0

    def test_service_config_validation(self):
        """Test ServiceConfig validation constraints."""
        # Positive circuit breaker values
        config = ServiceConfig(
            service_name="test-service",
            circuit_breaker_threshold=1,
            circuit_breaker_timeout=0.1,
        )
        assert config.circuit_breaker_threshold == 1
        assert config.circuit_breaker_timeout == 0.1

    def test_service_config_invalid_values(self):
        """Test ServiceConfig validation with invalid values."""
        # Missing required service_name
        with pytest.raises(ValidationError):
            ServiceConfig()
        
        # Zero circuit breaker threshold should fail
        with pytest.raises(ValidationError):
            ServiceConfig(
                service_name="test-service",
                circuit_breaker_threshold=0,
            )
        
        # Negative circuit breaker threshold should fail
        with pytest.raises(ValidationError):
            ServiceConfig(
                service_name="test-service",
                circuit_breaker_threshold=-1,
            )
        
        # Zero or negative timeout should fail
        with pytest.raises(ValidationError):
            ServiceConfig(
                service_name="test-service",
                circuit_breaker_timeout=0.0,
            )
        
        with pytest.raises(ValidationError):
            ServiceConfig(
                service_name="test-service",
                circuit_breaker_timeout=-1.0,
            )

    def test_service_config_nested_validation(self):
        """Test ServiceConfig with invalid nested RetryConfig."""
        # Invalid retry config should propagate validation error
        with pytest.raises(ValidationError):
            ServiceConfig(
                service_name="test-service",
                retry_config=RetryConfig(max_retries=-1),  # Invalid
            )

    def test_service_config_serialization(self):
        """Test ServiceConfig serialization."""
        custom_retry = RetryConfig(max_retries=7, base_delay=1.5)
        
        config = ServiceConfig(
            service_name="serialization-service",
            retry_config=custom_retry,
            enable_dlq=False,
            dlq_topic_suffix=".dlq_custom",
            circuit_breaker_threshold=15,
            circuit_breaker_timeout=45.0,
        )
        
        # Test dict conversion
        config_dict = config.model_dump()
        assert config_dict["service_name"] == "serialization-service"
        assert config_dict["enable_dlq"] is False
        assert config_dict["dlq_topic_suffix"] == ".dlq_custom"
        assert "retry_config" in config_dict
        
        # Test reconstruction
        new_config = ServiceConfig(**config_dict)
        assert new_config.service_name == config.service_name
        assert new_config.retry_config.max_retries == config.retry_config.max_retries
        assert new_config.enable_dlq == config.enable_dlq


class TestTopicConfig:
    """Test TopicConfig validation and functionality."""

    def test_topic_config_defaults(self):
        """Test TopicConfig default values."""
        config = TopicConfig(name="test-topic")
        
        assert config.name == "test-topic"
        assert config.partitions == 1
        assert config.replication_factor == 1
        assert config.config == {}

    def test_topic_config_custom_values(self):
        """Test TopicConfig with custom values."""
        custom_config = {
            "cleanup.policy": "compact",
            "retention.ms": "86400000",
            "compression.type": "snappy",
        }
        
        config = TopicConfig(
            name="custom-topic",
            partitions=6,
            replication_factor=3,
            config=custom_config,
        )
        
        assert config.name == "custom-topic"
        assert config.partitions == 6
        assert config.replication_factor == 3
        assert config.config == custom_config

    def test_topic_config_validation(self):
        """Test TopicConfig validation constraints."""
        # Minimum valid values
        config = TopicConfig(
            name="test-topic",
            partitions=1,
            replication_factor=1,
        )
        assert config.partitions == 1
        assert config.replication_factor == 1

    def test_topic_config_invalid_values(self):
        """Test TopicConfig validation with invalid values."""
        # Missing required name
        with pytest.raises(ValidationError):
            TopicConfig()
        
        # Zero partitions should fail
        with pytest.raises(ValidationError):
            TopicConfig(
                name="test-topic",
                partitions=0,
            )
        
        # Negative partitions should fail
        with pytest.raises(ValidationError):
            TopicConfig(
                name="test-topic",
                partitions=-1,
            )
        
        # Zero replication factor should fail
        with pytest.raises(ValidationError):
            TopicConfig(
                name="test-topic",
                replication_factor=0,
            )
        
        # Negative replication factor should fail
        with pytest.raises(ValidationError):
            TopicConfig(
                name="test-topic",
                replication_factor=-1,
            )

    def test_topic_config_serialization(self):
        """Test TopicConfig serialization."""
        custom_config = {
            "cleanup.policy": "delete",
            "retention.ms": "604800000",
            "segment.ms": "86400000",
        }
        
        config = TopicConfig(
            name="serialization-topic",
            partitions=12,
            replication_factor=2,
            config=custom_config,
        )
        
        # Test dict conversion
        config_dict = config.model_dump()
        assert config_dict["name"] == "serialization-topic"
        assert config_dict["partitions"] == 12
        assert config_dict["replication_factor"] == 2
        assert config_dict["config"] == custom_config
        
        # Test reconstruction
        new_config = TopicConfig(**config_dict)
        assert new_config.name == config.name
        assert new_config.partitions == config.partitions
        assert new_config.replication_factor == config.replication_factor
        assert new_config.config == config.config

    def test_topic_config_kafka_settings(self):
        """Test TopicConfig with various Kafka topic settings."""
        kafka_settings = {
            "cleanup.policy": "compact,delete",
            "compression.type": "lz4",
            "delete.retention.ms": "86400000",
            "file.delete.delay.ms": "60000",
            "flush.messages": "10000",
            "flush.ms": "1000",
            "index.interval.bytes": "4096",
            "max.compaction.lag.ms": "9223372036854775807",
            "max.message.bytes": "1000000",
            "message.timestamp.difference.max.ms": "9223372036854775807",
            "message.timestamp.type": "CreateTime",
            "min.cleanable.dirty.ratio": "0.5",
            "min.compaction.lag.ms": "0",
            "min.insync.replicas": "1",
            "preallocate": "false",
            "retention.bytes": "-1",
            "retention.ms": "604800000",
            "segment.bytes": "1073741824",
            "segment.index.bytes": "10485760",
            "segment.jitter.ms": "0",
            "segment.ms": "604800000",
            "unclean.leader.election.enable": "false",
        }
        
        config = TopicConfig(
            name="kafka-topic",
            partitions=8,
            replication_factor=3,
            config=kafka_settings,
        )
        
        assert config.name == "kafka-topic"
        assert config.config["cleanup.policy"] == "compact,delete"
        assert config.config["compression.type"] == "lz4"
        assert config.config["max.message.bytes"] == "1000000"


class TestConfigurationIntegration:
    """Test configuration integration and relationships."""

    def test_service_config_with_all_dependencies(self):
        """Test ServiceConfig with all nested configurations."""
        retry_config = RetryConfig(
            max_retries=5,
            base_delay=2.0,
            max_delay=60.0,
            exponential_base=2.5,
            jitter=True,
        )
        
        service_config = ServiceConfig(
            service_name="integration-service",
            retry_config=retry_config,
            enable_dlq=True,
            dlq_topic_suffix=".deadletter",
            circuit_breaker_threshold=8,
            circuit_breaker_timeout=45.0,
        )
        
        # Verify all configurations are properly nested
        assert service_config.service_name == "integration-service"
        assert service_config.retry_config.max_retries == 5
        assert service_config.retry_config.base_delay == 2.0
        assert service_config.enable_dlq is True
        assert service_config.dlq_topic_suffix == ".deadletter"

    def test_configuration_serialization_round_trip(self):
        """Test complete serialization and deserialization cycle."""
        # Create complex configuration
        retry_config = RetryConfig(
            max_retries=8,
            base_delay=1.5,
            max_delay=90.0,
            exponential_base=2.2,
            jitter=False,
        )
        
        service_config = ServiceConfig(
            service_name="round-trip-service",
            retry_config=retry_config,
            enable_dlq=False,
            dlq_topic_suffix=".failed",
            circuit_breaker_threshold=12,
            circuit_breaker_timeout=75.0,
        )
        
        consumer_config = ConsumerGroupConfig(
            group_id="round-trip-group",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            auto_commit_interval_ms=3000,
            session_timeout_ms=40000,
            max_poll_records=800,
        )
        
        topic_config = TopicConfig(
            name="round-trip-topic",
            partitions=4,
            replication_factor=2,
            config={"cleanup.policy": "delete", "retention.ms": "172800000"},
        )
        
        # Serialize all configurations
        service_dict = service_config.model_dump()
        consumer_dict = consumer_config.model_dump()
        topic_dict = topic_config.model_dump()
        
        # Convert to JSON and back
        service_json = json.dumps(service_dict)
        consumer_json = json.dumps(consumer_dict)
        topic_json = json.dumps(topic_dict)
        
        service_data = json.loads(service_json)
        consumer_data = json.loads(consumer_json)
        topic_data = json.loads(topic_json)
        
        # Reconstruct configurations
        new_service_config = ServiceConfig(**service_data)
        new_consumer_config = ConsumerGroupConfig(**consumer_data)
        new_topic_config = TopicConfig(**topic_data)
        
        # Verify all data is preserved
        assert new_service_config.service_name == service_config.service_name
        assert new_service_config.retry_config.max_retries == service_config.retry_config.max_retries
        assert new_service_config.enable_dlq == service_config.enable_dlq
        
        assert new_consumer_config.group_id == consumer_config.group_id
        assert new_consumer_config.auto_offset_reset == consumer_config.auto_offset_reset
        assert new_consumer_config.enable_auto_commit == consumer_config.enable_auto_commit
        
        assert new_topic_config.name == topic_config.name
        assert new_topic_config.partitions == topic_config.partitions
        assert new_topic_config.config == topic_config.config

    def test_configuration_validation_errors_propagation(self):
        """Test that validation errors are properly propagated."""
        # Test that invalid nested configuration causes parent validation to fail
        with pytest.raises(ValidationError) as exc_info:
            ServiceConfig(
                service_name="test-service",
                retry_config=RetryConfig(max_retries=-1),  # Invalid
            )
        
        # Verify error information is meaningful
        error = exc_info.value
        assert len(error.errors()) > 0

    def test_configuration_defaults_inheritance(self):
        """Test that configuration defaults are properly inherited."""
        # Test that creating ServiceConfig with minimal parameters
        # inherits proper defaults for RetryConfig
        service_config = ServiceConfig(service_name="minimal-service")
        
        # Should have default RetryConfig
        assert isinstance(service_config.retry_config, RetryConfig)
        assert service_config.retry_config.max_retries == 3  # Default
        assert service_config.retry_config.base_delay == 1.0  # Default
        assert service_config.enable_dlq is True  # Default
        assert service_config.dlq_topic_suffix == ".dlq"  # Default

    def test_configuration_field_validation_messages(self):
        """Test that field validation provides meaningful error messages."""
        # Test various validation scenarios and check error messages
        test_cases = [
            (lambda: RetryConfig(max_retries=-1), "max_retries"),
            (lambda: RetryConfig(base_delay=0), "base_delay"),
            (lambda: RetryConfig(exponential_base=1.0), "exponential_base"),
            (lambda: ConsumerGroupConfig(group_id="test", auto_commit_interval_ms=-1), "auto_commit_interval_ms"),
            (lambda: ServiceConfig(service_name="test", circuit_breaker_threshold=0), "circuit_breaker_threshold"),
            (lambda: TopicConfig(name="test", partitions=0), "partitions"),
        ]
        
        for test_func, expected_field in test_cases:
            with pytest.raises(ValidationError) as exc_info:
                test_func()
            
            # Verify that the error is related to the expected field
            errors = exc_info.value.errors()
            assert any(expected_field in str(error.get("loc", [])) for error in errors)

    def test_configuration_edge_cases(self):
        """Test configuration edge cases and boundary values."""
        # Test minimum valid values
        retry_config = RetryConfig(
            max_retries=0,  # Zero retries is valid
            base_delay=0.001,  # Very small delay
            max_delay=0.001,  # Very small max delay
            exponential_base=1.001,  # Minimum exponential base
            jitter=False,
        )
        assert retry_config.max_retries == 0
        
        # Test large valid values
        large_config = TopicConfig(
            name="large-topic",
            partitions=10000,  # Large number of partitions
            replication_factor=100,  # Large replication factor
        )
        assert large_config.partitions == 10000
        assert large_config.replication_factor == 100
        
        # Test empty string configurations where allowed
        empty_suffix_config = ServiceConfig(
            service_name="test-service",
            dlq_topic_suffix="",  # Empty suffix should be valid
        )
        assert empty_suffix_config.dlq_topic_suffix == ""