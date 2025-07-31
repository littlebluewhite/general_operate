#!/usr/bin/env python3
"""
Kafka Configuration Management Examples
Demonstrates the usage of the enhanced configuration system
Kafka配置管理示例，展示增強配置系統的使用方法
"""

import asyncio
import os
from pathlib import Path

from general_operate.kafka import (
    KafkaConfig,
    KafkaConfigFactory,
    KafkaClientFactory,
    create_kafka_config,
    TopicConfig,
    validate_kafka_config,
    validate_topic_config
)


async def example_basic_configuration():
    """Basic configuration example using defaults"""
    print("=== Basic Configuration Example ===")
    
    # Create basic configuration for development
    config = create_kafka_config(
        service_name="example-service",
        environment="development"
    )
    
    print(f"Service: {config.config.service_name}")
    print(f"Environment: {config.config.environment}")
    print(f"Bootstrap servers: {config.get_bootstrap_servers()}")
    print(f"Producer acks: {config.config.producer.acks}")
    print(f"Consumer group: {config.get_consumer_config()['group_id']}")
    print()


async def example_production_configuration():
    """Production configuration with security"""
    print("=== Production Configuration Example ===")
    
    # Set environment variables for production
    os.environ.update({
        "KAFKA_BOOTSTRAP_SERVERS": "kafka1.prod.com:9092,kafka2.prod.com:9092,kafka3.prod.com:9092",
        "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
        "KAFKA_SASL_MECHANISM": "PLAIN",
        "KAFKA_SASL_USERNAME": "prod-user",
        "KAFKA_SASL_PASSWORD": "prod-password",
        "KAFKA_SSL_CA_LOCATION": "/etc/ssl/ca-cert.pem"
    })
    
    config = create_kafka_config(
        service_name="production-service",
        environment="production"
    )
    
    print(f"Service: {config.config.service_name}")
    print(f"Bootstrap servers: {config.get_bootstrap_servers()}")
    print(f"Security protocol: {config.config.security.protocol}")
    print(f"SASL mechanism: {config.config.security.mechanism}")
    print(f"Producer idempotence: {config.config.producer.enable_idempotence}")
    print(f"Producer acks: {config.config.producer.acks}")
    print(f"Compression: {config.config.producer.compression_type}")
    print()


async def example_custom_configuration():
    """Custom configuration with overrides"""
    print("=== Custom Configuration Example ===")
    
    custom_config = {
        "bootstrap_servers": ["localhost:9093"],
        "service_type": "custom",
        "producer": {
            "acks": "all",
            "compression_type": "lz4",
            "batch_size": 32768
        },
        "consumer": {
            "max_poll_records": 100,
            "session_timeout_ms": 45000
        },
        "topics": [
            {
                "name": "custom-events",
                "partitions": 6,
                "replication_factor": 2,
                "config": {
                    "retention.ms": "604800000",  # 7 days
                    "compression.type": "lz4"
                }
            }
        ]
    }
    
    config = KafkaConfig(
        service_name="custom-service",
        environment="development",
        custom_config=custom_config
    )
    
    print(f"Service: {config.config.service_name}")
    print(f"Service type: {config.config.service_type}")
    print(f"Bootstrap servers: {config.get_bootstrap_servers()}")
    print(f"Producer batch size: {config.config.producer.batch_size}")
    print(f"Consumer max poll: {config.config.consumer.max_poll_records}")
    print(f"Topic configs: {len(config.get_topic_configs())} topics")
    for topic in config.get_topic_configs():
        print(f"  - {topic.name}: {topic.partitions} partitions")


async def example_factory_usage():
    """Using the enhanced factory with configuration"""
    print("=== Factory Usage Example ===")
    
    # Create configuration
    config = KafkaConfigFactory.create_development_config(
        service_name="factory-service",
        service_type="auth"
    )
    
    # Create components using the factory
    event_bus = KafkaClientFactory.create_event_bus(config=config)
    producer = KafkaClientFactory.create_async_producer(config=config)
    topic_manager = KafkaClientFactory.create_topic_manager(config=config)
    
    print(f"Created event bus for service: {config.config.service_name}")
    print(f"Event bus bootstrap servers: {event_bus.bootstrap_servers}")
    print(f"Producer service name: {producer.service_name}")
    print()
    
    # Create from environment directly
    consumer = KafkaClientFactory.create_from_environment(
        service_name="env-service",
        environment="development",
        component_type="consumer",
        topics=["test-topic"],
        group_id="test-group"
    )
    
    print(f"Created consumer from environment configuration")


async def example_environment_from_file():
    """Load configuration from YAML file"""
    print("=== Configuration from File Example ===")
    
    # Create a sample configuration file
    config_content = """
service_name: "file-service"
service_type: "notification"
environment: "development"
bootstrap_servers:
  - "localhost:9092"

security:
  protocol: "PLAINTEXT"

producer:
  acks: "1"
  compression_type: "gzip"
  batch_size: 16384
  linger_ms: 10

consumer:
  auto_offset_reset: "earliest"
  enable_auto_commit: false
  max_poll_records: 250

topics:
  - name: "notifications"
    partitions: 3
    replication_factor: 1
    config:
      retention.ms: "86400000"  # 1 day
      cleanup.policy: "delete"

circuit_breaker_enabled: true
monitoring_enabled: true
"""
    
    # Write to temporary file
    config_file = Path("temp_kafka_config.yaml")
    with open(config_file, 'w') as f:
        f.write(config_content)
    
    try:
        # Load configuration from file
        config = KafkaConfig(
            service_name="file-service",
            config_path=config_file
        )
        
        print(f"Loaded configuration from file: {config_file}")
        print(f"Service: {config.config.service_name}")
        print(f"Service type: {config.config.service_type}")
        print(f"Circuit breaker enabled: {config.config.circuit_breaker_enabled}")
        print(f"Topics: {[t.name for t in config.get_topic_configs()]}")
        
        # Export configuration
        exported_config = config.export_config()
        print(f"Exported config keys: {list(exported_config.keys())}")
        
    finally:
        # Clean up
        if config_file.exists():
            config_file.unlink()


async def example_validation():
    """Configuration validation examples"""
    print("=== Configuration Validation Example ===")
    
    # Valid configuration
    valid_config = {
        "service_name": "valid-service",
        "bootstrap_servers": ["localhost:9092"],
        "producer": {
            "acks": "all",
            "compression_type": "gzip"
        }
    }
    
    try:
        validated = validate_kafka_config(valid_config)
        print(f"✓ Valid configuration: {validated.service_name}")
    except ValueError as e:
        print(f"✗ Validation failed: {e}")
    
    # Invalid configuration - bad bootstrap server
    invalid_config = {
        "service_name": "invalid-service",
        "bootstrap_servers": ["invalid-server"],  # Missing port
        "producer": {
            "acks": "invalid"  # Invalid acks value
        }
    }
    
    try:
        validate_kafka_config(invalid_config)
        print("✗ Should have failed validation")
    except ValueError as e:
        print(f"✓ Correctly caught validation error: {str(e)[:100]}...")
    
    # Topic validation
    valid_topic = {
        "name": "valid-topic",
        "partitions": 3,
        "replication_factor": 2
    }
    
    try:
        topic = validate_topic_config(valid_topic)
        print(f"✓ Valid topic: {topic.name} with {topic.partitions} partitions")
    except ValueError as e:
        print(f"✗ Topic validation failed: {e}")


async def example_circuit_breaker_and_monitoring():
    """Configuration for circuit breaker and monitoring"""
    print("=== Circuit Breaker and Monitoring Configuration ===")
    
    config = create_kafka_config(
        service_name="monitored-service",
        environment="production",
        circuit_breaker_enabled=True,
        circuit_breaker_failure_threshold=10,
        circuit_breaker_recovery_timeout=120,
        monitoring_enabled=True,
        metrics_export_enabled=True
    )
    
    cb_config = config.get_circuit_breaker_config()
    monitoring_config = config.get_monitoring_config()
    
    print(f"Circuit breaker enabled: {cb_config['enabled']}")
    print(f"Failure threshold: {cb_config['failure_threshold']}")
    print(f"Recovery timeout: {cb_config['recovery_timeout']}s")
    print(f"Monitoring enabled: {monitoring_config['enabled']}")
    print(f"Health checks enabled: {monitoring_config['health_check_enabled']}")
    print(f"Metrics export enabled: {monitoring_config['metrics_export_enabled']}")


async def main():
    """Run all configuration examples"""
    print("Kafka Configuration Management Examples")
    print("=" * 50)
    
    await example_basic_configuration()
    await example_production_configuration()
    await example_custom_configuration()
    await example_factory_usage()
    await example_environment_from_file()
    await example_validation()
    await example_circuit_breaker_and_monitoring()
    
    print("All examples completed successfully!")


if __name__ == "__main__":
    asyncio.run(main())