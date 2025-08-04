"""
Performance and stress tests for modernized Kafka security configuration.

This module tests the performance characteristics and stress scenarios
of the SecurityConfigBuilder and KafkaConnectionBuilder classes.
"""

import asyncio
import time
import pytest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, patch
from typing import List, Dict, Any

from general_operate.kafka.kafka_client import (
    SecurityConfigBuilder,
    KafkaConnectionBuilder,
    KafkaAsyncProducer,
    KafkaAsyncConsumer,
)


class TestConfigurationPerformance:
    """Test performance characteristics of configuration builders"""

    def test_security_config_builder_performance(self):
        """Test SecurityConfigBuilder performance with various configurations"""
        configs = [
            # PLAINTEXT configuration
            {"security_protocol": "PLAINTEXT"},
            
            # SSL configuration
            {
                "security_protocol": "SSL",
                "ssl_cafile": "/path/to/ca.pem",
                "ssl_certfile": "/path/to/cert.pem",
                "ssl_keyfile": "/path/to/key.pem",
            },
            
            # SASL configuration
            {
                "security_protocol": "SASL_PLAINTEXT",
                "sasl_mechanism": "PLAIN",
                "sasl_plain_username": "user",
                "sasl_plain_password": "pass",
            },
            
            # SASL_SSL configuration
            {
                "security_protocol": "SASL_SSL",
                "ssl_cafile": "/path/to/ca.pem",
                "sasl_mechanism": "SCRAM-SHA-256",
                "sasl_plain_username": "user",
                "sasl_plain_password": "pass",
            },
        ]
        
        with patch.object(SecurityConfigBuilder, '_create_ssl_context') as mock_ssl:
            mock_ssl.return_value = Mock()  # Mock SSL context
            
            # Measure performance for each configuration type
            for config in configs:
                start_time = time.perf_counter()
                
                # Build configuration multiple times to measure performance
                for _ in range(100):
                    result = SecurityConfigBuilder.build(config)
                    assert "security_protocol" in result
                
                end_time = time.perf_counter()
                elapsed = end_time - start_time
                
                # Performance should be well under 1 second for 100 builds
                assert elapsed < 1.0, f"Configuration building too slow: {elapsed:.3f}s for {config['security_protocol']}"
                
                # Average time per build should be under 10ms
                avg_time_per_build = elapsed / 100
                assert avg_time_per_build < 0.01, f"Average build time too slow: {avg_time_per_build:.3f}s"

    def test_connection_builder_performance(self):
        """Test KafkaConnectionBuilder performance"""
        kafka_config = {
            "bootstrap_servers": ["broker1:9092", "broker2:9092", "broker3:9092"],
            "security_protocol": "SASL_SSL",
            "ssl_cafile": "/path/to/ca.pem",
            "sasl_mechanism": "SCRAM-SHA-256",
            "sasl_plain_username": "user",
            "sasl_plain_password": "pass",
        }
        
        with patch.object(SecurityConfigBuilder, 'build') as mock_security:
            mock_security.return_value = {
                "security_protocol": "SASL_SSL",
                "ssl_context": Mock(),
                "sasl_mechanism": "SCRAM-SHA-256",
                "sasl_plain_username": "user",
                "sasl_plain_password": "pass",
            }
            
            # Test producer config building performance
            start_time = time.perf_counter()
            for i in range(100):
                producer_config = KafkaConnectionBuilder.build_producer_config(
                    kafka_config, f"perf-service-{i}"
                )
                assert len(producer_config) > 5  # Should have multiple configuration keys
            end_time = time.perf_counter()
            
            producer_elapsed = end_time - start_time
            assert producer_elapsed < 1.0, f"Producer config building too slow: {producer_elapsed:.3f}s"
            
            # Test consumer config building performance
            start_time = time.perf_counter()
            for i in range(100):
                consumer_config = KafkaConnectionBuilder.build_consumer_config(
                    kafka_config, f"perf-group-{i}", f"perf-service-{i}"
                )
                assert len(consumer_config) > 10  # Should have multiple configuration keys
            end_time = time.perf_counter()
            
            consumer_elapsed = end_time - start_time
            assert consumer_elapsed < 1.0, f"Consumer config building too slow: {consumer_elapsed:.3f}s"

    def test_concurrent_config_building(self):
        """Test concurrent configuration building performance"""
        kafka_config = {
            "bootstrap_servers": ["localhost:9092"],
            "security_protocol": "SSL",
            "ssl_cafile": "/path/to/ca.pem",
            "ssl_certfile": "/path/to/cert.pem",
            "ssl_keyfile": "/path/to/key.pem",
        }
        
        def build_configs(thread_id: int) -> Dict[str, Any]:
            """Build configurations in a separate thread"""
            with patch.object(SecurityConfigBuilder, '_create_ssl_context') as mock_ssl:
                mock_ssl.return_value = Mock()
                
                results = {}
                for i in range(10):
                    service_name = f"thread-{thread_id}-service-{i}"
                    group_id = f"thread-{thread_id}-group-{i}"
                    
                    producer_config = KafkaConnectionBuilder.build_producer_config(
                        kafka_config, service_name
                    )
                    consumer_config = KafkaConnectionBuilder.build_consumer_config(
                        kafka_config, group_id, service_name
                    )
                    
                    results[f"producer-{i}"] = producer_config
                    results[f"consumer-{i}"] = consumer_config
                
                return results
        
        # Test concurrent configuration building
        start_time = time.perf_counter()
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(build_configs, i) for i in range(10)]
            results = [future.result() for future in futures]
        
        end_time = time.perf_counter()
        elapsed = end_time - start_time
        
        # Should complete within reasonable time even with concurrency
        assert elapsed < 5.0, f"Concurrent config building too slow: {elapsed:.3f}s"
        
        # Verify all threads produced results
        assert len(results) == 10
        for result in results:
            assert len(result) == 20  # 10 producer + 10 consumer configs

    def test_memory_usage_patterns(self):
        """Test memory usage patterns during configuration building"""
        import gc
        import sys
        
        kafka_config = {
            "bootstrap_servers": ["localhost:9092"] * 100,  # Large bootstrap servers list
            "security_protocol": "SASL_SSL",
            "ssl_cafile": "/path/to/ca.pem",
            "sasl_mechanism": "SCRAM-SHA-256",
            "sasl_plain_username": "x" * 1000,  # Large username
            "sasl_plain_password": "y" * 1000,  # Large password
        }
        
        with patch.object(SecurityConfigBuilder, '_create_ssl_context') as mock_ssl:
            mock_ssl.return_value = Mock()
            
            # Force garbage collection before test
            gc.collect()
            initial_objects = len(gc.get_objects())
            
            # Build many configurations
            configs = []
            for i in range(100):
                producer_config = KafkaConnectionBuilder.build_producer_config(
                    kafka_config, f"memory-test-service-{i}"
                )
                consumer_config = KafkaConnectionBuilder.build_consumer_config(
                    kafka_config, f"memory-test-group-{i}", f"memory-test-service-{i}"
                )
                configs.append((producer_config, consumer_config))
            
            # Force garbage collection after test
            gc.collect()
            final_objects = len(gc.get_objects())
            
            # Object count should not grow excessively
            object_growth = final_objects - initial_objects
            
            # Allow some object growth but ensure it's reasonable
            # This is a heuristic check - exact numbers may vary by Python version
            assert object_growth < 10000, f"Excessive object growth: {object_growth} new objects"
            
            # Clear references to allow cleanup
            del configs
            gc.collect()


class TestStressScenarios:
    """Test stress scenarios and edge cases"""

    def test_large_configuration_values(self):
        """Test handling of large configuration values"""
        large_config = {
            "bootstrap_servers": [f"broker-{i}:9092" for i in range(100)],
            "security_protocol": "SASL_SSL",
            "ssl_cafile": "/very/long/path/" + "x" * 500 + "/ca.pem",
            "ssl_certfile": "/very/long/path/" + "y" * 500 + "/cert.pem",
            "ssl_keyfile": "/very/long/path/" + "z" * 500 + "/key.pem",
            "sasl_mechanism": "SCRAM-SHA-512",
            "sasl_plain_username": "u" * 1000,
            "sasl_plain_password": "p" * 1000,
        }
        
        with patch.object(SecurityConfigBuilder, '_create_ssl_context') as mock_ssl:
            mock_ssl.return_value = Mock()
            
            # Should handle large configurations without issues
            producer_config = KafkaConnectionBuilder.build_producer_config(
                large_config, "large-config-service"
            )
            
            consumer_config = KafkaConnectionBuilder.build_consumer_config(
                large_config, "large-config-group", "large-config-service"
            )
            
            # Verify configurations are built correctly
            assert producer_config["bootstrap_servers"] == large_config["bootstrap_servers"]
            assert consumer_config["bootstrap_servers"] == large_config["bootstrap_servers"]
            assert len(producer_config["bootstrap_servers"]) == 100

    def test_rapid_client_creation_and_destruction(self):
        """Test rapid creation and destruction of Kafka clients"""
        kafka_config = {
            "bootstrap_servers": ["localhost:9092"],
            "security_protocol": "PLAINTEXT",
        }
        
        async def dummy_handler(event) -> dict[str, Any]:
            """Dummy message handler for testing"""
            from general_operate.kafka.kafka_client import ProcessingResult, MessageStatus
            return ProcessingResult(status=MessageStatus.SUCCESS)
            
        async def create_and_destroy_clients(count: int):
            """Create and destroy clients rapidly"""
            clients = []
            
            # Create clients
            for i in range(count):
                producer = KafkaAsyncProducer(kafka_config, f"rapid-producer-{i}")
                consumer = KafkaAsyncConsumer(
                    topics=[f"rapid-topic-{i}"],
                    group_id=f"rapid-group-{i}",
                    message_handler=dummy_handler,
                    kafka_config=kafka_config,
                    service_name=f"rapid-consumer-{i}"
                )
                clients.append((producer, consumer))
            
            # Clean up
            for producer, consumer in clients:
                # In real scenario, these would be properly stopped
                # For test, we just ensure they were created successfully
                assert producer.kafka_config == kafka_config
                assert consumer.kafka_config == kafka_config
        
        # Test with increasing numbers of clients
        for count in [10, 50, 100]:
            start_time = time.perf_counter()
            asyncio.run(create_and_destroy_clients(count))
            end_time = time.perf_counter()
            
            elapsed = end_time - start_time
            # Should complete within reasonable time
            assert elapsed < 5.0, f"Client creation/destruction too slow for {count} clients: {elapsed:.3f}s"

    def test_configuration_with_many_parameters(self):
        """Test configuration building with many parameters"""
        comprehensive_config = {
            "bootstrap_servers": ["broker1:9092", "broker2:9092"],
            "security_protocol": "SASL_SSL",
            "ssl_cafile": "/path/to/ca.pem",
            "ssl_certfile": "/path/to/cert.pem",
            "ssl_keyfile": "/path/to/key.pem",
            "ssl_password": "ssl_password",
            "sasl_mechanism": "SCRAM-SHA-256",
            "sasl_plain_username": "username",
            "sasl_plain_password": "password",
            "sasl_kerberos_service_name": "kafka",
            "sasl_kerberos_domain_name": "example.com",
            # Producer-specific parameters
            "acks": "all",
            "enable_idempotence": True,
            "compression_type": "gzip",
            "request_timeout_ms": 30000,
            "retries": 5,
            "batch_size": 16384,
            "linger_ms": 5,
            "buffer_memory": 33554432,
            # Consumer-specific parameters
            "auto_offset_reset": "earliest",
            "session_timeout_ms": 30000,
            "heartbeat_interval_ms": 3000,
            "max_poll_records": 500,
            "max_poll_interval_ms": 300000,
            "fetch_min_bytes": 1,
            "fetch_max_wait_ms": 500,
            "max_partition_fetch_bytes": 1048576,
            # Additional parameters
            "api_version": "auto",
            "connections_max_idle_ms": 540000,
            "metadata_max_age_ms": 300000,
            "reconnect_backoff_ms": 50,
            "reconnect_backoff_max_ms": 1000,
        }
        
        with patch.object(SecurityConfigBuilder, '_create_ssl_context') as mock_ssl:
            mock_ssl.return_value = Mock()
            
            start_time = time.perf_counter()
            
            # Build configurations with comprehensive parameters
            producer_config = KafkaConnectionBuilder.build_producer_config(
                comprehensive_config, "comprehensive-service"
            )
            
            consumer_config = KafkaConnectionBuilder.build_consumer_config(
                comprehensive_config, "comprehensive-group", "comprehensive-service"
            )
            
            end_time = time.perf_counter()
            elapsed = end_time - start_time
            
            # Should handle comprehensive configuration quickly
            assert elapsed < 0.1, f"Comprehensive config building too slow: {elapsed:.3f}s"
            
            # Verify all relevant parameters are preserved
            assert producer_config["acks"] == "all"
            assert producer_config["enable_idempotence"] is True
            assert producer_config["compression_type"] == "gzip"
            
            assert consumer_config["auto_offset_reset"] == "earliest"
            assert consumer_config["session_timeout_ms"] == 30000
            assert consumer_config["max_poll_records"] == 500

    @pytest.mark.asyncio
    async def test_async_client_initialization_performance(self):
        """Test performance of async client initialization with modern config"""
        kafka_config = {
            "bootstrap_servers": ["localhost:9092"],
            "security_protocol": "SSL",
            "ssl_cafile": "/path/to/ca.pem",
        }
        
        with patch('general_operate.kafka.kafka_client.AIOKafkaProducer') as mock_producer_class:
            with patch('general_operate.kafka.kafka_client.AIOKafkaConsumer') as mock_consumer_class:
                with patch.object(SecurityConfigBuilder, '_create_ssl_context') as mock_ssl:
                    mock_ssl.return_value = Mock()
                    
                    mock_producer_instance = Mock()
                    mock_producer_instance.start = Mock(return_value=asyncio.Future())
                    mock_producer_instance.start.return_value.set_result(None)
                    mock_producer_class.return_value = mock_producer_instance
                    
                    mock_consumer_instance = Mock()
                    mock_consumer_instance.start = Mock(return_value=asyncio.Future())
                    mock_consumer_instance.start.return_value.set_result(None)
                    mock_consumer_instance.subscribe = Mock()
                    mock_consumer_class.return_value = mock_consumer_instance
                    
                    # Test producer initialization performance
                    start_time = time.perf_counter()
                    
                    producers = []
                    for i in range(10):
                        producer = KafkaAsyncProducer(kafka_config, f"perf-producer-{i}")
                        await producer.start()
                        producers.append(producer)
                    
                    producer_elapsed = time.perf_counter() - start_time
                    
                    # Test consumer initialization performance
                    start_time = time.perf_counter()
                    
                    async def perf_handler(event) -> dict[str, Any]:
                        """Performance test message handler"""
                        from general_operate.kafka.kafka_client import ProcessingResult, MessageStatus
                        return ProcessingResult(status=MessageStatus.SUCCESS)
                    
                    consumers = []
                    for i in range(10):
                        consumer = KafkaAsyncConsumer(
                            topics=[f"perf-topic-{i}"],
                            group_id=f"perf-group-{i}",
                            message_handler=perf_handler,
                            kafka_config=kafka_config,
                            service_name=f"perf-consumer-{i}"
                        )
                        await consumer.start()
                        consumers.append(consumer)
                    
                    consumer_elapsed = time.perf_counter() - start_time
                    
                    # Performance assertions
                    assert producer_elapsed < 2.0, f"Producer initialization too slow: {producer_elapsed:.3f}s"
                    assert consumer_elapsed < 2.0, f"Consumer initialization too slow: {consumer_elapsed:.3f}s"
                    
                    # Verify all clients were configured correctly
                    assert len(producers) == 10
                    assert len(consumers) == 10
                    
                    # Verify mock calls for configuration building
                    # Note: Producer count is 20 because each consumer creates a DLQ producer
                    assert mock_producer_class.call_count == 20  # 10 main producers + 10 DLQ producers
                    assert mock_consumer_class.call_count == 10

    def test_serializer_deserializer_performance(self):
        """Test performance of built-in serializers and deserializers"""
        kafka_config = {
            "bootstrap_servers": ["localhost:9092"],
            "security_protocol": "PLAINTEXT",
        }
        
        with patch.object(SecurityConfigBuilder, 'build') as mock_security:
            mock_security.return_value = {"security_protocol": "PLAINTEXT"}
            
            producer_config = KafkaConnectionBuilder.build_producer_config(
                kafka_config, "serializer-perf-service"
            )
            
            consumer_config = KafkaConnectionBuilder.build_consumer_config(
                kafka_config, "serializer-perf-group", "serializer-perf-service"
            )
            
            value_serializer = producer_config["value_serializer"]
            key_serializer = producer_config["key_serializer"]
            value_deserializer = consumer_config["value_deserializer"]
            key_deserializer = consumer_config["key_deserializer"]
            
            # Test data
            test_strings = [f"test_message_{i}" for i in range(1000)]
            test_bytes = [s.encode('utf-8') for s in test_strings]
            
            # Test value serializer performance
            start_time = time.perf_counter()
            for test_string in test_strings:
                result = value_serializer(test_string)
                assert isinstance(result, bytes)
            serializer_elapsed = time.perf_counter() - start_time
            
            # Test value deserializer performance
            start_time = time.perf_counter()
            for test_byte in test_bytes:
                result = value_deserializer(test_byte)
                assert isinstance(result, str)
            deserializer_elapsed = time.perf_counter() - start_time
            
            # Performance assertions
            assert serializer_elapsed < 0.5, f"Serializer too slow: {serializer_elapsed:.3f}s for 1000 items"
            assert deserializer_elapsed < 0.5, f"Deserializer too slow: {deserializer_elapsed:.3f}s for 1000 items"
            
            # Test key serializer/deserializer performance
            start_time = time.perf_counter()
            for i, (test_string, test_byte) in enumerate(zip(test_strings, test_bytes)):
                serialized = key_serializer(test_string)
                deserialized = key_deserializer(test_byte)
                assert isinstance(serialized, bytes)
                assert isinstance(deserialized, str)
            key_elapsed = time.perf_counter() - start_time
            
            assert key_elapsed < 1.0, f"Key serializer/deserializer too slow: {key_elapsed:.3f}s for 1000 items"