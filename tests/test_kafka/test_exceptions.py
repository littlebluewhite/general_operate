"""
Unit tests for Kafka exceptions - no external dependencies required
"""

import pytest
from general_operate import GeneralOperateException
from general_operate.kafka.exceptions import (
    KafkaOperateException,
    KafkaConnectionException,
    KafkaProducerException,
    KafkaConsumerException,
    KafkaSerializationException,
    KafkaConfigurationException
)


class TestKafkaOperateException:
    """Test the base KafkaOperateException"""

    def test_inheritance(self):
        """Test that KafkaOperateException inherits from GeneralOperateException"""
        assert issubclass(KafkaOperateException, GeneralOperateException)

    def test_creation_with_minimal_parameters(self):
        """Test creating KafkaOperateException with minimal required parameters"""
        exc = KafkaOperateException(
            status_code=500,
            message_code=4000
        )
        
        # Should inherit behavior from GeneralOperateException
        assert isinstance(exc, GeneralOperateException)
        assert isinstance(exc, KafkaOperateException)
        assert exc.status_code == 500
        assert exc.message_code == 4000

    def test_creation_with_parameters(self):
        """Test creating KafkaOperateException with custom parameters"""
        exc = KafkaOperateException(
            status_code=500,
            message="Kafka operation failed",
            message_code=4001
        )
        
        assert exc.status_code == 500
        assert exc.message == "Kafka operation failed"
        assert exc.message_code == 4001

    def test_raise_and_catch(self):
        """Test raising and catching KafkaOperateException"""
        with pytest.raises(KafkaOperateException) as exc_info:
            raise KafkaOperateException(
                status_code=500,
                message="Test Kafka error",
                message_code=4000
            )
        
        assert exc_info.value.status_code == 500
        assert exc_info.value.message == "Test Kafka error"
        assert exc_info.value.message_code == 4000

    def test_can_be_caught_as_general_operate_exception(self):
        """Test that KafkaOperateException can be caught as GeneralOperateException"""
        with pytest.raises(GeneralOperateException) as exc_info:
            raise KafkaOperateException(
                status_code=500,
                message="Test error",
                message_code=4000
            )
        
        # Should be caught as the parent exception
        assert isinstance(exc_info.value, KafkaOperateException)
        assert isinstance(exc_info.value, GeneralOperateException)


class TestKafkaConnectionException:
    """Test KafkaConnectionException"""

    def test_inheritance(self):
        """Test that KafkaConnectionException inherits from KafkaOperateException"""
        assert issubclass(KafkaConnectionException, KafkaOperateException)
        assert issubclass(KafkaConnectionException, GeneralOperateException)

    def test_creation_and_usage(self):
        """Test creating and using KafkaConnectionException"""
        exc = KafkaConnectionException(
            status_code=503,
            message="Failed to connect to Kafka broker",
            message_code=4101
        )
        
        assert exc.status_code == 503
        assert exc.message == "Failed to connect to Kafka broker"
        assert exc.message_code == 4101

    def test_raise_and_catch_specific(self):
        """Test raising and catching KafkaConnectionException specifically"""
        with pytest.raises(KafkaConnectionException) as exc_info:
            raise KafkaConnectionException(
                status_code=503,
                message="Connection timeout",
                message_code=4101
            )
        
        assert exc_info.value.message == "Connection timeout"

    def test_can_be_caught_as_base_kafka_exception(self):
        """Test that KafkaConnectionException can be caught as KafkaOperateException"""
        with pytest.raises(KafkaOperateException):
            raise KafkaConnectionException(
                status_code=503,
                message="Connection failed",
                message_code=4101
            )


class TestKafkaProducerException:
    """Test KafkaProducerException"""

    def test_inheritance(self):
        """Test that KafkaProducerException inherits correctly"""
        assert issubclass(KafkaProducerException, KafkaOperateException)
        assert issubclass(KafkaProducerException, GeneralOperateException)

    def test_producer_specific_error(self):
        """Test KafkaProducerException for producer-specific errors"""
        exc = KafkaProducerException(
            status_code=500,
            message="Failed to send message to topic",
            message_code=4201
        )
        
        assert exc.status_code == 500
        assert "send message" in exc.message
        assert exc.message_code == 4201

    def test_raise_and_catch(self):
        """Test raising and catching KafkaProducerException"""
        with pytest.raises(KafkaProducerException) as exc_info:
            raise KafkaProducerException(
                status_code=500,
                message="Producer buffer full",
                message_code=4202
            )
        
        assert "Producer buffer full" in exc_info.value.message


class TestKafkaConsumerException:
    """Test KafkaConsumerException"""

    def test_inheritance(self):
        """Test that KafkaConsumerException inherits correctly"""
        assert issubclass(KafkaConsumerException, KafkaOperateException)
        assert issubclass(KafkaConsumerException, GeneralOperateException)

    def test_consumer_specific_error(self):
        """Test KafkaConsumerException for consumer-specific errors"""
        exc = KafkaConsumerException(
            status_code=500,
            message="Failed to consume messages from topic",
            message_code=4301
        )
        
        assert exc.status_code == 500
        assert "consume messages" in exc.message
        assert exc.message_code == 4301

    def test_consumer_group_error(self):
        """Test KafkaConsumerException for consumer group errors"""
        with pytest.raises(KafkaConsumerException) as exc_info:
            raise KafkaConsumerException(
                status_code=400,
                message="Invalid consumer group configuration",
                message_code=4302
            )
        
        assert exc_info.value.status_code == 400
        assert "consumer group" in exc_info.value.message


class TestKafkaSerializationException:
    """Test KafkaSerializationException"""

    def test_inheritance(self):
        """Test that KafkaSerializationException inherits correctly"""
        assert issubclass(KafkaSerializationException, KafkaOperateException)
        assert issubclass(KafkaSerializationException, GeneralOperateException)

    def test_serialization_error(self):
        """Test KafkaSerializationException for serialization errors"""
        exc = KafkaSerializationException(
            status_code=400,
            message="Failed to serialize message data",
            message_code=4401
        )
        
        assert exc.status_code == 400
        assert "serialize" in exc.message
        assert exc.message_code == 4401

    def test_deserialization_error(self):
        """Test KafkaSerializationException for deserialization errors"""
        with pytest.raises(KafkaSerializationException) as exc_info:
            raise KafkaSerializationException(
                status_code=400,
                message="Failed to deserialize message: invalid JSON",
                message_code=4402
            )
        
        assert "deserialize" in exc_info.value.message
        assert "invalid JSON" in exc_info.value.message


class TestKafkaConfigurationException:
    """Test KafkaConfigurationException"""

    def test_inheritance(self):
        """Test that KafkaConfigurationException inherits correctly"""
        assert issubclass(KafkaConfigurationException, KafkaOperateException)
        assert issubclass(KafkaConfigurationException, GeneralOperateException)

    def test_configuration_error(self):
        """Test KafkaConfigurationException for configuration errors"""
        exc = KafkaConfigurationException(
            status_code=400,
            message="Invalid bootstrap servers configuration",
            message_code=4501
        )
        
        assert exc.status_code == 400
        assert "bootstrap servers" in exc.message
        assert exc.message_code == 4501

    def test_missing_dependency_error(self):
        """Test KafkaConfigurationException for missing dependencies"""
        with pytest.raises(KafkaConfigurationException) as exc_info:
            raise KafkaConfigurationException(
                status_code=500,
                message="aiokafka is not installed. Please install it with: pip install aiokafka",
                message_code=4101
            )
        
        assert "aiokafka is not installed" in exc_info.value.message
        assert exc_info.value.status_code == 500


class TestExceptionHierarchy:
    """Test the complete exception hierarchy"""

    def test_all_kafka_exceptions_inherit_from_base(self):
        """Test that all Kafka exceptions inherit from KafkaOperateException"""
        kafka_exceptions = [
            KafkaConnectionException,
            KafkaProducerException,
            KafkaConsumerException,
            KafkaSerializationException,
            KafkaConfigurationException
        ]
        
        for exc_class in kafka_exceptions:
            assert issubclass(exc_class, KafkaOperateException)
            assert issubclass(exc_class, GeneralOperateException)

    def test_exception_catching_hierarchy(self):
        """Test that exceptions can be caught at different levels"""
        # All Kafka exceptions should be catchable as KafkaOperateException
        kafka_exceptions = [
            KafkaConnectionException(status_code=500, message="test", message_code=1),
            KafkaProducerException(status_code=500, message="test", message_code=1),
            KafkaConsumerException(status_code=500, message="test", message_code=1),
            KafkaSerializationException(status_code=500, message="test", message_code=1),
            KafkaConfigurationException(status_code=500, message="test", message_code=1)
        ]
        
        for exc in kafka_exceptions:
            with pytest.raises(KafkaOperateException):
                raise exc
            
            with pytest.raises(GeneralOperateException):
                raise exc

    def test_specific_exception_catching(self):
        """Test that specific exceptions can be caught individually"""
        # Test specific exception catching
        with pytest.raises(KafkaConnectionException):
            raise KafkaConnectionException(status_code=503, message="Connection failed", message_code=4101)
        
        with pytest.raises(KafkaProducerException):
            raise KafkaProducerException(status_code=500, message="Send failed", message_code=4201)
        
        with pytest.raises(KafkaConsumerException):
            raise KafkaConsumerException(status_code=500, message="Consume failed", message_code=4301)
        
        with pytest.raises(KafkaSerializationException):
            raise KafkaSerializationException(status_code=400, message="Serialization failed", message_code=4401)
        
        with pytest.raises(KafkaConfigurationException):
            raise KafkaConfigurationException(status_code=400, message="Config invalid", message_code=4501)


class TestRealWorldScenarios:
    """Test real-world usage scenarios for Kafka exceptions"""

    def test_connection_retry_scenario(self):
        """Test scenario where connection exceptions might be raised"""
        def simulate_kafka_connection():
            # Simulate connection failure
            raise KafkaConnectionException(
                status_code=503,
                message="Failed to connect to Kafka broker at localhost:9092",
                message_code=4101
            )
        
        with pytest.raises(KafkaConnectionException) as exc_info:
            simulate_kafka_connection()
        
        assert "localhost:9092" in exc_info.value.message
        assert exc_info.value.status_code == 503

    def test_producer_error_handling(self):
        """Test producer error handling scenario"""
        def simulate_message_send():
            # Simulate producer error
            raise KafkaProducerException(
                status_code=500,
                message="Failed to send message: Topic 'test-topic' does not exist",
                message_code=4203
            )
        
        with pytest.raises(KafkaProducerException) as exc_info:
            simulate_message_send()
        
        assert "test-topic" in exc_info.value.message
        assert "does not exist" in exc_info.value.message

    def test_serialization_error_handling(self):
        """Test serialization error handling scenario"""
        def simulate_message_serialization():
            # Simulate serialization error
            raise KafkaSerializationException(
                status_code=400,
                message="Cannot serialize object of type 'complex': Object is not JSON serializable",
                message_code=4403
            )
        
        with pytest.raises(KafkaSerializationException) as exc_info:
            simulate_message_serialization()
        
        assert "not JSON serializable" in exc_info.value.message
        assert exc_info.value.status_code == 400


if __name__ == "__main__":
    pass