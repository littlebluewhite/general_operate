"""
Unit tests for Kafka base operations - no external dependencies required
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Any
from contextlib import asynccontextmanager

from general_operate.kafka.kafka_operate import KafkaOperate
from general_operate.kafka.exceptions import KafkaOperateException


class MockKafkaOperate(KafkaOperate[dict]):
    """Mock implementation of KafkaOperate for testing"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._mock_started = False
    
    async def start(self) -> None:
        """Mock start implementation"""
        if self._mock_started:
            return
        self._mock_started = True
        self._started = True
    
    async def stop(self) -> None:
        """Mock stop implementation"""
        self._mock_started = False
        self._started = False


class TestKafkaOperateInitialization:
    """Test KafkaOperate initialization"""
    
    def test_initialization_minimal_config(self):
        """Test initialization with minimal configuration"""
        kafka_op = MockKafkaOperate(
            bootstrap_servers="localhost:9092"
        )
        
        assert kafka_op.bootstrap_servers == "localhost:9092"
        assert kafka_op.client_id == "general-operate"
        assert kafka_op.config == {}
        assert kafka_op._exc == KafkaOperateException
        assert kafka_op._started is False
    
    def test_initialization_full_config(self):
        """Test initialization with full configuration"""
        config = {"custom_param": "value", "timeout": 30}
        
        kafka_op = MockKafkaOperate(
            bootstrap_servers=["broker1:9092", "broker2:9092"],
            client_id="custom-client",
            config=config,
            exc=ValueError
        )
        
        assert kafka_op.bootstrap_servers == ["broker1:9092", "broker2:9092"]
        assert kafka_op.client_id == "custom-client"
        assert kafka_op.config == config
        assert kafka_op._exc == ValueError
    
    def test_logger_initialization(self):
        """Test that logger is properly initialized"""
        kafka_op = MockKafkaOperate(
            bootstrap_servers="localhost:9092",
            client_id="test-client"
        )
        
        # Logger should be bound with operator and kafka_client
        assert kafka_op.logger is not None


class TestKafkaOperateProperties:
    """Test KafkaOperate properties"""
    
    def test_is_started_property(self):
        """Test is_started property"""
        kafka_op = MockKafkaOperate(bootstrap_servers="localhost:9092")
        
        assert kafka_op.is_started is False
        
        kafka_op._started = True
        assert kafka_op.is_started is True


class TestKafkaOperateConfigValidation:
    """Test configuration validation"""
    
    def test_validate_config_empty_servers(self):
        """Test validation with empty bootstrap servers"""
        kafka_op = MockKafkaOperate(bootstrap_servers="")
        
        with pytest.raises(KafkaOperateException) as exc_info:
            kafka_op._validate_config()
        
        assert exc_info.value.status_code == 400
        assert "Bootstrap servers must be specified" in exc_info.value.message
        assert exc_info.value.message_code == 4100
    
    def test_validate_config_none_servers(self):
        """Test validation with None bootstrap servers"""
        kafka_op = MockKafkaOperate(bootstrap_servers=None)
        
        with pytest.raises(KafkaOperateException) as exc_info:
            kafka_op._validate_config()
        
        assert exc_info.value.status_code == 400
        assert "Bootstrap servers must be specified" in exc_info.value.message
    
    def test_validate_config_string_to_list_conversion(self):
        """Test that string bootstrap_servers is converted to list"""
        kafka_op = MockKafkaOperate(bootstrap_servers="localhost:9092")
        
        kafka_op._validate_config()
        
        assert kafka_op.bootstrap_servers == ["localhost:9092"]
    
    def test_validate_config_list_unchanged(self):
        """Test that list bootstrap_servers remains unchanged"""
        servers = ["broker1:9092", "broker2:9092"]
        kafka_op = MockKafkaOperate(bootstrap_servers=servers)
        
        kafka_op._validate_config()
        
        assert kafka_op.bootstrap_servers == servers


class TestKafkaOperateBaseConfig:
    """Test base configuration generation"""
    
    def test_get_base_config_minimal(self):
        """Test base config with minimal settings"""
        kafka_op = MockKafkaOperate(
            bootstrap_servers="localhost:9092",
            client_id="test-client"
        )
        
        config = kafka_op._get_base_config()
        
        assert config["bootstrap_servers"] == ["localhost:9092"]
        assert config["client_id"] == "test-client"
    
    def test_get_base_config_with_custom_config(self):
        """Test base config with custom configuration"""
        custom_config = {
            "timeout": 30,
            "retries": 3,
            "custom_param": "value"
        }
        
        kafka_op = MockKafkaOperate(
            bootstrap_servers="localhost:9092",
            client_id="test-client",
            config=custom_config
        )
        
        config = kafka_op._get_base_config()
        
        assert config["bootstrap_servers"] == ["localhost:9092"]
        assert config["client_id"] == "test-client"
        assert config["timeout"] == 30
        assert config["retries"] == 3
        assert config["custom_param"] == "value"
    
    def test_get_base_config_validation_called(self):
        """Test that _validate_config is called during _get_base_config"""
        kafka_op = MockKafkaOperate(bootstrap_servers="")
        
        with pytest.raises(KafkaOperateException):
            kafka_op._get_base_config()


class TestKafkaOperateErrorHandling:
    """Test error handling functionality"""
    
    @patch('general_operate.kafka.kafka_operate.kafka_errors')
    def test_handle_error_kafka_error(self, mock_kafka_errors):
        """Test handling of KafkaError"""
        # Mock KafkaError
        mock_error = Exception("Kafka error")
        mock_kafka_errors.KafkaError = Exception
        
        kafka_op = MockKafkaOperate(bootstrap_servers="localhost:9092")
        
        with pytest.raises(KafkaOperateException) as exc_info:
            kafka_op._handle_error(mock_error, "test context")
        
        assert exc_info.value.status_code == 487
        assert "Kafka error in test context" in exc_info.value.message
        assert exc_info.value.message_code == 4001
    
    @patch('general_operate.kafka.kafka_operate.kafka_errors')
    def test_handle_error_connection_error(self, mock_kafka_errors):
        """Test handling of KafkaConnectionError"""
        # Mock KafkaConnectionError
        mock_error = Exception("Connection error")
        mock_kafka_errors.KafkaError = type("MockKafkaError", (), {})
        mock_kafka_errors.KafkaConnectionError = Exception
        
        kafka_op = MockKafkaOperate(bootstrap_servers="localhost:9092")
        
        with pytest.raises(KafkaOperateException) as exc_info:
            kafka_op._handle_error(mock_error, "connection")
        
        assert exc_info.value.status_code == 487
        assert "Kafka connection error in connection" in exc_info.value.message
        assert exc_info.value.message_code == 4002
    
    @patch('general_operate.kafka.kafka_operate.kafka_errors')
    def test_handle_error_timeout_error(self, mock_kafka_errors):
        """Test handling of KafkaTimeoutError"""
        # Mock KafkaTimeoutError
        mock_error = Exception("Timeout error")
        mock_kafka_errors.KafkaError = type("MockKafkaError", (), {})
        mock_kafka_errors.KafkaConnectionError = type("MockKafkaConnectionError", (), {})
        mock_kafka_errors.KafkaTimeoutError = Exception
        
        kafka_op = MockKafkaOperate(bootstrap_servers="localhost:9092")
        
        with pytest.raises(KafkaOperateException) as exc_info:
            kafka_op._handle_error(mock_error, "timeout")
        
        assert exc_info.value.status_code == 487
        assert "Kafka timeout error in timeout" in exc_info.value.message
        assert exc_info.value.message_code == 4003
    
    def test_handle_error_existing_kafka_operate_exception(self):
        """Test handling of existing KafkaOperateException"""
        existing_exception = KafkaOperateException(
            status_code=400,
            message="Existing error",
            message_code=1000
        )
        
        kafka_op = MockKafkaOperate(bootstrap_servers="localhost:9092")
        
        with pytest.raises(KafkaOperateException) as exc_info:
            kafka_op._handle_error(existing_exception, "test context")
        
        # Should re-raise the existing exception unchanged
        assert exc_info.value == existing_exception
        assert exc_info.value.status_code == 400
        assert exc_info.value.message == "Existing error"
        assert exc_info.value.message_code == 1000
    
    def test_handle_error_generic_exception(self):
        """Test handling of generic exception"""
        generic_error = ValueError("Generic error")
        
        kafka_op = MockKafkaOperate(bootstrap_servers="localhost:9092")
        
        with pytest.raises(KafkaOperateException) as exc_info:
            kafka_op._handle_error(generic_error, "generic test")
        
        assert exc_info.value.status_code == 500
        assert "Unexpected error in generic test" in exc_info.value.message
        assert exc_info.value.message_code == 4999
    
    @patch('general_operate.kafka.kafka_operate.kafka_errors', None)
    def test_handle_error_no_aiokafka(self):
        """Test error handling when aiokafka is not available"""
        generic_error = Exception("Some error")
        
        kafka_op = MockKafkaOperate(bootstrap_servers="localhost:9092")
        
        with pytest.raises(KafkaOperateException) as exc_info:
            kafka_op._handle_error(generic_error, "no aiokafka")
        
        # Should fall back to generic error handling
        assert exc_info.value.status_code == 500
        assert "Unexpected error in no aiokafka" in exc_info.value.message
        assert exc_info.value.message_code == 4999
    
    def test_handle_error_custom_exception_class(self):
        """Test error handling with custom exception class"""
        custom_error = ValueError("Custom error")
        
        kafka_op = MockKafkaOperate(
            bootstrap_servers="localhost:9092",
            exc=ValueError
        )
        
        # If the custom exception class doesn't support KafkaOperateException constructor args,
        # it will fail. So we use a compatible exception for this test.
        with pytest.raises(ValueError) as exc_info:
            kafka_op._handle_error(custom_error, "custom test")
        
        # When the error is already of the custom exception type, it should be re-raised
        assert exc_info.value == custom_error


class TestKafkaOperateLifecycleManager:
    """Test lifecycle management context manager"""
    
    @pytest.mark.asyncio
    async def test_lifespan_not_started(self):
        """Test lifespan manager when not started"""
        kafka_op = MockKafkaOperate(bootstrap_servers="localhost:9092")
        
        async with kafka_op.lifespan() as managed_op:
            assert managed_op == kafka_op
            assert kafka_op.is_started is True
        
        # Should be stopped after context exit
        assert kafka_op.is_started is False
    
    @pytest.mark.asyncio
    async def test_lifespan_already_started(self):
        """Test lifespan manager when already started"""
        kafka_op = MockKafkaOperate(bootstrap_servers="localhost:9092")
        
        # Start manually first
        await kafka_op.start()
        assert kafka_op.is_started is True
        
        async with kafka_op.lifespan() as managed_op:
            assert managed_op == kafka_op
            assert kafka_op.is_started is True
        
        # Should be stopped after context exit
        assert kafka_op.is_started is False
    
    @pytest.mark.asyncio
    async def test_lifespan_exception_handling(self):
        """Test lifespan manager with exception in context"""
        kafka_op = MockKafkaOperate(bootstrap_servers="localhost:9092")
        
        with pytest.raises(ValueError):
            async with kafka_op.lifespan():
                assert kafka_op.is_started is True
                raise ValueError("Test exception")
        
        # Should still be stopped after exception
        assert kafka_op.is_started is False


class TestKafkaOperateAbstractMethods:
    """Test abstract method enforcement"""
    
    def test_cannot_instantiate_base_class(self):
        """Test that KafkaOperate cannot be instantiated directly"""
        with pytest.raises(TypeError):
            KafkaOperate(bootstrap_servers="localhost:9092")


class TestKafkaOperateEdgeCases:
    """Test edge cases and error scenarios"""
    
    def test_bootstrap_servers_type_handling(self):
        """Test various bootstrap_servers type inputs"""
        # String input
        kafka_op1 = MockKafkaOperate(bootstrap_servers="localhost:9092")
        kafka_op1._validate_config()
        assert kafka_op1.bootstrap_servers == ["localhost:9092"]
        
        # List input
        kafka_op2 = MockKafkaOperate(bootstrap_servers=["host1:9092", "host2:9092"])
        kafka_op2._validate_config()
        assert kafka_op2.bootstrap_servers == ["host1:9092", "host2:9092"]
    
    def test_client_id_default_handling(self):
        """Test client_id default value handling"""
        kafka_op1 = MockKafkaOperate(bootstrap_servers="localhost:9092")
        assert kafka_op1.client_id == "general-operate"
        
        kafka_op2 = MockKafkaOperate(
            bootstrap_servers="localhost:9092",
            client_id="custom-client"
        )
        assert kafka_op2.client_id == "custom-client"
        
        kafka_op3 = MockKafkaOperate(
            bootstrap_servers="localhost:9092",
            client_id=None
        )
        assert kafka_op3.client_id == "general-operate"
    
    def test_config_default_handling(self):
        """Test config default value handling"""
        kafka_op1 = MockKafkaOperate(bootstrap_servers="localhost:9092")
        assert kafka_op1.config == {}
        
        kafka_op2 = MockKafkaOperate(
            bootstrap_servers="localhost:9092",
            config=None
        )
        assert kafka_op2.config == {}
        
        custom_config = {"key": "value"}
        kafka_op3 = MockKafkaOperate(
            bootstrap_servers="localhost:9092",
            config=custom_config
        )
        assert kafka_op3.config == custom_config


class TestKafkaOperateIntegration:
    """Test realistic integration scenarios"""
    
    @pytest.mark.asyncio
    async def test_complete_lifecycle_workflow(self):
        """Test complete lifecycle workflow"""
        kafka_op = MockKafkaOperate(
            bootstrap_servers="localhost:9092",
            client_id="integration-test",
            config={"timeout": 30, "retries": 3}
        )
        
        # Initial state
        assert kafka_op.is_started is False
        
        # Start
        await kafka_op.start()
        assert kafka_op.is_started is True
        
        # Multiple starts should be idempotent
        await kafka_op.start()
        assert kafka_op.is_started is True
        
        # Stop
        await kafka_op.stop()
        assert kafka_op.is_started is False
        
        # Multiple stops should be safe
        await kafka_op.stop()
        assert kafka_op.is_started is False
    
    @pytest.mark.asyncio
    async def test_lifespan_integration(self):
        """Test lifespan manager integration"""
        kafka_op = MockKafkaOperate(
            bootstrap_servers=["broker1:9092", "broker2:9092"],
            client_id="lifespan-test"
        )
        
        # Use lifespan manager for automatic lifecycle
        async with kafka_op.lifespan() as managed_kafka:
            assert managed_kafka.is_started is True
            
            # Get base config while active
            config = managed_kafka._get_base_config()
            assert config["bootstrap_servers"] == ["broker1:9092", "broker2:9092"]
            assert config["client_id"] == "lifespan-test"
        
        # Should be automatically stopped
        assert kafka_op.is_started is False


if __name__ == "__main__":
    pass