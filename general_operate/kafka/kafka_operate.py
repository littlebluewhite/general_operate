"""
Main Kafka operations module for GeneralOperate
Provides high-level interface for Kafka producer and consumer operations
Enhanced with service-specific initialization and topic management
"""

from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar
from contextlib import asynccontextmanager
import structlog
import aiokafka.errors as kafka_errors
from .exceptions import KafkaOperateException

T = TypeVar('T')


class KafkaOperate(Generic[T], ABC):
    """
    Base class for Kafka operations
    Provides unified interface for Kafka operations within GeneralOperate architecture
    """
    
    def __init__(
        self,
        bootstrap_servers: str | list[str],
        client_id: str | None = None,
        config: dict[str, Any] | None = None,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id or "general-operate"
        self.config = config or {}
        self._exc = KafkaOperateException
        self._started = False
        
        # Set up logging
        self.logger = structlog.get_logger().bind(
            operator=self.__class__.__name__,
            kafka_client=self.client_id
        )
    
    @abstractmethod
    async def start(self) -> None:
        """Start Kafka client"""
        pass
    
    @abstractmethod
    async def stop(self) -> None:
        """Stop Kafka client"""
        pass
    
    @asynccontextmanager
    async def lifespan(self):
        """Lifecycle management context manager"""
        try:
            if not self._started:
                await self.start()
            yield self
        finally:
            if self._started:
                await self.stop()
    
    def _handle_error(self, error: Exception, context: str) -> None:
        """Unified error handling following GeneralOperate patterns"""
        self.logger.error(
            f"Kafka operation failed: {context}",
            error=str(error),
            error_type=type(error).__name__
        )
        
        # Import aiokafka errors dynamically to avoid import issues
        try:
            if isinstance(error, kafka_errors.KafkaError):
                raise self._exc(
                    status_code=487,
                    message=f"Kafka error in {context}: {error}",
                    message_code=4001
                )
            elif isinstance(error, kafka_errors.KafkaConnectionError):
                raise self._exc(
                    status_code=487,
                    message=f"Kafka connection error in {context}: {error}",
                    message_code=4002
                )
            elif isinstance(error, kafka_errors.KafkaTimeoutError):
                raise self._exc(
                    status_code=487,
                    message=f"Kafka timeout error in {context}: {error}",
                    message_code=4003
                )
        except (ImportError, AttributeError):
            # If aiokafka is not available or kafka_errors is None, treat as generic error
            pass
        
        # Generic error handling
        if isinstance(error, self._exc):
            raise error
        else:
            raise self._exc(
                status_code=500,
                message=f"Unexpected error in {context}: {error}",
                message_code=4999
            )
    
    @property
    def is_started(self) -> bool:
        """Check if the Kafka client is started"""
        return self._started
    
    def _validate_config(self) -> None:
        """Validate Kafka configuration"""
        if not self.bootstrap_servers:
            raise self._exc(
                status_code=400,
                message="Bootstrap servers must be specified",
                message_code=4100
            )
        
        if isinstance(self.bootstrap_servers, str):
            self.bootstrap_servers = [self.bootstrap_servers]
    
    def _get_base_config(self) -> dict[str, Any]:
        """Get base configuration for Kafka clients"""
        self._validate_config()
        
        return {
            "bootstrap_servers": self.bootstrap_servers,
            "client_id": self.client_id,
            **self.config
        }


# Import enhanced components for convenient access
from .producer_operate import KafkaProducerOperate
from .consumer_operate import KafkaConsumerOperate
from .event_bus import KafkaEventBus
from .models.event_message import EventMessage
from .enhanced_client import (
    KafkaAsyncProducer,
    KafkaAsyncConsumer, 
    KafkaTopicManager,
    EnhancedEventBus
)
from .service_initializer import ServiceEventBusInitializer
from .middleware_factory import EventBusMiddlewareFactory

__all__ = [
    'KafkaOperate',
    'KafkaProducerOperate',
    'KafkaConsumerOperate', 
    'KafkaEventBus',
    'EventMessage',
    'KafkaOperateException',
    'KafkaAsyncProducer',
    'KafkaAsyncConsumer',
    'KafkaTopicManager', 
    'EnhancedEventBus',
    'ServiceEventBusInitializer',
    'EventBusMiddlewareFactory'
]