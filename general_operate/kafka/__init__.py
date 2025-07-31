"""
Kafka module for GeneralOperate
Provides event-driven architecture support with Kafka integration
Enhanced with service-specific initialization and middleware capabilities
"""

# Core components
from .kafka_operate import KafkaOperate
from .producer_operate import KafkaProducerOperate
from .consumer_operate import KafkaConsumerOperate
from .event_bus import KafkaEventBus
from .models.event_message import EventMessage
from .exceptions import KafkaOperateException

# Enhanced components from account project integration
from .enhanced_client import (
    KafkaAsyncProducer,
    KafkaAsyncConsumer,
    KafkaTopicManager,
    EnhancedEventBus,
    enhanced_event_bus,
    KafkaClientFactory
)

# Service initialization and middleware
from .service_initializer import (
    ServiceEventBusInitializer,
    ServiceConfig,
    PreConfiguredInitializers,
    FlexibleEventBusInitializer,
    initialize_for_auth_service,
    initialize_for_user_service,
    initialize_for_notification_service,
    initialize_kafka_event_bus,
    cleanup_event_bus
)

from .middleware_factory import (
    EventBusMiddlewareFactory,
    EventBusMiddleware,
    create_auth_service_app,
    create_user_service_app,
    create_notification_service_app,
    create_passwordless_auth_service_app,
    create_user_management_service_app,
    create_service_from_settings,
    get_event_bus,
    get_service_info
)

# Configuration and validation
from .config import (
    KafkaConfig,
    KafkaConfigFactory,
    KafkaConfigDefaults,
    create_kafka_config,
    load_environment_config
)

from .validation import (
    KafkaServiceConfig,
    TopicConfig,
    SecurityConfig,
    ProducerConfig,
    ConsumerConfig,
    EnvironmentConfig,
    validate_kafka_config,
    validate_topic_config,
    validate_environment_config
)

__all__ = [
    # Core components
    "KafkaOperate",
    "KafkaProducerOperate", 
    "KafkaConsumerOperate",
    "KafkaEventBus",
    "EventMessage",
    "KafkaOperateException",
    
    # Enhanced components
    "KafkaAsyncProducer",
    "KafkaAsyncConsumer", 
    "KafkaTopicManager",
    "EnhancedEventBus",
    "enhanced_event_bus",
    "KafkaClientFactory",
    
    # Service initialization
    "ServiceEventBusInitializer",
    "ServiceConfig",
    "PreConfiguredInitializers",
    "FlexibleEventBusInitializer",
    "initialize_for_auth_service",
    "initialize_for_user_service", 
    "initialize_for_notification_service",
    "initialize_kafka_event_bus",
    "cleanup_event_bus",
    
    # Middleware
    "EventBusMiddlewareFactory",
    "EventBusMiddleware",
    "create_auth_service_app",
    "create_user_service_app",
    "create_notification_service_app",
    "create_passwordless_auth_service_app",
    "create_user_management_service_app",
    "create_service_from_settings",
    "get_event_bus",
    "get_service_info",
    
    # Configuration and validation
    "KafkaConfig",
    "KafkaConfigFactory",
    "KafkaConfigDefaults",
    "create_kafka_config",
    "load_environment_config",
    "KafkaServiceConfig",
    "TopicConfig",
    "SecurityConfig",
    "ProducerConfig",
    "ConsumerConfig",
    "EnvironmentConfig",
    "validate_kafka_config",
    "validate_topic_config",
    "validate_environment_config"
]