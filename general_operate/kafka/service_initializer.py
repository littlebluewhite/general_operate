"""
Service-specific event bus initializer for GeneralOperate
Provides dynamic initialization patterns for different service types
為不同服務類型提供動態初始化模式的事件總線初始化器
"""

import structlog
import asyncio
import os
import yaml
from pathlib import Path
from typing import Any, Callable, Awaitable, Dict, List, Optional
from abc import ABC, abstractmethod

from .enhanced_client import EnhancedEventBus, KafkaTopicManager, KafkaClientFactory
from .models.event_message import EventMessage

logger = structlog.get_logger()


class ServiceConfig:
    """Configuration for different service types"""
    
    @staticmethod
    def get_default_topics() -> list[dict[str, Any]]:
        """Get default topic configurations"""
        return [
            {
                "name": "general-events",
                "partitions": 3,
                "replication_factor": 1,
                "config": {
                    "cleanup.policy": "delete",
                    "retention.ms": "604800000",  # 7 days
                }
            }
        ]
    
    @staticmethod
    def load_from_config(config_path: str | None = None) -> dict[str, list[dict[str, Any]]]:
        """
        Load service configurations from external source
        從外部配置源載入服務配置
        """
        if config_path and Path(config_path).exists():
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    if config_path.endswith('.yaml') or config_path.endswith('.yml'):
                        config_data = yaml.safe_load(f)
                        return config_data.get('kafka_topics', {})
                    else:
                        import json
                        config_data = json.load(f)
                        return config_data.get('kafka_topics', {})
            except Exception as e:
                logger.warning(f"Failed to load config from {config_path}", error=str(e))
        
        # Default configurations if no external config found
        return {
            "auth": ServiceConfig.get_auth_service_topics(),
            "user": ServiceConfig.get_auth_service_topics(),
            "notification": ServiceConfig.get_notification_service_topics(),
            "audit": ServiceConfig.get_audit_service_topics(),
            "default": ServiceConfig.get_default_topics()
        }
    
    @staticmethod
    def get_auth_service_topics() -> list[dict[str, Any]]:
        """Topics specifically for authentication services"""
        return [
            {
                "name": "auth-events",
                "partitions": 3,
                "replication_factor": 1,
                "config": {
                    "cleanup.policy": "delete",
                    "retention.ms": "604800000",  # 7 days
                }
            },
            {
                "name": "user-events", 
                "partitions": 3,
                "replication_factor": 1,
                "config": {
                    "cleanup.policy": "delete",
                    "retention.ms": "2592000000",  # 30 days
                }
            }
        ]
    
    @staticmethod
    def get_notification_service_topics() -> list[dict[str, Any]]:
        """Topics specifically for notification services"""
        return [
            {
                "name": "notification-events",
                "partitions": 3,
                "replication_factor": 1,
                "config": {
                    "cleanup.policy": "delete",
                    "retention.ms": "86400000",  # 1 day
                }
            }
        ]
    
    @staticmethod
    def get_audit_service_topics() -> list[dict[str, Any]]:
        """Topics specifically for audit services"""
        return [
            {
                "name": "audit-events",
                "partitions": 5,  # More partitions for high volume
                "replication_factor": 1,
                "config": {
                    "cleanup.policy": "delete",
                    "retention.ms": "7776000000",  # 90 days
                }
            }
        ]


class ServiceEventBusInitializer:
    """Dynamic event bus initializer for different service types"""
    
    def __init__(
        self,
        bootstrap_servers: str | list[str],
        service_name: str,
        config: dict[str, Any] | None = None
    ):
        self.bootstrap_servers = bootstrap_servers
        self.service_name = service_name
        self.config = config or {}
        self.event_bus: EnhancedEventBus | None = None
        self.topic_manager: KafkaTopicManager | None = None
        self._initialized = False
    
    async def initialize_topics(self, topic_configs: list[dict[str, Any]] | None = None) -> None:
        """Initialize Kafka topics"""
        logger.info(f"Initializing Kafka topics for {self.service_name}")
        
        if not topic_configs:
            topic_configs = ServiceConfig.get_default_topics()
        
        self.topic_manager = KafkaTopicManager(
            self.bootstrap_servers,
            f"{self.service_name}-topic-manager",
            self.config
        )
        
        try:
            await self.topic_manager.create_topics(topic_configs)
            logger.info(
                "Kafka topics initialized successfully",
                service=self.service_name,
                topics=[config["name"] for config in topic_configs]
            )
        except Exception as e:
            logger.error(f"Failed to initialize Kafka topics for {self.service_name}", error=str(e))
            raise
    
    async def initialize_event_bus(self) -> EnhancedEventBus:
        """Initialize event bus"""
        logger.info(f"Initializing Kafka event bus for {self.service_name}")
        
        try:
            self.event_bus = EnhancedEventBus(
                self.bootstrap_servers,
                f"{self.service_name}-event-bus",
                self.config,
                self.service_name
            )
            
            await self.event_bus.start()
            logger.info(f"Kafka event bus initialized successfully for {self.service_name}")
            
            return self.event_bus
            
        except Exception as e:
            logger.error(f"Failed to initialize Kafka event bus for {self.service_name}", error=str(e))
            raise
    
    async def shutdown_event_bus(self) -> None:
        """Shutdown event bus"""
        logger.info(f"Shutting down Kafka event bus for {self.service_name}")
        
        try:
            if self.event_bus:
                await self.event_bus.stop()
            
            if self.topic_manager:
                await self.topic_manager.stop()
                
            self._initialized = False
            logger.info(f"Kafka event bus shutdown completed for {self.service_name}")
            
        except Exception as e:
            logger.error(f"Failed to shutdown Kafka event bus for {self.service_name}", error=str(e))
            raise
    
    async def full_initialization(
        self,
        topic_configs: list[dict[str, Any]] | None = None,
        consumer_handlers: dict[str, Callable[[EventMessage], Awaitable[None]]] | None = None
    ) -> EnhancedEventBus:
        """Complete initialization flow"""
        logger.info(f"Starting full event bus initialization for {self.service_name}")
        
        # Initialize topics
        await self.initialize_topics(topic_configs)
        
        # Wait for topics to be fully created
        await asyncio.sleep(2)
        
        # Initialize event bus
        event_bus = await self.initialize_event_bus()
        
        # Set up consumers if provided
        if consumer_handlers:
            await self._setup_consumers(event_bus, consumer_handlers)
        
        self._initialized = True
        logger.info(f"Full event bus initialization completed for {self.service_name}")
        
        return event_bus
    
    async def _setup_consumers(
        self,
        event_bus: EnhancedEventBus,
        consumer_handlers: dict[str, Callable[[EventMessage], Awaitable[None]]]
    ) -> None:
        """Set up event consumers"""
        for topic_pattern, handler in consumer_handlers.items():
            group_id = f"{self.service_name}-{topic_pattern.replace('*', 'wildcard')}"
            
            # For simplicity, assume topics are provided as list
            topics = [topic_pattern] if not topic_pattern.endswith('*') else ["general-events"]
            
            await event_bus.subscribe(topics, group_id, handler)
            logger.info(f"Consumer set up for {self.service_name}", topics=topics, group_id=group_id)
    
    @property
    def is_initialized(self) -> bool:
        """Check if the initializer is fully initialized"""
        return self._initialized


class PreConfiguredInitializers:
    """Pre-configured initializers for common service types"""
    
    @staticmethod
    def create_auth_service_initializer(
        bootstrap_servers: str | list[str],
        service_name: str = "auth-service",
        config: dict[str, Any] | None = None
    ) -> ServiceEventBusInitializer:
        """Create initializer for auth service"""
        initializer = ServiceEventBusInitializer(bootstrap_servers, service_name, config)
        return initializer
    
    @staticmethod
    async def initialize_auth_service(
        bootstrap_servers: str | list[str],
        service_name: str = "auth-service",
        config: dict[str, Any] | None = None
    ) -> EnhancedEventBus:
        """Initialize auth service with standard configuration"""
        initializer = PreConfiguredInitializers.create_auth_service_initializer(
            bootstrap_servers, service_name, config
        )
        
        topic_configs = ServiceConfig.get_auth_service_topics()
        return await initializer.full_initialization(topic_configs)
    
    @staticmethod
    def create_user_service_initializer(
        bootstrap_servers: str | list[str],
        service_name: str = "user-service",
        config: dict[str, Any] | None = None
    ) -> ServiceEventBusInitializer:
        """Create initializer for user service"""
        initializer = ServiceEventBusInitializer(bootstrap_servers, service_name, config)
        return initializer
    
    @staticmethod
    async def initialize_user_service(
        bootstrap_servers: str | list[str],
        service_name: str = "user-service", 
        config: dict[str, Any] | None = None
    ) -> EnhancedEventBus:
        """Initialize user service with standard configuration"""
        initializer = PreConfiguredInitializers.create_user_service_initializer(
            bootstrap_servers, service_name, config
        )
        
        topic_configs = ServiceConfig.get_auth_service_topics()  # User service uses auth topics
        return await initializer.full_initialization(topic_configs)
    
    @staticmethod
    def create_notification_service_initializer(
        bootstrap_servers: str | list[str],
        service_name: str = "notification-service",
        config: dict[str, Any] | None = None
    ) -> ServiceEventBusInitializer:
        """Create initializer for notification service"""
        initializer = ServiceEventBusInitializer(bootstrap_servers, service_name, config)
        return initializer
    
    @staticmethod
    async def initialize_notification_service(
        bootstrap_servers: str | list[str],
        service_name: str = "notification-service",
        config: dict[str, Any] | None = None,
        consumer_handlers: dict[str, Callable[[EventMessage], Awaitable[None]]] | None = None
    ) -> EnhancedEventBus:
        """Initialize notification service with consumer capabilities"""
        initializer = PreConfiguredInitializers.create_notification_service_initializer(
            bootstrap_servers, service_name, config
        )
        
        # Notification service needs both its own topics and ability to consume from others
        topic_configs = (
            ServiceConfig.get_notification_service_topics() + 
            ServiceConfig.get_auth_service_topics()
        )
        
        return await initializer.full_initialization(topic_configs, consumer_handlers)
    
    @staticmethod
    async def initialize_generic_service(
        bootstrap_servers: str | list[str],
        service_name: str,
        topic_configs: list[dict[str, Any]] | None = None,
        consumer_handlers: dict[str, Callable[[EventMessage], Awaitable[None]]] | None = None,
        config: dict[str, Any] | None = None
    ) -> EnhancedEventBus:
        """Initialize any generic service with custom configuration"""
        initializer = ServiceEventBusInitializer(bootstrap_servers, service_name, config)
        
        if not topic_configs:
            topic_configs = ServiceConfig.get_default_topics()
        
        return await initializer.full_initialization(topic_configs, consumer_handlers)


class FlexibleEventBusInitializer:
    """
    Flexible event bus initializer with enhanced configuration support
    具有增強配置支持的靈活事件總線初始化器
    """
    
    def __init__(
        self,
        bootstrap_servers: str | list[str] = "localhost:9092",
        config_path: Optional[str] = None
    ):
        self.bootstrap_servers = bootstrap_servers
        self.config_path = config_path
        self._service_configs = ServiceConfig.load_from_config(config_path)
    
    async def create_service_event_bus(
        self,
        service_name: str,
        service_type: str = "default",
        consumer_handlers: Optional[dict[str, Callable[[EventMessage], Awaitable[None]]]] = None,
        custom_config: Optional[dict[str, Any]] = None
    ) -> EnhancedEventBus:
        """
        Create and initialize event bus for any service type
        為任何服務類型創建和初始化事件總線
        """
        logger.info(f"Creating event bus for {service_name} (type: {service_type})")
        
        # Get topic configurations
        topic_configs = self._service_configs.get(service_type, self._service_configs["default"])
        
        # Create initializer
        initializer = ServiceEventBusInitializer(
            self.bootstrap_servers,
            service_name,
            custom_config
        )
        
        # Initialize with full setup
        event_bus = await initializer.full_initialization(topic_configs, consumer_handlers)
        
        logger.info(f"Event bus created successfully for {service_name}")
        return event_bus
    
    async def initialize_multiple_services(
        self,
        services_config: dict[str, dict[str, Any]]
    ) -> dict[str, EnhancedEventBus]:
        """
        Initialize multiple services at once
        一次初始化多個服務
        
        Args:
            services_config: Dictionary with service names as keys and config as values
                Example: {
                    "auth-service": {"type": "auth"},
                    "user-service": {"type": "user"},
                    "notification-service": {"type": "notification", "handlers": {...}}
                }
        """
        event_buses = {}
        
        for service_name, config in services_config.items():
            service_type = config.get("type", "default")
            consumer_handlers = config.get("handlers")
            custom_config = config.get("config")
            
            event_bus = await self.create_service_event_bus(
                service_name,
                service_type,
                consumer_handlers,
                custom_config
            )
            
            event_buses[service_name] = event_bus
        
        logger.info(f"Initialized {len(event_buses)} services successfully")
        return event_buses


# Account project compatibility functions - 與 account 項目兼容的函數
async def initialize_kafka_event_bus() -> EnhancedEventBus:
    """Initialize basic event bus - account project compatibility"""
    initializer = FlexibleEventBusInitializer()
    return await initializer.create_service_event_bus("default-service", "default")


# Convenient async functions for backward compatibility with account project patterns
async def initialize_for_auth_service(
    bootstrap_servers: str | list[str] = "localhost:9092",
    service_name: str = "auth-service",
    config: dict[str, Any] | None = None
) -> EnhancedEventBus:
    """Initialize for Auth Service - backward compatible"""
    return await PreConfiguredInitializers.initialize_auth_service(
        bootstrap_servers, service_name, config
    )


async def initialize_for_user_service(
    bootstrap_servers: str | list[str] = "localhost:9092", 
    service_name: str = "user-service",
    config: dict[str, Any] | None = None
) -> EnhancedEventBus:
    """Initialize for User Service - backward compatible"""
    return await PreConfiguredInitializers.initialize_user_service(
        bootstrap_servers, service_name, config
    )


async def initialize_for_notification_service(
    bootstrap_servers: str | list[str] = "localhost:9092",
    service_name: str = "notification-service", 
    config: dict[str, Any] | None = None,
    consumer_handlers: dict[str, Callable[[EventMessage], Awaitable[None]]] | None = None
) -> EnhancedEventBus:
    """Initialize for Notification Service - backward compatible"""
    return await PreConfiguredInitializers.initialize_notification_service(
        bootstrap_servers, service_name, config, consumer_handlers
    )


async def cleanup_event_bus(event_bus: EnhancedEventBus) -> None:
    """Cleanup event bus - backward compatible"""
    if event_bus:
        await event_bus.stop()