"""
Enhanced Kafka client integration from account project
Provides advanced async producer/consumer and topic management capabilities
支持多語言環境的企業級 Kafka 客戶端
"""

from collections.abc import Callable
from typing import Any, Awaitable
import json
import asyncio
import uuid
from datetime import datetime, UTC
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
import structlog

from .kafka_operate import KafkaOperate
from .models.event_message import EventMessage
from .exceptions import KafkaOperateException
from .config import KafkaConfig, create_kafka_config

logger = structlog.get_logger()


class CircuitBreaker:
    """Simple circuit breaker for Kafka operations"""
    
    def __init__(self, failure_threshold: int = 5, timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "closed"  # closed, open, half-open
    
    def is_open(self) -> bool:
        return self.state == "open"
    
    def record_success(self):
        self.failure_count = 0
        self.state = "closed"
    
    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = datetime.now(UTC)
        if self.failure_count >= self.failure_threshold:
            self.state = "open"


class KafkaAsyncProducer(KafkaOperate):
    """Enhanced async Kafka producer with account project capabilities"""
    
    def __init__(
        self,
        bootstrap_servers: str | list[str],
        client_id: str = "enhanced-producer",
        config: dict[str, Any] | None = None,
        service_name: str = "general-operate"
    ):
        super().__init__(bootstrap_servers, client_id, config)
        self.service_name = service_name
        self.producer: AIOKafkaProducer | None = None
    
    async def start(self) -> None:
        """Start the async producer"""
        if self._started:
            return
        
        try:
            base_config = self._get_base_config()
            
            # Enhanced producer configuration
            producer_config = {
                **base_config,
                "value_serializer": lambda v: v.encode('utf-8') if isinstance(v, str) else v,
                "key_serializer": lambda k: k.encode('utf-8') if isinstance(k, str) else k,
                "client_id": f"{self.service_name}-producer",
                "acks": self.config.get("acks", "all"),
                "enable_idempotence": self.config.get("enable_idempotence", True),
                "compression_type": self.config.get("compression_type", "gzip"),
            }
            
            self.producer = AIOKafkaProducer(**producer_config)
            await self.producer.start()
            self._started = True
            
            self.logger.info("Enhanced Kafka async producer started")
            
        except Exception as e:
            self._handle_error(e, "producer start")
    
    async def stop(self) -> None:
        """Stop the async producer"""
        if self.producer and self._started:
            await self.producer.stop()
            self._started = False
            self.logger.info("Enhanced Kafka async producer stopped")
    
    async def send_event(
        self,
        topic: str,
        event: EventMessage,
        key: str | None = None,
        partition: int | None = None
    ) -> None:
        """Send event message to Kafka"""
        if not self._started:
            await self.start()
        
        try:
            # Use tenant_id as default key for ordering
            message_key = key or event.tenant_id
            
            future = await self.producer.send_and_wait(
                topic=topic,
                value=event.to_json(),
                key=message_key,
                partition=partition
            )
            
            self.logger.info(
                "Event sent to Kafka",
                topic=topic,
                event_type=event.event_type,
                correlation_id=event.correlation_id,
                partition=future.partition,
                offset=future.offset
            )
            
        except Exception as e:
            self.logger.error(
                "Failed to send event to Kafka",
                topic=topic,
                event_type=event.event_type,
                error=str(e)
            )
            self._handle_error(e, "send event")


class KafkaAsyncConsumer(KafkaOperate):
    """Enhanced async Kafka consumer with account project capabilities"""
    
    def __init__(
        self,
        bootstrap_servers: str | list[str],
        topics: list[str],
        group_id: str,
        message_handler: Callable[[EventMessage], Awaitable[None]],
        client_id: str = "enhanced-consumer",
        config: dict[str, Any] | None = None,
        service_name: str = "general-operate"
    ):
        super().__init__(bootstrap_servers, client_id, config)
        self.topics = topics
        self.group_id = group_id
        self.message_handler = message_handler
        self.service_name = service_name
        self.consumer: AIOKafkaConsumer | None = None
        self._running = False
    
    async def start(self) -> None:
        """Start the async consumer"""
        try:
            base_config = self._get_base_config()
            
            # Enhanced consumer configuration
            consumer_config = {
                **base_config,
                "group_id": self.group_id,
                "client_id": f"{self.service_name}-consumer-{self.group_id}",
                "auto_offset_reset": self.config.get("auto_offset_reset", "latest"),
                "enable_auto_commit": self.config.get("enable_auto_commit", False),
                "value_deserializer": lambda m: m.decode('utf-8') if m else None,
                "key_deserializer": lambda k: k.decode('utf-8') if k else None,
                "isolation_level": 'read_committed'
            }
            
            self.consumer = AIOKafkaConsumer(
                *self.topics,
                **consumer_config
            )
            
            await self.consumer.start()
            self._started = True
            
            self.logger.info(
                "Enhanced Kafka async consumer started",
                topics=self.topics,
                group_id=self.group_id
            )
            
        except Exception as e:
            self._handle_error(e, "consumer start")
    
    async def stop(self) -> None:
        """Stop the async consumer"""
        self._running = False
        if self.consumer and self._started:
            await self.consumer.stop()
            self._started = False
            self.logger.info("Enhanced Kafka async consumer stopped", group_id=self.group_id)
    
    async def consume(self) -> None:
        """Start consuming messages"""
        if not self.consumer:
            await self.start()
        
        self._running = True
        
        try:
            async for message in self.consumer:
                if not self._running:
                    break
                
                try:
                    # Parse event message
                    event = EventMessage.from_json(message.value)
                    
                    self.logger.info(
                        "Processing event from Kafka",
                        topic=message.topic,
                        partition=message.partition,
                        offset=message.offset,
                        event_type=event.event_type,
                        correlation_id=event.correlation_id
                    )
                    
                    # Process message
                    await self.message_handler(event)
                    
                    # Manual commit for reliability
                    await self.consumer.commit()
                    
                    self.logger.debug(
                        "Event processed successfully",
                        correlation_id=event.correlation_id,
                        offset=message.offset
                    )
                    
                except json.JSONDecodeError as e:
                    self.logger.error(
                        "Failed to decode event message",
                        message_value=message.value,
                        error=str(e)
                    )
                    # Skip invalid message and commit offset
                    await self.consumer.commit()
                    
                except Exception as e:
                    self.logger.error(
                        "Failed to process event",
                        topic=message.topic,
                        offset=message.offset,
                        error=str(e)
                    )
                    # TODO: Implement retry logic or dead letter queue
                    await self.consumer.commit()
                    
        except Exception as e:
            self._handle_error(e, "consumer loop")


class KafkaTopicManager(KafkaOperate):
    """Enhanced Kafka topic management with account project capabilities"""
    
    def __init__(
        self,
        bootstrap_servers: str | list[str],
        client_id: str = "topic-manager",
        config: dict[str, Any] | None = None
    ):
        super().__init__(bootstrap_servers, client_id, config)
        self.admin_client: AIOKafkaAdminClient | None = None
    
    async def start(self) -> None:
        """Start the admin client"""
        if self._started:
            return
        
        try:
            base_config = self._get_base_config()
            self.admin_client = AIOKafkaAdminClient(**base_config)
            await self.admin_client.start()
            self._started = True
            
            self.logger.info("Kafka topic manager started")
            
        except Exception as e:
            self._handle_error(e, "topic manager start")
    
    async def stop(self) -> None:
        """Stop the admin client"""
        if self.admin_client and self._started:
            await self.admin_client.close()
            self._started = False
            self.logger.info("Kafka topic manager stopped")
    
    async def create_topics(self, topics_config: list[dict[str, Any]]) -> None:
        """Create topics from configuration"""
        if not self._started:
            await self.start()
        
        topics = []
        
        for config in topics_config:
            topic = NewTopic(
                name=config['name'],
                num_partitions=config.get('partitions', 3),
                replication_factor=config.get('replication_factor', 1),
                topic_configs=config.get('config', {})
            )
            topics.append(topic)
        
        try:
            await self.admin_client.create_topics(topics, validate_only=False)
            self.logger.info("Topics created successfully", topic_count=len(topics))
            
        except Exception as e:
            if "already exists" in str(e).lower() or "topic already exists" in str(e).lower():
                self.logger.info("Some topics already exist", error=str(e))
            else:
                self.logger.error("Failed to create topics", error=str(e))
                self._handle_error(e, "create topics")
    
    async def delete_topics(self, topic_names: list[str]) -> None:
        """Delete topics by name"""
        if not self._started:
            await self.start()
        
        try:
            await self.admin_client.delete_topics(topic_names)
            self.logger.info("Topics deleted successfully", topics=topic_names)
            
        except Exception as e:
            self._handle_error(e, "delete topics")
    
    async def list_topics(self) -> dict[str, Any]:
        """List all topics with metadata"""
        if not self._started:
            await self.start()
        
        try:
            metadata = await self.admin_client.describe_topics()
            return {
                topic_name: {
                    "partitions": len(topic_metadata.partitions),
                    "replication_factor": len(topic_metadata.partitions[0].replicas) if topic_metadata.partitions else 0
                }
                for topic_name, topic_metadata in metadata.items()
            }
        except Exception as e:
            self._handle_error(e, "list topics")
            return {}


class EnhancedEventBus(KafkaOperate):
    """Enhanced Kafka event bus combining producer and consumer capabilities"""
    
    def __init__(
        self,
        bootstrap_servers: str | list[str],
        client_id: str = "enhanced-event-bus",
        config: dict[str, Any] | None = None,
        service_name: str = "general-operate"
    ):
        super().__init__(bootstrap_servers, client_id, config)
        self.service_name = service_name
        self.producer = KafkaAsyncProducer(
            bootstrap_servers, f"{client_id}-producer", config, service_name
        )
        self._consumers: dict[str, KafkaAsyncConsumer] = {}
    
    async def start(self) -> None:
        """Start the enhanced event bus"""
        if self._started:
            return
        
        await self.producer.start()
        self._started = True
        self.logger.info("Enhanced Kafka event bus started")
    
    async def stop(self) -> None:
        """Stop the enhanced event bus"""
        if not self._started:
            return
        
        # Stop all consumers
        for consumer in self._consumers.values():
            await consumer.stop()
        
        # Stop producer
        await self.producer.stop()
        
        self._started = False
        self.logger.info("Enhanced Kafka event bus stopped")
    
    async def publish(
        self,
        topic: str,
        event_type: str,
        tenant_id: str,
        data: dict[str, Any],
        user_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        key: str | None = None
    ) -> None:
        """Publish event with enhanced capabilities"""
        event = EventMessage(
            event_type=event_type,
            tenant_id=tenant_id,
            user_id=user_id,
            data=data,
            metadata=metadata
        )
        
        await self.producer.send_event(topic, event, key)
    
    async def subscribe(
        self,
        topics: list[str],
        group_id: str,
        handler: Callable[[EventMessage], Awaitable[None]]
    ) -> KafkaAsyncConsumer:
        """Subscribe to events with enhanced consumer"""
        consumer = KafkaAsyncConsumer(
            self.bootstrap_servers,
            topics,
            group_id,
            handler,
            f"{self.client_id}-consumer",
            self.config,
            self.service_name
        )
        
        self._consumers[group_id] = consumer
        
        # Start consuming in background task
        asyncio.create_task(consumer.consume())
        
        return consumer
    
    async def health_check(self) -> dict[str, Any]:
        """Enhanced health check"""
        return {
            "event_bus": self._started,
            "producer": self.producer.is_started,
            "consumers": {
                group_id: consumer.is_started
                for group_id, consumer in self._consumers.items()
            },
            "service_name": self.service_name
        }


# Enhanced factory methods with configuration management support
class KafkaClientFactory:
    """
    Factory for creating Kafka clients with enhanced configuration management
    增強的Kafka客戶端工廠，支持配置管理
    """
    
    @staticmethod
    def _ensure_config(
        config: KafkaConfig | None,
        bootstrap_servers: str | list[str] | None,
        service_name: str,
        environment: str | None,
        **kwargs
    ) -> KafkaConfig:
        """
        Ensure we have a valid KafkaConfig instance
        Helper method to consolidate configuration creation logic
        """
        if config is not None:
            return config
        
        # Create config if not provided
        if bootstrap_servers is None:
            bootstrap_servers = "localhost:9092"
        
        return create_kafka_config(
            service_name=service_name,
            environment=environment,
            bootstrap_servers=[bootstrap_servers] if isinstance(bootstrap_servers, str) else bootstrap_servers,
            **kwargs
        )
    
    @staticmethod
    def create_async_producer(
        bootstrap_servers: str | list[str] = None,
        service_name: str = "general-operate",
        config: KafkaConfig = None,
        environment: str = None,
        **kwargs
    ) -> KafkaAsyncProducer:
        """
        Create async producer with enhanced configuration support
        
        Args:
            bootstrap_servers: Kafka bootstrap servers (deprecated, use config)
            service_name: Name of the service
            config: KafkaConfig instance for advanced configuration
            environment: Environment name for auto-configuration
            **kwargs: Additional configuration overrides
        """
        config = KafkaClientFactory._ensure_config(
            config, bootstrap_servers, service_name, environment, **kwargs
        )
        
        # Use config for producer creation
        producer_config = config.get_producer_config()
        
        return KafkaAsyncProducer(
            bootstrap_servers=config.get_bootstrap_servers(),
            service_name=service_name,
            config=producer_config
        )
    
    @staticmethod
    def create_async_consumer(
        topics: list[str] = None,
        group_id: str = None,
        message_handler: Callable[[EventMessage], Awaitable[None]] = None,
        bootstrap_servers: str | list[str] = None,
        service_name: str = "general-operate",
        config: KafkaConfig = None,
        environment: str = None,
        **kwargs
    ) -> KafkaAsyncConsumer:
        """
        Create async consumer with enhanced configuration support
        
        Args:
            topics: List of topics to subscribe to
            group_id: Consumer group ID
            message_handler: Message handling function
            bootstrap_servers: Kafka bootstrap servers (deprecated, use config)
            service_name: Name of the service
            config: KafkaConfig instance for advanced configuration
            environment: Environment name for auto-configuration
            **kwargs: Additional configuration overrides
        """
        config = KafkaClientFactory._ensure_config(
            config, bootstrap_servers, service_name, environment, **kwargs
        )
        
        # Use config for consumer creation
        consumer_config = config.get_consumer_config(group_id=group_id)
        
        return KafkaAsyncConsumer(
            bootstrap_servers=config.get_bootstrap_servers(),
            topics=topics or [],
            group_id=group_id or f"{service_name}-consumer-group",
            message_handler=message_handler,
            service_name=service_name,
            config=consumer_config
        )
    
    @staticmethod
    def create_event_bus(
        bootstrap_servers: str | list[str] = None,
        service_name: str = "general-operate",
        config: KafkaConfig = None,
        environment: str = None,
        **kwargs
    ) -> EnhancedEventBus:
        """
        Create event bus with enhanced configuration support
        
        Args:
            bootstrap_servers: Kafka bootstrap servers (deprecated, use config)
            service_name: Name of the service
            config: KafkaConfig instance for advanced configuration
            environment: Environment name for auto-configuration
            **kwargs: Additional configuration overrides
        """
        config = KafkaClientFactory._ensure_config(
            config, bootstrap_servers, service_name, environment, **kwargs
        )
        
        return EnhancedEventBus(
            bootstrap_servers=config.get_bootstrap_servers(),
            service_name=service_name,
            config=config.raw_config
        )
    
    @staticmethod
    def create_topic_manager(
        bootstrap_servers: str | list[str] = None,
        config: KafkaConfig = None,
        service_name: str = "general-operate",
        environment: str = None,
        **kwargs
    ) -> KafkaTopicManager:
        """
        Create topic manager with enhanced configuration support
        
        Args:
            bootstrap_servers: Kafka bootstrap servers (deprecated, use config)
            config: KafkaConfig instance for advanced configuration
            service_name: Name of the service for configuration context
            environment: Environment name for auto-configuration
            **kwargs: Additional configuration overrides
        """
        config = KafkaClientFactory._ensure_config(
            config, bootstrap_servers, service_name, environment, **kwargs
        )
        
        # Use config for admin client
        admin_config = config.get_admin_config()
        
        return KafkaTopicManager(
            bootstrap_servers=config.get_bootstrap_servers(),
            config=admin_config
        )
    
    @staticmethod
    def create_from_config(
        config: KafkaConfig,
        component_type: str = "event_bus"
    ) -> Any:
        """
        Create Kafka component directly from configuration
        直接從配置創建Kafka組件
        
        Args:
            config: KafkaConfig instance
            component_type: Type of component to create (event_bus, producer, consumer, topic_manager)
        """
        if component_type == "event_bus":
            return KafkaClientFactory.create_event_bus(config=config)
        elif component_type == "producer":
            return KafkaClientFactory.create_async_producer(config=config)
        elif component_type == "consumer":
            return KafkaClientFactory.create_async_consumer(config=config)
        elif component_type == "topic_manager":
            return KafkaClientFactory.create_topic_manager(config=config)
        else:
            raise ValueError(f"Unknown component type: {component_type}")
    
    @staticmethod
    def create_from_environment(
        service_name: str,
        environment: str,
        component_type: str = "event_bus",
        config_path: str = None,
        **kwargs
    ) -> Any:
        """
        Create Kafka component from environment configuration
        從環境配置創建Kafka組件
        
        Args:
            service_name: Name of the service
            environment: Environment name (development, staging, production, test)
            component_type: Type of component to create
            config_path: Optional path to configuration file
            **kwargs: Additional configuration overrides
        """
        config = create_kafka_config(
            service_name=service_name,
            environment=environment,
            config_path=config_path,
            **kwargs
        )
        
        return KafkaClientFactory.create_from_config(config, component_type)


# Global enhanced event bus instance for backward compatibility
enhanced_event_bus = KafkaClientFactory.create_event_bus()

# Account project compatibility aliases - 保持與 account 項目的兼容性
KafkaEventBus = EnhancedEventBus  # Alias for account project compatibility