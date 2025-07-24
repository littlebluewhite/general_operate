"""
Kafka event bus implementation for GeneralOperate
"""

from typing import Any, Callable, Awaitable
from dataclasses import dataclass
import asyncio
import uuid
from datetime import datetime, UTC
from collections import defaultdict

from .producer_operate import KafkaProducerOperate
from .consumer_operate import KafkaConsumerOperate
from .models.event_message import EventMessage


@dataclass
class EventSubscription:
    """Event subscription information"""
    subscription_id: str
    event_type: str
    handler: Callable[[EventMessage], Awaitable[None]]
    filter_func: Callable[[EventMessage], bool] | None = None


class KafkaEventBus:
    """
    Kafka event bus for publish/subscribe pattern
    Provides high-level event processing with routing and filtering
    """
    
    def __init__(
        self,
        bootstrap_servers: str | list[str],
        client_id: str = "event-bus",
        default_topic: str = "events",
        config: dict[str, Any] | None = None,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.default_topic = default_topic
        self.config = config or {}
        
        # Producer for publishing events
        producer_config = self.config.copy()
        if producer_config.get("enable_transactions"):
            producer_config["transactional_id"] = f"{client_id}-tx"
        
        self.producer = KafkaProducerOperate(
            bootstrap_servers=bootstrap_servers,
            client_id=f"{client_id}-producer",
            config=producer_config
        )
        
        # Consumer dictionary (by topic)
        self._consumers: dict[str, KafkaConsumerOperate] = {}
        
        # Subscription management
        self._subscriptions: dict[str, list[EventSubscription]] = defaultdict(list)
        self._subscription_tasks: dict[str, asyncio.Task] = {}
        
        # Started flag
        self._started = False
    
    async def start(self) -> None:
        """Start the event bus"""
        if self._started:
            return
        
        await self.producer.start()
        self._started = True
    
    async def stop(self) -> None:
        """Stop the event bus"""
        if not self._started:
            return
        
        # Cancel all subscription tasks
        for task in self._subscription_tasks.values():
            task.cancel()
        
        # Wait for tasks to complete (if any)
        tasks = list(self._subscription_tasks.values())
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        # Stop all consumers
        for consumer in self._consumers.values():
            await consumer.stop()
        
        # Stop producer
        await self.producer.stop()
        self._started = False
    
    async def publish(
        self,
        event_type: str,
        data: dict[str, Any],
        tenant_id: str | None = None,
        user_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        topic: str | None = None,
        key: str | None = None,
    ) -> EventMessage:
        """
        Publish an event
        
        Args:
            event_type: Event type
            data: Event data
            tenant_id: Tenant ID
            user_id: User ID
            metadata: Event metadata
            topic: Target topic (default: default_topic)
            key: Message key (default: tenant_id)
        
        Returns:
            EventMessage: Published event
        """
        if not self._started:
            await self.start()
        
        # Create event message
        event = EventMessage(
            event_type=event_type,
            tenant_id=tenant_id,
            user_id=user_id,
            data=data,
            metadata=metadata or {},
            timestamp=datetime.now(UTC).isoformat(),
            correlation_id=str(uuid.uuid4())
        )
        
        # Send event
        await self.producer.send_event(
            topic=topic or self.default_topic,
            value=event.to_dict(),
            key=key or tenant_id,
            headers={
                "event_type": event_type.encode('utf-8'),
                "correlation_id": event.correlation_id.encode('utf-8')
            }
        )
        
        return event
    
    async def subscribe(
        self,
        event_type: str,
        handler: Callable[[EventMessage], Awaitable[None]],
        topic: str | None = None,
        group_id: str | None = None,
        filter_func: Callable[[EventMessage], bool] | None = None,
    ) -> str:
        """
        Subscribe to events
        
        Args:
            event_type: Event type (supports wildcard *)
            handler: Event handler function
            topic: Topic to subscribe to (default: default_topic)
            group_id: Consumer group ID
            filter_func: Event filter function
        
        Returns:
            str: Subscription ID
        """
        topic = topic or self.default_topic
        group_id = group_id or f"{self.client_id}-{event_type}"
        
        # Create subscription
        subscription = EventSubscription(
            subscription_id=str(uuid.uuid4()),
            event_type=event_type,
            handler=handler,
            filter_func=filter_func
        )
        
        self._subscriptions[topic].append(subscription)
        
        # Create consumer for this topic if it doesn't exist
        if topic not in self._consumers:
            consumer = KafkaConsumerOperate(
                bootstrap_servers=self.bootstrap_servers,
                topics=[topic],
                group_id=group_id,
                client_id=f"{self.client_id}-consumer-{topic}",
                config={
                    **self.config,
                    "enable_auto_commit": False  # Use manual commit
                }
            )
            
            self._consumers[topic] = consumer
            
            # Start consumption task
            task = asyncio.create_task(
                self._consume_topic(topic, consumer)
            )
            self._subscription_tasks[topic] = task
        
        return subscription.subscription_id
    
    async def unsubscribe(self, subscription_id: str) -> None:
        """Unsubscribe from events"""
        for topic, subscriptions in self._subscriptions.items():
            self._subscriptions[topic] = [
                s for s in subscriptions
                if s.subscription_id != subscription_id
            ]
    
    async def _consume_topic(
        self,
        topic: str,
        consumer: KafkaConsumerOperate
    ) -> None:
        """Consume messages from a specific topic"""
        async def handle_message(data: dict[str, Any]) -> None:
            # Parse event message
            try:
                event = EventMessage.from_dict(data)
            except Exception as e:
                consumer.logger.error(f"Failed to parse event message: {e}")
                return
            
            # Distribute to matching subscribers
            for subscription in self._subscriptions[topic]:
                # Check if event type matches
                if self._match_event_type(event.event_type, subscription.event_type):
                    # Apply filter if present
                    if subscription.filter_func and not subscription.filter_func(event):
                        continue
                    
                    # Handle event
                    try:
                        await subscription.handler(event)
                    except Exception as e:
                        # Log error but continue processing other subscribers
                        consumer.logger.error(
                            "Error handling event",
                            event_type=event.event_type,
                            subscription_id=subscription.subscription_id,
                            error=str(e)
                        )
        
        # Start consuming
        await consumer.consume_events(handle_message)
    
    @staticmethod
    def _match_event_type(event_type: str, pattern: str) -> bool:
        """Check if event type matches pattern"""
        if pattern == "*":
            return True
        
        if pattern.endswith("*"):
            prefix = pattern[:-1]
            return event_type.startswith(prefix)
        
        return event_type == pattern
    
    async def health_check(self) -> dict[str, Any]:
        """Check health of event bus components"""
        health = {
            "event_bus": self._started,
            "producer": self.producer.is_started if self.producer else False,
            "consumers": {}
        }
        
        for topic, consumer in self._consumers.items():
            health["consumers"][topic] = consumer.is_started if consumer else False
        
        return health