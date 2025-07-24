"""
Kafka consumer operations for GeneralOperate
"""

from typing import Any, Callable, Awaitable

try:
    import aiokafka
    from aiokafka import AIOKafkaConsumer
except ImportError:
    aiokafka = None
    AIOKafkaConsumer = None

from .kafka_operate import KafkaOperate, T
from .utils.serializers import SerializerFactory
from .exceptions import KafkaConsumerException, KafkaConfigurationException


class KafkaConsumerOperate(KafkaOperate[T]):
    """
    Kafka consumer operations
    Supports auto-commit, manual commit, and batch processing
    """
    
    def __init__(
        self,
        bootstrap_servers: str | list[str],
        topics: list[str],
        group_id: str,
        client_id: str | None = None,
        config: dict[str, Any] | None = None,
        deserializer: Callable[[bytes], T] | None = None,
    ):
        if aiokafka is None:
            raise KafkaConfigurationException(
                status_code=500,
                message="aiokafka is not installed. Please install it with: pip install aiokafka",
                message_code=4101
            )
        
        super().__init__(bootstrap_servers, client_id, config, KafkaConsumerException)
        
        self.topics = topics
        self.group_id = group_id
        
        # Set up deserializer
        serializer_factory = SerializerFactory()
        self.deserializer = deserializer or serializer_factory.get_value_deserializer("json")
        
        # Consumer configuration
        base_config = self._get_base_config()
        self.consumer_config = {
            **base_config,
            "group_id": self.group_id,
            "value_deserializer": self.deserializer,
            "key_deserializer": lambda k: k.decode('utf-8') if k else None,
            "auto_offset_reset": self.config.get("auto_offset_reset", "earliest"),
            "enable_auto_commit": self.config.get("enable_auto_commit", True),
            "auto_commit_interval_ms": self.config.get("auto_commit_interval_ms", 5000),
            "isolation_level": self.config.get("isolation_level", "read_committed"),
            **self.config.get("consumer_config", {})
        }
        
        self.consumer: AIOKafkaConsumer | None = None
        self._running = False
    
    async def start(self) -> None:
        """Start the consumer"""
        if self._started:
            return
        
        try:
            self.consumer = AIOKafkaConsumer(
                *self.topics,
                **self.consumer_config
            )
            await self.consumer.start()
            self._started = True
            self.logger.info(
                "Kafka consumer started",
                topics=self.topics,
                group_id=self.group_id
            )
        except Exception as e:
            self._handle_error(e, "start consumer")
    
    async def stop(self) -> None:
        """Stop the consumer"""
        self._running = False
        if self.consumer and self._started:
            try:
                await self.consumer.stop()
                self._started = False
                self.logger.info("Kafka consumer stopped")
            except Exception as e:
                self.logger.warning(f"Error stopping consumer: {e}")
    
    async def consume_events(
        self,
        handler: Callable[[T], Awaitable[None]],
        error_handler: Callable[[Exception, Any], Awaitable[None]] | None = None,
    ) -> None:
        """
        Consume events and process them
        
        Args:
            handler: Event processing function
            error_handler: Error handling function
        """
        if not self._started:
            await self.start()
        
        self._running = True
        
        try:
            async for msg in self.consumer:
                if not self._running:
                    break
                
                try:
                    # Process message
                    await handler(msg.value)
                    
                    # Manual commit if auto-commit is disabled
                    if not self.consumer_config["enable_auto_commit"]:
                        await self.consumer.commit()
                    
                    self.logger.debug(
                        "Event processed",
                        topic=msg.topic,
                        partition=msg.partition,
                        offset=msg.offset
                    )
                    
                except Exception as e:
                    self.logger.error(
                        "Error processing event",
                        topic=msg.topic,
                        partition=msg.partition,
                        offset=msg.offset,
                        error=str(e)
                    )
                    
                    if error_handler:
                        await error_handler(e, msg)
                    else:
                        # Default behavior: log error and continue
                        pass
                        
        except Exception as e:
            self._handle_error(e, "consume events")
    
    async def consume_batch(
        self,
        handler: Callable[[list[T]], Awaitable[None]],
        batch_size: int = 100,
        timeout_ms: int = 1000,
    ) -> None:
        """
        Consume events in batches
        
        Args:
            handler: Batch processing function
            batch_size: Batch size
            timeout_ms: Timeout in milliseconds
        """
        if not self._started:
            await self.start()
        
        self._running = True
        batch = []
        
        try:
            while self._running:
                # Get messages
                data = await self.consumer.getmany(
                    timeout_ms=timeout_ms,
                    max_records=batch_size
                )
                
                if not data:
                    # If there's an unprocessed batch, process it
                    if batch:
                        await handler(batch)
                        batch = []
                        
                        if not self.consumer_config["enable_auto_commit"]:
                            await self.consumer.commit()
                    continue
                
                # Collect messages
                for tp, messages in data.items():
                    for msg in messages:
                        batch.append(msg.value)
                
                # Process batch when full
                if len(batch) >= batch_size:
                    await handler(batch)
                    batch = []
                    
                    if not self.consumer_config["enable_auto_commit"]:
                        await self.consumer.commit()
                        
        except Exception as e:
            self._handle_error(e, "consume batch")
    
    async def seek_to_beginning(self, partitions: list[Any] | None = None) -> None:
        """Seek to beginning of partitions"""
        if not self._started:
            await self.start()
        
        if partitions:
            await self.consumer.seek_to_beginning(*partitions)
        else:
            await self.consumer.seek_to_beginning()
    
    async def seek_to_end(self, partitions: list[Any] | None = None) -> None:
        """Seek to end of partitions"""
        if not self._started:
            await self.start()
        
        if partitions:
            await self.consumer.seek_to_end(*partitions)
        else:
            await self.consumer.seek_to_end()
    
    async def commit(self, offsets: dict[Any, int] | None = None) -> None:
        """Manually commit offsets"""
        if not self._started:
            await self.start()
        
        await self.consumer.commit(offsets)
    
    async def pause(self, partitions: list[Any] | None = None) -> None:
        """Pause consumption from partitions"""
        if not self._started:
            await self.start()
        
        if partitions:
            self.consumer.pause(*partitions)
        else:
            # Pause all assigned partitions
            assigned = self.consumer.assignment()
            if assigned:
                self.consumer.pause(*assigned)
    
    async def resume(self, partitions: list[Any] | None = None) -> None:
        """Resume consumption from partitions"""
        if not self._started:
            await self.start()
        
        if partitions:
            self.consumer.resume(*partitions)
        else:
            # Resume all assigned partitions
            assigned = self.consumer.assignment()
            if assigned:
                self.consumer.resume(*assigned)