"""
Kafka producer operations for GeneralOperate
"""

from typing import Any, Callable
import asyncio

try:
    import aiokafka
    from aiokafka import AIOKafkaProducer
except ImportError:
    aiokafka = None
    AIOKafkaProducer = None

from .kafka_operate import KafkaOperate, T
from .utils.serializers import SerializerFactory
from .exceptions import KafkaProducerException, KafkaConfigurationException


class KafkaProducerOperate(KafkaOperate[T]):
    """
    Kafka producer operations
    Supports synchronous, asynchronous, batch, and transactional sending
    """
    
    def __init__(
        self,
        bootstrap_servers: str | list[str],
        client_id: str | None = None,
        config: dict[str, Any] | None = None,
        serializer: Callable[[T], bytes] | None = None,
        key_serializer: Callable[[Any], bytes] | None = None,
    ):
        if aiokafka is None:
            from ..core.exceptions import ErrorCode, ErrorContext
            raise KafkaConfigurationException(
                code=ErrorCode.KAFKA_CONFIGURATION_ERROR,
                message="aiokafka is not installed. Please install it with: pip install aiokafka",
                context=ErrorContext(operation="kafka_producer_init", details={"missing_dependency": "aiokafka"})
            )
        
        super().__init__(bootstrap_servers, client_id, config)
        
        # Set up serializers
        serializer_factory = SerializerFactory()
        self.serializer = serializer or serializer_factory.get_value_serializer("json")
        self.key_serializer = key_serializer or serializer_factory.get_key_serializer("default")
        
        # Producer configuration
        base_config = self._get_base_config()
        self.producer_config = {
            **base_config,
            "value_serializer": self.serializer,
            "key_serializer": self.key_serializer,
            "acks": self.config.get("acks", "all"),
            "enable_idempotence": self.config.get("enable_idempotence", True),
            "compression_type": self.config.get("compression_type", "gzip"),
            "max_batch_size": self.config.get("max_batch_size", 16384),
            "linger_ms": self.config.get("linger_ms", 10),
            **self.config.get("producer_config", {})
        }
        
        self.producer: AIOKafkaProducer | None = None
    
    async def start(self) -> None:
        """Start the producer"""
        if self._started:
            return
        
        try:
            self.producer = AIOKafkaProducer(**self.producer_config)
            await self.producer.start()
            self._started = True
            self.logger.info("Kafka producer started", config=self.producer_config)
        except Exception as e:
            self._handle_error(e, "start producer")
    
    async def stop(self) -> None:
        """Stop the producer"""
        if self.producer and self._started:
            try:
                await self.producer.stop()
                self._started = False
                self.logger.info("Kafka producer stopped")
            except Exception as e:
                self.logger.warning(f"Error stopping producer: {e}")
    
    async def send_event(
        self,
        topic: str,
        value: T,
        key: str | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, bytes] | None = None,
    ) -> Any:
        """
        Send a single event
        
        Args:
            topic: Target topic
            value: Event data
            key: Message key (for partitioning)
            partition: Specific partition
            timestamp_ms: Timestamp
            headers: Message headers
        
        Returns:
            RecordMetadata: Contains partition, offset, and other metadata
        """
        if not self._started:
            await self.start()
        
        try:
            # Convert headers format
            kafka_headers = [(k, v) for k, v in headers.items()] if headers else None
            
            # Send message
            metadata = await self.producer.send_and_wait(
                topic=topic,
                value=value,
                key=key,
                partition=partition,
                timestamp_ms=timestamp_ms,
                headers=kafka_headers
            )
            
            self.logger.info(
                "Event sent successfully",
                topic=topic,
                partition=metadata.partition,
                offset=metadata.offset,
                key=key
            )
            
            return metadata
            
        except Exception as e:
            self._handle_error(e, f"send event to {topic}")
    
    async def send_batch(
        self,
        topic: str,
        messages: list[tuple[str | None, T, dict[str, bytes] | None]],
        partition: int | None = None,
    ) -> list[Any] | None:
        """
        Send events in batch
        
        Args:
            topic: Target topic
            messages: List of (key, value, headers) tuples
            partition: Specific partition
        
        Returns:
            list[RecordMetadata]: Metadata for each message
        """
        if not self._started:
            await self.start()
        
        try:
            # Handle empty batch case
            if not messages:
                return []
            
            # Send all messages concurrently
            tasks = []
            for key, value, headers in messages:
                kafka_headers = [(k, v) for k, v in headers.items()] if headers else None
                
                task = self.producer.send_and_wait(
                    topic=topic,
                    value=value,
                    key=key,
                    partition=partition,
                    headers=kafka_headers
                )
                tasks.append(task)
            
            # Wait for all messages to be sent
            results = await asyncio.gather(*tasks)
            
            self.logger.info(
                "Batch sent successfully",
                topic=topic,
                message_count=len(messages),
                batch_count=len(results)
            )
            
            return results
            
        except Exception as e:
            self._handle_error(e, f"send batch to {topic}")
    
    async def send_event_async(
        self,
        topic: str,
        value: T,
        key: str | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, bytes] | None = None,
    ) -> Any:
        """
        Send event asynchronously (fire-and-forget)
        
        Returns a future that can be awaited later for the result
        """
        if not self._started:
            await self.start()
        
        try:
            # Convert headers format
            kafka_headers = [(k, v) for k, v in headers.items()] if headers else None
            
            # Send message asynchronously
            future = await self.producer.send(
                topic=topic,
                value=value,
                key=key,
                partition=partition,
                timestamp_ms=timestamp_ms,
                headers=kafka_headers
            )
            
            self.logger.debug(
                "Event sent asynchronously",
                topic=topic,
                key=key
            )
            
            return future
            
        except Exception as e:
            self._handle_error(e, f"send async event to {topic}")
    
    async def flush(self, timeout: float | None = None) -> None:
        """Flush pending messages"""
        if not self._started or not self.producer:
            return
        
        try:
            await self.producer.flush(timeout)
            self.logger.debug("Producer flushed successfully")
        except Exception as e:
            self._handle_error(e, "flush producer")