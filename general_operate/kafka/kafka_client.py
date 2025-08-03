"""
Kafka Client with Modern Patterns
Fixes critical bugs and implements best practices for resilient event processing
"""

import asyncio
import json
import ssl
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any

import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import KafkaConnectionError, KafkaTimeoutError
from .manager_config import RetryConfig

logger = structlog.get_logger()

# Re-export for backward compatibility
__all__ = [
    "RetryConfig",
    "MessageStatus", 
    "CircuitOpenError",
    "ProcessingResult",
    "EventMessage",
    "CircuitBreaker",
    "KafkaAsyncConsumer",
    "KafkaAsyncProducer",
    "KafkaEventBus",
    "KafkaAsyncAdmin",
]


class MessageStatus(Enum):
    """Message processing status"""

    SUCCESS = "success"
    RETRY = "retry"
    FAILED = "failed"
    DEAD_LETTER = "dead_letter"
    SKIP = "skip"


class CircuitOpenError(Exception):
    """Exception raised when circuit breaker is open"""
    pass


# RetryConfig moved to config module to eliminate duplication


@dataclass
class ProcessingResult:
    """Result of message processing"""

    status: MessageStatus
    error_message: str | None = None
    retry_count: int = 0
    should_commit: bool = True


@dataclass
class EventMessage:
    """event message structure with retry metadata"""

    event_type: str
    tenant_id: str
    user_id: str | None = None
    data: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime | str | None = None
    correlation_id: str | None = None
    retry_count: int = 0
    first_attempt_timestamp: datetime | str | None = None

    def __post_init__(self):
        # Convert timestamp to datetime if it's a string
        if self.timestamp is None:
            self.timestamp = datetime.now(UTC)
        elif isinstance(self.timestamp, str):
            self.timestamp = datetime.fromisoformat(self.timestamp.replace('Z', '+00:00'))
        
        if self.correlation_id is None:
            self.correlation_id = str(uuid.uuid4())
            
        # Handle first_attempt_timestamp
        if self.first_attempt_timestamp is None:
            self.first_attempt_timestamp = self.timestamp
        elif isinstance(self.first_attempt_timestamp, str):
            self.first_attempt_timestamp = datetime.fromisoformat(self.first_attempt_timestamp.replace('Z', '+00:00'))

    def to_json(self) -> str:
        """Convert to JSON string with sanitization"""
        sanitized_data = self._sanitize_data(self._to_serializable_dict())
        return json.dumps(sanitized_data, ensure_ascii=False)

    @classmethod
    def from_json(cls, json_str: str) -> "EventMessage":
        """Create from JSON string"""
        data = json.loads(json_str)
        return cls(**data)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary (with JSON-serializable data)"""
        return self._to_serializable_dict()

    def _to_serializable_dict(self) -> dict[str, Any]:
        """Convert to dictionary with datetime objects as ISO strings"""
        data = asdict(self)
        # Convert datetime objects to ISO strings for serialization
        if isinstance(data.get('timestamp'), datetime):
            data['timestamp'] = data['timestamp'].isoformat()
        if isinstance(data.get('first_attempt_timestamp'), datetime):
            data['first_attempt_timestamp'] = data['first_attempt_timestamp'].isoformat()
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "EventMessage":
        """Create from dictionary"""
        return cls(**data)

    @staticmethod
    def _sanitize_data(data: dict[str, Any]) -> dict[str, Any]:
        """Remove sensitive information from event data with patterns"""
        import re

        # sensitive patterns with case-insensitive regex
        sensitive_patterns = [
            re.compile(r".*password.*", re.IGNORECASE),
            re.compile(r".*secret.*", re.IGNORECASE),
            re.compile(r".*token.*", re.IGNORECASE),
            re.compile(r".*key.*", re.IGNORECASE),
            re.compile(r".*credential.*", re.IGNORECASE),
            re.compile(r".*ssn.*", re.IGNORECASE),
            re.compile(r".*social.?security.*", re.IGNORECASE),
            re.compile(r".*credit.?card.*", re.IGNORECASE),
            re.compile(r".*card.?number.*", re.IGNORECASE),
            re.compile(r".*pin.*", re.IGNORECASE),
            re.compile(r".*cvv.*", re.IGNORECASE),
            re.compile(r".*cvc.*", re.IGNORECASE),
            re.compile(r".*account.?number.*", re.IGNORECASE),
            re.compile(r".*routing.?number.*", re.IGNORECASE),
            re.compile(r".*auth.*", re.IGNORECASE),
            re.compile(r".*bearer.*", re.IGNORECASE),
            re.compile(r".*api.?key.*", re.IGNORECASE),
            re.compile(r".*private.*", re.IGNORECASE),
        ]

        # Value patterns for encoded/obfuscated data
        value_patterns = [
            re.compile(r"^[A-Za-z0-9+/]{32,}={0,2}$"),  # Base64-like
            re.compile(r"^[a-f0-9]{32,}$"),  # Hex strings (long)
            re.compile(r"^Bearer\s+.+", re.IGNORECASE),  # Bearer tokens
            re.compile(r"^\d{13,19}$"),  # Credit card numbers
            re.compile(r"^\d{3}-\d{2}-\d{4}$"),  # SSN format
        ]

        def is_sensitive_key(key: str) -> bool:
            """Check if key matches sensitive patterns"""
            return any(pattern.match(key) for pattern in sensitive_patterns)

        def is_sensitive_value(value: Any) -> bool:
            """Check if value appears to be sensitive data"""
            if not isinstance(value, str) or len(value) < 8:
                return False
            return any(pattern.match(value) for pattern in value_patterns)

        def sanitize_dict(d: dict[str, Any]) -> dict[str, Any]:
            sanitized = {}
            for k, v in d.items():
                if is_sensitive_key(k) or is_sensitive_value(v):
                    sanitized[k] = "[REDACTED]"
                elif isinstance(v, dict):
                    sanitized[k] = sanitize_dict(v)
                elif isinstance(v, list):
                    sanitized[k] = [
                        sanitize_dict(item)
                        if isinstance(item, dict)
                        else "[REDACTED]"
                        if is_sensitive_value(item)
                        else item
                        for item in v
                    ]
                else:
                    sanitized[k] = v
            return sanitized

        return sanitize_dict(data)


class CircuitBreaker:
    """Circuit breaker for message processing"""

    def __init__(self, threshold: int = 5, timeout: float = 60.0):
        self.threshold = threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "closed"  # closed, open, half_open

    @property
    def is_closed(self) -> bool:
        """Check if circuit breaker is closed"""
        return self.state == "closed"

    def record_success(self):
        """Record a successful operation"""
        self.failure_count = 0
        self.state = "closed"

    def record_failure(self):
        """Record a failed operation"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.threshold:
            self.state = "open"

    async def call(self, func: Callable[[], Awaitable[Any]]) -> Any:
        """Execute a function with circuit breaker protection"""
        # Check if we should allow execution
        if self.state == "open":
            # Check if timeout has passed
            if self.last_failure_time and (time.time() - self.last_failure_time) > self.timeout:
                self.state = "half_open"
            else:
                raise CircuitOpenError("Circuit breaker is open")
        
        try:
            result = await func()
            self.record_success()
            return result
        except Exception as e:
            self.record_failure()
            raise e

    def can_execute(self) -> bool:
        """Check if circuit breaker allows execution"""
        if self.state == "closed":
            return True
        elif self.state == "open":
            if self.last_failure_time and (time.time() - self.last_failure_time) > self.timeout:
                self.state = "half_open"
                return True
            return False
        else:  # half_open
            return True

    def on_success(self):
        """Record successful execution"""
        self.failure_count = 0
        self.state = "closed"

    def on_failure(self):
        """Record failed execution"""
        self.failure_count += 1
        self.last_failure_time = time.time()

        if self.failure_count >= self.threshold:
            self.state = "open"


class KafkaAsyncConsumer:
    """Kafka consumer with retry logic and circuit breaker"""

    def __init__(
        self,
        topics: list[str],
        group_id: str,
        message_handler: Callable[[EventMessage], Awaitable[ProcessingResult]],
        kafka_config: dict[str, Any],
        service_name: str,
        retry_config: RetryConfig = None,
        dead_letter_topic: str | None = None,
        enable_dlq: bool = True,
        filter_event_types: list[str] | None = None,
        circuit_breaker_threshold: int = 5,
        circuit_breaker_timeout: float = 60.0,
    ):
        self.topics = topics
        self.group_id = group_id
        self.message_handler = message_handler
        self.consumer: AIOKafkaConsumer | None = None
        self.dead_letter_producer: AIOKafkaProducer | None = None
        self._running = False
        self.kafka_config = kafka_config
        self.service_name = service_name
        self.retry_config = retry_config or RetryConfig()
        self.dead_letter_topic = dead_letter_topic or f"{topics[0]}.dead_letter"
        self.enable_dlq = enable_dlq
        self.filter_event_types = filter_event_types or []
        self.circuit_breaker = CircuitBreaker(
            threshold=circuit_breaker_threshold,
            timeout=circuit_breaker_timeout
        )

    async def start(self):
        """Start consumer and dead letter producer"""
        try:
            # Build consumer configuration with SSL/SASL support
            consumer_config = {
                "bootstrap_servers": self.kafka_config["bootstrap_servers"],
                "group_id": self.group_id,
                "client_id": f"{self.service_name}-consumer-{self.group_id}",
                "auto_offset_reset": self.kafka_config["auto_offset_reset"],
                "enable_auto_commit": False,  # Manual commit for better control
                "value_deserializer": lambda m: m.decode("utf-8") if m else None,
                "key_deserializer": lambda k: k.decode("utf-8") if k else None,
                "isolation_level": "read_committed",
                "session_timeout_ms": self.kafka_config.get("session_timeout_ms", 30000),
                "heartbeat_interval_ms": self.kafka_config.get("heartbeat_interval_ms", 3000),
                "max_poll_records": self.kafka_config.get("max_poll_records", 500),
                "max_poll_interval_ms": self.kafka_config.get("max_poll_interval_ms", 300000),
            }
            
            # Add security protocol configuration
            if "security_protocol" in self.kafka_config:
                consumer_config["security_protocol"] = self.kafka_config["security_protocol"]
            
            # Add SSL configuration if present
            ssl_configs = [
                "ssl_check_hostname", "ssl_cafile", "ssl_certfile", "ssl_keyfile",
                "ssl_password", "ssl_context", "ssl_crlfile", "ssl_ciphers"
            ]
            for ssl_key in ssl_configs:
                if ssl_key in self.kafka_config:
                    consumer_config[ssl_key] = self.kafka_config[ssl_key]
            
            # Add SASL configuration if present
            sasl_configs = [
                "sasl_mechanism", "sasl_plain_username", "sasl_plain_password",
                "sasl_kerberos_service_name", "sasl_kerberos_domain_name",
                "sasl_oauth_token_provider"
            ]
            for sasl_key in sasl_configs:
                if sasl_key in self.kafka_config:
                    consumer_config[sasl_key] = self.kafka_config[sasl_key]

            # Start main consumer
            self.consumer = AIOKafkaConsumer(**consumer_config)

            # Start dead letter producer only if DLQ is enabled
            if self.enable_dlq:
                # Build DLQ producer configuration with SSL/SASL support
                dlq_producer_config = {
                    "bootstrap_servers": self.kafka_config["bootstrap_servers"],
                    "value_serializer": lambda v: v.encode("utf-8") if isinstance(v, str) else v,
                    "key_serializer": lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                    "client_id": f"{self.service_name}-dlq-producer",
                    "acks": self.kafka_config["acks"],
                    "enable_idempotence": self.kafka_config["enable_idempotence"],
                    "request_timeout_ms": self.kafka_config.get("request_timeout_ms", 30000),
                }
                
                # Add security configuration to DLQ producer
                if "security_protocol" in self.kafka_config:
                    dlq_producer_config["security_protocol"] = self.kafka_config["security_protocol"]
                
                for ssl_key in ssl_configs:
                    if ssl_key in self.kafka_config:
                        dlq_producer_config[ssl_key] = self.kafka_config[ssl_key]
                
                for sasl_key in sasl_configs:
                    if sasl_key in self.kafka_config:
                        dlq_producer_config[sasl_key] = self.kafka_config[sasl_key]

                self.dead_letter_producer = AIOKafkaProducer(**dlq_producer_config)
                await self.dead_letter_producer.start()

            await self.consumer.start()
            
            # Subscribe to topics after starting
            self.consumer.subscribe(self.topics)

            logger.info(
                "Kafka consumer started",
                topics=self.topics,
                group_id=self.group_id,
                dead_letter_topic=self.dead_letter_topic
                if self.enable_dlq
                else "disabled",
                dlq_enabled=self.enable_dlq,
            )

        except Exception as e:
            logger.error("Failed to start Kafka consumer", error=str(e))
            raise

    async def stop(self):
        """Stop consumer and producer"""
        self._running = False
        if self.consumer:
            await self.consumer.stop()
        if self.dead_letter_producer:
            await self.dead_letter_producer.stop()
        logger.info("Kafka consumer stopped", group_id=self.group_id)

    async def consume(self):
        """Start consuming messages with error handling"""
        if not self.consumer:
            await self.start()

        self._running = True

        try:
            async for message in self.consumer:
                if not self._running:
                    break

                # Check circuit breaker
                if not self.circuit_breaker.can_execute():
                    logger.warning(
                        "Circuit breaker is open, skipping message processing",
                        group_id=self.group_id,
                    )
                    await asyncio.sleep(1)
                    continue

                await self._process_message_with_retry(message)

        except Exception as e:
            logger.error("Error in consumer loop", error=str(e))
            raise

    async def _process_message_with_retry(self, message) -> None:
        """Process message with retry logic and proper offset management"""
        try:
            # Parse event message
            event = EventMessage.from_json(message.value)

            # Apply event type filtering if configured
            if self.filter_event_types and not self._should_process_event(
                event.event_type
            ):
                logger.debug(
                    "Event filtered out - not matching configured event types",
                    topic=message.topic,
                    partition=message.partition,
                    offset=message.offset,
                    event_type=event.event_type,
                    filter_event_types=self.filter_event_types,
                    correlation_id=event.correlation_id,
                )
                # Skip this message by committing the offset
                await self.consumer.commit()
                return

            logger.info(
                "Processing event with retry support",
                topic=message.topic,
                partition=message.partition,
                offset=message.offset,
                event_type=event.event_type,
                correlation_id=event.correlation_id,
                retry_count=event.retry_count,
            )

            # Process message
            result = await self._execute_with_circuit_breaker(event)

            # Handle result
            await self._handle_processing_result(message, event, result)

        except json.JSONDecodeError as e:
            logger.error(
                "Failed to decode event message - committing to skip",
                message_value=message.value[:200],  # Truncate for logging
                error=str(e),
            )
            # Skip invalid messages by committing
            await self.consumer.commit()

        except Exception as e:
            logger.error(
                "Unexpected error processing message",
                topic=message.topic,
                offset=message.offset,
                error=str(e),
            )
            # Don't commit on unexpected errors - let retry logic handle
            # This prevents message loss

    async def _execute_with_circuit_breaker(
        self, event: EventMessage
    ) -> ProcessingResult:
        """Execute message handler with circuit breaker protection"""
        try:
            result = await self.message_handler(event)

            if result.status == MessageStatus.SUCCESS:
                self.circuit_breaker.on_success()
            else:
                self.circuit_breaker.on_failure()

            return result

        except Exception as e:
            self.circuit_breaker.on_failure()
            return ProcessingResult(
                status=MessageStatus.RETRY, error_message=str(e), should_commit=False
            )

    async def _handle_processing_result(
        self, message, event: EventMessage, result: ProcessingResult
    ) -> None:
        """Handle processing result based on status"""

        if result.status == MessageStatus.SUCCESS:
            # Success - commit offset
            await self.consumer.commit()
            logger.debug(
                "Message processed successfully",
                correlation_id=event.correlation_id,
                offset=message.offset,
            )

        elif result.status == MessageStatus.SKIP:
            # Skip message - commit offset to avoid reprocessing
            await self.consumer.commit()
            logger.info(
                "Message skipped as requested",
                correlation_id=event.correlation_id,
                offset=message.offset,
            )

        elif result.status == MessageStatus.RETRY:
            # Check if we should retry or send to DLQ
            if event.retry_count < self.retry_config.max_retries:
                await self._schedule_retry(message, event, result)
            else:
                # Only send to DLQ if enabled, otherwise commit and log error
                if self.enable_dlq:
                    await self._send_to_dead_letter_queue(message, event, result)
                else:
                    await self._handle_failed_message_without_dlq(
                        message, event, result
                    )

        elif result.status == MessageStatus.DEAD_LETTER:
            # Send to DLQ if enabled, otherwise commit and log error
            if self.enable_dlq:
                await self._send_to_dead_letter_queue(message, event, result)
            else:
                await self._handle_failed_message_without_dlq(message, event, result)

        # Always commit if result indicates we should
        if result.should_commit:
            await self.consumer.commit()

    async def _schedule_retry(
        self, message, event: EventMessage, result: ProcessingResult
    ) -> None:
        """Schedule message for retry"""
        retry_delay = self._calculate_retry_delay(event.retry_count)

        logger.info(
            "Scheduling message retry",
            correlation_id=event.correlation_id,
            retry_count=event.retry_count + 1,
            retry_delay=retry_delay,
            error=result.error_message,
        )

        # Increment retry count
        event.retry_count += 1

        # Wait before retry (in production, consider using Kafka delay queues)
        await asyncio.sleep(retry_delay)

        # Retry processing
        retry_result = await self._execute_with_circuit_breaker(event)
        await self._handle_processing_result(message, event, retry_result)

    async def _send_to_dead_letter_queue(
        self, message, event: EventMessage, result: ProcessingResult
    ) -> None:
        """Send message to dead letter queue"""
        if not self.enable_dlq:
            logger.error(
                "Attempted to send message to DLQ but DLQ is disabled",
                correlation_id=event.correlation_id,
            )
            await self._handle_failed_message_without_dlq(message, event, result)
            return

        if not self.dead_letter_producer:
            logger.error(
                "Dead letter producer not initialized",
                correlation_id=event.correlation_id,
            )
            await self._handle_failed_message_without_dlq(message, event, result)
            return

        try:
            # Add DLQ metadata
            dlq_event = EventMessage(
                event_type=f"dlq.{event.event_type}",
                tenant_id=event.tenant_id,
                user_id=event.user_id,
                data=event.data,
                metadata={
                    **event.metadata,
                    "dlq_reason": result.error_message,
                    "original_topic": message.topic,
                    "original_partition": message.partition,
                    "original_offset": message.offset,
                    "final_retry_count": event.retry_count,
                    "dlq_timestamp": datetime.now(UTC).isoformat(),
                },
                correlation_id=event.correlation_id,
                retry_count=event.retry_count,
            )

            # Send to dead letter queue
            await self.dead_letter_producer.send_and_wait(
                topic=self.dead_letter_topic,
                value=dlq_event.to_json(),
                key=event.tenant_id,
                headers=[
                    ("dlq_reason", (result.error_message or "").encode("utf-8")),
                    ("original_topic", message.topic.encode("utf-8")),
                    ("correlation_id", event.correlation_id.encode("utf-8")),
                ],
            )

            logger.error(
                "Message sent to dead letter queue",
                correlation_id=event.correlation_id,
                dead_letter_topic=self.dead_letter_topic,
                retry_count=event.retry_count,
                error=result.error_message,
            )

            # Commit original message offset
            await self.consumer.commit()

        except Exception as e:
            logger.error(
                "Failed to send message to dead letter queue",
                correlation_id=event.correlation_id,
                error=str(e),
            )
            # Don't commit - let the message be retried

    async def _handle_failed_message_without_dlq(
        self, message, event: EventMessage, result: ProcessingResult
    ) -> None:
        """Handle failed message when DLQ is disabled"""
        logger.error(
            "Message processing failed and DLQ is disabled - committing to prevent infinite retry",
            correlation_id=event.correlation_id,
            topic=message.topic,
            partition=message.partition,
            offset=message.offset,
            retry_count=event.retry_count,
            error=result.error_message,
            dlq_enabled=False,
        )

        # Commit the message to prevent infinite processing
        # This is the trade-off when DLQ is disabled - we lose failed messages
        # but prevent the consumer from getting stuck
        await self.consumer.commit()

    def _calculate_retry_delay(self, retry_count: int) -> float:
        """Calculate exponential backoff delay with jitter"""
        delay = min(
            self.retry_config.base_delay
            * (self.retry_config.exponential_base**retry_count),
            self.retry_config.max_delay,
        )

        if self.retry_config.jitter:
            import random

            delay = delay * (0.5 + random.random() * 0.5)  # Add ±50% jitter

        return delay

    def _should_process_event(self, event_type: str) -> bool:
        """
        Check if event should be processed based on configured filter_event_types.
        Uses efficient prefix matching with any() and startswith().

        Args:
            event_type: The event type to check (e.g., "auth.login", "user.created")

        Returns:
            True if event should be processed, False if it should be filtered out
        """
        if not self.filter_event_types:
            # No filters configured, process all events
            return True

        # Use efficient prefix matching - event is processed if it matches any configured prefix
        return any(event_type.startswith(prefix) for prefix in self.filter_event_types)


class KafkaAsyncAdmin:
    """
    Modern async Kafka admin client with comprehensive topic management
    Implements resource lifecycle, dependency injection, and proper error handling
    """

    def __init__(
        self,
        kafka_config: dict[str, Any],
        service_name: str = "default",
        *,
        request_timeout_ms: int = 30000,
        connection_timeout_ms: int = 10000,
        retry_backoff_ms: int = 100,
        max_retries: int = 3,
    ):
        """
        Initialize KafkaAsyncAdmin with modern configuration patterns

        Args:
            kafka_config: Kafka connection configuration
            service_name: Service identifier for client_id
            request_timeout_ms: Request timeout in milliseconds
            connection_timeout_ms: Connection timeout in milliseconds
            retry_backoff_ms: Retry backoff in milliseconds
            max_retries: Maximum number of retry attempts
        """
        self.kafka_config = kafka_config
        self.service_name = service_name
        self.request_timeout_ms = request_timeout_ms
        self.connection_timeout_ms = connection_timeout_ms
        self.retry_backoff_ms = retry_backoff_ms
        self.max_retries = max_retries

        # Internal state
        self._admin_client: AIOKafkaAdminClient | None = None
        self._started = False
        self._lock = asyncio.Lock()
        self.logger = logger.bind(component="kafka_async_admin", service=service_name)

    async def __aenter__(self) -> "KafkaAsyncAdmin":
        """Async context manager entry"""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit with proper cleanup"""
        await self.stop()

    @property
    def is_started(self) -> bool:
        """Check if admin client is started"""
        return self._started and self._admin_client is not None

    async def start(self) -> None:
        """Start admin client with connection management"""
        async with self._lock:
            if self._started:
                self.logger.debug("Admin client already started")
                return

            try:
                # Import here to avoid circular imports
                from aiokafka.admin import AIOKafkaAdminClient

                self._admin_client = AIOKafkaAdminClient(
                    bootstrap_servers=self.kafka_config.get(
                        "bootstrap_servers", "localhost:9092"
                    ),
                    client_id=f"{self.service_name}-admin",
                    request_timeout_ms=self.request_timeout_ms,
                    connections_max_idle_ms=self.kafka_config.get(
                        "connections_max_idle_ms", 540000
                    ),
                    retry_backoff_ms=self.retry_backoff_ms,
                    # Security configuration
                    security_protocol=self.kafka_config.get(
                        "security_protocol", "PLAINTEXT"
                    ),
                    ssl_context=self._create_ssl_context()
                    if self.kafka_config.get("security_protocol") in ["SSL", "SASL_SSL"]
                    else None,
                )

                # Start with timeout
                start_task = asyncio.create_task(self._admin_client.start())
                await asyncio.wait_for(
                    start_task, timeout=self.connection_timeout_ms / 1000
                )

                self._started = True
                self.logger.info(
                    "Kafka admin client started successfully",
                    bootstrap_servers=self.kafka_config.get("bootstrap_servers"),
                    timeout_ms=self.request_timeout_ms,
                )

            except TimeoutError:
                error_msg = f"Failed to start Kafka admin client within {self.connection_timeout_ms}ms"
                self.logger.error(error_msg)
                await self._cleanup_failed_start()
                raise KafkaConnectionError(error_msg)
            except Exception as e:
                self.logger.error("Failed to start Kafka admin client", error=str(e))
                await self._cleanup_failed_start()
                raise

    async def stop(self) -> None:
        """Stop admin client with proper cleanup"""
        async with self._lock:
            if not self._started or not self._admin_client:
                return

            try:
                await self._admin_client.close()
                self.logger.info("Kafka admin client stopped successfully")
            except Exception as e:
                self.logger.error("Error stopping Kafka admin client", error=str(e))
            finally:
                self._admin_client = None
                self._started = False

    async def _cleanup_failed_start(self) -> None:
        """Clean up after failed start attempt"""
        if self._admin_client:
            try:
                await self._admin_client.close()
            except Exception as e:
                self.logger.debug("Error cleaning up failed admin client", error=str(e))
            finally:
                self._admin_client = None
                self._started = False

    def _create_ssl_context(self) -> ssl.SSLContext:
        """Create SSL context from configuration with security"""

        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)

        # Set minimum TLS version for security
        context.minimum_version = ssl.TLSVersion.TLSv1_2

        # Configure secure cipher suites
        context.set_ciphers(
            "ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20:!aNULL:!MD5:!DSS"
        )

        # Hostname verification (can be disabled for testing)
        if self.kafka_config.get("ssl_check_hostname", True):
            context.check_hostname = True
            context.verify_mode = ssl.CERT_REQUIRED
        else:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            self.logger.warning(
                "SSL hostname verification disabled - not recommended for production"
            )

        # Load CA certificate if provided
        if self.kafka_config.get("ssl_cafile"):
            context.load_verify_locations(self.kafka_config["ssl_cafile"])
            self.logger.debug(
                "SSL CA certificate loaded", cafile=self.kafka_config["ssl_cafile"]
            )

        # Load client certificate and key if provided
        if self.kafka_config.get("ssl_certfile") and self.kafka_config.get(
            "ssl_keyfile"
        ):
            context.load_cert_chain(
                self.kafka_config["ssl_certfile"],
                self.kafka_config["ssl_keyfile"],
                password=self.kafka_config.get("ssl_password"),
            )
            self.logger.debug(
                "SSL client certificate loaded",
                certfile=self.kafka_config["ssl_certfile"],
            )

        # Additional security options
        context.options |= ssl.OP_NO_SSLv2
        context.options |= ssl.OP_NO_SSLv3
        context.options |= ssl.OP_NO_TLSv1
        context.options |= ssl.OP_NO_TLSv1_1
        context.options |= ssl.OP_SINGLE_DH_USE
        context.options |= ssl.OP_SINGLE_ECDH_USE

        return context

    async def ensure_started(self) -> None:
        """Ensure admin client is started, start if not"""
        if not self.is_started:
            await self.start()

    async def create_topics(
        self,
        topics: list[str],
        *,
        validate_only: bool = False,
        timeout_ms: int | None = None,
    ) -> dict[str, bool]:
        """
        Create Kafka topics with comprehensive error handling

        Args:
            topics: List of topic configurations to create
            validate_only: Only validate topic creation without actually creating
            timeout_ms: Optional timeout override

        Returns:
            Dictionary mapping topic names to creation success status
        """
        if not topics:
            self.logger.info("No topics to create")
            return {}

        await self.ensure_started()

        # Convert to NewTopic objects
        new_topics = []
        for topic in topics:
            try:
                new_topic = NewTopic(
                    name=topic,
                    num_partitions=3,
                    replication_factor=1,
                    topic_configs={
                        "cleanup.policy": "delete",
                        "retention.ms": "604800000",  # 7 days default
                    },
                )
                new_topics.append(new_topic)

                self.logger.debug(
                    "Prepared topic for creation",
                    topic=topic,
                    partitions=3,
                    replication_factor=1,
                    config={
                        "cleanup.policy": "delete",
                        "retention.ms": "604800000",  # 7 days default
                    },
                )
            except Exception as e:
                self.logger.error(
                    "Invalid topic configuration",
                    topic=topic,
                    error=str(e),
                )
                continue

        if not new_topics:
            self.logger.warning("No valid topics to create after validation")
            return {}

        results = {}
        retry_count = 0
        creation_timeout = (
            timeout_ms or self.request_timeout_ms
        )  # Initialize here to avoid reference error

        while retry_count <= self.max_retries:
            try:
                self.logger.info(
                    "Creating Kafka topics",
                    topic_count=len(new_topics),
                    validate_only=validate_only,
                    retry_attempt=retry_count,
                )

                # Create topics with timeout
                create_task = self._admin_client.create_topics(
                    new_topics, validate_only=validate_only
                )
                await asyncio.wait_for(create_task, timeout=creation_timeout / 1000)

                # Mark all topics as successful since create_topics completed without exception
                for topic in topics:
                    results[topic] = True
                    self.logger.info(
                        "Topic created successfully",
                        topic=topic,
                        validate_only=validate_only,
                    )

                # If we got here, we successfully attempted all topics
                break

            except TimeoutError:
                retry_count += 1
                if retry_count > self.max_retries:
                    self.logger.error(
                        "Topic creation timed out after all retries",
                        timeout_ms=creation_timeout,
                        max_retries=self.max_retries,
                    )
                    # Mark all topics as failed
                    for topic in topics:
                        results[topic] = False
                    break

                wait_time = self.retry_backoff_ms * (2**retry_count) / 1000
                self.logger.warning(
                    "Topic creation timed out, retrying",
                    retry_attempt=retry_count,
                    wait_time_seconds=wait_time,
                )
                await asyncio.sleep(wait_time)

            except Exception as e:
                retry_count += 1
                if retry_count > self.max_retries:
                    self.logger.error(
                        "Topic creation failed after all retries",
                        error=str(e),
                        max_retries=self.max_retries,
                    )
                    # Mark all topics as failed
                    for topic in topics:
                        results[topic] = False
                    break

                wait_time = self.retry_backoff_ms * (2**retry_count) / 1000
                self.logger.warning(
                    "Topic creation failed, retrying",
                    error=str(e),
                    retry_attempt=retry_count,
                    wait_time_seconds=wait_time,
                )
                await asyncio.sleep(wait_time)

        # Log final summary
        successful_topics = sum(1 for success in results.values() if success)
        self.logger.info(
            "Topic creation completed",
            total_topics=len(topics),
            successful=successful_topics,
            failed=len(topics) - successful_topics,
            results=results,
        )

        return results

    async def list_topics(self, *, timeout_ms: int | None = None) -> set[str]:
        """
        List all topics in the cluster

        Returns:
            Set of topic names
        """
        await self.ensure_started()

        try:
            timeout = (timeout_ms or self.request_timeout_ms) / 1000

            # Get cluster metadata to list topics with timeout
            metadata_task = self._admin_client.describe_cluster()
            metadata = await asyncio.wait_for(metadata_task, timeout=timeout)

            # Extract topic names from metadata
            # Note: This is a simplified implementation - actual aiokafka API may differ
            topics = set()
            if hasattr(metadata, "topics"):
                topics = set(metadata.topics.keys())

            self.logger.debug(
                "Listed topics successfully",
                topic_count=len(topics),
            )

            return topics

        except TimeoutError:
            self.logger.error(
                "Failed to list topics - operation timed out", timeout_ms=timeout_ms
            )
            raise
        except Exception as e:
            self.logger.error("Failed to list topics", error=str(e))
            raise

    async def describe_topics(
        self, topic_names: list[str], *, timeout_ms: int | None = None
    ) -> dict[str, dict[str, Any]]:
        """
        Describe topic configurations and metadata

        Args:
            topic_names: List of topic names to describe
            timeout_ms: Optional timeout override

        Returns:
            Dictionary mapping topic names to their metadata
        """
        await self.ensure_started()

        try:
            timeout = (timeout_ms or self.request_timeout_ms) / 1000
            # Note: AIOKafkaAdminClient might not have describe_topics directly
            # This is a placeholder for the actual implementation that will use the timeout
            self.logger.debug(
                "Describing topics",
                topics=topic_names,
                timeout_seconds=timeout,
            )

            # TODO: Implement actual topic description with timeout when API is available
            # For now, return empty dict but log the timeout for future implementation
            return {}

        except Exception as e:
            self.logger.error(
                "Failed to describe topics", topics=topic_names, error=str(e)
            )
            raise

    async def delete_topics(
        self,
        topic_names: list[str],
        *,
        timeout_ms: int | None = None,
    ) -> dict[str, bool]:
        """
        Delete topics with proper error handling

        Args:
            topic_names: List of topic names to delete
            timeout_ms: Optional timeout override

        Returns:
            Dictionary mapping topic names to deletion success status
        """
        await self.ensure_started()

        if not topic_names:
            self.logger.info("No topics to delete")
            return {}

        try:
            timeout = (timeout_ms or self.request_timeout_ms) / 1000
            delete_task = self._admin_client.delete_topics(topic_names)
            await asyncio.wait_for(delete_task, timeout=timeout)

            # Mark all topics as successfully deleted since delete_topics completed without exception
            results = {}
            for topic_name in topic_names:
                results[topic_name] = True
                self.logger.info("Topic deleted successfully", topic=topic_name)

            return results

        except Exception as e:
            self.logger.error(
                "Failed to delete topics", topics=topic_names, error=str(e)
            )
            # Mark all as failed
            return dict.fromkeys(topic_names, False)

    async def health_check(self) -> dict[str, Any]:
        """
        Perform admin client health check

        Returns:
            Health status information
        """
        try:
            if not self.is_started:
                return {
                    "status": "unhealthy",
                    "reason": "admin_client_not_started",
                    "started": False,
                }

            # Try to list topics as a health check
            start_time = asyncio.get_event_loop().time()
            try:
                await asyncio.wait_for(
                    self.list_topics(),
                    timeout=5.0,  # Quick health check timeout
                )
                response_time = asyncio.get_event_loop().time() - start_time

                return {
                    "status": "healthy",
                    "started": True,
                    "response_time_seconds": round(response_time, 3),
                    "service_name": self.service_name,
                }
            except TimeoutError:
                return {
                    "status": "unhealthy",
                    "reason": "health_check_timeout",
                    "started": True,
                }
        except Exception as e:
            return {
                "status": "unhealthy",
                "reason": str(e),
                "started": self.is_started,
            }


class KafkaEventBus:
    """Kafka event bus with modern patterns"""

    def __init__(self, kafka_config: dict[str, Any], service_name: str = "default"):
        self.producer = KafkaAsyncProducer(kafka_config, service_name)
        self.consumers: dict[str, KafkaAsyncConsumer] = {}
        self.kafka_config = kafka_config
        self.service_name = service_name

    async def start(self):
        """Start event bus"""
        await self.producer.start()
        logger.info("Kafka event bus started")

    async def stop(self):
        """Stop event bus"""
        # Stop all consumers
        for consumer in self.consumers.values():
            await consumer.stop()

        # Stop producer
        await self.producer.stop()

        logger.info("Kafka event bus stopped")

    async def subscribe_with_retry(
        self,
        topics: list[str],
        group_id: str,
        handler: Callable[[EventMessage], Awaitable[ProcessingResult]],
        service_name: str,
        retry_config: RetryConfig = None,
        dead_letter_topic: str | None = None,
        enable_dlq: bool = True,
        filter_event_types: list[str] | None = None,
        circuit_breaker_threshold: int = 5,
        circuit_breaker_timeout: float = 60.0,
    ) -> KafkaAsyncConsumer:
        """Subscribe to events with retry and DLQ support"""
        consumer = KafkaAsyncConsumer(
            topics=topics,
            group_id=group_id,
            message_handler=handler,
            kafka_config=self.kafka_config,
            service_name=service_name,
            retry_config=retry_config,
            dead_letter_topic=dead_letter_topic,
            enable_dlq=enable_dlq,
            filter_event_types=filter_event_types,
            circuit_breaker_threshold=circuit_breaker_threshold,
            circuit_breaker_timeout=circuit_breaker_timeout,
        )

        self.consumers[group_id] = consumer

        # Start consumer in background task
        asyncio.create_task(consumer.consume())

        return consumer

    async def publish(
        self,
        topic: str,
        event_type: str,
        tenant_id: str,
        data: dict[str, Any],
        user_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> None:
        """Publish event with features"""
        event = EventMessage(
            event_type=event_type,
            tenant_id=tenant_id,
            user_id=user_id,
            data=data,
            metadata=metadata,
            correlation_id=correlation_id,
        )

        await self.producer.send_event(topic, event)


# Keep the original classes for backward compatibility
class KafkaAsyncProducer:
    """Original async Kafka producer - kept for backward compatibility"""

    def __init__(self, kafka_config: dict[str, Any], service_name: str):
        self.producer: AIOKafkaProducer | None = None
        self.started = False
        self.kafka_config = kafka_config
        self.service_name = service_name

    async def start(self):
        """Start producer"""
        if self.started:
            return

        try:
            # Build producer configuration with SSL/SASL support
            producer_config = {
                "bootstrap_servers": self.kafka_config["bootstrap_servers"],
                "value_serializer": lambda v: v.encode("utf-8") if isinstance(v, str) else v,
                "key_serializer": lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                "client_id": f"{self.service_name}-producer",
                "acks": self.kafka_config["acks"],
                "enable_idempotence": self.kafka_config["enable_idempotence"],
                "compression_type": self.kafka_config.get("compression_type", "gzip"),
                "request_timeout_ms": self.kafka_config.get("request_timeout_ms", 30000),
            }
            
            # Add security protocol configuration
            if "security_protocol" in self.kafka_config:
                producer_config["security_protocol"] = self.kafka_config["security_protocol"]
            
            # Add SSL configuration if present
            ssl_configs = [
                "ssl_check_hostname", "ssl_cafile", "ssl_certfile", "ssl_keyfile",
                "ssl_password", "ssl_context", "ssl_crlfile", "ssl_ciphers"
            ]
            for ssl_key in ssl_configs:
                if ssl_key in self.kafka_config:
                    producer_config[ssl_key] = self.kafka_config[ssl_key]
            
            # Add SASL configuration if present
            sasl_configs = [
                "sasl_mechanism", "sasl_plain_username", "sasl_plain_password",
                "sasl_kerberos_service_name", "sasl_kerberos_domain_name",
                "sasl_oauth_token_provider"
            ]
            for sasl_key in sasl_configs:
                if sasl_key in self.kafka_config:
                    producer_config[sasl_key] = self.kafka_config[sasl_key]

            self.producer = AIOKafkaProducer(**producer_config)

            await self.producer.start()
            self.started = True
            logger.info("Kafka async producer started")

        except Exception as e:
            logger.error("Failed to start Kafka async producer", error=str(e))
            raise

    async def stop(self):
        """Stop producer"""
        if self.producer and self.started:
            await self.producer.stop()
            self.started = False
            logger.info("Kafka async producer stopped")

    async def send_event(
        self,
        topic: str,
        event: EventMessage,
        key: str | None = None,
        partition: int | None = None,
    ) -> None:
        """Send event message with retry logic"""
        if not self.started:
            await self.start()

        max_retries = 3
        retry_count = 0

        while retry_count <= max_retries:
            try:
                # Use tenant_id as default key for partitioning
                message_key = key or event.tenant_id

                future = await self.producer.send_and_wait(
                    topic=topic,
                    value=event.to_json(),
                    key=message_key,
                    partition=partition,
                )

                logger.info(
                    "Event sent to Kafka",
                    topic=topic,
                    event_type=event.event_type,
                    correlation_id=event.correlation_id,
                    partition=future.partition,
                    offset=future.offset,
                    retry_count=retry_count,
                )

                return  # Success, exit retry loop

            except (KafkaTimeoutError, KafkaConnectionError) as e:
                retry_count += 1
                if retry_count > max_retries:
                    logger.error(
                        "Failed to send event to Kafka after retries",
                        topic=topic,
                        event_type=event.event_type,
                        error=str(e),
                        retry_count=retry_count,
                    )
                    raise

                # Exponential backoff
                await asyncio.sleep(2**retry_count)
                logger.warning(
                    "Retrying event send",
                    topic=topic,
                    event_type=event.event_type,
                    retry_count=retry_count,
                    error=str(e),
                )

            except Exception as e:
                logger.error(
                    "Failed to send event to Kafka",
                    topic=topic,
                    event_type=event.event_type,
                    error=str(e),
                )
                raise

    async def send_event_with_retry(
        self,
        topic: str,
        event: EventMessage,
        retry_config: RetryConfig,
        key: str | None = None,
        partition: int | None = None,
    ) -> None:
        """Send event message with custom retry configuration"""
        if not self.started:
            await self.start()

        retry_count = 0

        while retry_count <= retry_config.max_retries:
            try:
                # Use tenant_id as default key for partitioning
                message_key = key or event.tenant_id

                future = await self.producer.send_and_wait(
                    topic=topic,
                    value=event.to_json(),
                    key=message_key,
                    partition=partition,
                )

                # Handle case where future might be None (in tests)
                partition_info = getattr(future, 'partition', None) if future else None
                offset_info = getattr(future, 'offset', None) if future else None

                logger.info(
                    "Event sent to Kafka with retry config",
                    topic=topic,
                    event_type=event.event_type,
                    correlation_id=event.correlation_id,
                    partition=partition_info,
                    offset=offset_info,
                    retry_count=retry_count,
                )

                return  # Success, exit retry loop

            except (KafkaTimeoutError, KafkaConnectionError) as e:
                retry_count += 1
                if retry_count > retry_config.max_retries:
                    logger.error(
                        "Failed to send event to Kafka after custom retries",
                        topic=topic,
                        event_type=event.event_type,
                        error=str(e),
                        retry_count=retry_count,
                        max_retries=retry_config.max_retries,
                    )
                    raise

                # Use configured delay calculation
                delay = retry_config.calculate_delay(retry_count - 1)
                await asyncio.sleep(delay)
                logger.warning(
                    "Retrying event send with custom config",
                    topic=topic,
                    event_type=event.event_type,
                    retry_count=retry_count,
                    delay=delay,
                    error=str(e),
                )

            except Exception as e:
                logger.error(
                    "Failed to send event to Kafka",
                    topic=topic,
                    event_type=event.event_type,
                    error=str(e),
                )
                raise
