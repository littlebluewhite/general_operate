"""
Dead Letter Queue Handler and Monitoring System
Provides comprehensive DLQ management, monitoring, and recovery capabilities
"""

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from enum import Enum
from typing import Any

import structlog

from .kafka_client import (
    EventMessage,
    KafkaAsyncConsumer,
    KafkaEventBus,
    MessageStatus,
    ProcessingResult,
    RetryConfig,
)

logger = structlog.get_logger()


class DLQEventType(Enum):
    """Types of DLQ events"""

    PROCESSING_FAILURE = "processing_failure"
    PROCESSING_ERROR = "processing_error"  # Alias for PROCESSING_FAILURE
    VALIDATION_ERROR = "validation_error"
    TIMEOUT_ERROR = "timeout_error"
    TIMEOUT = "timeout"  # Alias for TIMEOUT_ERROR
    CIRCUIT_BREAKER_OPEN = "circuit_breaker_open"
    RESOURCE_UNAVAILABLE = "resource_unavailable"
    SERIALIZATION_ERROR = "serialization_error"
    UNKNOWN_ERROR = "unknown_error"


@dataclass
class DLQMetrics:
    """DLQ processing metrics"""

    total_dlq_events: int = 0
    recovered_events: int = 0
    permanently_failed_events: int = 0
    events_by_type: dict[str, int] = None
    events_by_service: dict[str, int] = None
    last_processed_at: str | None = None

    def __post_init__(self):
        if self.events_by_type is None:
            self.events_by_type = {}
        if self.events_by_service is None:
            self.events_by_service = {}


@dataclass
class DLQEvent:
    """Enhanced DLQ event structure"""

    correlation_id: str
    original_topic: str
    service_name: str
    dlq_type: DLQEventType
    error_message: str
    retry_count: int
    event_data: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None
    created_at: datetime | None = None
    recovery_attempts: int = 0
    tenant_id: str | None = None
    last_recovery_attempt: str | None = None
    is_recoverable: bool = True
    original_event: EventMessage | None = None

    def __post_init__(self):
        """Initialize default values after creation"""
        if self.created_at is None:
            self.created_at = datetime.now(UTC)
        if self.event_data is None:
            self.event_data = {}
        if self.metadata is None:
            self.metadata = {}

    @property 
    def failed_at(self) -> str:
        """Get failed_at timestamp as ISO string"""
        if self.created_at:
            return self.created_at.isoformat()
        return datetime.now(UTC).isoformat()

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for storage/serialization"""
        result = {
            "correlation_id": self.correlation_id,
            "original_topic": self.original_topic,
            "service_name": self.service_name,
            "dlq_type": self.dlq_type.name,
            "error_message": self.error_message,
            "retry_count": self.retry_count,
            "event_data": self.event_data,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "failed_at": self.failed_at,
            "recovery_attempts": self.recovery_attempts,
            "tenant_id": self.tenant_id,
            "last_recovery_attempt": self.last_recovery_attempt,
            "is_recoverable": self.is_recoverable,
        }
        if self.original_event:
            result["original_event"] = asdict(self.original_event)
        return result

    @classmethod
    def from_event_message(cls, event: EventMessage) -> "DLQEvent":
        """Create DLQEvent from EventMessage"""
        metadata = event.metadata or {}
        
        # Parse timestamp from metadata or use current time
        created_at_str = metadata.get("dlq_timestamp", datetime.now(UTC).isoformat())
        if isinstance(created_at_str, str):
            try:
                # Try to parse ISO format
                created_at = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
            except ValueError:
                created_at = datetime.now(UTC)
        else:
            created_at = datetime.now(UTC)

        return cls(
            correlation_id=event.correlation_id,
            original_topic=metadata.get("original_topic", "unknown"),
            service_name=metadata.get("service_name", "unknown"),
            dlq_type=DLQEventType(metadata.get("dlq_type", "unknown_error")),
            error_message=metadata.get("dlq_reason", "Unknown error"),
            retry_count=event.retry_count,
            event_data=event.data,
            metadata=metadata,
            created_at=created_at,
            recovery_attempts=metadata.get("recovery_attempts", 0),
            tenant_id=event.tenant_id,
            last_recovery_attempt=metadata.get("last_recovery_attempt"),
            is_recoverable=metadata.get("is_recoverable", True),
            original_event=event,
        )


class DLQRecoveryStrategy:
    """Base class for DLQ recovery strategies"""

    def __init__(self, event_bus: KafkaEventBus | None = None):
        """Initialize recovery strategy with optional event bus for republishing"""
        self.event_bus = event_bus

    async def can_recover(self, dlq_event: DLQEvent) -> bool:
        """Check if event can be recovered"""
        raise NotImplementedError

    async def recover(self, dlq_event: DLQEvent) -> bool:
        """Attempt to recover the event"""
        raise NotImplementedError


class TimeBasedRecoveryStrategy(DLQRecoveryStrategy):
    """Recovery strategy based on time elapsed since failure"""

    def __init__(
        self,
        event_bus: KafkaEventBus | None = None,
        min_wait_hours: int = 1,
        max_recovery_attempts: int = 3,
    ):
        super().__init__(event_bus)
        self.min_wait_hours = min_wait_hours
        self.max_recovery_attempts = max_recovery_attempts

    async def can_recover(self, dlq_event: DLQEvent) -> bool:
        """Check if enough time has passed and attempts remaining"""
        if not dlq_event.is_recoverable:
            return False

        if dlq_event.recovery_attempts >= self.max_recovery_attempts:
            return False

        # Check if enough time has passed
        if dlq_event.created_at:
            failed_at = dlq_event.created_at
            if failed_at.tzinfo is None:
                failed_at = failed_at.replace(tzinfo=UTC)
        else:
            # Fallback to parsing failed_at string
            failed_at = datetime.fromisoformat(dlq_event.failed_at.replace("Z", "+00:00"))
        
        hours_elapsed = (datetime.now(UTC) - failed_at).total_seconds() / 3600
        return hours_elapsed >= self.min_wait_hours

    async def recover(self, dlq_event: DLQEvent) -> bool:
        """Attempt recovery by republishing to original topic"""
        if not self.event_bus:
            logger.warning(
                "Recovery attempted but no event bus configured for republishing",
                correlation_id=dlq_event.correlation_id,
            )
            return False

        try:
            # Update recovery metadata
            dlq_event.recovery_attempts += 1
            dlq_event.last_recovery_attempt = datetime.now(UTC).isoformat()

            # Create recovery event with updated metadata
            recovery_metadata = {
                **(dlq_event.metadata or {}),
                "recovery_attempt": dlq_event.recovery_attempts,
                "original_failure": dlq_event.error_message,
                "recovered_at": datetime.now(UTC).isoformat(),
                "is_recovery": True,
            }

            # Get data and user_id from original event or fallback
            event_data = dlq_event.event_data or {}
            user_id = None
            event_type = "dlq.recovery.time_based"
            
            if dlq_event.original_event:
                event_data = dlq_event.original_event.data
                user_id = dlq_event.original_event.user_id

            # Republish to original topic
            await self.event_bus.publish(
                topic=dlq_event.original_topic,
                event_type=event_type,
                tenant_id=dlq_event.tenant_id,
                data=event_data,
                user_id=user_id,
                metadata=recovery_metadata,
                correlation_id=dlq_event.correlation_id,
            )

            logger.info(
                "DLQ event recovery successful - republished to original topic",
                correlation_id=dlq_event.correlation_id,
                recovery_attempt=dlq_event.recovery_attempts,
                original_topic=dlq_event.original_topic,
            )

            return True

        except Exception as e:
            logger.error(
                "DLQ event recovery failed",
                correlation_id=dlq_event.correlation_id,
                error=str(e),
            )
            return False


class ValidationBasedRecoveryStrategy(DLQRecoveryStrategy):
    """Recovery strategy for validation errors"""

    def __init__(
        self,
        event_bus: KafkaEventBus | None = None,
        data_fixer: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]] | None = None,
    ):
        super().__init__(event_bus)
        self.data_fixer = data_fixer

    async def can_recover(self, dlq_event: DLQEvent) -> bool:
        """Only recover validation errors that might be transient"""
        return (
            dlq_event.dlq_type == DLQEventType.VALIDATION_ERROR
            and dlq_event.recovery_attempts < 2
            and dlq_event.is_recoverable
        )

    async def recover(self, dlq_event: DLQEvent) -> bool:
        """Attempt to fix validation issues and recover"""
        if not self.event_bus:
            logger.warning(
                "Recovery attempted but no event bus configured for republishing",
                correlation_id=dlq_event.correlation_id,
            )
            return False

        try:
            # Attempt to fix data if data_fixer is provided
            fixed_data = dlq_event.event_data or {}
            if dlq_event.original_event:
                fixed_data = dlq_event.original_event.data
                
            if self.data_fixer:
                fixed_data = await self.data_fixer(fixed_data)

            # Update recovery metadata
            dlq_event.recovery_attempts += 1
            dlq_event.last_recovery_attempt = datetime.now(UTC).isoformat()

            # Create recovery event with fixed data
            recovery_metadata = {
                **(dlq_event.metadata or {}),
                "recovery_attempt": dlq_event.recovery_attempts,
                "original_failure": dlq_event.error_message,
                "recovered_at": datetime.now(UTC).isoformat(),
                "is_recovery": True,
                "validation_fixed": True,
            }

            # Get event info from original event or fallback
            event_type = "dlq.recovery.validation_based"
            user_id = None
            
            if dlq_event.original_event:
                user_id = dlq_event.original_event.user_id

            # Republish to original topic with fixed data
            await self.event_bus.publish(
                topic=dlq_event.original_topic,
                event_type=event_type,
                tenant_id=dlq_event.tenant_id,
                data=fixed_data,
                user_id=user_id,
                metadata=recovery_metadata,
                correlation_id=dlq_event.correlation_id,
            )

            logger.info(
                "Validation error recovery successful - republished with fixed data",
                correlation_id=dlq_event.correlation_id,
                error_message=dlq_event.error_message,
            )

            return True

        except Exception as e:
            logger.error(
                "Validation error recovery failed",
                correlation_id=dlq_event.correlation_id,
                error=str(e),
            )
            return False


class DLQHandler:
    """Dead Letter Queue handler with monitoring and recovery"""

    def __init__(
        self,
        event_bus: KafkaEventBus,
        dlq_topics: list[str] | None = None,
        recovery_strategies: list[DLQRecoveryStrategy] | None = None,
        enable_periodic_reporting: bool = True,
        reporting_interval_seconds: int = 3600,
    ):
        """
        Initialize DLQ handler with dependency injection

        Args:
            event_bus: Injected KafkaEventBus instance for DLQ operations
            dlq_topics: List of DLQ topics to monitor (configurable)
            recovery_strategies: Optional list of recovery strategies
            enable_periodic_reporting: Whether to enable periodic summary reporting
            reporting_interval_seconds: Interval for periodic reporting in seconds
        """
        self.event_bus = event_bus
        self.metrics = DLQMetrics()
        self.consumers: dict[str, KafkaAsyncConsumer] = {}

        # Configurable DLQ topics
        self.dlq_topics = dlq_topics or []

        # Recovery strategies with event bus injection
        if recovery_strategies:
            self.recovery_strategies = recovery_strategies
        else:
            # Default strategies with event bus injection
            self.recovery_strategies = [
                TimeBasedRecoveryStrategy(
                    event_bus=event_bus, min_wait_hours=1, max_recovery_attempts=3
                ),
                ValidationBasedRecoveryStrategy(event_bus=event_bus),
            ]

        # Periodic reporting configuration
        self.enable_periodic_reporting = enable_periodic_reporting
        self.reporting_interval_seconds = reporting_interval_seconds

        # Async context management
        self._running = False
        self._reporting_task: asyncio.Task | None = None

    async def __aenter__(self):
        """Async context manager entry"""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.stop()

    async def initialize(self):
        """Initialize DLQ consumers for monitoring"""
        if not self.dlq_topics:
            logger.warning("No DLQ topics configured for monitoring")
            return

        logger.info(
            "Initializing DLQ handler",
            monitored_topics=self.dlq_topics,
            recovery_strategies=[
                s.__class__.__name__ for s in self.recovery_strategies
            ],
        )

        # Create consumers for each DLQ topic
        for topic in self.dlq_topics:
            consumer = await self.event_bus.subscribe_with_retry(
                topics=[topic],
                group_id=f"dlq-monitor-{topic.replace('.', '-')}",
                handler=self._handle_dlq_event,
                service_name="dlq-handler",
                retry_config=RetryConfig(max_retries=1, base_delay=5.0),
                dead_letter_topic=f"{topic}.monitor_dlq",
                enable_dlq=False,  # Avoid recursive DLQ for DLQ handler
            )

            self.consumers[topic] = consumer

        logger.info("DLQ handler initialized", monitored_topics=self.dlq_topics)

    async def _handle_dlq_event(self, event: EventMessage) -> ProcessingResult:
        """Handle DLQ events for monitoring and recovery"""
        try:
            # Create DLQ event object
            dlq_event = DLQEvent.from_event_message(event)

            # Update metrics
            self._update_metrics(dlq_event)

            # Log DLQ event
            logger.warning(
                "DLQ event detected",
                correlation_id=dlq_event.correlation_id,
                dlq_type=dlq_event.dlq_type.value,
                service_name=dlq_event.service_name,
                error_message=dlq_event.error_message,
                retry_count=dlq_event.retry_count,
            )

            # Attempt recovery if applicable
            await self._attempt_recovery(dlq_event)

            # Store DLQ event for analysis
            await self._store_dlq_event(dlq_event)

            # Send alerts if needed
            await self._send_dlq_alerts(dlq_event)

            return ProcessingResult(status=MessageStatus.SUCCESS)

        except Exception as e:
            logger.error(
                "Error handling DLQ event",
                correlation_id=event.correlation_id,
                error=str(e),
            )
            return ProcessingResult(status=MessageStatus.RETRY, error_message=str(e))

    def _update_metrics(self, dlq_event: DLQEvent):
        """Update DLQ metrics"""
        self.metrics.total_dlq_events += 1
        self.metrics.last_processed_at = datetime.now(UTC).isoformat()

        # Update by type
        dlq_type = dlq_event.dlq_type.value
        self.metrics.events_by_type[dlq_type] = (
            self.metrics.events_by_type.get(dlq_type, 0) + 1
        )

        # Update by service
        service = dlq_event.service_name
        self.metrics.events_by_service[service] = (
            self.metrics.events_by_service.get(service, 0) + 1
        )

    async def _attempt_recovery(self, dlq_event: DLQEvent) -> bool:
        """Attempt to recover DLQ event using available strategies"""
        for strategy in self.recovery_strategies:
            try:
                if await strategy.can_recover(dlq_event):
                    success = await strategy.recover(dlq_event)
                    if success:
                        self.metrics.recovered_events += 1
                        logger.info(
                            "DLQ event recovery successful",
                            correlation_id=dlq_event.correlation_id,
                            strategy=strategy.__class__.__name__,
                        )
                        return True

            except Exception as e:
                logger.error(
                    "DLQ recovery strategy failed",
                    correlation_id=dlq_event.correlation_id,
                    strategy=strategy.__class__.__name__,
                    error=str(e),
                )

        # Mark as permanently failed if no recovery possible
        if dlq_event.recovery_attempts >= 3:
            self.metrics.permanently_failed_events += 1
            logger.error(
                "DLQ event marked as permanently failed",
                correlation_id=dlq_event.correlation_id,
                recovery_attempts=dlq_event.recovery_attempts,
            )

        return False

    @staticmethod
    async def _store_dlq_event(dlq_event: DLQEvent):
        """Store DLQ event for analysis (placeholder for database storage)"""
        # Implementation would store in database for analysis
        logger.debug(
            "Storing DLQ event for analysis", correlation_id=dlq_event.correlation_id
        )

    async def _send_dlq_alerts(self, dlq_event: DLQEvent):
        """Send alerts for critical DLQ events"""
        # Send alert for high-priority events
        if dlq_event.dlq_type in [
            DLQEventType.CIRCUIT_BREAKER_OPEN,
            DLQEventType.RESOURCE_UNAVAILABLE,
        ]:
            logger.critical(
                "Critical DLQ event requires immediate attention",
                correlation_id=dlq_event.correlation_id,
                dlq_type=dlq_event.dlq_type.value,
                service_name=dlq_event.service_name,
            )
            # Implementation would send to alerting system

        # Send summary alerts for accumulating errors
        if self.metrics.total_dlq_events % 100 == 0:
            logger.warning(
                "DLQ event count threshold reached",
                total_dlq_events=self.metrics.total_dlq_events,
                events_by_service=self.metrics.events_by_service,
            )

    async def get_dlq_summary(self) -> dict[str, Any]:
        """Get comprehensive DLQ summary"""
        return {
            "metrics": asdict(self.metrics),
            "monitored_topics": self.dlq_topics,
            "recovery_strategies": [
                s.__class__.__name__ for s in self.recovery_strategies
            ],
            "last_updated": datetime.now(UTC).isoformat(),
            "is_running": self._running,
        }

    @staticmethod
    async def force_recovery_attempt(
        correlation_id: str, topic: str | None = None
    ) -> bool:
        """
        Force recovery attempt for specific event

        Args:
            correlation_id: Correlation ID of the event to recover
            topic: Optional topic hint to narrow search

        Returns:
            bool: True if recovery was attempted, False otherwise
        """
        # Implementation would fetch DLQ event by correlation_id and attempt recovery
        logger.info(
            "Force recovery requested",
            correlation_id=correlation_id,
            topic=topic,
        )
        # TODO: Implement actual recovery from storage
        return False

    async def _periodic_summary_reporting(self):
        """Send periodic DLQ summary reports"""
        while self._running:
            try:
                await asyncio.sleep(self.reporting_interval_seconds)

                if self.metrics.total_dlq_events > 0:
                    summary = await self.get_dlq_summary()
                    logger.info("DLQ periodic summary", **summary["metrics"])

            except asyncio.CancelledError:
                # Normal cancellation during shutdown
                break
            except Exception as e:
                logger.error("Error in periodic DLQ reporting", error=str(e))

    async def start(self):
        """Start DLQ monitoring with proper async lifecycle management"""
        if self._running:
            logger.warning("DLQ handler already running")
            return

        self._running = True
        await self.initialize()

        # Start periodic reporting if enabled
        if self.enable_periodic_reporting:
            self._reporting_task = asyncio.create_task(
                self._periodic_summary_reporting()
            )

        logger.info("DLQ handler started")

    async def stop(self):
        """Stop DLQ monitoring with proper cleanup"""
        if not self._running:
            logger.warning("DLQ handler not running")
            return

        self._running = False

        # Cancel periodic reporting task
        if self._reporting_task and not self._reporting_task.done():
            self._reporting_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reporting_task

        # Stop all consumers
        for topic, consumer in self.consumers.items():
            try:
                await consumer.stop()
            except Exception as e:
                logger.error(
                    "Error stopping DLQ consumer",
                    topic=topic,
                    error=str(e),
                )

        self.consumers.clear()
        logger.info("DLQ handler stopped")

    async def health_check(self) -> dict[str, Any]:
        """Perform health check on DLQ handler"""
        try:
            consumer_status = {}
            # This is where the exception should be caught
            for topic, consumer in self.consumers.items():
                # Check consumer health without accessing private attributes
                consumer_healthy = hasattr(consumer, "consume") and self._running
                consumer_status[topic] = {
                    "healthy": consumer_healthy,
                    "group_id": getattr(consumer, "group_id", "unknown"),
                }

            return {
                "status": "healthy" if self._running else "stopped",
                "is_running": self._running,
                "monitored_topics": self.dlq_topics,
                "consumer_status": consumer_status,
                "metrics_summary": {
                    "total_events": self.metrics.total_dlq_events,
                    "recovered": self.metrics.recovered_events,
                    "failed": self.metrics.permanently_failed_events,
                },
                "last_processed": self.metrics.last_processed_at,
            }

        except Exception as e:
            # This should catch the exception from accessing self.consumers
            return {
                "status": "error",
                "is_running": self._running,
                "error": str(e),
            }

    @property
    def running(self):
        return self._running


# Convenience function for creating DLQ handler with ServiceEventManager integration
async def create_dlq_handler_for_service(
    kafka_config: dict[str, Any] | None = None,
    service_config: Any | None = None,
    dlq_topics: list[str] | None = None,
    event_bus: KafkaEventBus | None = None,
    service_name: str | None = None,
    dlq_topic_suffixes: list[str] | None = None,
) -> DLQHandler:
    """
    Create a DLQ handler for a specific service

    Args:
        kafka_config: Kafka configuration dict (for new interface)
        service_config: Service configuration object (for new interface)
        dlq_topics: List of DLQ topics to monitor (for new interface)
        event_bus: Event bus instance (for legacy interface)
        service_name: Name of the service (for legacy interface)
        dlq_topic_suffixes: Optional list of DLQ topic suffixes (for legacy interface)

    Returns:
        Configured DLQHandler instance
    """
    # New interface: create event bus from config
    if kafka_config is not None and service_config is not None:
        # Create event bus from kafka_config
        effective_event_bus = KafkaEventBus(kafka_config, service_config.service_name)
        effective_dlq_topics = dlq_topics or [f"{service_config.service_name}.dlq"]
        
        dlq_handler = DLQHandler(
            event_bus=effective_event_bus,
            dlq_topics=effective_dlq_topics,
        )
        return dlq_handler
    
    # Legacy interface: use provided event_bus
    if event_bus is None or service_name is None:
        raise ValueError("Either (kafka_config, service_config) or (event_bus, service_name) must be provided")
    
    # Default DLQ topic patterns if not specified
    if dlq_topic_suffixes is None:
        dlq_topic_suffixes = [
            f"{service_name}.events.dlq",
            f"{service_name}.events.processing_dlq",
        ]

    dlq_handler = DLQHandler(
        event_bus=event_bus,
        dlq_topics=dlq_topic_suffixes,
    )

    return dlq_handler


# Example usage with async context manager
if __name__ == "__main__":

    async def example_usage():
        """Example of how to use the refactored DLQ handler"""
        # Configuration
        kafka_config = {
            "bootstrap_servers": "localhost:9092",
            "security_protocol": "PLAINTEXT",
            # ... other config ...
        }

        # Create event bus
        event_bus = KafkaEventBus(kafka_config, "dlq-example")
        await event_bus.start()

        # Configure DLQ topics
        dlq_topics = [
            "notification.auth_events.dlq",
            "notification.user_events.dlq",
            "audit.events.dlq",
        ]

        # Use DLQ handler with async context manager
        async with DLQHandler(event_bus, dlq_topics=dlq_topics) as dlq_handler:
            # Let it run for a while
            await asyncio.sleep(30)

            # Get summary
            summary = await dlq_handler.get_dlq_summary()
            logger.info("DLQ Summary", summary=summary)

            # Check health
            health = await dlq_handler.health_check()
            logger.info("DLQ Health", health=health)

        # Event bus cleanup
        await event_bus.stop()

    # Run example
    asyncio.run(example_usage())
