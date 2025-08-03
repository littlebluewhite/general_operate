"""
Kafka configuration module with unified configuration classes.
Consolidates retry and other configurations used across the Kafka module.
"""

import datetime
from typing import Any

from pydantic import BaseModel, Field


class RetryConfig(BaseModel):
    """Unified retry configuration with validation."""

    max_retries: int = Field(default=3, ge=0, description="Maximum number of retries")
    base_delay: float = Field(default=1.0, gt=0, description="Base delay in seconds")
    max_delay: float = Field(default=60.0, gt=0, description="Maximum delay in seconds")
    exponential_base: float = Field(
        default=2.0, gt=1, description="Exponential backoff base"
    )
    jitter: bool = Field(default=True, description="Enable random jitter")

    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for given attempt number with exponential backoff and jitter."""
        import random
        
        # Calculate exponential backoff delay
        delay = self.base_delay * (self.exponential_base ** attempt)
        
        # Cap at max_delay
        delay = min(delay, self.max_delay)
        
        # Add jitter if enabled
        if self.jitter:
            # Add random jitter between 50% and 150% of calculated delay
            jitter_factor = 0.5 + random.random()  # Random between 0.5 and 1.5
            delay = delay * jitter_factor
            # Ensure we don't exceed max_delay even with jitter
            delay = min(delay, self.max_delay)
        
        return delay


class ConsumerGroupConfig(BaseModel):
    """Configuration for Kafka consumer groups."""

    group_id: str = Field(..., description="Consumer group ID")
    auto_offset_reset: str = Field(default="earliest", pattern="^(earliest|latest)$", description="Auto offset reset strategy")
    enable_auto_commit: bool = Field(default=False, description="Enable auto commit")
    max_poll_records: int = Field(
        default=500, ge=1, description="Maximum records per poll"
    )
    session_timeout_ms: int = Field(
        default=30000, ge=1000, description="Session timeout in milliseconds"
    )


class ServiceConfig(BaseModel):
    """Service-level configuration for Kafka components."""

    service_name: str = Field(..., description="Service name")
    health_check_enabled: bool = Field(default=True, description="Enable health checks")
    metrics_enabled: bool = Field(default=True, description="Enable metrics collection")
    circuit_breaker_enabled: bool = Field(
        default=True, description="Enable circuit breaker"
    )
    created_at: datetime.datetime = Field(
        default_factory=datetime.datetime.now, description="Creation timestamp"
    )


class TopicConfig(BaseModel):
    """Configuration for Kafka topics."""

    name: str = Field(..., description="Topic name")
    partitions: int = Field(default=1, ge=1, description="Number of partitions")
    replication_factor: int = Field(default=1, ge=1, description="Replication factor")
    config: dict[str, Any] = Field(
        default_factory=dict, description="Additional topic configuration"
    )
