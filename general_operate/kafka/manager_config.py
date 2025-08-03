from pydantic import BaseModel, Field
from .config import RetryConfig


class ConsumerGroupConfig(BaseModel):
    """Consumer group configuration"""

    group_id: str = Field(description="Consumer group ID")
    topics: list[str] = Field(default_factory=list, description="Topics to subscribe to")
    auto_offset_reset: str = Field(default="latest", description="Auto offset reset strategy")
    enable_auto_commit: bool = Field(default=True, description="Enable auto commit")
    auto_commit_interval_ms: int = Field(default=5000, ge=1, description="Auto commit interval in ms")
    session_timeout_ms: int = Field(default=30000, ge=1000, description="Session timeout in ms")
    max_poll_records: int = Field(default=500, ge=1, description="Max poll records")
    filter_event_types: list[str] = Field(default_factory=list, description="Event types to filter")


class ServiceConfig(BaseModel):
    """Service configuration"""

    service_name: str = Field(description="Service name")
    retry_config: RetryConfig = Field(default_factory=RetryConfig, description="Retry configuration")
    enable_dlq: bool = Field(default=True, description="Enable Dead Letter Queue")
    dlq_topic_suffix: str = Field(default=".dlq", description="DLQ topic suffix")
    circuit_breaker_threshold: int = Field(default=5, ge=1, description="Circuit breaker failure threshold")
    circuit_breaker_timeout: float = Field(default=30.0, gt=0, description="Circuit breaker timeout in seconds")
    topics: list[str] = Field(default_factory=list, description="List of topics for this service")
    consumer_groups: list[ConsumerGroupConfig] = Field(default_factory=list, description="Consumer group configurations")


class TopicConfig(BaseModel):
    """Topic configuration"""

    name: str = Field(description="Topic name")
    partitions: int = Field(default=1, ge=1, description="Number of partitions")
    replication_factor: int = Field(default=1, ge=1, description="Replication factor")
    config: dict[str, str] = Field(default_factory=dict, description="Topic configuration")
