"""
Event message model for Kafka operations
"""

import json
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, UTC
from typing import Any


@dataclass
class EventMessage:
    """
    Standardized event message structure for Kafka
    
    This class provides a unified format for all events in the system,
    ensuring consistency across different services and event types.
    """
    event_type: str
    tenant_id: str | None = None
    user_id: str | None = None
    data: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None
    timestamp: str | None = None
    correlation_id: str | None = None
    
    def __post_init__(self):
        """Initialize default values for optional fields"""
        if self.timestamp is None:
            self.timestamp = datetime.now(UTC).isoformat()
        if self.correlation_id is None:
            self.correlation_id = str(uuid.uuid4())
        if self.data is None:
            self.data = {}
        if self.metadata is None:
            self.metadata = {}
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format"""
        return asdict(self)
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict(), ensure_ascii=False)
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> 'EventMessage':
        """Create EventMessage from dictionary"""
        return cls(**data)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'EventMessage':
        """Create EventMessage from JSON string"""
        data = json.loads(json_str)
        return cls.from_dict(data)
    
    def add_metadata(self, key: str, value: Any) -> None:
        """Add metadata to the event"""
        if self.metadata is None:
            self.metadata = {}
        self.metadata[key] = value
    
    def get_metadata(self, key: str, default: Any = None) -> Any:
        """Get metadata value by key"""
        if self.metadata is None:
            return default
        return self.metadata.get(key, default)