"""
Unit tests for EventMessage model - no external dependencies required
"""

import json
import pytest
from datetime import datetime, UTC
from unittest.mock import patch
from dataclasses import FrozenInstanceError

from general_operate.kafka.models.event_message import EventMessage


class TestEventMessageCreation:
    """Test EventMessage creation and initialization"""

    def test_event_message_minimal_creation(self):
        """Test EventMessage creation with minimal required fields"""
        event = EventMessage(event_type="test.event")
        
        assert event.event_type == "test.event"
        assert event.tenant_id is None
        assert event.user_id is None
        assert event.data == {}
        assert event.metadata == {}
        assert event.timestamp is not None
        assert event.correlation_id is not None
        
        # Verify timestamp format
        datetime.fromisoformat(event.timestamp.replace('Z', '+00:00'))
        
        # Verify correlation_id is UUID format
        import uuid
        uuid.UUID(event.correlation_id)

    def test_event_message_full_creation(self):
        """Test EventMessage creation with all fields"""
        test_data = {"key": "value", "number": 42}
        test_metadata = {"source": "test", "version": "1.0"}
        test_timestamp = "2023-12-01T10:00:00Z"
        test_correlation_id = "test-correlation-id"
        
        event = EventMessage(
            event_type="user.registered",
            tenant_id="tenant-123",
            user_id="user-456",
            data=test_data,
            metadata=test_metadata,
            timestamp=test_timestamp,
            correlation_id=test_correlation_id
        )
        
        assert event.event_type == "user.registered"
        assert event.tenant_id == "tenant-123"
        assert event.user_id == "user-456"
        assert event.data == test_data
        assert event.metadata == test_metadata
        assert event.timestamp == test_timestamp
        assert event.correlation_id == test_correlation_id

    def test_event_message_post_init_defaults(self):
        """Test __post_init__ method sets proper defaults"""
        with patch('general_operate.kafka.models.event_message.datetime') as mock_datetime, \
             patch('general_operate.kafka.models.event_message.uuid') as mock_uuid:
            
            mock_datetime.now.return_value.isoformat.return_value = "2023-12-01T10:00:00Z"
            mock_uuid.uuid4.return_value = "mock-uuid-1234"
            
            event = EventMessage(event_type="test.event")
            
            assert event.timestamp == "2023-12-01T10:00:00Z"
            assert event.correlation_id == "mock-uuid-1234"
            assert event.data == {}
            assert event.metadata == {}
            
            mock_datetime.now.assert_called_once_with(UTC)
            mock_uuid.uuid4.assert_called_once()

    def test_event_message_none_values_handled(self):
        """Test that None values are properly converted to defaults"""
        event = EventMessage(
            event_type="test.event",
            data=None,
            metadata=None,
            timestamp=None,
            correlation_id=None
        )
        
        assert event.data == {}
        assert event.metadata == {}
        assert event.timestamp is not None
        assert event.correlation_id is not None


class TestEventMessageSerialization:
    """Test EventMessage serialization methods"""

    def test_to_dict(self):
        """Test conversion to dictionary"""
        event = EventMessage(
            event_type="test.event",
            tenant_id="tenant-123",
            user_id="user-456",
            data={"key": "value"},
            metadata={"source": "test"}
        )
        
        result = event.to_dict()
        
        assert isinstance(result, dict)
        assert result["event_type"] == "test.event"
        assert result["tenant_id"] == "tenant-123"
        assert result["user_id"] == "user-456"
        assert result["data"] == {"key": "value"}
        assert result["metadata"] == {"source": "test"}
        assert "timestamp" in result
        assert "correlation_id" in result

    def test_to_json(self):
        """Test conversion to JSON string"""
        event = EventMessage(
            event_type="test.event",
            tenant_id="tenant-123",
            data={"key": "value", "unicode": "测试"}
        )
        
        json_str = event.to_json()
        
        assert isinstance(json_str, str)
        
        # Verify it's valid JSON
        parsed = json.loads(json_str)
        assert parsed["event_type"] == "test.event"
        assert parsed["tenant_id"] == "tenant-123"
        assert parsed["data"]["key"] == "value"
        assert parsed["data"]["unicode"] == "测试"  # Unicode preserved

    def test_to_json_ensure_ascii_false(self):
        """Test JSON serialization preserves Unicode characters"""
        event = EventMessage(
            event_type="test.event",
            data={"chinese": "中文", "emoji": "🚀"}
        )
        
        json_str = event.to_json()
        
        # Should contain actual Unicode characters, not escaped versions
        assert "中文" in json_str
        assert "🚀" in json_str


class TestEventMessageDeserialization:
    """Test EventMessage deserialization methods"""

    def test_from_dict(self):
        """Test creation from dictionary"""
        data = {
            "event_type": "user.created",
            "tenant_id": "tenant-789",
            "user_id": "user-101",
            "data": {"name": "John", "age": 30},
            "metadata": {"source": "api"},
            "timestamp": "2023-12-01T15:30:00Z",
            "correlation_id": "corr-123"
        }
        
        event = EventMessage.from_dict(data)
        
        assert event.event_type == "user.created"
        assert event.tenant_id == "tenant-789"
        assert event.user_id == "user-101"
        assert event.data == {"name": "John", "age": 30}
        assert event.metadata == {"source": "api"}
        assert event.timestamp == "2023-12-01T15:30:00Z"
        assert event.correlation_id == "corr-123"

    def test_from_dict_minimal(self):
        """Test creation from dictionary with minimal fields"""
        data = {"event_type": "test.event"}
        
        event = EventMessage.from_dict(data)
        
        assert event.event_type == "test.event"
        # Should still trigger __post_init__ defaults
        assert event.data == {}
        assert event.metadata == {}
        assert event.timestamp is not None
        assert event.correlation_id is not None

    def test_from_json(self):
        """Test creation from JSON string"""
        json_str = json.dumps({
            "event_type": "order.placed",
            "tenant_id": "tenant-456",
            "data": {"order_id": "order-123", "amount": 99.99},
            "metadata": {"currency": "USD"}
        })
        
        event = EventMessage.from_json(json_str)
        
        assert event.event_type == "order.placed"
        assert event.tenant_id == "tenant-456"
        assert event.data["order_id"] == "order-123"
        assert event.data["amount"] == 99.99
        assert event.metadata["currency"] == "USD"

    def test_from_json_invalid(self):
        """Test creation from invalid JSON string"""
        invalid_json = '{"event_type": "test.event", invalid}'
        
        with pytest.raises(json.JSONDecodeError):
            EventMessage.from_json(invalid_json)

    def test_from_dict_extra_fields_raise_error(self):
        """Test that extra fields in dict raise TypeError"""
        data = {
            "event_type": "test.event",
            "extra_field": "should_cause_error",
            "another_extra": 123
        }
        
        # Should raise TypeError for unexpected fields
        with pytest.raises(TypeError, match="unexpected keyword argument"):
            EventMessage.from_dict(data)


class TestEventMessageMetadata:
    """Test EventMessage metadata operations"""

    def test_add_metadata(self):
        """Test adding metadata to event"""
        event = EventMessage(event_type="test.event")
        
        event.add_metadata("source", "test-source")
        event.add_metadata("version", "1.0")
        
        assert event.metadata["source"] == "test-source"
        assert event.metadata["version"] == "1.0"

    def test_add_metadata_to_none(self):
        """Test adding metadata when metadata is None"""
        event = EventMessage(event_type="test.event", metadata=None)
        
        # Should handle None metadata gracefully
        event.add_metadata("key", "value")
        
        assert event.metadata is not None
        assert event.metadata["key"] == "value"

    def test_add_metadata_initializes_none_metadata(self):
        """Test that add_metadata properly initializes None metadata"""
        # Create event and manually set metadata to None to test the conditional
        event = EventMessage(event_type="test.event")
        event.metadata = None  # Force None to test line 61
        
        event.add_metadata("test_key", "test_value")
        
        assert event.metadata is not None
        assert event.metadata["test_key"] == "test_value"

    def test_get_metadata(self):
        """Test getting metadata values"""
        event = EventMessage(
            event_type="test.event",
            metadata={"source": "api", "version": "2.0"}
        )
        
        assert event.get_metadata("source") == "api"
        assert event.get_metadata("version") == "2.0"
        assert event.get_metadata("nonexistent") is None
        assert event.get_metadata("nonexistent", "default") == "default"

    def test_get_metadata_from_none(self):
        """Test getting metadata when metadata is None"""
        event = EventMessage(event_type="test.event", metadata=None)
        
        assert event.get_metadata("key") is None
        assert event.get_metadata("key", "default") == "default"

    def test_get_metadata_handles_none_metadata(self):
        """Test get_metadata when metadata is explicitly set to None"""
        # Create event and manually set metadata to None to test line 67
        event = EventMessage(event_type="test.event")
        event.metadata = None  # Force None to test line 67
        
        assert event.get_metadata("any_key") is None
        assert event.get_metadata("any_key", "fallback") == "fallback"

    def test_metadata_operations_preserve_existing(self):
        """Test that metadata operations preserve existing metadata"""
        event = EventMessage(
            event_type="test.event",
            metadata={"existing": "value"}
        )
        
        event.add_metadata("new", "new_value")
        
        assert event.metadata["existing"] == "value"
        assert event.metadata["new"] == "new_value"
        assert len(event.metadata) == 2


class TestEventMessageRoundTrip:
    """Test round-trip serialization/deserialization"""

    def test_dict_round_trip(self):
        """Test that dict serialization/deserialization preserves data"""
        original = EventMessage(
            event_type="user.updated",
            tenant_id="tenant-999", 
            user_id="user-888",
            data={"field": "value", "number": 42, "nested": {"key": "val"}},
            metadata={"source": "test", "priority": "high"}
        )
        
        # Round trip through dict
        dict_data = original.to_dict()
        restored = EventMessage.from_dict(dict_data)
        
        assert restored.event_type == original.event_type
        assert restored.tenant_id == original.tenant_id
        assert restored.user_id == original.user_id
        assert restored.data == original.data
        assert restored.metadata == original.metadata
        assert restored.timestamp == original.timestamp
        assert restored.correlation_id == original.correlation_id

    def test_json_round_trip(self):
        """Test that JSON serialization/deserialization preserves data"""
        original = EventMessage(
            event_type="product.created",
            tenant_id="tenant-777",
            data={"name": "Product", "price": 29.99, "tags": ["new", "featured"]},
            metadata={"category": "electronics"}
        )
        
        # Round trip through JSON
        json_str = original.to_json()
        restored = EventMessage.from_json(json_str)
        
        assert restored.event_type == original.event_type
        assert restored.tenant_id == original.tenant_id
        assert restored.data == original.data
        assert restored.metadata == original.metadata
        assert restored.timestamp == original.timestamp
        assert restored.correlation_id == original.correlation_id

    def test_json_round_trip_with_unicode(self):
        """Test JSON round trip with Unicode characters"""
        original = EventMessage(
            event_type="message.sent",
            data={"content": "Hello 世界 🌍", "language": "多语言"}
        )
        
        json_str = original.to_json()
        restored = EventMessage.from_json(json_str)
        
        assert restored.data["content"] == "Hello 世界 🌍"
        assert restored.data["language"] == "多语言"


class TestEventMessageEdgeCases:
    """Test EventMessage edge cases and error conditions"""

    def test_empty_event_type(self):
        """Test creation with empty event type"""
        event = EventMessage(event_type="")
        assert event.event_type == ""

    def test_complex_data_structures(self):
        """Test with complex nested data structures"""
        complex_data = {
            "array": [1, 2, {"nested": "value"}],
            "nested_dict": {
                "level1": {
                    "level2": ["item1", "item2"],
                    "level2_dict": {"key": "value"}
                }
            },
            "mixed_types": [1, "string", {"dict": "value"}, [1, 2, 3]]
        }
        
        event = EventMessage(
            event_type="complex.data",
            data=complex_data
        )
        
        # Should handle complex structures
        assert event.data == complex_data
        
        # Should serialize/deserialize correctly
        json_str = event.to_json()
        restored = EventMessage.from_json(json_str)
        assert restored.data == complex_data

    def test_dataclass_immutability_considerations(self):
        """Test considerations around dataclass mutability"""
        event = EventMessage(event_type="test.event")
        
        # Basic field access should work
        assert event.event_type == "test.event"
        
        # Metadata operations should work (they modify the dict, not the field)
        event.add_metadata("key", "value")
        assert event.get_metadata("key") == "value"

    def test_timestamp_format_validation(self):
        """Test that custom timestamps are preserved"""
        custom_timestamp = "2023-01-01T00:00:00.000Z"
        event = EventMessage(
            event_type="test.event",
            timestamp=custom_timestamp
        )
        
        assert event.timestamp == custom_timestamp

    def test_correlation_id_format_validation(self):
        """Test that custom correlation IDs are preserved"""
        custom_correlation_id = "custom-correlation-12345"
        event = EventMessage(
            event_type="test.event",
            correlation_id=custom_correlation_id
        )
        
        assert event.correlation_id == custom_correlation_id


if __name__ == "__main__":
    pass