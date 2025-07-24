"""
Unit tests for Kafka serializers - no external dependencies required
"""

import json
import pytest
from unittest.mock import patch

from general_operate.kafka.utils.serializers import (
    DefaultSerializer,
    JsonSerializer,
    DefaultKeySerializer,
    default_value_serializer,
    json_value_serializer,
    default_key_serializer,
    json_value_deserializer,
    string_deserializer,
    SerializerFactory
)


class TestDefaultSerializer:
    """Test DefaultSerializer class"""

    def test_serialize_bytes(self):
        """Test serializing bytes (should return as-is)"""
        data = b'test bytes'
        result = DefaultSerializer.serialize(data)
        
        assert result == data
        assert isinstance(result, bytes)

    def test_serialize_string(self):
        """Test serializing string"""
        data = "test string"
        result = DefaultSerializer.serialize(data)
        
        assert result == b'test string'
        assert isinstance(result, bytes)

    def test_serialize_unicode_string(self):
        """Test serializing Unicode string"""
        data = "测试字符串 🚀"
        result = DefaultSerializer.serialize(data)
        
        assert result == data.encode('utf-8')
        assert isinstance(result, bytes)
        
        # Verify it can be decoded back
        assert result.decode('utf-8') == data

    def test_serialize_none(self):
        """Test serializing None"""
        result = DefaultSerializer.serialize(None)
        
        assert result == b''
        assert isinstance(result, bytes)

    def test_serialize_integer(self):
        """Test serializing integer (should use JSON)"""
        data = 42
        result = DefaultSerializer.serialize(data)
        
        expected = json.dumps(data, ensure_ascii=False).encode('utf-8')
        assert result == expected
        assert isinstance(result, bytes)

    def test_serialize_float(self):
        """Test serializing float (should use JSON)"""
        data = 3.14159
        result = DefaultSerializer.serialize(data)
        
        expected = json.dumps(data, ensure_ascii=False).encode('utf-8')
        assert result == expected

    def test_serialize_boolean(self):
        """Test serializing boolean (should use JSON)"""
        result_true = DefaultSerializer.serialize(True)
        result_false = DefaultSerializer.serialize(False)
        
        assert result_true == b'true'
        assert result_false == b'false'

    def test_serialize_list(self):
        """Test serializing list (should use JSON)"""
        data = [1, 2, "three", {"four": 4}]
        result = DefaultSerializer.serialize(data)
        
        expected = json.dumps(data, ensure_ascii=False).encode('utf-8')
        assert result == expected

    def test_serialize_dict(self):
        """Test serializing dictionary (should use JSON)"""
        data = {"name": "test", "value": 123, "unicode": "测试"}
        result = DefaultSerializer.serialize(data)
        
        expected = json.dumps(data, ensure_ascii=False).encode('utf-8')
        assert result == expected
        
        # Verify Unicode is preserved
        assert "测试".encode('utf-8') in result

    def test_serialize_complex_object(self):
        """Test serializing complex nested object"""
        data = {
            "users": [
                {"id": 1, "name": "Alice", "active": True},
                {"id": 2, "name": "Bob", "active": False}
            ],
            "metadata": {
                "total": 2,
                "page": 1,
                "timestamp": "2023-12-01T10:00:00Z"
            }
        }
        result = DefaultSerializer.serialize(data)
        
        # Verify it's valid JSON bytes
        parsed = json.loads(result.decode('utf-8'))
        assert parsed == data


class TestJsonSerializer:
    """Test JsonSerializer class"""

    def test_serialize_dict(self):
        """Test JSON serialization of dictionary"""
        data = {"key": "value", "number": 42}
        result = JsonSerializer.serialize(data)
        
        expected = json.dumps(data, ensure_ascii=False).encode('utf-8')
        assert result == expected

    def test_serialize_list(self):
        """Test JSON serialization of list"""
        data = [1, "two", {"three": 3}]
        result = JsonSerializer.serialize(data)
        
        expected = json.dumps(data, ensure_ascii=False).encode('utf-8')
        assert result == expected

    def test_serialize_string(self):
        """Test JSON serialization of string"""
        data = "test string"
        result = JsonSerializer.serialize(data)
        
        # String should be JSON-encoded (with quotes)
        assert result == b'"test string"'

    def test_serialize_unicode(self):
        """Test JSON serialization preserves Unicode"""
        data = {"chinese": "中文", "emoji": "🚀"}
        result = JsonSerializer.serialize(data)
        
        # Unicode should be preserved (not escaped)
        assert "中文".encode('utf-8') in result
        assert "🚀".encode('utf-8') in result

    def test_deserialize_valid_json(self):
        """Test JSON deserialization of valid JSON"""
        data = {"key": "value", "number": 42}
        json_bytes = json.dumps(data).encode('utf-8')
        
        result = JsonSerializer.deserialize(json_bytes)
        
        assert result == data

    def test_deserialize_empty_bytes(self):
        """Test deserialization of empty bytes"""
        result = JsonSerializer.deserialize(b'')
        
        assert result is None

    def test_deserialize_invalid_json(self):
        """Test deserialization of invalid JSON (should return as string)"""
        invalid_json = b'not valid json content'
        
        result = JsonSerializer.deserialize(invalid_json)
        
        # Should return as decoded string when JSON fails
        assert result == 'not valid json content'

    def test_deserialize_unicode_json(self):
        """Test deserialization of Unicode JSON"""
        data = {"message": "Hello 世界", "emoji": "🎉"}
        json_bytes = json.dumps(data, ensure_ascii=False).encode('utf-8')
        
        result = JsonSerializer.deserialize(json_bytes)
        
        assert result == data
        assert result["message"] == "Hello 世界"
        assert result["emoji"] == "🎉"

    def test_deserialize_json_decode_error_handling(self):
        """Test handling of JSON decode errors"""
        with patch('json.loads', side_effect=json.JSONDecodeError("test", "doc", 0)):
            result = JsonSerializer.deserialize(b'test data')
            
            # Should fallback to string when JSON parsing fails
            assert result == 'test data'


class TestDefaultKeySerializer:
    """Test DefaultKeySerializer class"""

    def test_serialize_none_key(self):
        """Test serializing None key"""
        result = DefaultKeySerializer.serialize(None)
        
        assert result is None

    def test_serialize_bytes_key(self):
        """Test serializing bytes key (should return as-is)"""
        key = b'test_key'
        result = DefaultKeySerializer.serialize(key)
        
        assert result == key

    def test_serialize_string_key(self):
        """Test serializing string key"""
        key = "test_key"
        result = DefaultKeySerializer.serialize(key)
        
        assert result == b'test_key'

    def test_serialize_integer_key(self):
        """Test serializing integer key (should convert to string)"""
        key = 12345
        result = DefaultKeySerializer.serialize(key)
        
        assert result == b'12345'

    def test_serialize_uuid_key(self):
        """Test serializing UUID-like key"""
        import uuid
        key = uuid.uuid4()
        result = DefaultKeySerializer.serialize(key)
        
        assert result == str(key).encode('utf-8')

    def test_serialize_complex_key(self):
        """Test serializing complex object as key (should convert to string)"""
        key = {"id": 123, "type": "user"}
        result = DefaultKeySerializer.serialize(key)
        
        # Should convert to string representation
        expected = str(key).encode('utf-8')
        assert result == expected


class TestSerializerFunctions:
    """Test standalone serializer functions"""

    def test_default_value_serializer(self):
        """Test default_value_serializer function"""
        data = {"test": "value"}
        result = default_value_serializer(data)
        
        expected = DefaultSerializer.serialize(data)
        assert result == expected

    def test_json_value_serializer(self):
        """Test json_value_serializer function"""
        data = {"test": "value"}
        result = json_value_serializer(data)
        
        expected = JsonSerializer.serialize(data)
        assert result == expected

    def test_default_key_serializer(self):
        """Test default_key_serializer function"""
        key = "test_key"
        result = default_key_serializer(key)
        
        expected = DefaultKeySerializer.serialize(key)
        assert result == expected

    def test_json_value_deserializer(self):
        """Test json_value_deserializer function"""
        data = json.dumps({"test": "value"}).encode('utf-8')
        result = json_value_deserializer(data)
        
        expected = JsonSerializer.deserialize(data)
        assert result == expected

    def test_string_deserializer(self):
        """Test string_deserializer function"""
        data = b'test string'
        result = string_deserializer(data)
        
        assert result == 'test string'

    def test_string_deserializer_none(self):
        """Test string_deserializer with None"""
        result = string_deserializer(None)
        
        assert result is None

    def test_string_deserializer_unicode(self):
        """Test string_deserializer with Unicode"""
        data = "测试字符串 🚀".encode('utf-8')
        result = string_deserializer(data)
        
        assert result == "测试字符串 🚀"


class TestSerializerFactory:
    """Test SerializerFactory class"""

    def test_get_default_value_serializer(self):
        """Test getting default value serializer"""
        serializer = SerializerFactory.get_value_serializer("default")
        
        assert serializer == default_value_serializer
        
        # Test that it works
        data = {"test": "value"}
        result = serializer(data)
        expected = default_value_serializer(data)
        assert result == expected

    def test_get_json_value_serializer(self):
        """Test getting JSON value serializer"""
        serializer = SerializerFactory.get_value_serializer("json")
        
        assert serializer == json_value_serializer

    def test_get_unknown_value_serializer_returns_default(self):
        """Test that unknown serializer type returns default"""
        serializer = SerializerFactory.get_value_serializer("unknown")
        
        assert serializer == default_value_serializer

    def test_get_value_serializer_no_param_returns_default(self):
        """Test that no parameter returns default serializer"""
        serializer = SerializerFactory.get_value_serializer()
        
        assert serializer == default_value_serializer

    def test_get_default_key_serializer(self):
        """Test getting default key serializer"""
        serializer = SerializerFactory.get_key_serializer("default")
        
        assert serializer == default_key_serializer

    def test_get_unknown_key_serializer_returns_default(self):
        """Test that unknown key serializer type returns default"""
        serializer = SerializerFactory.get_key_serializer("unknown")
        
        assert serializer == default_key_serializer

    def test_get_key_serializer_no_param_returns_default(self):
        """Test that no parameter returns default key serializer"""
        serializer = SerializerFactory.get_key_serializer()
        
        assert serializer == default_key_serializer

    def test_get_json_value_deserializer(self):
        """Test getting JSON value deserializer"""
        deserializer = SerializerFactory.get_value_deserializer("json")
        
        assert deserializer == json_value_deserializer

    def test_get_string_value_deserializer(self):
        """Test getting string value deserializer"""
        deserializer = SerializerFactory.get_value_deserializer("string")
        
        assert deserializer == string_deserializer

    def test_get_unknown_value_deserializer_returns_json(self):
        """Test that unknown deserializer type returns JSON deserializer"""
        deserializer = SerializerFactory.get_value_deserializer("unknown")
        
        assert deserializer == json_value_deserializer

    def test_get_value_deserializer_no_param_returns_json(self):
        """Test that no parameter returns JSON deserializer"""
        deserializer = SerializerFactory.get_value_deserializer()
        
        assert deserializer == json_value_deserializer


class TestSerializerIntegration:
    """Test serializer integration scenarios"""

    def test_round_trip_json_serialization(self):
        """Test complete round-trip JSON serialization/deserialization"""
        original_data = {
            "user_id": 12345,
            "username": "test_user",
            "metadata": {
                "created_at": "2023-12-01T10:00:00Z",
                "tags": ["python", "kafka", "testing"],
                "active": True
            },
            "unicode_field": "测试数据 🎯"
        }
        
        # Serialize
        json_bytes = JsonSerializer.serialize(original_data)
        
        # Deserialize
        result = JsonSerializer.deserialize(json_bytes)
        
        assert result == original_data
        assert result["unicode_field"] == "测试数据 🎯"

    def test_round_trip_default_serialization(self):
        """Test round-trip with default serializer and JSON deserializer"""
        original_data = {"message": "Hello World", "count": 42}
        
        # Serialize with default serializer
        serialized = DefaultSerializer.serialize(original_data)
        
        # Deserialize with JSON deserializer
        result = JsonSerializer.deserialize(serialized)
        
        assert result == original_data

    def test_key_serialization_scenarios(self):
        """Test various key serialization scenarios"""
        test_keys = [
            ("string_key", b"string_key"),
            (12345, b"12345"),
            (None, None),
            (b"bytes_key", b"bytes_key")
        ]
        
        for key, expected in test_keys:
            result = DefaultKeySerializer.serialize(key)
            assert result == expected

    def test_serializer_factory_integration(self):
        """Test using SerializerFactory in realistic scenarios"""
        # Get serializers from factory
        value_serializer = SerializerFactory.get_value_serializer("json")
        key_serializer = SerializerFactory.get_key_serializer("default")
        value_deserializer = SerializerFactory.get_value_deserializer("json")
        
        # Test data
        key = "user:12345"
        value = {"name": "Alice", "age": 30, "active": True}
        
        # Serialize
        serialized_key = key_serializer(key)
        serialized_value = value_serializer(value)
        
        # Verify serialization
        assert serialized_key == b"user:12345"
        assert isinstance(serialized_value, bytes)
        
        # Deserialize value
        deserialized_value = value_deserializer(serialized_value)
        
        assert deserialized_value == value

    def test_error_handling_scenarios(self):
        """Test error handling in serialization scenarios"""
        # Test JSON deserialization with malformed data
        malformed_json = b'{"key": "value", invalid}'
        result = JsonSerializer.deserialize(malformed_json)
        
        # Should fallback to string
        assert isinstance(result, str)
        assert result == '{"key": "value", invalid}'


class TestEdgeCases:
    """Test edge cases and boundary conditions"""

    def test_empty_data_serialization(self):
        """Test serialization of empty data structures"""
        test_cases = [
            ({}, b'{}'),
            ([], b'[]'),
            ("", b'""'),  # JSON serializer adds quotes
            (0, b'0'),
            (False, b'false')
        ]
        
        for data, expected in test_cases:
            result = JsonSerializer.serialize(data)
            assert result == expected

    def test_large_data_serialization(self):
        """Test serialization of large data structures"""
        # Create a reasonably large data structure
        large_data = {
            "items": [{"id": i, "name": f"item_{i}"} for i in range(1000)],
            "metadata": {"total": 1000, "generated": True}
        }
        
        # Should handle large data without issues
        result = DefaultSerializer.serialize(large_data)
        assert isinstance(result, bytes)
        assert len(result) > 0
        
        # Should be deserializable
        deserialized = JsonSerializer.deserialize(result)
        assert deserialized["metadata"]["total"] == 1000
        assert len(deserialized["items"]) == 1000

    def test_special_character_handling(self):
        """Test handling of special characters"""
        special_data = {
            "newlines": "line1\nline2\r\nline3",
            "tabs": "col1\tcol2\tcol3",
            "quotes": 'He said "Hello" and \'Hi\'',
            "unicode": "Émojis: 🎉🚀💡 Chinese: 你好世界"
        }
        
        # Serialize and deserialize
        serialized = JsonSerializer.serialize(special_data)
        deserialized = JsonSerializer.deserialize(serialized)
        
        assert deserialized == special_data
        assert deserialized["unicode"] == "Émojis: 🎉🚀💡 Chinese: 你好世界"


if __name__ == "__main__":
    pass