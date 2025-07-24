"""
Serialization utilities for Kafka operations
"""

import json
from typing import Any, Callable


class DefaultSerializer:
    """Default serializer that handles various data types"""
    
    @staticmethod
    def serialize(value: Any) -> bytes:
        """Serialize value to bytes"""
        if isinstance(value, bytes):
            return value
        elif isinstance(value, str):
            return value.encode('utf-8')
        elif value is None:
            return b''
        else:
            # For complex objects, use JSON serialization
            return json.dumps(value, ensure_ascii=False).encode('utf-8')


class JsonSerializer:
    """JSON-specific serializer"""
    
    @staticmethod
    def serialize(value: Any) -> bytes:
        """Serialize value to JSON bytes"""
        return json.dumps(value, ensure_ascii=False).encode('utf-8')
    
    @staticmethod
    def deserialize(data: bytes) -> Any:
        """Deserialize JSON bytes to value"""
        if not data:
            return None
        try:
            return json.loads(data.decode('utf-8'))
        except json.JSONDecodeError:
            # If not JSON, return as string
            return data.decode('utf-8')


class DefaultKeySerializer:
    """Default key serializer"""
    
    @staticmethod
    def serialize(key: Any) -> bytes | None:
        """Serialize key to bytes"""
        if key is None:
            return None
        elif isinstance(key, bytes):
            return key
        else:
            return str(key).encode('utf-8')


# Pre-configured serializer functions for common use cases
def default_value_serializer(value: Any) -> bytes:
    """Default value serializer function"""
    return DefaultSerializer.serialize(value)


def json_value_serializer(value: Any) -> bytes:
    """JSON value serializer function"""
    return JsonSerializer.serialize(value)


def default_key_serializer(key: Any) -> bytes | None:
    """Default key serializer function"""
    return DefaultKeySerializer.serialize(key)


def json_value_deserializer(data: bytes) -> Any:
    """JSON value deserializer function"""
    return JsonSerializer.deserialize(data)


def string_deserializer(data: bytes) -> str | None:
    """String deserializer function"""
    if data is None:
        return None
    return data.decode('utf-8')


# Serializer factory
class SerializerFactory:
    """Factory for creating serializer functions"""
    
    @staticmethod
    def get_value_serializer(serializer_type: str = "default") -> Callable[[Any], bytes]:
        """Get value serializer by type"""
        serializers = {
            "default": default_value_serializer,
            "json": json_value_serializer,
        }
        return serializers.get(serializer_type, default_value_serializer)
    
    @staticmethod
    def get_key_serializer(serializer_type: str = "default") -> Callable[[Any], bytes | None]:
        """Get key serializer by type"""
        if serializer_type == "default":
            return default_key_serializer
        return default_key_serializer
    
    @staticmethod
    def get_value_deserializer(deserializer_type: str = "json") -> Callable[[bytes], Any]:
        """Get value deserializer by type"""
        deserializers = {
            "json": json_value_deserializer,
            "string": string_deserializer,
        }
        return deserializers.get(deserializer_type, json_value_deserializer)