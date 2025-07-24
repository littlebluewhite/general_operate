"""
Kafka 序列化工具測試
測試 Kafka 消息序列化和反序列化功能
"""

import pytest
import json
from typing import Any
from datetime import datetime

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
    """測試 DefaultSerializer 類"""
    
    def test_serialize_bytes(self):
        """測試序列化 bytes 數據"""
        data = b"test bytes"
        result = DefaultSerializer.serialize(data)
        
        assert result == data
        assert isinstance(result, bytes)
    
    def test_serialize_string(self):
        """測試序列化字符串"""
        data = "test string"
        result = DefaultSerializer.serialize(data)
        
        assert result == data.encode('utf-8')
        assert isinstance(result, bytes)
    
    def test_serialize_string_with_chinese(self):
        """測試序列化中文字符串"""
        data = "測試中文字符串"
        result = DefaultSerializer.serialize(data)
        
        assert result == data.encode('utf-8')
        assert result.decode('utf-8') == data
    
    def test_serialize_none(self):
        """測試序列化 None 值"""
        result = DefaultSerializer.serialize(None)
        
        assert result == b''
        assert isinstance(result, bytes)
    
    def test_serialize_dict(self):
        """測試序列化字典對象"""
        data = {"key": "value", "number": 123}
        result = DefaultSerializer.serialize(data)
        
        expected = json.dumps(data, ensure_ascii=False).encode('utf-8')
        assert result == expected
    
    def test_serialize_list(self):
        """測試序列化列表對象"""
        data = [1, 2, 3, "test", {"nested": True}]
        result = DefaultSerializer.serialize(data)
        
        expected = json.dumps(data, ensure_ascii=False).encode('utf-8')
        assert result == expected
    
    def test_serialize_number(self):
        """測試序列化數字"""
        data = 12345
        result = DefaultSerializer.serialize(data)
        
        expected = json.dumps(data, ensure_ascii=False).encode('utf-8')
        assert result == expected
    
    def test_serialize_boolean(self):
        """測試序列化布爾值"""
        # True
        result_true = DefaultSerializer.serialize(True)
        expected_true = json.dumps(True, ensure_ascii=False).encode('utf-8')
        assert result_true == expected_true
        
        # False
        result_false = DefaultSerializer.serialize(False)
        expected_false = json.dumps(False, ensure_ascii=False).encode('utf-8')
        assert result_false == expected_false
    
    def test_serialize_complex_object(self):
        """測試序列化複雜對象"""
        data = {
            "user": {
                "id": 1,
                "name": "張三",
                "email": "zhangsan@example.com",
                "active": True,
                "roles": ["admin", "user"],
                "metadata": {
                    "created_at": "2023-01-01T00:00:00Z",
                    "updated_at": None
                }
            }
        }
        
        result = DefaultSerializer.serialize(data)
        expected = json.dumps(data, ensure_ascii=False).encode('utf-8')
        assert result == expected


class TestJsonSerializer:
    """測試 JsonSerializer 類"""
    
    def test_serialize_dict(self):
        """測試 JSON 序列化字典"""
        data = {"key": "value", "number": 123}
        result = JsonSerializer.serialize(data)
        
        expected = json.dumps(data, ensure_ascii=False).encode('utf-8')
        assert result == expected
    
    def test_serialize_list(self):
        """測試 JSON 序列化列表"""
        data = [1, 2, 3, "test"]
        result = JsonSerializer.serialize(data)
        
        expected = json.dumps(data, ensure_ascii=False).encode('utf-8')
        assert result == expected
    
    def test_serialize_string(self):
        """測試 JSON 序列化字符串"""
        data = "simple string"
        result = JsonSerializer.serialize(data)
        
        expected = json.dumps(data, ensure_ascii=False).encode('utf-8')
        assert result == expected
    
    def test_serialize_chinese_characters(self):
        """測試 JSON 序列化中文字符"""
        data = {"message": "你好世界", "user": "用戶"}
        result = JsonSerializer.serialize(data)
        
        # 解析回來驗證
        parsed = json.loads(result.decode('utf-8'))
        assert parsed["message"] == "你好世界"
        assert parsed["user"] == "用戶"
    
    def test_deserialize_valid_json(self):
        """測試反序列化有效 JSON"""
        data = {"key": "value", "number": 123}
        serialized = json.dumps(data, ensure_ascii=False).encode('utf-8')
        
        result = JsonSerializer.deserialize(serialized)
        
        assert result == data
        assert result["key"] == "value"
        assert result["number"] == 123
    
    def test_deserialize_empty_bytes(self):
        """測試反序列化空 bytes"""
        result = JsonSerializer.deserialize(b'')
        
        assert result is None
    
    def test_deserialize_invalid_json(self):
        """測試反序列化無效 JSON"""
        invalid_json = b'not a json string'
        
        result = JsonSerializer.deserialize(invalid_json)
        
        # 應該返回原始字符串
        assert result == "not a json string"
    
    def test_deserialize_chinese_json(self):
        """測試反序列化中文 JSON"""
        data = {"消息": "測試消息", "數據": [1, 2, 3]}
        serialized = json.dumps(data, ensure_ascii=False).encode('utf-8')
        
        result = JsonSerializer.deserialize(serialized)
        
        assert result == data
        assert result["消息"] == "測試消息"
    
    def test_round_trip_serialization(self):
        """測試往返序列化"""
        original_data = {
            "user": "王五",
            "items": [{"id": 1, "name": "商品A"}, {"id": 2, "name": "商品B"}],
            "total": 199.99,
            "valid": True
        }
        
        # 序列化
        serialized = JsonSerializer.serialize(original_data)
        # 反序列化
        deserialized = JsonSerializer.deserialize(serialized)
        
        assert deserialized == original_data


class TestDefaultKeySerializer:
    """測試 DefaultKeySerializer 類"""
    
    def test_serialize_none_key(self):
        """測試序列化 None 鍵"""
        result = DefaultKeySerializer.serialize(None)
        
        assert result is None
    
    def test_serialize_bytes_key(self):
        """測試序列化 bytes 鍵"""
        key = b"byte_key"
        result = DefaultKeySerializer.serialize(key)
        
        assert result == key
        assert isinstance(result, bytes)
    
    def test_serialize_string_key(self):
        """測試序列化字符串鍵"""
        key = "string_key"
        result = DefaultKeySerializer.serialize(key)
        
        assert result == key.encode('utf-8')
        assert isinstance(result, bytes)
    
    def test_serialize_number_key(self):
        """測試序列化數字鍵"""
        key = 12345
        result = DefaultKeySerializer.serialize(key)
        
        assert result == str(key).encode('utf-8')
        assert result == b"12345"
    
    def test_serialize_chinese_key(self):
        """測試序列化中文鍵"""
        key = "中文鍵名"
        result = DefaultKeySerializer.serialize(key)
        
        assert result == key.encode('utf-8')
        assert result.decode('utf-8') == key


class TestSerializerFunctions:
    """測試序列化函數"""
    
    def test_default_value_serializer_function(self):
        """測試默認值序列化函數"""
        data = {"test": "data"}
        result = default_value_serializer(data)
        
        expected = DefaultSerializer.serialize(data)
        assert result == expected
    
    def test_json_value_serializer_function(self):
        """測試 JSON 值序列化函數"""
        data = {"test": "data"}
        result = json_value_serializer(data)
        
        expected = JsonSerializer.serialize(data)
        assert result == expected
    
    def test_default_key_serializer_function(self):
        """測試默認鍵序列化函數"""
        key = "test_key"
        result = default_key_serializer(key)
        
        expected = DefaultKeySerializer.serialize(key)
        assert result == expected
    
    def test_json_value_deserializer_function(self):
        """測試 JSON 值反序列化函數"""
        data = {"test": "data"}
        serialized = json.dumps(data, ensure_ascii=False).encode('utf-8')
        
        result = json_value_deserializer(serialized)
        
        assert result == data
    
    def test_string_deserializer_function(self):
        """測試字符串反序列化函數"""
        data = "test string"
        serialized = data.encode('utf-8')
        
        result = string_deserializer(serialized)
        
        assert result == data
    
    def test_string_deserializer_none(self):
        """測試字符串反序列化 None"""
        result = string_deserializer(None)
        
        assert result is None


class TestSerializerFactory:
    """測試 SerializerFactory 類"""
    
    def test_get_default_value_serializer(self):
        """測試獲取默認值序列化器"""
        serializer = SerializerFactory.get_value_serializer("default")
        
        assert serializer == default_value_serializer
        
        # 測試功能
        data = {"test": "data"}
        result = serializer(data)
        expected = default_value_serializer(data)
        assert result == expected
    
    def test_get_json_value_serializer(self):
        """測試獲取 JSON 值序列化器"""
        serializer = SerializerFactory.get_value_serializer("json")
        
        assert serializer == json_value_serializer
        
        # 測試功能
        data = {"test": "data"}
        result = serializer(data)
        expected = json_value_serializer(data)
        assert result == expected
    
    def test_get_unknown_value_serializer(self):
        """測試獲取未知類型的值序列化器（應返回默認）"""
        serializer = SerializerFactory.get_value_serializer("unknown")
        
        assert serializer == default_value_serializer
    
    def test_get_default_key_serializer(self):
        """測試獲取默認鍵序列化器"""
        serializer = SerializerFactory.get_key_serializer("default")
        
        assert serializer == default_key_serializer
        
        # 測試功能
        key = "test_key"
        result = serializer(key)
        expected = default_key_serializer(key)
        assert result == expected
    
    def test_get_unknown_key_serializer(self):
        """測試獲取未知類型的鍵序列化器（應返回默認）"""
        serializer = SerializerFactory.get_key_serializer("unknown")
        
        assert serializer == default_key_serializer
    
    def test_get_json_value_deserializer(self):
        """測試獲取 JSON 值反序列化器"""
        deserializer = SerializerFactory.get_value_deserializer("json")
        
        assert deserializer == json_value_deserializer
        
        # 測試功能
        data = {"test": "data"}
        serialized = json.dumps(data, ensure_ascii=False).encode('utf-8')
        result = deserializer(serialized)
        assert result == data
    
    def test_get_string_value_deserializer(self):
        """測試獲取字符串值反序列化器"""
        deserializer = SerializerFactory.get_value_deserializer("string")
        
        assert deserializer == string_deserializer
        
        # 測試功能
        data = "test string"
        serialized = data.encode('utf-8')
        result = deserializer(serialized)
        assert result == data
    
    def test_get_unknown_value_deserializer(self):
        """測試獲取未知類型的值反序列化器（應返回默認）"""
        deserializer = SerializerFactory.get_value_deserializer("unknown")
        
        assert deserializer == json_value_deserializer


class TestSerializationIntegration:
    """測試序列化集成場景"""
    
    def test_kafka_message_simulation(self):
        """測試模擬 Kafka 消息處理"""
        # 模擬生產者端
        message_key = "user_123"
        message_value = {
            "event": "user_login",
            "user_id": 123,
            "timestamp": "2023-01-01T12:00:00Z",
            "metadata": {
                "ip": "192.168.1.1",
                "user_agent": "Mozilla/5.0"
            }
        }
        
        # 序列化
        key_serializer = SerializerFactory.get_key_serializer("default")
        value_serializer = SerializerFactory.get_value_serializer("json")
        
        serialized_key = key_serializer(message_key)
        serialized_value = value_serializer(message_value)
        
        # 模擬消費者端
        value_deserializer = SerializerFactory.get_value_deserializer("json")
        
        deserialized_key = serialized_key.decode('utf-8')
        deserialized_value = value_deserializer(serialized_value)
        
        # 驗證
        assert deserialized_key == message_key
        assert deserialized_value == message_value
        assert deserialized_value["event"] == "user_login"
        assert deserialized_value["user_id"] == 123
    
    def test_error_handling_integration(self):
        """測試錯誤處理集成"""
        # 測試反序列化損壞的數據
        corrupted_data = b"corrupted json data {"
        deserializer = SerializerFactory.get_value_deserializer("json")
        
        result = deserializer(corrupted_data)
        
        # 應該回退到字符串
        assert result == "corrupted json data {"
    
    def test_different_data_types_handling(self):
        """測試不同數據類型處理"""
        test_cases = [
            # (原始數據, 序列化器類型, 預期結果類型)
            ("string", "default", str),
            (123, "default", int),
            (123.45, "default", float),
            (True, "default", bool),
            (None, "default", type(None)),
            ([1, 2, 3], "json", list),
            ({"key": "value"}, "json", dict),
        ]
        
        for original_data, serializer_type, expected_type in test_cases:
            # 序列化
            serializer = SerializerFactory.get_value_serializer(serializer_type)
            serialized = serializer(original_data)
            
            # 反序列化
            deserializer = SerializerFactory.get_value_deserializer("json")
            deserialized = deserializer(serialized)
            
            if original_data is not None:
                assert isinstance(deserialized, expected_type)
                assert deserialized == original_data
            else:
                assert deserialized is None
    
    def test_chinese_characters_end_to_end(self):
        """測試中文字符端到端處理"""
        chinese_data = {
            "用戶名": "張三",
            "消息": "你好，世界！",
            "標籤": ["重要", "緊急", "待處理"],
            "詳情": {
                "創建時間": "2023-01-01T12:00:00Z",
                "狀態": "活躍"
            }
        }
        
        # 完整的序列化-反序列化循環
        serializer = SerializerFactory.get_value_serializer("json")
        deserializer = SerializerFactory.get_value_deserializer("json")
        
        serialized = serializer(chinese_data)
        deserialized = deserializer(serialized)
        
        assert deserialized == chinese_data
        assert deserialized["用戶名"] == "張三"
        assert deserialized["消息"] == "你好，世界！"
        assert deserialized["標籤"] == ["重要", "緊急", "待處理"]
        assert deserialized["詳情"]["狀態"] == "活躍"


if __name__ == "__main__":
    # 運行測試
    pytest.main([__file__, "-v"])