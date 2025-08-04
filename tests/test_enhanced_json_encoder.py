"""
Comprehensive tests for EnhancedJSONEncoder
Tests the datetime JSON serialization fix in the GeneralOperate package.
"""

import json
import pytest
from datetime import datetime, date, time, timezone, UTC
from decimal import Decimal
from uuid import UUID, uuid4
from collections.abc import Mapping, Set
from typing import Any
import asyncio
from unittest.mock import AsyncMock, Mock

# Import the class we're testing
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from general_operate.app.cache_operate import CacheOperate
from general_operate.utils.json_encoder import EnhancedJSONEncoder


class TestEnhancedJSONEncoder:
    """Unit tests for EnhancedJSONEncoder class"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.encoder = EnhancedJSONEncoder()
        self.sample_datetime = datetime(2025, 1, 15, 12, 30, 45, 123456, tzinfo=UTC)
        self.sample_date = date(2025, 1, 15)
        self.sample_time = time(12, 30, 45, 123456)
        self.sample_uuid = UUID('12345678-1234-5678-9abc-123456789abc')
        self.sample_decimal = Decimal('123.456')
    
    def test_datetime_serialization(self):
        """Test datetime objects are converted to ISO format"""
        result = self.encoder.default(self.sample_datetime)
        expected = "2025-01-15T12:30:45.123456+00:00"
        assert result == expected
        
        # Test serialization with json.dumps
        data = {"timestamp": self.sample_datetime}
        json_str = json.dumps(data, cls=EnhancedJSONEncoder)
        assert expected in json_str
        
        # Test timezone-aware datetime
        tz_datetime = datetime(2025, 1, 15, 12, 30, 45, tzinfo=timezone.utc)
        result = self.encoder.default(tz_datetime)
        assert result.endswith('+00:00')
    
    def test_date_serialization(self):
        """Test date objects are converted to ISO format"""
        result = self.encoder.default(self.sample_date)
        expected = "2025-01-15"
        assert result == expected
        
        data = {"birth_date": self.sample_date}
        json_str = json.dumps(data, cls=EnhancedJSONEncoder)
        assert expected in json_str
    
    def test_time_serialization(self):
        """Test time objects are converted to ISO format"""
        result = self.encoder.default(self.sample_time)
        expected = "12:30:45.123456"
        assert result == expected
        
        data = {"appointment_time": self.sample_time}
        json_str = json.dumps(data, cls=EnhancedJSONEncoder)
        assert expected in json_str
    
    def test_decimal_serialization(self):
        """Test Decimal objects are converted to float"""
        result = self.encoder.default(self.sample_decimal)
        assert isinstance(result, float)
        assert result == 123.456
        
        # Test with very precise decimal
        precise_decimal = Decimal('123.123456789012345')
        result = self.encoder.default(precise_decimal)
        assert isinstance(result, float)
        
        data = {"price": self.sample_decimal}
        json_str = json.dumps(data, cls=EnhancedJSONEncoder)
        assert "123.456" in json_str
    
    def test_bytes_serialization(self):
        """Test bytes objects are decoded to UTF-8 strings"""
        test_bytes = b"Hello World"
        result = self.encoder.default(test_bytes)
        assert result == "Hello World"
        
        # Test with non-UTF8 bytes (should handle gracefully)
        binary_data = b'\xff\xfe\x00\x01'
        result = self.encoder.default(binary_data)
        assert isinstance(result, str)  # Should not raise exception
        
        data = {"binary_data": test_bytes}
        json_str = json.dumps(data, cls=EnhancedJSONEncoder)
        assert "Hello World" in json_str
    
    def test_set_serialization(self):
        """Test set objects are converted to lists"""
        test_set = {"apple", "banana", "cherry"}
        result = self.encoder.default(test_set)
        assert isinstance(result, list)
        assert len(result) == 3
        assert all(item in result for item in test_set)
        
        data = {"tags": test_set}
        json_str = json.dumps(data, cls=EnhancedJSONEncoder)
        parsed = json.loads(json_str)
        assert isinstance(parsed["tags"], list)
        assert len(parsed["tags"]) == 3
    
    def test_uuid_serialization(self):
        """Test UUID objects are converted to strings"""
        result = self.encoder.default(self.sample_uuid)
        expected = "12345678-1234-5678-9abc-123456789abc"
        assert result == expected
        
        data = {"user_id": self.sample_uuid}
        json_str = json.dumps(data, cls=EnhancedJSONEncoder)
        assert expected in json_str
        
        # Test with random UUID
        random_uuid = uuid4()
        result = self.encoder.default(random_uuid)
        assert isinstance(result, str)
        assert len(result) == 36  # Standard UUID string length
    
    def test_object_with_dict_serialization(self):
        """Test objects with __dict__ are converted to filtered dictionaries"""
        
        class MockUser:
            def __init__(self):
                self.id = 123
                self.name = "John Doe"
                self.email = "john@example.com"
                self._password = "secret"  # Should be filtered out
                self._internal_state = "private"  # Should be filtered out
        
        user = MockUser()
        result = self.encoder.default(user)
        
        assert isinstance(result, dict)
        assert result["id"] == 123
        assert result["name"] == "John Doe"
        assert result["email"] == "john@example.com"
        assert "_password" not in result
        assert "_internal_state" not in result
        
        data = {"user": user}
        json_str = json.dumps(data, cls=EnhancedJSONEncoder)
        parsed = json.loads(json_str)
        assert parsed["user"]["name"] == "John Doe"
        assert "_password" not in parsed["user"]
    
    def test_unsupported_type_with_dict_fallback(self):
        """Test that objects with __dict__ get converted to dict representation"""
        
        class ObjectWithDict:
            def __init__(self):
                self.name = "test"
                self.value = 42
                self._private = "hidden"  # Should be filtered out
        
        obj = ObjectWithDict()
        result = self.encoder.default(obj)
        
        # Should return dict representation without private attributes
        expected = {"name": "test", "value": 42}
        assert result == expected
    
    def test_unsupported_type_without_dict_raises_typeerror(self):
        """Test that objects without __dict__ fall back to base class and raise TypeError"""
        
        class UnsupportedTypeWithoutDict:
            __slots__ = []  # No __dict__ attribute
        
        obj = UnsupportedTypeWithoutDict()
        with pytest.raises(TypeError):
            self.encoder.default(obj)
    
    def test_complex_nested_data(self):
        """Test serialization of complex nested data structures"""
        complex_data = {
            "user_id": self.sample_uuid,
            "created_at": self.sample_datetime,
            "profile": {
                "birth_date": self.sample_date,
                "preferences": {"theme", "notifications", "email"},
                "balance": self.sample_decimal,
                "metadata": {
                    "last_login": self.sample_datetime,
                    "session_data": b"binary_session_data"
                }
            },
            "tags": {"premium", "verified"},
            "schedule": {
                "meeting_time": self.sample_time
            }
        }
        
        # Should not raise any exceptions
        json_str = json.dumps(complex_data, cls=EnhancedJSONEncoder)
        
        # Verify we can parse it back
        parsed = json.loads(json_str)
        
        assert parsed["user_id"] == str(self.sample_uuid)
        assert "2025-01-15T12:30:45.123456+00:00" in parsed["created_at"]
        assert parsed["profile"]["birth_date"] == "2025-01-15"
        assert isinstance(parsed["profile"]["preferences"], list)
        assert isinstance(parsed["profile"]["balance"], float)
        assert isinstance(parsed["tags"], list)
    
    def test_performance_benchmark(self):
        """Test performance with large amounts of datetime objects"""
        import time
        
        # Create data with many datetime objects
        test_data = {
            f"timestamp_{i}": datetime.now(UTC)
            for i in range(1000)
        }
        
        start_time = time.time()
        json_str = json.dumps(test_data, cls=EnhancedJSONEncoder)
        end_time = time.time()
        
        # Should complete in reasonable time (< 1 second for 1000 objects)
        duration = end_time - start_time
        assert duration < 1.0, f"Serialization took too long: {duration:.2f}s"
        
        # Verify result is valid JSON
        parsed = json.loads(json_str)
        assert len(parsed) == 1000
    
    def test_original_datetime_issue_regression(self):
        """Regression test - ensure the original datetime issue is fixed"""
        
        # This is the exact scenario that was failing before the fix
        cache_data = {
            "user_id": "12345",
            "created_at": datetime.now(UTC),  # This was causing TypeError
            "updated_at": datetime.now(UTC),
            "metadata": {
                "last_access": datetime.now(UTC),
                "preferences": {"dark_mode", "notifications"},  # Set was also problematic
                "balance": Decimal('999.99')  # Decimal was also problematic
            }
        }
        
        # This should NOT raise a TypeError anymore
        try:
            json_str = json.dumps(cache_data, cls=EnhancedJSONEncoder)
            parsed = json.loads(json_str)
            
            # Verify all the problematic types were handled correctly
            assert isinstance(parsed["created_at"], str)
            assert isinstance(parsed["metadata"]["preferences"], list)
            assert isinstance(parsed["metadata"]["balance"], float)
            
        except TypeError as e:
            pytest.fail(f"Regression: datetime serialization still failing: {e}")


class TestCacheOperateIntegration:
    """Integration tests for CacheOperate with EnhancedJSONEncoder"""
    
    def setup_method(self):
        """Set up mock Redis client for testing"""
        self.mock_redis = AsyncMock()
        self.cache_operate = CacheOperate(self.mock_redis)
    
    @pytest.mark.asyncio
    async def test_store_cache_with_datetime(self):
        """Test storing cache data with datetime objects"""
        
        # Mock Redis setex to succeed
        self.mock_redis.setex = AsyncMock(return_value=True)
        
        cache_data = {
            "user_id": "test_user",
            "login_time": datetime.now(UTC),
            "last_access": datetime.now(UTC),
            "preferences": {"theme", "lang"},
            "balance": Decimal('123.45')
        }
        
        # This should not raise any exceptions
        await self.cache_operate.store_cache("user_session", "test_user", cache_data)
        
        # Verify Redis setex was called
        self.mock_redis.setex.assert_called_once()
        
        # Get the actual serialized data that would be stored
        call_args = self.mock_redis.setex.call_args
        key, ttl, serialized_data = call_args[0]
        
        # Verify the serialized data is valid JSON
        parsed_data = json.loads(serialized_data)
        
        # Verify datetime was serialized correctly
        assert isinstance(parsed_data["login_time"], str)
        assert "T" in parsed_data["login_time"]  # ISO format
        assert isinstance(parsed_data["preferences"], list)
        assert isinstance(parsed_data["balance"], float)
    
    @pytest.mark.asyncio
    async def test_store_caches_batch_with_datetime(self):
        """Test batch storing of cache data with datetime objects"""
        
        # Mock Redis pipeline with proper async context manager
        mock_pipeline = AsyncMock()
        mock_pipeline.setex = Mock()
        mock_pipeline.execute = AsyncMock(return_value=[True, True])
        
        # Create a proper async context manager mock
        pipeline_context_manager = AsyncMock()
        pipeline_context_manager.__aenter__ = AsyncMock(return_value=mock_pipeline)
        pipeline_context_manager.__aexit__ = AsyncMock(return_value=None)
        
        # Configure pipeline() to return the context manager
        self.mock_redis.pipeline = Mock(return_value=pipeline_context_manager)
        
        batch_data = {
            "user1": {
                "created_at": datetime.now(UTC),
                "data": {"tags": {"premium", "verified"}}
            },
            "user2": {
                "created_at": datetime.now(UTC), 
                "data": {"balance": Decimal('456.78')}
            }
        }
        
        # This should not raise any exceptions
        result = await self.cache_operate.store_caches("user_data", batch_data)
        
        assert result is True
        mock_pipeline.execute.assert_called_once()
    
    def test_cache_metadata_with_datetime(self):
        """Test that cache metadata with _created_at datetime works"""
        
        # Simulate the __store_cache_inner method behavior
        test_data = {"user_id": "123", "name": "Test User"}
        
        # This mimics what happens in __store_cache_inner
        enriched_data = {
            **test_data,
            "_created_at": datetime.now(UTC).isoformat(),  # Already ISO in real code
            "prefix": "test",
            "_identifier": "123"
        }
        
        # The issue was when test_data itself contained datetime objects
        test_data_with_datetime = {
            "user_id": "123",
            "name": "Test User", 
            "last_login": datetime.now(UTC),  # This was the problem
            "metadata": {
                "created": datetime.now(UTC),
                "tags": {"admin", "user"}
            }
        }
        
        enriched_data_with_datetime = {
            **test_data_with_datetime,
            "_created_at": datetime.now(UTC).isoformat(),
            "prefix": "test",
            "_identifier": "123"
        }
        
        # This should work now with our EnhancedJSONEncoder
        serialized = json.dumps(enriched_data_with_datetime, cls=EnhancedJSONEncoder)
        parsed = json.loads(serialized)
        
        assert isinstance(parsed["last_login"], str)
        assert isinstance(parsed["metadata"]["created"], str) 
        assert isinstance(parsed["metadata"]["tags"], list)


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__, "-v"])