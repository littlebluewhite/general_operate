"""Tests for core error handling and logging functionality"""

import pytest
import asyncio
from pathlib import Path
from unittest.mock import Mock, patch
import tempfile

from general_operate.core import (
    ErrorCode,
    ErrorContext,
    GeneralOperateException,
    DatabaseException,
    CacheException,
    ValidationException,
    handle_errors,
    retry_on_error,
    configure_logging,
    get_logger,
    LogLevel,
    LogContext,
    log_performance,
)
from structlog.testing import capture_logs


class TestExceptions:
    """Test exception classes"""
    
    def test_general_operate_exception(self):
        """Test GeneralOperateException"""
        context = ErrorContext(
            operation="test_op",
            resource="test_resource",
            details={"key": "value"}
        )
        
        exc = GeneralOperateException(
            code=ErrorCode.UNKNOWN_ERROR,
            message="Test error",
            context=context
        )
        
        assert exc.code == ErrorCode.UNKNOWN_ERROR
        assert exc.message == "Test error"
        assert exc.context == context
        assert "[UNKNOWN_ERROR]" in str(exc)
        assert "test_op" in str(exc)
        assert "test_resource" in str(exc)
    
    def test_exception_to_dict(self):
        """Test exception to_dict method"""
        context = ErrorContext(
            operation="test_op",
            resource="test_resource",
            details={"key": "value"}
        )
        
        cause = ValueError("Original error")
        exc = GeneralOperateException(
            code=ErrorCode.DB_CONNECTION_ERROR,
            message="Connection failed",
            context=context,
            cause=cause
        )
        
        exc_dict = exc.to_dict()
        assert exc_dict["error_code"] == ErrorCode.DB_CONNECTION_ERROR.value
        assert exc_dict["error_name"] == "DB_CONNECTION_ERROR"
        assert exc_dict["message"] == "Connection failed"
        assert exc_dict["context"]["operation"] == "test_op"
        assert exc_dict["context"]["resource"] == "test_resource"
        assert exc_dict["context"]["details"]["key"] == "value"
        assert exc_dict["cause"] == "Original error"
    
    def test_database_exception(self):
        """Test DatabaseException"""
        exc = DatabaseException(
            code=ErrorCode.DB_QUERY_ERROR,
            message="Query failed"
        )
        assert isinstance(exc, GeneralOperateException)
        assert exc.code == ErrorCode.DB_QUERY_ERROR
    
    def test_validation_exception(self):
        """Test ValidationException"""
        exc = ValidationException(
            field="email",
            value="invalid",
            message="Invalid email format"
        )
        assert exc.code == ErrorCode.VALIDATION_ERROR
        assert exc.context.details["field"] == "email"
        assert exc.context.details["value"] == "invalid"


class TestDecorators:
    """Test decorator functions"""
    
    def test_handle_errors_sync(self):
        """Test handle_errors decorator with sync function"""
        @handle_errors(
            operation="test_sync",
            default_error_code=ErrorCode.UNKNOWN_ERROR
        )
        def test_func():
            raise ValueError("Test error")
        
        with pytest.raises(GeneralOperateException) as exc_info:
            test_func()
        
        assert exc_info.value.code == ErrorCode.UNKNOWN_ERROR
        assert exc_info.value.message == "Test error"
        assert exc_info.value.context.operation == "test_sync"
    
    @pytest.mark.asyncio
    async def test_handle_errors_async(self):
        """Test handle_errors decorator with async function"""
        @handle_errors(
            operation="test_async",
            default_error_code=ErrorCode.DB_QUERY_ERROR
        )
        async def test_func():
            raise RuntimeError("Async error")
        
        with pytest.raises(GeneralOperateException) as exc_info:
            await test_func()
        
        assert exc_info.value.code == ErrorCode.DB_QUERY_ERROR
        assert exc_info.value.message == "Async error"
    
    def test_handle_errors_with_resource_getter(self):
        """Test handle_errors with resource_getter"""
        class Service:
            def __init__(self, name):
                self.name = name
            
            @handle_errors(
                operation="service_op",
                resource_getter=lambda self: self.name
            )
            def do_something(self):
                raise Exception("Service error")
        
        service = Service("test_service")
        
        with pytest.raises(GeneralOperateException) as exc_info:
            service.do_something()
        
        assert exc_info.value.context.resource == "test_service"
    
    def test_handle_errors_no_raise(self):
        """Test handle_errors with raise_on_error=False"""
        @handle_errors(
            operation="test_no_raise",
            raise_on_error=False,
            log_errors=False
        )
        def test_func():
            raise ValueError("Should not raise")
        
        result = test_func()
        assert result is None
    
    def test_retry_on_error_sync(self):
        """Test retry_on_error decorator with sync function"""
        call_count = 0
        
        @retry_on_error(
            max_attempts=3,
            delay=0.01,
            exceptions=(ValueError,)
        )
        def test_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("Retry me")
            return "Success"
        
        result = test_func()
        assert result == "Success"
        assert call_count == 3
    
    @pytest.mark.asyncio
    async def test_retry_on_error_async(self):
        """Test retry_on_error decorator with async function"""
        call_count = 0
        
        @retry_on_error(
            max_attempts=2,
            delay=0.01,
            exceptions=(RuntimeError,)
        )
        async def test_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise RuntimeError("Retry async")
            return "Async success"
        
        result = await test_func()
        assert result == "Async success"
        assert call_count == 2
    
    def test_retry_on_error_max_attempts_exceeded(self):
        """Test retry_on_error when max attempts exceeded"""
        @retry_on_error(
            max_attempts=2,
            delay=0.01
        )
        def test_func():
            raise ValueError("Always fails")
        
        with pytest.raises(ValueError, match="Always fails"):
            test_func()


class TestLogging:
    """Test logging functionality"""
    
    def test_configure_logging_development(self):
        """Test logging configuration for development"""
        with capture_logs() as cap_logs:
            configure_logging(
                level=LogLevel.DEBUG,
                use_json=False,
                add_timestamp=True,
                add_caller_info=True
            )
            
            logger = get_logger("test")
            logger.debug("Debug message", key="value")
            
            assert len(cap_logs) > 0
            assert cap_logs[0]["event"] == "Debug message"
            assert cap_logs[0]["key"] == "value"
            assert cap_logs[0]["log_level"] == "debug"
    
    def test_configure_logging_production(self):
        """Test logging configuration for production"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            log_file = Path(f.name)
        
        try:
            configure_logging(
                level=LogLevel.INFO,
                use_json=True,
                log_file=log_file,
                add_caller_info=False
            )
            
            logger = get_logger("test")
            logger.info("Production log", env="prod")
            
            # Check file was created
            assert log_file.exists()
            
        finally:
            log_file.unlink(missing_ok=True)
    
    def test_log_context(self):
        """Test LogContext"""
        with capture_logs() as cap_logs:
            logger = get_logger("test")
            
            with LogContext(request_id="123", user_id="456"):
                logger.info("With context")
            
            logger.info("Without context")
            
            assert cap_logs[0]["request_id"] == "123"
            assert cap_logs[0]["user_id"] == "456"
            assert "request_id" not in cap_logs[1]
    
    def test_log_performance_sync(self):
        """Test log_performance decorator with sync function"""
        with capture_logs() as cap_logs:
            @log_performance("test_operation", threshold_ms=10)
            def slow_func():
                import time
                time.sleep(0.02)  # 20ms
                return "Done"
            
            result = slow_func()
            assert result == "Done"
            
            # Find the operation_completed log
            perf_logs = [log for log in cap_logs if log["event"] == "operation_completed"]
            assert len(perf_logs) == 1
            assert perf_logs[0]["operation"] == "test_operation"
            assert perf_logs[0]["slow"] is True
            assert perf_logs[0]["duration_ms"] > 10
    
    @pytest.mark.asyncio
    async def test_log_performance_async(self):
        """Test log_performance decorator with async function"""
        with capture_logs() as cap_logs:
            @log_performance("async_operation", threshold_ms=50)
            async def fast_func():
                await asyncio.sleep(0.01)  # 10ms
                return "Fast"
            
            result = await fast_func()
            assert result == "Fast"
            
            perf_logs = [log for log in cap_logs if log["event"] == "operation_completed"]
            assert len(perf_logs) == 1
            assert perf_logs[0]["operation"] == "async_operation"
            assert perf_logs[0]["slow"] is False
    
    def test_log_performance_with_error(self):
        """Test log_performance when function raises error"""
        with capture_logs() as cap_logs:
            @log_performance("failing_operation")
            def failing_func():
                raise ValueError("Operation failed")
            
            with pytest.raises(ValueError):
                failing_func()
            
            error_logs = [log for log in cap_logs if log["event"] == "operation_failed"]
            assert len(error_logs) == 1
            assert error_logs[0]["operation"] == "failing_operation"
            assert "Operation failed" in error_logs[0]["error"]


class TestIntegration:
    """Integration tests"""
    
    @pytest.mark.asyncio
    async def test_combined_decorators(self):
        """Test combining multiple decorators"""
        call_count = 0
        
        @handle_errors(operation="combined_test")
        @retry_on_error(max_attempts=2, delay=0.01)
        @log_performance("combined_operation")
        async def complex_func():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("First attempt fails")
            await asyncio.sleep(0.01)
            return {"result": "success", "attempts": call_count}
        
        with capture_logs() as cap_logs:
            result = await complex_func()
            assert result["result"] == "success"
            assert result["attempts"] == 2
            
            # Check retry log
            retry_logs = [log for log in cap_logs if log["event"] == "retry_attempt"]
            assert len(retry_logs) == 1
            
            # Check performance log
            perf_logs = [log for log in cap_logs if log["event"] == "operation_completed"]
            assert len(perf_logs) == 1