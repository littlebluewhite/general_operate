"""
InfluxDB操作測試
測試 InfluxOperate 類的所有功能，包括寫入、查詢、異常處理等
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
import influxdb_client
from influxdb_client.rest import ApiException
from urllib3.exceptions import NewConnectionError

from general_operate.app.influxdb_operate import InfluxOperate
from general_operate.app.client.influxdb import InfluxDB
from general_operate.utils.exception import GeneralOperateException


@pytest.fixture
def influx_config():
    """InfluxDB 測試配置"""
    return {
        "host": "localhost",
        "port": 8086,
        "org": "test-org",
        "token": "test-token",
        "bucket": "test-bucket"
    }


@pytest.fixture
def mock_influxdb_client():
    """模擬 InfluxDB 客戶端"""
    mock_client = Mock()
    mock_writer = Mock()
    mock_reader = Mock()
    
    mock_client.write_api.return_value = mock_writer
    mock_client.query_api.return_value = mock_reader
    
    return mock_client, mock_writer, mock_reader


@pytest.fixture
def mock_influxdb(influx_config, mock_influxdb_client):
    """模擬 InfluxDB 實例"""
    mock_client, mock_writer, mock_reader = mock_influxdb_client
    
    with patch('general_operate.app.client.influxdb.influxdb_client.InfluxDBClient', return_value=mock_client):
        influxdb = InfluxDB(influx_config)
        return influxdb, mock_writer, mock_reader


class TestInfluxOperateInitialization:
    """測試 InfluxOperate 初始化"""
    
    def test_init_with_influxdb(self, mock_influxdb):
        """測試使用 InfluxDB 客戶端初始化"""
        influxdb, mock_writer, mock_reader = mock_influxdb
        
        operate = InfluxOperate(influxdb)
        
        assert operate.influxdb == influxdb
        assert operate.show_bucket() == "test-bucket"
        assert operate.show_org() == "test-org"
        assert operate.writer is not None
        assert operate.reader is not None
    
    def test_init_without_influxdb(self):
        """測試不使用 InfluxDB 客戶端初始化"""
        operate = InfluxOperate(None)
        
        assert operate.influxdb is None
        assert operate.show_bucket() is None
        assert operate.show_org() is None
        assert operate.writer is None
        assert operate.reader is None
    
    def test_init_with_custom_exception(self, mock_influxdb):
        """測試使用自定義異常類初始化"""
        influxdb, _, _ = mock_influxdb
        
        class CustomException(Exception):
            pass
        
        operate = InfluxOperate(influxdb, CustomException)
        assert operate._InfluxOperate__exc == CustomException


class TestInfluxOperateBucketAndOrg:
    """測試 bucket 和 org 管理"""
    
    def test_change_bucket(self, mock_influxdb):
        """測試更改 bucket"""
        influxdb, _, _ = mock_influxdb
        operate = InfluxOperate(influxdb)
        
        new_bucket = "new-test-bucket"
        operate.change_bucket(new_bucket)
        
        assert operate.show_bucket() == new_bucket
    
    def test_change_org(self, mock_influxdb):
        """測試更改 org"""
        influxdb, _, _ = mock_influxdb
        operate = InfluxOperate(influxdb)
        
        new_org = "new-test-org"
        operate.change_org(new_org)
        
        assert operate.show_org() == new_org
    
    def test_show_bucket_and_org(self, mock_influxdb):
        """測試顯示 bucket 和 org"""
        influxdb, _, _ = mock_influxdb
        operate = InfluxOperate(influxdb)
        
        assert operate.show_bucket() == "test-bucket"
        assert operate.show_org() == "test-org"


class TestInfluxOperateWrite:
    """測試 InfluxDB 寫入操作"""
    
    def test_write_single_point(self, mock_influxdb):
        """測試寫入單個數據點"""
        influxdb, mock_writer, _ = mock_influxdb
        operate = InfluxOperate(influxdb)
        
        # 創建測試數據點
        point = influxdb_client.Point("temperature").tag("location", "room1").field("value", 25.0)
        
        operate.write(point)
        
        mock_writer.write.assert_called_once_with(
            bucket="test-bucket", 
            org="test-org", 
            record=point
        )
    
    def test_write_multiple_points(self, mock_influxdb):
        """測試寫入多個數據點"""
        influxdb, mock_writer, _ = mock_influxdb
        operate = InfluxOperate(influxdb)
        
        # 創建測試數據點列表
        points = [
            influxdb_client.Point("temperature").tag("location", "room1").field("value", 25.0),
            influxdb_client.Point("temperature").tag("location", "room2").field("value", 23.5)
        ]
        
        operate.write(points)
        
        mock_writer.write.assert_called_once_with(
            bucket="test-bucket", 
            org="test-org", 
            record=points
        )
    
    def test_write_without_client(self):
        """測試未配置客戶端時寫入"""
        operate = InfluxOperate(None)
        point = influxdb_client.Point("temperature").field("value", 25.0)
        
        with pytest.raises(GeneralOperateException) as exc_info:
            operate.write(point)
        
        assert exc_info.value.status_code == 488
        # 由於異常處理裝飾器會重新包裝異常，檢查消息內容
        assert ("InfluxDB is not configured" in str(exc_info.value.message) or 
                "InfluxDB error" in str(exc_info.value.message))
        assert exc_info.value.message_code in [100, 99]  # 原始錯誤碼或通用錯誤碼
    
    def test_write_connection_error(self, mock_influxdb):
        """測試寫入時連接錯誤"""
        influxdb, mock_writer, _ = mock_influxdb
        operate = InfluxOperate(influxdb)
        
        # 模擬連接錯誤
        mock_writer.write.side_effect = NewConnectionError("Connection failed", "InfluxDB")
        
        point = influxdb_client.Point("temperature").field("value", 25.0)
        
        with pytest.raises(GeneralOperateException) as exc_info:
            operate.write(point)
        
        assert exc_info.value.status_code == 488
        assert "InfluxDB connection failed" in str(exc_info.value.message)
        assert exc_info.value.message_code == 1
    
    def test_write_api_exception(self, mock_influxdb):
        """測試寫入時 API 異常"""
        influxdb, mock_writer, _ = mock_influxdb
        operate = InfluxOperate(influxdb)
        
        # 模擬 API 異常
        api_error = ApiException(status=400, reason="Bad Request")
        api_error.message = "Invalid data format"
        mock_writer.write.side_effect = api_error
        
        point = influxdb_client.Point("temperature").field("value", 25.0)
        
        with pytest.raises(GeneralOperateException) as exc_info:
            operate.write(point)
        
        assert exc_info.value.status_code == 488
        assert "Invalid data format" in str(exc_info.value.message)
        assert exc_info.value.message_code == 400
    
    def test_write_validation_error(self, mock_influxdb):
        """測試寫入時數據驗證錯誤"""
        influxdb, mock_writer, _ = mock_influxdb
        operate = InfluxOperate(influxdb)
        
        # 模擬數據驗證錯誤
        mock_writer.write.side_effect = ValueError("Invalid data type")
        
        point = influxdb_client.Point("temperature").field("value", 25.0)
        
        with pytest.raises(GeneralOperateException) as exc_info:
            operate.write(point)
        
        assert exc_info.value.status_code == 400
        assert "InfluxDB data validation error" in str(exc_info.value.message)
        assert exc_info.value.message_code == 98


class TestInfluxOperateQuery:
    """測試 InfluxDB 查詢操作"""
    
    def test_query_success(self, mock_influxdb):
        """測試成功查詢"""
        influxdb, _, mock_reader = mock_influxdb
        operate = InfluxOperate(influxdb)
        
        # 模擬查詢結果
        mock_result = [{"measurement": "temperature", "value": 25.0}]
        mock_reader.query.return_value = mock_result
        
        query_string = 'from(bucket:"test-bucket") |> range(start: -1h)'
        result = operate.query(query_string)
        
        mock_reader.query.assert_called_once_with(org="test-org", query=query_string)
        assert result == mock_result
    
    def test_query_without_client(self):
        """測試未配置客戶端時查詢"""
        operate = InfluxOperate(None)
        query_string = 'from(bucket:"test-bucket") |> range(start: -1h)'
        
        with pytest.raises(GeneralOperateException) as exc_info:
            operate.query(query_string)
        
        assert exc_info.value.status_code == 488
        # 由於異常處理裝飾器會重新包裝異常，檢查消息內容
        assert ("InfluxDB is not configured" in str(exc_info.value.message) or 
                "InfluxDB error" in str(exc_info.value.message))
        assert exc_info.value.message_code in [100, 99]  # 原始錯誤碼或通用錯誤碼
    
    def test_query_connection_error(self, mock_influxdb):
        """測試查詢時連接錯誤"""
        influxdb, _, mock_reader = mock_influxdb
        operate = InfluxOperate(influxdb)
        
        # 模擬連接錯誤
        mock_reader.query.side_effect = NewConnectionError("Connection failed", "InfluxDB")
        
        query_string = 'from(bucket:"test-bucket") |> range(start: -1h)'
        
        with pytest.raises(GeneralOperateException) as exc_info:
            operate.query(query_string)
        
        assert exc_info.value.status_code == 488
        assert "InfluxDB connection failed" in str(exc_info.value.message)
        assert exc_info.value.message_code == 1
    
    def test_query_api_exception(self, mock_influxdb):
        """測試查詢時 API 異常"""
        influxdb, _, mock_reader = mock_influxdb
        operate = InfluxOperate(influxdb)
        
        # 模擬 API 異常
        api_error = ApiException(status=404, reason="Not Found")
        api_error.message = "Bucket not found"
        mock_reader.query.side_effect = api_error
        
        query_string = 'from(bucket:"test-bucket") |> range(start: -1h)'
        
        with pytest.raises(GeneralOperateException) as exc_info:
            operate.query(query_string)
        
        assert exc_info.value.status_code == 488
        assert "Bucket not found" in str(exc_info.value.message)
        assert exc_info.value.message_code == 404
    
    def test_query_attribute_error(self, mock_influxdb):
        """測試查詢時屬性錯誤"""
        influxdb, _, mock_reader = mock_influxdb
        operate = InfluxOperate(influxdb)
        
        # 模擬屬性錯誤
        mock_reader.query.side_effect = AttributeError("'NoneType' object has no attribute 'query'")
        
        query_string = 'from(bucket:"test-bucket") |> range(start: -1h)'
        
        with pytest.raises(GeneralOperateException) as exc_info:
            operate.query(query_string)
        
        assert exc_info.value.status_code == 488
        assert "InfluxDB configuration error" in str(exc_info.value.message)
        assert exc_info.value.message_code == 97


class TestInfluxOperateExceptionHandler:
    """測試異常處理裝飾器"""
    
    def test_unexpected_exception(self, mock_influxdb):
        """測試意外異常處理"""
        influxdb, mock_writer, _ = mock_influxdb
        operate = InfluxOperate(influxdb)
        
        # 模擬意外異常
        mock_writer.write.side_effect = RuntimeError("Unexpected error")
        
        point = influxdb_client.Point("temperature").field("value", 25.0)
        
        with pytest.raises(GeneralOperateException) as exc_info:
            operate.write(point)
        
        assert exc_info.value.status_code == 488
        assert "InfluxDB error: RuntimeError: Unexpected error" in str(exc_info.value.message)
        assert exc_info.value.message_code == 99
    
    def test_custom_exception_class(self, mock_influxdb):
        """測試自定義異常類別"""
        influxdb, mock_writer, _ = mock_influxdb
        
        class CustomException(Exception):
            def __init__(self, status_code, message, message_code):
                self.status_code = status_code
                self.message = message
                self.message_code = message_code
                super().__init__(message)
        
        operate = InfluxOperate(influxdb, CustomException)
        
        # 模擬連接錯誤
        mock_writer.write.side_effect = NewConnectionError("Connection failed", "InfluxDB")
        
        point = influxdb_client.Point("temperature").field("value", 25.0)
        
        with pytest.raises(CustomException) as exc_info:
            operate.write(point)
        
        assert exc_info.value.status_code == 488
        assert "InfluxDB connection failed" in str(exc_info.value.message)
        assert exc_info.value.message_code == 1


class TestInfluxOperateIntegration:
    """測試 InfluxDB 操作集成場景"""
    
    def test_write_and_query_workflow(self, mock_influxdb):
        """測試寫入和查詢工作流程"""
        influxdb, mock_writer, mock_reader = mock_influxdb
        operate = InfluxOperate(influxdb)
        
        # 1. 寫入數據
        point = influxdb_client.Point("temperature").tag("location", "room1").field("value", 25.0)
        operate.write(point)
        
        # 2. 查詢數據
        mock_result = [{"measurement": "temperature", "location": "room1", "value": 25.0}]
        mock_reader.query.return_value = mock_result
        
        query_string = 'from(bucket:"test-bucket") |> range(start: -1h) |> filter(fn: (r) => r._measurement == "temperature")'
        result = operate.query(query_string)
        
        # 驗證調用
        mock_writer.write.assert_called_once()
        mock_reader.query.assert_called_once()
        assert result == mock_result
    
    def test_bucket_org_change_workflow(self, mock_influxdb):
        """測試 bucket 和 org 變更工作流程"""
        influxdb, mock_writer, _ = mock_influxdb
        operate = InfluxOperate(influxdb)
        
        # 變更 bucket 和 org
        operate.change_bucket("new-bucket")
        operate.change_org("new-org")
        
        # 寫入數據
        point = influxdb_client.Point("temperature").field("value", 25.0)
        operate.write(point)
        
        # 驗證使用新的 bucket 和 org
        mock_writer.write.assert_called_once_with(
            bucket="new-bucket", 
            org="new-org", 
            record=point
        )
    
    def test_batch_write_operations(self, mock_influxdb):
        """測試批量寫入操作"""
        influxdb, mock_writer, _ = mock_influxdb
        operate = InfluxOperate(influxdb)
        
        # 準備批量數據
        points = []
        for i in range(5):
            point = influxdb_client.Point("sensor_data") \
                .tag("sensor_id", f"sensor_{i}") \
                .field("temperature", 20 + i) \
                .field("humidity", 60 + i)
            points.append(point)
        
        # 批量寫入
        operate.write(points)
        
        # 驗證批量寫入
        mock_writer.write.assert_called_once_with(
            bucket="test-bucket", 
            org="test-org", 
            record=points
        )
    
    def test_error_recovery_scenario(self, mock_influxdb):
        """測試錯誤恢復場景"""
        influxdb, mock_writer, _ = mock_influxdb
        operate = InfluxOperate(influxdb)
        
        point = influxdb_client.Point("temperature").field("value", 25.0)
        
        # 第一次寫入失敗
        mock_writer.write.side_effect = NewConnectionError("Connection timeout", "InfluxDB")
        
        with pytest.raises(GeneralOperateException):
            operate.write(point)
        
        # 模擬連接恢復
        mock_writer.write.side_effect = None
        
        # 第二次寫入成功
        operate.write(point)
        
        # 驗證兩次調用
        assert mock_writer.write.call_count == 2


if __name__ == "__main__":
    # 運行測試
    pytest.main([__file__, "-v"])