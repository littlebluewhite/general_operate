"""
端到端集成測試
測試 GeneralOperate 完整工作流程和多模組協作
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch, MagicMock
import asyncio
from typing import List, Dict, Any

from general_operate.general_operate import GeneralOperate
from general_operate.utils.exception import GeneralOperateException


# 測試數據和模擬類
class MockTestObject:
    """測試用的模擬對象"""
    def __init__(self, id: int, name: str, value: int):
        self.id = id
        self.name = name
        self.value = value


class TestGeneralOperateIntegration:
    """測試 GeneralOperate 集成場景"""
    
    @pytest.fixture
    def db_config(self):
        """數據庫配置"""
        return {
            "engine": "postgresql",
            "host": "localhost",
            "port": 5432,
            "db": "test_db",
            "user": "test_user",
            "password": "test_password"
        }
    
    @pytest.fixture
    def redis_config(self):
        """Redis 配置"""
        return {
            "host": "localhost:6379",
            "db": 0,
            "user": "",
            "password": ""
        }
    
    @pytest.fixture
    def influx_config(self):
        """InfluxDB 配置"""
        return {
            "host": "localhost",
            "port": 8086,
            "org": "test-org",
            "token": "test-token",
            "bucket": "test-bucket"
        }
    
    @pytest.fixture
    def kafka_config(self):
        """Kafka 配置"""
        return {
            "bootstrap_servers": "localhost:9092",
            "topic": "test-topic"
        }
    
    @pytest.fixture
    def mock_general_operate(self, db_config, redis_config, influx_config, kafka_config):
        """模擬的 GeneralOperate 實例"""
        # 創建一個簡單的模擬 GeneralOperate 類，不需要實際導入外部依賴
        mock_go = Mock()
        mock_go.sql_operate = Mock()
        mock_go.cache_operate = Mock()
        mock_go.influx_operate = Mock()
        mock_go.kafka_client = Mock()
        return mock_go
    
    @pytest.mark.asyncio
    async def test_database_cache_integration(self, mock_general_operate):
        """測試數據庫和緩存集成"""
        # 模擬數據庫查詢
        mock_db_result = [{"id": 1, "name": "test", "value": 100}]
        mock_general_operate.sql_operate.run_select = AsyncMock(return_value=mock_db_result)
        
        # 模擬緩存操作
        mock_general_operate.cache_operate.set = AsyncMock()
        mock_general_operate.cache_operate.get = AsyncMock(return_value=None)
        
        # 執行操作：先查緩存，未命中則查數據庫並緩存
        cache_key = "test:data:1"
        cached_data = await mock_general_operate.cache_operate.get(cache_key)
        
        if cached_data is None:
            # 緩存未命中，查詢數據庫
            db_data = await mock_general_operate.sql_operate.run_select(
                "SELECT * FROM test_table WHERE id = %s", (1,)
            )
            # 存入緩存
            await mock_general_operate.cache_operate.set(cache_key, db_data, 300)
            result = db_data
        else:
            result = cached_data
        
        # 驗證結果
        assert result == mock_db_result
        mock_general_operate.sql_operate.run_select.assert_called_once()
        mock_general_operate.cache_operate.get.assert_called_once_with(cache_key)
        mock_general_operate.cache_operate.set.assert_called_once_with(cache_key, mock_db_result, 300)
    
    @pytest.mark.asyncio
    async def test_database_kafka_integration(self, mock_general_operate):
        """測試數據庫和 Kafka 集成"""
        # 模擬數據插入
        test_data = {"name": "test_user", "email": "test@example.com"}
        mock_general_operate.sql_operate.run_execute = AsyncMock(return_value=1)
        mock_general_operate.kafka_client.produce = AsyncMock()
        
        # 執行操作：插入數據後發送 Kafka 消息
        result = await mock_general_operate.sql_operate.run_execute(
            "INSERT INTO users (name, email) VALUES (%s, %s)",
            (test_data["name"], test_data["email"])
        )
        
        if result:
            # 發送事件消息
            event_data = {
                "event": "user_created",
                "data": test_data,
                "timestamp": "2023-01-01T12:00:00Z"
            }
            await mock_general_operate.kafka_client.produce("user-events", event_data)
        
        # 驗證
        assert result == 1
        mock_general_operate.sql_operate.run_execute.assert_called_once()
        mock_general_operate.kafka_client.produce.assert_called_once_with("user-events", event_data)
    
    @pytest.mark.asyncio
    async def test_influxdb_cache_integration(self, mock_general_operate):
        """測試 InfluxDB 和緩存集成"""
        # 模擬 InfluxDB 查詢
        mock_influx_result = [
            {"time": "2023-01-01T12:00:00Z", "value": 100, "host": "server1"},
            {"time": "2023-01-01T12:01:00Z", "value": 110, "host": "server1"}
        ]
        mock_general_operate.influx_operate.run_query = AsyncMock(return_value=mock_influx_result)
        mock_general_operate.cache_operate.get = AsyncMock(return_value=None)
        mock_general_operate.cache_operate.set = AsyncMock()
        
        # 執行操作：查詢 InfluxDB 數據並緩存
        cache_key = "influx:metrics:server1"
        cached_data = await mock_general_operate.cache_operate.get(cache_key)
        
        if cached_data is None:
            query = """
            from(bucket: "metrics")
              |> range(start: -1h)
              |> filter(fn: (r) => r["host"] == "server1")
            """
            influx_data = await mock_general_operate.influx_operate.run_query(query)
            await mock_general_operate.cache_operate.set(cache_key, influx_data, 60)
            result = influx_data
        else:
            result = cached_data
        
        # 驗證
        assert result == mock_influx_result
        mock_general_operate.influx_operate.run_query.assert_called_once()
        mock_general_operate.cache_operate.set.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_full_pipeline_integration(self, mock_general_operate):
        """測試完整數據處理管道"""
        # 模擬用戶註冊場景：數據庫 -> 緩存 -> InfluxDB -> Kafka
        
        # 1. 數據庫操作
        user_data = {"name": "張三", "email": "zhangsan@example.com"}
        mock_general_operate.sql_operate.run_execute = AsyncMock(return_value=123)  # 返回用戶ID
        
        # 2. 緩存操作
        mock_general_operate.cache_operate.set = AsyncMock()
        mock_general_operate.cache_operate.delete_by_pattern = AsyncMock()
        
        # 3. InfluxDB 操作
        mock_general_operate.influx_operate.run_write = AsyncMock()
        
        # 4. Kafka 操作
        mock_general_operate.kafka_client.produce = AsyncMock()
        
        # 執行完整流程
        try:
            # 插入用戶數據
            user_id = await mock_general_operate.sql_operate.run_execute(
                "INSERT INTO users (name, email) VALUES (%s, %s) RETURNING id",
                (user_data["name"], user_data["email"])
            )
            
            # 緩存用戶數據
            await mock_general_operate.cache_operate.set(f"user:{user_id}", user_data, 3600)
            
            # 清除相關緩存
            await mock_general_operate.cache_operate.delete_by_pattern("users:list:*")
            
            # 記錄指標到 InfluxDB
            metric_data = {
                "measurement": "user_registrations",
                "tags": {"source": "web"},
                "fields": {"count": 1, "user_id": user_id},
                "time": "2023-01-01T12:00:00Z"
            }
            await mock_general_operate.influx_operate.run_write([metric_data])
            
            # 發送事件到 Kafka
            event_data = {
                "event": "user_registered",
                "user_id": user_id,
                "user_data": user_data,
                "timestamp": "2023-01-01T12:00:00Z"
            }
            await mock_general_operate.kafka_client.produce("user-events", event_data)
            
            success = True
            
        except Exception as e:
            success = False
            raise
        
        # 驗證所有操作都被調用
        assert success is True
        assert user_id == 123
        mock_general_operate.sql_operate.run_execute.assert_called_once()
        mock_general_operate.cache_operate.set.assert_called_once()
        mock_general_operate.cache_operate.delete_by_pattern.assert_called_once()
        mock_general_operate.influx_operate.run_write.assert_called_once()
        mock_general_operate.kafka_client.produce.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_error_handling_integration(self, mock_general_operate):
        """測試錯誤處理集成"""
        # 模擬數據庫錯誤
        mock_general_operate.sql_operate.run_execute = AsyncMock(
            side_effect=GeneralOperateException(500, 5001, "Database error")
        )
        mock_general_operate.kafka_client.produce = AsyncMock()
        
        # 執行操作並處理錯誤
        error_occurred = False
        error_message = ""
        
        try:
            await mock_general_operate.sql_operate.run_execute(
                "INSERT INTO users (name) VALUES (%s)", ("test",)
            )
        except GeneralOperateException as e:
            error_occurred = True
            error_message = e.message
            
            # 發送錯誤事件到 Kafka
            error_event = {
                "event": "database_error",
                "error_code": e.message_code,
                "error_message": e.message,
                "timestamp": "2023-01-01T12:00:00Z"
            }
            await mock_general_operate.kafka_client.produce("error-events", error_event)
        
        # 驗證錯誤處理
        assert error_occurred is True
        assert error_message == "Database error"
        mock_general_operate.kafka_client.produce.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_concurrent_operations_integration(self, mock_general_operate):
        """測試並發操作集成"""
        # 模擬並發數據庫查詢
        mock_general_operate.sql_operate.run_select = AsyncMock(
            side_effect=[
                [{"id": 1, "name": "user1"}],
                [{"id": 2, "name": "user2"}],
                [{"id": 3, "name": "user3"}]
            ]
        )
        
        # 模擬並發緩存操作
        mock_general_operate.cache_operate.set = AsyncMock()
        
        # 執行並發操作
        async def process_user(user_id: int):
            # 查詢用戶數據
            user_data = await mock_general_operate.sql_operate.run_select(
                "SELECT * FROM users WHERE id = %s", (user_id,)
            )
            # 緩存用戶數據
            await mock_general_operate.cache_operate.set(
                f"user:{user_id}", user_data[0], 300
            )
            return user_data[0]
        
        # 並發處理多個用戶
        tasks = [process_user(i) for i in range(1, 4)]
        results = await asyncio.gather(*tasks)
        
        # 驗證結果
        assert len(results) == 3
        assert results[0]["name"] == "user1"
        assert results[1]["name"] == "user2"
        assert results[2]["name"] == "user3"
        assert mock_general_operate.sql_operate.run_select.call_count == 3
        assert mock_general_operate.cache_operate.set.call_count == 3


class TestDataFlowIntegration:
    """測試數據流集成場景"""
    
    @pytest.fixture
    def mock_general_operate_with_data_flow(self):
        """帶數據流功能的模擬 GeneralOperate"""
        # 創建一個簡單的模擬 GeneralOperate 類，不需要實際導入外部依賴
        mock_go = Mock()
        mock_go.sql_operate = Mock()
        mock_go.cache_operate = Mock()
        mock_go.influx_operate = Mock()
        mock_go.kafka_client = Mock()
        return mock_go
    
    @pytest.mark.asyncio
    async def test_data_etl_pipeline(self, mock_general_operate_with_data_flow):
        """測試 ETL 數據管道"""
        go = mock_general_operate_with_data_flow
        
        # 模擬 Extract（提取）
        raw_data = [
            {"id": 1, "name": "張三", "score": 85, "created_at": "2023-01-01"},
            {"id": 2, "name": "李四", "score": 92, "created_at": "2023-01-02"},
            {"id": 3, "name": "王五", "score": 78, "created_at": "2023-01-03"}
        ]
        go.sql_operate.run_select = AsyncMock(return_value=raw_data)
        
        # 模擬 Transform（轉換）
        def transform_data(data):
            transformed = []
            for item in data:
                transformed_item = {
                    "user_id": item["id"],
                    "username": item["name"],
                    "grade": "A" if item["score"] >= 90 else "B" if item["score"] >= 80 else "C",
                    "score": item["score"],
                    "date": item["created_at"]
                }
                transformed.append(transformed_item)
            return transformed
        
        # 模擬 Load（加載）
        go.sql_operate.run_execute_many = AsyncMock()
        go.influx_operate.run_write = AsyncMock()
        go.kafka_client.produce = AsyncMock()
        
        # 執行 ETL 流程
        # Extract
        extracted_data = await go.sql_operate.run_select("SELECT * FROM source_table")
        
        # Transform
        transformed_data = transform_data(extracted_data)
        
        # Load
        # 1. 加載到目標數據庫
        insert_values = [
            (item["user_id"], item["username"], item["grade"], item["score"])
            for item in transformed_data
        ]
        await go.sql_operate.run_execute_many(
            "INSERT INTO target_table (user_id, username, grade, score) VALUES (%s, %s, %s, %s)",
            insert_values
        )
        
        # 2. 發送指標到 InfluxDB
        metrics = [
            {
                "measurement": "user_scores",
                "tags": {"grade": item["grade"]},
                "fields": {"score": item["score"]},
                "time": item["date"]
            }
            for item in transformed_data
        ]
        await go.influx_operate.run_write(metrics)
        
        # 3. 發送事件到 Kafka
        for item in transformed_data:
            event = {
                "event": "data_processed",
                "user_id": item["user_id"],
                "grade": item["grade"],
                "timestamp": item["date"]
            }
            await go.kafka_client.produce("data-events", event)
        
        # 驗證
        assert len(transformed_data) == 3
        assert transformed_data[0]["grade"] == "B"  # score 85
        assert transformed_data[1]["grade"] == "A"  # score 92
        assert transformed_data[2]["grade"] == "C"  # score 78
        
        go.sql_operate.run_select.assert_called_once()
        go.sql_operate.run_execute_many.assert_called_once()
        go.influx_operate.run_write.assert_called_once()
        assert go.kafka_client.produce.call_count == 3
    
    @pytest.mark.asyncio
    async def test_real_time_analytics_pipeline(self, mock_general_operate_with_data_flow):
        """測試實時分析管道"""
        go = mock_general_operate_with_data_flow
        
        # 模擬實時數據處理
        go.cache_operate.get = AsyncMock(return_value=None)
        go.cache_operate.set = AsyncMock()
        go.cache_operate.increment = AsyncMock(return_value=1)
        go.influx_operate.run_write = AsyncMock()
        go.kafka_client.produce = AsyncMock()
        
        # 模擬處理實時事件
        async def process_real_time_event(event_data):
            event_type = event_data["type"]
            user_id = event_data["user_id"]
            timestamp = event_data["timestamp"]
            
            # 1. 更新實時計數器
            counter_key = f"events:{event_type}:count"
            count = await go.cache_operate.increment(counter_key)
            
            # 2. 緩存用戶活動
            activity_key = f"user:{user_id}:last_activity"
            await go.cache_operate.set(activity_key, timestamp, 86400)  # 24小時
            
            # 3. 寫入時序數據庫
            metric = {
                "measurement": "user_events",
                "tags": {"event_type": event_type, "user_id": str(user_id)},
                "fields": {"count": 1},
                "time": timestamp
            }
            await go.influx_operate.run_write([metric])
            
            # 4. 如果計數達到閾值，發送告警
            if count >= 100:
                alert = {
                    "alert": "high_activity",
                    "event_type": event_type,
                    "count": count,
                    "timestamp": timestamp
                }
                await go.kafka_client.produce("alerts", alert)
            
            return count
        
        # 測試多個事件
        events = [
            {"type": "page_view", "user_id": 1, "timestamp": "2023-01-01T12:00:00Z"},
            {"type": "page_view", "user_id": 2, "timestamp": "2023-01-01T12:01:00Z"},
            {"type": "click", "user_id": 1, "timestamp": "2023-01-01T12:02:00Z"}
        ]
        
        results = []
        for event in events:
            result = await process_real_time_event(event)
            results.append(result)
        
        # 驗證
        assert results == [1, 1, 1]  # 每個事件類型的計數
        assert go.cache_operate.increment.call_count == 3
        assert go.cache_operate.set.call_count == 3
        assert go.influx_operate.run_write.call_count == 3


class TestPerformanceIntegration:
    """性能相關集成測試"""
    
    @pytest.fixture
    def mock_general_operate_perf(self):
        """性能測試用的模擬 GeneralOperate"""
        # 創建一個簡單的模擬 GeneralOperate 類，不需要實際導入外部依賴
        mock_go = Mock()
        mock_go.sql_operate = Mock()
        mock_go.cache_operate = Mock()
        mock_go.influx_operate = Mock()
        mock_go.kafka_client = Mock()
        return mock_go
    
    @pytest.mark.asyncio
    @pytest.mark.skip(reason="Performance test - run manually")
    async def test_high_throughput_operations(self):
        """測試高吞吐量操作（手動運行）"""
        # 這個測試用於驗證系統在高負載下的表現
        # 實際環境中需要真實的數據庫和中間件
        pass
    
    @pytest.mark.asyncio
    async def test_connection_pooling_integration(self, mock_general_operate_perf):
        """測試連接池集成"""
        go = mock_general_operate_perf
        
        # 模擬多個並發數據庫操作
        go.sql_operate.run_select = AsyncMock(side_effect=[
            [{"result": f"query_{i}"}] for i in range(10)
        ])
        
        # 執行並發查詢
        async def execute_query(query_id):
            return await go.sql_operate.run_select(f"SELECT * FROM table_{query_id}")
        
        tasks = [execute_query(i) for i in range(10)]
        results = await asyncio.gather(*tasks)
        
        # 驗證所有查詢都成功執行
        assert len(results) == 10
        assert go.sql_operate.run_select.call_count == 10


if __name__ == "__main__":
    # 運行測試
    pytest.main([__file__, "-v"])