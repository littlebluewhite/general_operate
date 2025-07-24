"""
客戶端模組測試
測試 SQLClient, RedisDB, 和 InfluxDB 客戶端的配置和連接功能
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
import redis.asyncio as redis
from redis.cluster import ClusterNode

from general_operate.app.client.database import SQLClient
from general_operate.app.client.redis_db import RedisDB
from general_operate.app.client.influxdb import InfluxDB


class TestSQLClient:
    """測試 SQLClient 數據庫客戶端"""
    
    def test_postgresql_client_initialization(self):
        """測試 PostgreSQL 客戶端初始化"""
        config = {
            "engine": "postgresql",
            "host": "localhost",
            "port": 5432,
            "db": "test_db",
            "user": "test_user",
            "password": "test_password",
            "pool_recycle": 7200
        }
        
        with patch('general_operate.app.client.database.create_async_engine') as mock_engine:
            client = SQLClient(config, echo=True)
            
            assert client.engine_type == "postgresql"
            assert client.host == "localhost"
            assert client.port == 5432
            assert client.db == "test_db"
            assert client.user == "test_user"
            assert client.password == "test_password"
            assert client.pool_recycle == 7200
            assert "postgresql+asyncpg://" in client.url
            
            # 驗證引擎創建參數
            mock_engine.assert_called_once()
            call_args = mock_engine.call_args
            assert call_args[0][0] == client.url  # URL 參數
            assert call_args[1]['echo'] == True
            assert call_args[1]['pool_size'] == 10
            assert call_args[1]['max_overflow'] == 20
            assert call_args[1]['pool_recycle'] == 7200
    
    def test_mysql_client_initialization(self):
        """測試 MySQL 客戶端初始化"""
        config = {
            "engine": "mysql",
            "host": "localhost",
            "port": 3306,
            "db": "test_db",
            "user": "test_user",
            "password": "test_password"
        }
        
        with patch('general_operate.app.client.database.create_async_engine') as mock_engine:
            client = SQLClient(config)
            
            assert client.engine_type == "mysql"
            assert client.pool_recycle == 3600  # 默認值
            assert "mysql+aiomysql://" in client.url
            
            # 驗證引擎創建參數
            mock_engine.assert_called_once()
            call_args = mock_engine.call_args
            assert call_args[1]['echo'] == False  # 默認值
            assert call_args[1]['pool_recycle'] == 3600
    
    def test_default_mysql_client(self):
        """測試默認 MySQL 客戶端（當引擎類型不是 PostgreSQL 時）"""
        config = {
            "engine": "unknown",
            "host": "localhost", 
            "port": 3306,
            "db": "test_db",
            "user": "test_user",
            "password": "test_password"
        }
        
        with patch('general_operate.app.client.database.create_async_engine'):
            client = SQLClient(config)
            
            assert "mysql+aiomysql://" in client.url
    
    def test_url_format_postgresql(self):
        """測試 PostgreSQL URL 格式"""
        config = {
            "engine": "postgresql",
            "host": "db.example.com",
            "port": 5432,
            "db": "production_db",
            "user": "admin",
            "password": "secret123"
        }
        
        with patch('general_operate.app.client.database.create_async_engine'):
            client = SQLClient(config)
            
            expected_url = "postgresql+asyncpg://admin:secret123@db.example.com:5432/production_db"
            assert client.url == expected_url
    
    def test_url_format_mysql(self):
        """測試 MySQL URL 格式"""
        config = {
            "engine": "mysql",
            "host": "db.example.com",
            "port": 3306,
            "db": "production_db", 
            "user": "admin",
            "password": "secret123"
        }
        
        with patch('general_operate.app.client.database.create_async_engine'):
            client = SQLClient(config)
            
            expected_url = "mysql+aiomysql://admin:secret123@db.example.com:3306/production_db"
            assert client.url == expected_url
    
    def test_get_engine(self):
        """測試獲取引擎實例"""
        config = {
            "engine": "postgresql",
            "host": "localhost",
            "port": 5432,
            "db": "test_db",
            "user": "test_user",
            "password": "test_password"
        }
        
        mock_engine = Mock()
        with patch('general_operate.app.client.database.create_async_engine', return_value=mock_engine):
            client = SQLClient(config)
            
            assert client.get_engine() == mock_engine
    
    def test_case_insensitive_engine_type(self):
        """測試引擎類型大小寫不敏感"""
        config = {
            "engine": "POSTGRESQL",  # 大寫
            "host": "localhost",
            "port": 5432,
            "db": "test_db", 
            "user": "test_user",
            "password": "test_password"
        }
        
        with patch('general_operate.app.client.database.create_async_engine'):
            client = SQLClient(config)
            
            assert "postgresql+asyncpg://" in client.url


class TestRedisDB:
    """測試 RedisDB 客戶端"""
    
    def test_single_redis_initialization(self):
        """測試單機 Redis 初始化"""
        config = {
            "host": "localhost:6379",
            "db": 0,
            "user": "redis_user",
            "password": "redis_password"
        }
        
        redis_db = RedisDB(config)
        
        assert redis_db.host == "localhost:6379"
        assert redis_db.db == 0
        assert redis_db.username == "redis_user"
        assert redis_db.password == "redis_password"
        assert redis_db.decode_responses == False  # 默認值
        assert redis_db.is_cluster == False
    
    def test_cluster_redis_initialization(self):
        """測試 Redis 集群初始化"""
        config = {
            "host": "node1:6379,node2:6379,node3:6379",
            "db": 0,
            "user": "cluster_user", 
            "password": "cluster_password"
        }
        
        redis_db = RedisDB(config, decode_responses=True)
        
        assert redis_db.host == "node1:6379,node2:6379,node3:6379"
        assert redis_db.decode_responses == True
        assert redis_db.is_cluster == True
    
    def test_single_redis_client_creation(self):
        """測試單機 Redis 客戶端創建"""
        config = {
            "host": "127.0.0.1:6380",
            "db": 1,
            "user": "test_user",
            "password": "test_password"
        }
        
        with patch('redis.asyncio.Redis') as mock_redis:
            redis_db = RedisDB(config, decode_responses=True)
            client = redis_db.redis_client()
            
            mock_redis.assert_called_once_with(
                host="127.0.0.1",
                port=6380,
                db=1,
                username="test_user",
                password="test_password",
                decode_responses=True,
                socket_timeout=5
            )
    
    def test_single_redis_default_port(self):
        """測試單機 Redis 默認端口"""
        config = {
            "host": "redis.example.com",  # 沒有指定端口
            "db": 0,
            "user": "",
            "password": ""
        }
        
        with patch('redis.asyncio.Redis') as mock_redis:
            redis_db = RedisDB(config)
            client = redis_db.redis_client()
            
            mock_redis.assert_called_once_with(
                host="redis.example.com",
                port=6379,  # 默認端口
                db=0,
                username="",
                password="",
                decode_responses=False,
                socket_timeout=5
            )
    
    def test_cluster_redis_client_creation(self):
        """測試 Redis 集群客戶端創建"""
        config = {
            "host": "node1:7000,node2:7001,node3:7002",
            "db": 0,
            "user": "cluster_user",
            "password": "cluster_password"
        }
        
        with patch('redis.asyncio.RedisCluster') as mock_cluster:
            redis_db = RedisDB(config)
            client = redis_db.redis_client()
            
            # 驗證調用參數
            call_args = mock_cluster.call_args
            startup_nodes = call_args[1]['startup_nodes']
            
            assert len(startup_nodes) == 3
            assert isinstance(startup_nodes[0], ClusterNode)
            assert call_args[1]['username'] == "cluster_user"
            assert call_args[1]['password'] == "cluster_password"
            assert call_args[1]['socket_timeout'] == 5
            assert call_args[1]['decode_responses'] == False
    
    def test_cluster_detection_single_host(self):
        """測試集群檢測 - 單個主機"""
        config = {
            "host": "localhost:6379",
            "db": 0,
            "user": "",
            "password": ""
        }
        
        redis_db = RedisDB(config)
        assert redis_db._RedisDB__is_redis_cluster() == False
    
    def test_cluster_detection_multiple_hosts(self):
        """測試集群檢測 - 多個主機"""
        config = {
            "host": "node1:6379,node2:6379",
            "db": 0,
            "user": "",
            "password": ""
        }
        
        redis_db = RedisDB(config)
        assert redis_db._RedisDB__is_redis_cluster() == True
    
    def test_cluster_node_parsing(self):
        """測試集群節點解析"""
        config = {
            "host": "192.168.1.10:7000, 192.168.1.11:7001 ,192.168.1.12:7002",  # 有空格
            "db": 0,
            "user": "",
            "password": ""
        }
        
        with patch('redis.asyncio.RedisCluster') as mock_cluster:
            redis_db = RedisDB(config)
            client = redis_db.redis_client()
            
            startup_nodes = mock_cluster.call_args[1]['startup_nodes']
            
            # 檢查節點解析正確性
            assert len(startup_nodes) == 3
            # 檢查第一個節點（應該去除空格）
            first_node = startup_nodes[0]
            assert first_node.host == "192.168.1.10"
            assert first_node.port == "7000"


class TestInfluxDB:
    """測試 InfluxDB 客戶端"""
    
    def test_influxdb_initialization(self):
        """測試 InfluxDB 客戶端初始化"""
        config = {
            "host": "localhost",
            "port": 8086,
            "org": "my-org",
            "token": "my-token",
            "bucket": "my-bucket"
        }
        
        with patch('influxdb_client.InfluxDBClient') as mock_client:
            mock_client_instance = Mock()
            mock_write_api = Mock()
            mock_query_api = Mock()
            
            mock_client_instance.write_api.return_value = mock_write_api
            mock_client_instance.query_api.return_value = mock_query_api
            mock_client.return_value = mock_client_instance
            
            influx_db = InfluxDB(config)
            
            assert influx_db.host == "localhost"
            assert influx_db.port == 8086
            assert influx_db.org == "my-org"
            assert influx_db.token == "my-token"
            assert influx_db.bucket == "my-bucket"
            assert influx_db.url == "http://localhost:8086"
            
            # 驗證客戶端創建
            mock_client.assert_called_once_with(
                url="http://localhost:8086",
                token="my-token",
                org="my-org",
                timeout=10_000
            )
            
            # 驗證 API 創建
            assert influx_db.write == mock_write_api
            assert influx_db.query == mock_query_api
    
    def test_influxdb_custom_timeout(self):
        """測試 InfluxDB 自定義超時時間"""
        config = {
            "host": "influx.example.com",
            "port": 8087,
            "org": "test-org",
            "token": "test-token",
            "bucket": "test-bucket"
        }
        
        with patch('influxdb_client.InfluxDBClient') as mock_client:
            influx_db = InfluxDB(config, timeout=20_000)
            
            assert influx_db.url == "http://influx.example.com:8087"
            
            # 驗證超時參數
            mock_client.assert_called_once_with(
                url="http://influx.example.com:8087",
                token="test-token",
                org="test-org",
                timeout=20_000
            )
    
    def test_influxdb_url_construction(self):
        """測試 InfluxDB URL 構建"""
        config = {
            "host": "192.168.1.100",
            "port": 9999,
            "org": "production",
            "token": "production-token",
            "bucket": "metrics"
        }
        
        with patch('influxdb_client.InfluxDBClient'):
            influx_db = InfluxDB(config)
            
            assert influx_db.url == "http://192.168.1.100:9999"
    
    def test_influxdb_api_initialization(self):
        """測試 InfluxDB API 初始化"""
        config = {
            "host": "localhost",
            "port": 8086,
            "org": "test-org",
            "token": "test-token",
            "bucket": "test-bucket"
        }
        
        with patch('influxdb_client.InfluxDBClient') as mock_client:
            mock_client_instance = Mock()
            mock_write_api = Mock()
            mock_query_api = Mock()
            
            mock_client_instance.write_api.return_value = mock_write_api
            mock_client_instance.query_api.return_value = mock_query_api
            mock_client.return_value = mock_client_instance
            
            influx_db = InfluxDB(config)
            
            # 驗證 write API 和 query API 被調用
            assert mock_client_instance.write_api.call_count == 1
            assert mock_client_instance.query_api.call_count == 1
            
            assert influx_db.client == mock_client_instance
            assert influx_db.write == mock_write_api
            assert influx_db.query == mock_query_api


class TestClientIntegration:
    """測試客戶端集成場景"""
    
    def test_sql_client_postgresql_vs_mysql_url_difference(self):
        """測試 PostgreSQL 和 MySQL 客戶端 URL 差異"""
        base_config = {
            "host": "localhost",
            "port": 5432,
            "db": "test_db",
            "user": "user",
            "password": "password"
        }
        
        # PostgreSQL 配置
        pg_config = {**base_config, "engine": "postgresql"}
        # MySQL 配置  
        mysql_config = {**base_config, "engine": "mysql", "port": 3306}
        
        with patch('general_operate.app.client.database.create_async_engine'):
            pg_client = SQLClient(pg_config)
            mysql_client = SQLClient(mysql_config)
            
            assert "postgresql+asyncpg://" in pg_client.url
            assert "mysql+aiomysql://" in mysql_client.url
    
    def test_redis_single_vs_cluster_client_type(self):
        """測試 Redis 單機 vs 集群客戶端類型"""
        single_config = {
            "host": "localhost:6379",
            "db": 0,
            "user": "",
            "password": ""
        }
        
        cluster_config = {
            "host": "node1:6379,node2:6379",
            "db": 0,
            "user": "",
            "password": ""
        }
        
        single_redis = RedisDB(single_config)
        cluster_redis = RedisDB(cluster_config)
        
        assert single_redis.is_cluster == False
        assert cluster_redis.is_cluster == True
        
        # 測試客戶端創建類型不同
        with patch('redis.asyncio.Redis') as mock_single, \
             patch('redis.asyncio.RedisCluster') as mock_cluster:
            
            single_redis.redis_client()
            cluster_redis.redis_client()
            
            mock_single.assert_called_once()
            mock_cluster.assert_called_once()
    
    def test_all_clients_configuration_completeness(self):
        """測試所有客戶端配置完整性"""
        # SQL 客戶端配置
        sql_config = {
            "engine": "postgresql",
            "host": "localhost",
            "port": 5432,
            "db": "test_db",
            "user": "user",
            "password": "password",
            "pool_recycle": 3600
        }
        
        # Redis 客戶端配置
        redis_config = {
            "host": "localhost:6379",
            "db": 0,
            "user": "redis_user",
            "password": "redis_password"
        }
        
        # InfluxDB 客戶端配置
        influx_config = {
            "host": "localhost",
            "port": 8086,
            "org": "test-org",
            "token": "test-token",
            "bucket": "test-bucket"
        }
        
        with patch('general_operate.app.client.database.create_async_engine'), \
             patch('influxdb_client.InfluxDBClient'):
            
            sql_client = SQLClient(sql_config)
            redis_client = RedisDB(redis_config)
            influx_client = InfluxDB(influx_config)
            
            # 驗證所有客戶端都正常初始化
            assert sql_client.engine_type == "postgresql"
            assert redis_client.host == "localhost:6379"
            assert influx_client.bucket == "test-bucket"


if __name__ == "__main__":
    # 運行測試
    pytest.main([__file__, "-v"])