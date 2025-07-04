系統架構設計
============

本章深入探討 General Operate 的架構設計理念、技術決策和實現細節。

整體架構設計
------------

分層架構
~~~~~~~~

General Operate 採用清晰的分層架構設計：

.. code-block:: text

   ┌─────────────────────────────────────────┐
   │          應用層 (Application)           │
   │    FastAPI / Django / Flask / CLI      │
   └────────────────┬───────────────────────┘
                    │
   ┌────────────────┴───────────────────────┐
   │      統一介面層 (GeneralOperate)        │
   │         Multiple Inheritance           │
   │    CacheOperate + SQLOperate + ...     │
   └────────────────┬───────────────────────┘
                    │
   ┌────────────────┴───────────────────────┐
   │        操作組件層 (Components)          │
   ├────────────┬───────────┬───────────────┤
   │SQLOperate  │CacheOperate│InfluxOperate │
   │  (CRUD)    │(Redis)     │(Time Series) │
   └────────────┴───────────┴───────────────┘
                    │
   ┌────────────────┴───────────────────────┐
   │         客戶端層 (Clients)              │
   ├────────────┬───────────┬───────────────┤
   │ SQLClient  │RedisClient │InfluxClient  │
   │ (asyncpg)  │(redis-py)  │(influxdb)    │
   └────────────┴───────────┴───────────────┘
                    │
   ┌────────────────┴───────────────────────┐
   │        儲存系統層 (Storage)             │
   ├────────────┬───────────┬───────────────┤
   │PostgreSQL  │   Redis    │  InfluxDB    │
   │   MySQL    │            │              │
   └────────────┴───────────┴───────────────┘

核心設計模式
------------

多重繼承架構
~~~~~~~~~~~~

使用 Python 的多重繼承實現功能組合：

.. code-block:: python

   # 基礎組件定義
   class CacheOperate:
       """快取操作組件"""
       def __init__(self, redis_client=None):
           self.redis_client = redis_client
       
       async def read_from_cache(self, key):
           # 快取讀取邏輯
           pass
   
   class SQLOperate:
       """SQL 操作組件"""
       def __init__(self, database_client=None):
           self.database_client = database_client
       
       async def read_from_database(self, id):
           # 資料庫讀取邏輯
           pass
   
   # 統一介面通過多重繼承組合功能
   class GeneralOperate(CacheOperate, SQLOperate, InfluxOperate):
       """
       統一操作介面
       
       通過多重繼承獲得所有組件的功能，
       並實現組件間的協調工作
       """
       def __init__(self, module, database_client, redis_client, influxdb=None):
           # 初始化所有父類
           CacheOperate.__init__(self, redis_client)
           SQLOperate.__init__(self, database_client)
           if influxdb:
               InfluxOperate.__init__(self, influxdb)
           
           self.module = module
           self.model_name = module.__name__

方法解析順序 (MRO)
~~~~~~~~~~~~~~~~~~~

Python 使用 C3 線性化算法決定方法調用順序：

.. code-block:: python

   # 查看方法解析順序
   print(GeneralOperate.__mro__)
   # (<class 'GeneralOperate'>, 
   #  <class 'CacheOperate'>, 
   #  <class 'SQLOperate'>, 
   #  <class 'InfluxOperate'>, 
   #  <class 'object'>)
   
   # 這意味著：
   # 1. 首先查找 GeneralOperate 本身
   # 2. 然後是 CacheOperate
   # 3. 接著是 SQLOperate
   # 4. 最後是 InfluxOperate

方法協調機制
~~~~~~~~~~~~

GeneralOperate 協調不同組件的互動：

.. code-block:: python

   class GeneralOperate(CacheOperate, SQLOperate):
       async def read_data_by_id(self, id_set: set):
           """
           協調快取和資料庫的讀取操作
           """
           # 1. 嘗試從快取讀取（來自 CacheOperate）
           cached_data = await self.read_from_cache(id_set)
           
           # 2. 識別未命中的 ID
           missing_ids = id_set - set(cached_data.keys())
           
           if missing_ids:
               # 3. 從資料庫讀取未命中的資料（來自 SQLOperate）
               db_data = await self.read_from_database(missing_ids)
               
               # 4. 更新快取（來自 CacheOperate）
               await self.update_cache(db_data)
               
               # 5. 合併結果
               cached_data.update(db_data)
           
           return list(cached_data.values())

異步架構設計
------------

完全異步實現
~~~~~~~~~~~~

所有 I/O 操作都是異步的：

.. code-block:: python

   # 異步資料庫連接
   class SQLClient:
       def __init__(self, config):
           self.__engine = create_async_engine(
               self.url,
               pool_size=10,
               max_overflow=20
           )
   
   # 異步 Redis 操作
   import redis.asyncio as redis
   
   class CacheOperate:
       async def get_from_redis(self, keys):
           # 使用異步 Redis 客戶端
           values = await self.redis_client.mget(keys)
           return values
   
   # 異步 ORM 操作
   from sqlalchemy.ext.asyncio import AsyncSession
   
   async def query_async(self, session: AsyncSession):
       result = await session.execute(select(User).where(User.id == 1))
       return result.scalars().first()

並發控制設計
~~~~~~~~~~~~

.. code-block:: python

   class ConcurrencyControl:
       """並發控制機制"""
       
       def __init__(self, max_concurrent=100):
           self.semaphore = asyncio.Semaphore(max_concurrent)
           self.rate_limiter = RateLimiter(calls=1000, period=60)
       
       async def execute_with_limit(self, coro):
           """限制並發執行"""
           async with self.semaphore:
               await self.rate_limiter.acquire()
               return await coro

連接池管理
~~~~~~~~~~

.. code-block:: python

   # SQLAlchemy 連接池配置
   engine = create_async_engine(
       DATABASE_URL,
       pool_size=20,           # 基礎連接數
       max_overflow=40,        # 額外連接數
       pool_timeout=30,        # 獲取連接超時
       pool_recycle=3600,      # 連接回收時間
       pool_pre_ping=True,     # 連接健康檢查
       echo_pool=True          # 連接池日誌
   )
   
   # Redis 連接池
   redis_pool = redis.ConnectionPool(
       host='localhost',
       port=6379,
       max_connections=100,
       decode_responses=True
   )

快取架構設計
------------

快取策略層次
~~~~~~~~~~~~

.. code-block:: text

   ┌─────────────────────────────────┐
   │     應用層快取 (Application)     │
   │      (本地記憶體快取)            │
   └────────────┬────────────────────┘
                │
   ┌────────────┴────────────────────┐
   │    分散式快取層 (Redis)          │
   │   (Cache-Aside Pattern)         │
   └────────────┬────────────────────┘
                │
   ┌────────────┴────────────────────┐
   │   資料庫查詢快取 (Database)      │
   │    (Query Result Cache)         │
   └────────────┬────────────────────┘
                │
   ┌────────────┴────────────────────┐
   │     持久層 (Persistent)          │
   │   (PostgreSQL / MySQL)          │
   └─────────────────────────────────┘

快取一致性保證
~~~~~~~~~~~~~~

.. code-block:: python

   class CacheConsistency:
       """快取一致性機制"""
       
       async def update_with_consistency(self, data):
           """確保快取一致性的更新操作"""
           # 1. 先刪除快取（避免髒讀）
           await self.delete_cache(data['id'])
           
           # 2. 更新資料庫
           result = await self.update_database(data)
           
           # 3. 延遲刪除（處理並發更新）
           await asyncio.sleep(0.5)
           await self.delete_cache(data['id'])
           
           # 4. 可選：預熱新資料
           if self.warm_cache_on_update:
               await self.warm_cache({data['id']})
           
           return result

快取鍵設計
~~~~~~~~~~

.. code-block:: python

   class CacheKeyBuilder:
       """快取鍵構建器"""
       
       def __init__(self, app_name: str, version: str):
           self.prefix = f"{app_name}:v{version}"
       
       def build_entity_key(self, model: str, id: int) -> str:
           """實體快取鍵"""
           return f"{self.prefix}:entity:{model}:{id}"
       
       def build_query_key(self, model: str, query_hash: str) -> str:
           """查詢結果快取鍵"""
           return f"{self.prefix}:query:{model}:{query_hash}"
       
       def build_list_key(self, model: str, filter_hash: str) -> str:
           """列表快取鍵"""
           return f"{self.prefix}:list:{model}:{filter_hash}"
       
       def build_count_key(self, model: str, filter_hash: str) -> str:
           """計數快取鍵"""
           return f"{self.prefix}:count:{model}:{filter_hash}"

安全架構設計
------------

SQL 注入防護
~~~~~~~~~~~~

.. code-block:: python

   class SQLSecurityLayer:
       """SQL 安全層"""
       
       def __init__(self):
           # 只允許字母、數字和下劃線
           self._identifier_pattern = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
       
       def validate_identifier(self, identifier: str) -> bool:
           """驗證 SQL 標識符"""
           if not identifier:
               raise ValueError("Identifier cannot be empty")
           
           if not self._identifier_pattern.match(identifier):
               raise ValueError(f"Invalid identifier: {identifier}")
           
           # 檢查保留字
           if identifier.upper() in SQL_RESERVED_WORDS:
               raise ValueError(f"Reserved word: {identifier}")
           
           return True
       
       def build_safe_query(self, table: str, conditions: dict) -> tuple:
           """構建安全的參數化查詢"""
           self.validate_identifier(table)
           
           # 使用參數化查詢
           query = f"SELECT * FROM {table} WHERE "
           params = {}
           
           for field, value in conditions.items():
               self.validate_identifier(field)
               params[field] = value
           
           return query, params

資料驗證層
~~~~~~~~~~

.. code-block:: python

   from pydantic import BaseModel, validator
   
   class DataValidationLayer:
       """資料驗證層"""
       
       @staticmethod
       def validate_input(schema: BaseModel, data: dict):
           """使用 Pydantic 進行資料驗證"""
           try:
               validated = schema(**data)
               return validated.dict()
           except ValidationError as e:
               raise GeneralOperateException(
                   status_code=400,
                   message_code=2001,
                   message=f"Validation error: {e.errors()}"
               )

性能優化架構
------------

批量操作優化
~~~~~~~~~~~~

.. code-block:: python

   class BatchOptimizer:
       """批量操作優化器"""
       
       async def batch_insert_optimized(self, items: list, batch_size=1000):
           """優化的批量插入"""
           # 使用 COPY 命令（PostgreSQL）
           if self.engine.dialect.name == 'postgresql':
               await self._bulk_insert_postgresql(items)
           else:
               # 使用批量 INSERT
               for i in range(0, len(items), batch_size):
                   batch = items[i:i + batch_size]
                   await self._batch_insert_generic(batch)
       
       async def _bulk_insert_postgresql(self, items):
           """PostgreSQL COPY 優化"""
           async with self.engine.begin() as conn:
               # 使用 COPY FROM 獲得最佳性能
               await conn.run_sync(
                   lambda sync_conn: sync_conn.connection.cursor().copy_from(
                       self._items_to_csv(items),
                       self.table_name,
                       columns=self.columns
                   )
               )

查詢優化器
~~~~~~~~~~

.. code-block:: python

   class QueryOptimizer:
       """查詢優化器"""
       
       def optimize_filter_query(self, filters: dict) -> Query:
           """優化過濾查詢"""
           query = select(self.model)
           
           # 智能索引使用
           indexed_fields = self.get_indexed_fields()
           
           # 優先使用索引字段
           for field in indexed_fields:
               if field in filters:
                   query = query.where(
                       getattr(self.model, field) == filters[field]
                   )
           
           # 添加其他過濾條件
           for field, condition in filters.items():
               if field not in indexed_fields:
                   query = self._add_condition(query, field, condition)
           
           return query

監控與可觀測性
--------------

指標收集架構
~~~~~~~~~~~~

.. code-block:: python

   from prometheus_client import Counter, Histogram, Gauge
   
   class MetricsCollector:
       """指標收集器"""
       
       def __init__(self):
           # 操作計數器
           self.operation_counter = Counter(
               'general_operate_operations_total',
               'Total operations',
               ['operation', 'model', 'status']
           )
           
           # 延遲直方圖
           self.latency_histogram = Histogram(
               'general_operate_latency_seconds',
               'Operation latency',
               ['operation', 'model']
           )
           
           # 活躍連接數
           self.active_connections = Gauge(
               'general_operate_active_connections',
               'Active database connections',
               ['database']
           )
       
       def record_operation(self, operation, model, status, duration):
           """記錄操作指標"""
           self.operation_counter.labels(
               operation=operation,
               model=model,
               status=status
           ).inc()
           
           self.latency_histogram.labels(
               operation=operation,
               model=model
           ).observe(duration)

分散式追蹤
~~~~~~~~~~

.. code-block:: python

   from opentelemetry import trace
   from opentelemetry.trace import Status, StatusCode
   
   class DistributedTracing:
       """分散式追蹤"""
       
       def __init__(self):
           self.tracer = trace.get_tracer(__name__)
       
       async def trace_operation(self, operation_name, func, **kwargs):
           """追蹤操作執行"""
           with self.tracer.start_as_current_span(operation_name) as span:
               # 添加屬性
               span.set_attributes({
                   "db.system": "postgresql",
                   "db.operation": operation_name,
                   **kwargs
               })
               
               try:
                   result = await func()
                   span.set_status(Status(StatusCode.OK))
                   return result
               except Exception as e:
                   span.set_status(
                       Status(StatusCode.ERROR, str(e))
                   )
                   span.record_exception(e)
                   raise

可擴展性設計
------------

插件架構
~~~~~~~~

.. code-block:: python

   class PluginSystem:
       """插件系統架構"""
       
       def __init__(self):
           self.plugins = {}
           self.hooks = defaultdict(list)
       
       def register_plugin(self, name: str, plugin: Plugin):
           """註冊插件"""
           self.plugins[name] = plugin
           plugin.register_hooks(self)
       
       def register_hook(self, event: str, handler: callable):
           """註冊鉤子"""
           self.hooks[event].append(handler)
       
       async def emit_hook(self, event: str, *args, **kwargs):
           """觸發鉤子"""
           for handler in self.hooks[event]:
               await handler(*args, **kwargs)
   
   # 使用插件
   class CachePlugin(Plugin):
       async def before_read(self, model, filters):
           # 在讀取前執行
           pass
       
       async def after_write(self, model, data):
           # 在寫入後執行
           pass

水平擴展支援
~~~~~~~~~~~~

.. code-block:: python

   class ShardingStrategy:
       """分片策略"""
       
       def __init__(self, shard_count: int):
           self.shard_count = shard_count
           self.shards = {}
       
       def get_shard(self, key: str) -> int:
           """獲取分片索引"""
           hash_value = hashlib.md5(key.encode()).hexdigest()
           return int(hash_value, 16) % self.shard_count
       
       async def execute_on_shard(self, key: str, operation):
           """在特定分片上執行操作"""
           shard_id = self.get_shard(key)
           shard = self.shards[shard_id]
           return await operation(shard)

總結
----

General Operate 的架構設計遵循以下原則：

1. **模組化**：通過多重繼承實現功能組合
2. **異步優先**：完全異步實現，支援高並發
3. **安全性**：多層安全防護機制
4. **可擴展**：插件系統和水平擴展支援
5. **可觀測**：完善的監控和追蹤機制

這種架構設計確保了系統的高性能、可維護性和可擴展性。