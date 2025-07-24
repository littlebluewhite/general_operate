# GeneralOperate Kafka Integration Examples

This directory contains examples and resources for using Kafka with GeneralOperate.

## Quick Start

### 1. Start Kafka Environment

```bash
# Start Kafka, Zookeeper, and Kafka UI
docker-compose -f docker-compose.kafka.yml up -d

# Check services are running
docker-compose -f docker-compose.kafka.yml ps
```

Services:
- **Kafka**: `localhost:9092`
- **Kafka UI**: `http://localhost:8080`
- **Schema Registry**: `localhost:8081` (optional)

### 2. Install Dependencies

```bash
# Install aiokafka for Kafka support
pip install aiokafka

# Optional: For compression support
pip install python-snappy
```

### 3. Run the Example

```bash
python order_service_example.py
```

## Examples Included

### 1. Order Service Example (`order_service_example.py`)

Demonstrates a complete order processing service with Kafka events:

- **Order Creation**: Creates orders in SQL and publishes events
- **Event Handling**: Processes payment and shipping events
- **Event Bus**: Uses publish/subscribe patterns
- **Transaction Support**: Ensures data consistency across SQL and Kafka

#### Key Features:

```python
# Create order with automatic event publishing
order = await order_operate.create_order_with_events({
    "user_id": "user123",
    "product_id": "prod456",
    "quantity": 2,
    "price": 99.99
})

# Subscribe to events with filtering
await order_operate.subscribe_event(
    event_type="order.*",  # Wildcard subscription
    handler=order_operate.handle_order_events
)

# Publish high-level events
await order_operate.publish_event(
    event_type="payment.completed",
    data={"order_id": order.id, "amount": 99.99},
    tenant_id="tenant1"
)
```

## Configuration Examples

### Basic Configuration

```python
kafka_config = {
    "bootstrap_servers": "localhost:9092",
    "client_id": "my-service",
    "producer": {
        "acks": "all",
        "enable_idempotence": True
    },
    "consumer": {
        "topics": ["my-events"],
        "group_id": "my-service-group"
    }
}
```

### Advanced Configuration

```python
kafka_config = {
    "bootstrap_servers": ["kafka1:9092", "kafka2:9092"],
    "client_id": "order-service",
    "producer": {
        "acks": "all",
        "enable_idempotence": True,
        "compression_type": "snappy",
        "max_batch_size": 16384,
        "linger_ms": 10
    },
    "consumer": {
        "topics": ["order-events", "payment-events"],
        "group_id": "order-service-group",
        "enable_auto_commit": False,
        "auto_offset_reset": "earliest"
    },
    "event_bus": {
        "default_topic": "business-events",
        "enable_transactions": True
    }
}
```

## Usage Patterns

### 1. Event-Driven CRUD Operations

```python
class UserOperate(GeneralOperate[UserSchema]):
    async def create_user_with_events(self, user_data):
        async with self.transaction() as session:
            # Create in database
            user = await self.create_data([user_data], session=session)
            
            # Publish event
            await self.send_event(
                event_type="user.created",
                data=user[0].model_dump()
            )
            
            return user[0]
```

### 2. Event Aggregation

```python
# Track statistics across events
stats = {"total_orders": 0, "revenue": 0}

async def stats_handler(event):
    if event.event_type == "order.created":
        stats["total_orders"] += 1
        stats["revenue"] += event.data.get("price", 0)

await operate.subscribe_event("order.created", stats_handler)
```

### 3. Event Correlation

```python
# Correlate related events
async def correlation_handler(event):
    correlation_id = event.correlation_id
    # Process related events based on correlation_id
    await process_correlated_event(correlation_id, event)

await operate.subscribe_event("*", correlation_handler)
```

### 4. Event Filtering

```python
# Only process high-value orders
await operate.subscribe_event(
    event_type="order.created",
    handler=high_value_handler,
    filter_func=lambda e: e.data.get("price", 0) > 1000
)
```

## Module Configuration

To use Kafka with your GeneralOperate modules, add a `topic_name` to your module:

```python
@dataclass
class OrderModule:
    table_name = "orders"
    topic_name = "order-events"  # Kafka topic for events
    
    @dataclass
    class OrderSchema:
        id: int
        user_id: str
        product_id: str
        price: float
        status: str
    
    main_schemas = OrderSchema
    create_schemas = OrderSchema
    update_schemas = OrderSchema
```

## Lifecycle Management

Always use the `kafka_lifespan()` context manager:

```python
async with operate.kafka_lifespan():
    # Kafka components are started
    await operate.send_event("test.event", {"data": "value"})
    
    # Subscribe to events
    subscription = await operate.subscribe_event(
        "test.*", 
        handler=my_handler
    )
    
    # Process events...
    await asyncio.sleep(10)
    
# Kafka components are automatically stopped
```

## Error Handling

GeneralOperate provides unified error handling for Kafka operations:

```python
from general_operate.kafka.exceptions import KafkaOperateException

try:
    await operate.send_event("invalid.topic", {})
except KafkaOperateException as e:
    print(f"Kafka error: {e.message} (code: {e.message_code})")
```

## Health Monitoring

```python
# Check health of all Kafka components
health = await operate.kafka_health_check()
print(f"Producer: {health['producer']}")
print(f"Consumer: {health['consumer']}")
print(f"Event Bus: {health['event_bus']}")
```

## Performance Tips

1. **Batching**: Use batch operations for high throughput
2. **Compression**: Enable snappy compression for large messages
3. **Partitioning**: Use appropriate message keys for partitioning
4. **Connection Pooling**: Reuse connections with lifecycle management
5. **Async Processing**: Use async/await for non-blocking operations

## Troubleshooting

### Common Issues

1. **Connection Errors**: Ensure Kafka is running and accessible
2. **Serialization Errors**: Check message format and serializers
3. **Consumer Lag**: Monitor consumer group lag in Kafka UI
4. **Topic Not Found**: Enable auto-topic creation or create manually

### Debug Logging

Enable debug logging for Kafka operations:

```python
import logging
logging.getLogger("aiokafka").setLevel(logging.DEBUG)
```

### Kafka UI

Access Kafka UI at `http://localhost:8080` to:
- View topics and messages
- Monitor consumer groups
- Check cluster health
- Debug message flow

## Testing

Run the test suite:

```bash
# Unit tests (no Kafka required)
pytest tests/test_kafka/ -v

# Integration tests (requires running Kafka)
pytest tests/test_kafka/ -v -m integration
```

## Next Steps

1. **Schema Registry**: Implement schema evolution support
2. **Monitoring**: Add Prometheus metrics
3. **Dead Letter Queue**: Implement error handling queues
4. **Transactions**: Add cross-service transaction support
5. **Stream Processing**: Implement Kafka Streams integration