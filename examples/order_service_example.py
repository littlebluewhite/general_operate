"""
Order Service Example with Kafka Integration
Demonstrates how to use GeneralOperate with Kafka for event-driven architecture
"""

import asyncio
from dataclasses import dataclass
from typing import Any
from general_operate import GeneralOperate


@dataclass
class OrderModule:
    """Order module with Kafka topic configuration"""
    table_name = "orders"
    topic_name = "order-events"  # Kafka topic for order events
    
    @dataclass
    class CreateOrderSchema:
        user_id: str
        product_id: str
        quantity: int
        price: float
        status: str = "pending"
    
    @dataclass 
    class OrderSchema:
        id: int
        user_id: str
        product_id: str
        quantity: int
        price: float
        status: str
        created_at: str | None = None
        updated_at: str | None = None
    
    main_schemas = OrderSchema
    create_schemas = CreateOrderSchema
    update_schemas = CreateOrderSchema


class OrderOperate(GeneralOperate[OrderModule.OrderSchema]):
    """Order operations with Kafka event integration"""
    
    def get_module(self):
        return OrderModule
    
    async def create_order_with_events(self, order_data: dict[str, Any]) -> OrderModule.OrderSchema:
        """Create order and publish events (transactional)"""
        async with self.transaction() as session:
            # Create order in database
            orders = await self.create_data([order_data], session=session)
            order = orders[0]
            
            # Send order created event
            await self.send_event(
                event_type="order.created",
                data=order.model_dump(),
                key=str(order.id)
            )
            
            # Cache the order
            await self.set_value(
                key=f"order:{order.id}",
                value=order.model_dump(),
                expire=3600
            )
            
            return order
    
    async def update_order_status(self, order_id: int, status: str) -> OrderModule.OrderSchema:
        """Update order status and publish event"""
        # Update in database
        updated_orders = await self.update_data([{
            "id": order_id,
            "status": status
        }])
        
        if updated_orders:
            order = updated_orders[0]
            
            # Send status updated event
            await self.send_event(
                event_type="order.status_updated",
                data={
                    "order_id": order.id,
                    "old_status": order_data.get("status", "unknown"),
                    "new_status": status
                },
                key=str(order.id)
            )
            
            return order
        
        raise ValueError(f"Order {order_id} not found")
    
    async def handle_order_events(self, event: dict[str, Any]) -> None:
        """Handle incoming order events"""
        event_type = event.get('event_type')
        data = event.get('data', {})
        
        self.logger.info(f"Processing order event: {event_type}", data=data)
        
        if event_type == "order.payment_completed":
            # Update order status when payment is completed
            order_id = data.get('order_id')
            if order_id:
                await self.update_order_status(order_id, "paid")
                
                # Send shipping event
                await self.send_event(
                    event_type="order.ready_to_ship",
                    data={"order_id": order_id}
                )
        
        elif event_type == "order.shipped":
            # Update order status when shipped
            order_id = data.get('order_id')
            if order_id:
                await self.update_order_status(order_id, "shipped")
        
        elif event_type == "order.delivered":
            # Update order status when delivered
            order_id = data.get('order_id')
            if order_id:
                await self.update_order_status(order_id, "delivered")


# Payment service event handler
class PaymentEventHandler:
    """Handles payment-related events"""
    
    def __init__(self, order_operate: OrderOperate):
        self.order_operate = order_operate
    
    async def handle_payment_event(self, event: dict[str, Any]) -> None:
        """Handle payment events"""
        event_type = event.get('event_type')
        data = event.get('data', {})
        
        if event_type == "payment.completed":
            # Notify order service about successful payment
            await self.order_operate.send_event(
                event_type="order.payment_completed",
                data={
                    "order_id": data.get('order_id'),
                    "payment_id": data.get('payment_id'),
                    "amount": data.get('amount')
                }
            )


async def main():
    """Main example demonstrating Kafka integration"""
    
    # Kafka configuration
    kafka_config = {
        "bootstrap_servers": "localhost:9092",
        "client_id": "order-service",
        "producer": {
            "acks": "all",
            "enable_idempotence": True,
            "compression_type": "snappy"
        },
        "consumer": {
            "topics": ["order-events", "payment-events"],
            "group_id": "order-service-group",
            "enable_auto_commit": False
        },
        "event_bus": {
            "default_topic": "order-events"
        }
    }
    
    # Create order operate instance (you would pass real DB/Redis clients)
    order_operate = OrderOperate(
        database_client=None,  # Your database client
        redis_client=None,     # Your Redis client
        kafka_config=kafka_config
    )
    
    # Payment event handler
    payment_handler = PaymentEventHandler(order_operate)
    
    try:
        # Start Kafka components
        async with order_operate.kafka_lifespan():
            print("🚀 Kafka components started")
            
            # Example 1: Create order with events
            print("\n📦 Creating order...")
            order = await order_operate.create_order_with_events({
                "user_id": "user123",
                "product_id": "prod456", 
                "quantity": 2,
                "price": 99.99
            })
            print(f"✅ Order created: {order.id}")
            
            # Example 2: Subscribe to events using event bus
            print("\n🔔 Setting up event subscriptions...")
            
            # Subscribe to order events
            order_subscription = await order_operate.subscribe_event(
                event_type="order.*",  # Wildcard subscription
                handler=order_operate.handle_order_events
            )
            
            # Subscribe to payment events
            payment_subscription = await order_operate.subscribe_event(
                event_type="payment.*",
                handler=payment_handler.handle_payment_event,
                topic="payment-events"
            )
            
            print(f"📡 Subscribed to order events: {order_subscription}")
            print(f"📡 Subscribed to payment events: {payment_subscription}")
            
            # Example 3: Simulate payment completion
            print("\n💳 Simulating payment completion...")
            await order_operate.publish_event(
                event_type="payment.completed",
                data={
                    "order_id": order.id,
                    "payment_id": "pay_789",
                    "amount": 99.99
                },
                tenant_id="tenant1"
            )
            
            # Example 4: Check Kafka health
            print("\n🏥 Checking Kafka health...")
            health = await order_operate.kafka_health_check()
            print(f"Health status: {health}")
            
            # Example 5: Direct event sending (low-level)
            print("\n📨 Sending direct event...")
            await order_operate.send_event(
                event_type="order.shipped",
                data={"order_id": order.id, "tracking_number": "TRK123"},
                key=str(order.id)
            )
            
            # Keep running to process events
            print("\n⏳ Processing events for 10 seconds...")
            await asyncio.sleep(10)
            
            print("✅ Example completed successfully")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        raise


# Advanced example with event bus patterns
async def event_bus_example():
    """Advanced example showing event bus patterns"""
    
    kafka_config = {
        "bootstrap_servers": "localhost:9092",
        "event_bus": {
            "default_topic": "business-events"
        }
    }
    
    order_operate = OrderOperate(kafka_config=kafka_config)
    
    async with order_operate.kafka_lifespan():
        # Pattern 1: Event filtering
        async def filtered_handler(event):
            print(f"High-value order event: {event.event_type}")
        
        # Only handle events for orders > $100
        await order_operate.subscribe_event(
            event_type="order.created",
            handler=filtered_handler,
            filter_func=lambda e: e.data.get('price', 0) > 100
        )
        
        # Pattern 2: Event aggregation
        order_stats = {"total_orders": 0, "total_revenue": 0}
        
        async def stats_handler(event):
            if event.event_type == "order.created":
                order_stats["total_orders"] += 1
                order_stats["total_revenue"] += event.data.get('price', 0)
                print(f"📊 Stats: {order_stats}")
        
        await order_operate.subscribe_event(
            event_type="order.created",
            handler=stats_handler
        )
        
        # Pattern 3: Event correlation
        async def correlation_handler(event):
            correlation_id = event.correlation_id
            print(f"🔗 Correlated event {correlation_id}: {event.event_type}")
        
        await order_operate.subscribe_event(
            event_type="*",  # All events
            handler=correlation_handler
        )
        
        # Generate some test events
        for i in range(5):
            await order_operate.publish_event(
                event_type="order.created",
                data={"order_id": i, "price": 50 + i * 25},
                tenant_id="test-tenant"
            )
        
        await asyncio.sleep(5)


if __name__ == "__main__":
    print("🎯 GeneralOperate Kafka Integration Example")
    print("=" * 50)
    
    # Run basic example
    asyncio.run(main())
    
    print("\n" + "=" * 50)
    print("🎯 Advanced Event Bus Example")
    print("=" * 50)
    
    # Run advanced example  
    asyncio.run(event_bus_example())