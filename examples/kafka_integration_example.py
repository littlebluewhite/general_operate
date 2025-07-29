"""
Example demonstrating the enhanced Kafka integration capabilities
Shows how to use the refactored components from account project integration
"""

import asyncio
from typing import Any
from general_operate.kafka import (
    # Enhanced components
    ServiceEventBusInitializer,
    EventBusMiddlewareFactory,
    EnhancedEventBus,
    
    # Backward compatible functions
    initialize_for_auth_service,
    initialize_for_user_service,
    initialize_for_notification_service,
    
    # FastAPI integration
    create_auth_service_app,
    create_user_service_app,
    create_notification_service_app,
    get_event_bus,
    
    # Core components
    EventMessage
)


# Example 1: Using the service initializer directly
async def example_direct_initialization():
    """Example of using ServiceEventBusInitializer directly"""
    
    print("=== Direct Service Initialization Example ===")
    
    # Initialize a custom service
    initializer = ServiceEventBusInitializer(
        bootstrap_servers="localhost:9092",
        service_name="example-service",
        config={
            "acks": "all",
            "enable_idempotence": True
        }
    )
    
    # Custom topic configuration
    custom_topics = [
        {
            "name": "example-events",
            "partitions": 3,
            "replication_factor": 1,
            "config": {
                "retention.ms": "86400000"  # 1 day
            }
        }
    ]
    
    try:
        # Full initialization
        event_bus = await initializer.full_initialization(topic_configs=custom_topics)
        print(f"✅ Event bus initialized for example-service")
        
        # Publish an event
        await event_bus.publish(
            topic="example-events",
            event_type="example.test_event",
            tenant_id="tenant-123",
            data={"message": "Hello from GeneralOperate!"},
            user_id="user-456"
        )
        print("✅ Event published successfully")
        
        # Health check
        health = await event_bus.health_check()
        print(f"📊 Health status: {health}")
        
    finally:
        # Cleanup
        await initializer.shutdown_event_bus()
        print("🛑 Event bus shutdown completed")


# Example 2: Using backward compatible functions
async def example_backward_compatible():
    """Example using backward compatible initialization functions"""
    
    print("\n=== Backward Compatible Functions Example ===")
    
    try:
        # Initialize auth service (backward compatible)
        auth_event_bus = await initialize_for_auth_service(
            bootstrap_servers="localhost:9092",
            service_name="demo-auth-service"
        )
        print("✅ Auth service event bus initialized")
        
        # Publish auth event
        await auth_event_bus.publish(
            topic="auth-events",
            event_type="auth.user_login",
            tenant_id="tenant-789",
            data={
                "user_email": "user@example.com",
                "login_method": "passwordless"
            }
        )
        print("✅ Auth event published")
        
        # Initialize user service 
        user_event_bus = await initialize_for_user_service(
            bootstrap_servers="localhost:9092",
            service_name="demo-user-service"
        )
        print("✅ User service event bus initialized")
        
        # Publish user event
        await user_event_bus.publish(
            topic="user-events", 
            event_type="user.profile_updated",
            tenant_id="tenant-789",
            data={
                "user_id": "user-123",
                "updated_fields": ["email", "profile_image"]
            }
        )
        print("✅ User event published")
        
    finally:
        # Cleanup
        if 'auth_event_bus' in locals():
            await auth_event_bus.stop()
        if 'user_event_bus' in locals():
            await user_event_bus.stop()
        print("🛑 All event buses stopped")


# Example 3: FastAPI integration
def example_fastapi_integration():
    """Example of FastAPI integration with dynamic middleware"""
    
    print("\n=== FastAPI Integration Example ===")
    
    # Create auth service app
    auth_app = create_auth_service_app(
        bootstrap_servers="localhost:9092",
        service_name="fastapi-auth-service",
        app_config={
            "title": "Demo Auth Service",
            "description": "Demo authentication service with Kafka integration"
        }
    )
    
    # Add route that uses event bus
    @auth_app.post("/login")
    async def login_endpoint(request: dict):
        """Example login endpoint that publishes events"""
        
        # Get event bus from app state
        event_bus = get_event_bus(auth_app)
        
        # Publish login event
        await event_bus.publish(
            topic="auth-events",
            event_type="auth.login_attempt",
            tenant_id=request.get("tenant_id", "default"),
            data={
                "email": request.get("email"),
                "timestamp": "2024-01-01T00:00:00Z",
                "success": True
            }
        )
        
        return {"status": "success", "message": "Login event published"}
    
    print("✅ FastAPI auth service app created with event bus integration")
    print("   - Event bus will be automatically initialized on startup")
    print("   - Routes can access event bus via get_event_bus(app)")
    
    return auth_app


# Example 4: Custom service with consumer
async def example_custom_service_with_consumer():
    """Example of creating a custom service with both producer and consumer"""
    
    print("\n=== Custom Service with Consumer Example ===")
    
    # Define event handler
    async def handle_user_events(event: EventMessage):
        """Handle user events"""
        print(f"📨 Received event: {event.event_type}")
        print(f"   Tenant: {event.tenant_id}")
        print(f"   Data: {event.data}")
        print(f"   Correlation ID: {event.correlation_id}")
    
    # Consumer handlers mapping
    consumer_handlers = {
        "user-events": handle_user_events
    }
    
    try:
        # Initialize notification service with consumer
        notification_event_bus = await initialize_for_notification_service(
            bootstrap_servers="localhost:9092",
            service_name="demo-notification-service",
            consumer_handlers=consumer_handlers
        )
        print("✅ Notification service with consumer initialized")
        
        # Give consumer time to start
        await asyncio.sleep(1)
        
        # Publish event that will be consumed
        await notification_event_bus.publish(
            topic="user-events",
            event_type="user.notification_requested", 
            tenant_id="tenant-demo",
            data={
                "notification_type": "email",
                "recipient": "demo@example.com",
                "message": "Welcome to our service!"
            }
        )
        print("✅ Notification event published")
        
        # Wait for event processing
        await asyncio.sleep(2)
        
    finally:
        # Cleanup
        if 'notification_event_bus' in locals():
            await notification_event_bus.stop()
        print("🛑 Notification service stopped")


# Example 5: Using the middleware factory for custom services
def example_middleware_factory():
    """Example using the middleware factory for custom service types"""
    
    print("\n=== Middleware Factory Example ===")
    
    # Define custom consumer handlers
    async def handle_custom_events(event: EventMessage):
        print(f"🔧 Custom handler processing: {event.event_type}")
    
    consumer_handlers = {
        "custom-events": handle_custom_events
    }
    
    # Custom topic configuration
    custom_topics = [
        {
            "name": "custom-events",
            "partitions": 2,
            "replication_factor": 1,
            "config": {"retention.ms": "3600000"}  # 1 hour
        }
    ]
    
    # Create generic service app using middleware factory
    custom_app = EventBusMiddlewareFactory.create_generic_service_app(
        service_type="analytics",
        bootstrap_servers="localhost:9092",
        service_name="analytics-service",
        consumer_handlers=consumer_handlers,
        custom_topic_configs=custom_topics,
        app_config={
            "title": "Analytics Service",
            "description": "Custom analytics service with Kafka integration"
        }
    )
    
    print("✅ Custom analytics service app created")
    print("   - Will initialize custom topics on startup")
    print("   - Will set up consumers for custom events")
    
    return custom_app


async def main():
    """Run all examples"""
    print("🚀 GeneralOperate Kafka Integration Examples")
    print("=" * 50)
    
    # Note: These examples assume Kafka is running on localhost:9092
    # In production, you would use actual Kafka cluster URLs
    
    try:
        # Run examples
        await example_direct_initialization()
        await example_backward_compatible()
        
        # FastAPI examples (just create apps, don't run servers)
        example_fastapi_integration()
        example_middleware_factory()
        
        # Consumer example
        await example_custom_service_with_consumer()
        
        print("\n✅ All examples completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Example failed: {e}")
        print("   Make sure Kafka is running on localhost:9092")


if __name__ == "__main__":
    # Run examples
    asyncio.run(main())