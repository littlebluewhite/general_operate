#!/usr/bin/env python3
"""
Enhanced Kafka Integration Example for GeneralOperate
Demonstrates the integrated functionality from account project
展示來自 account 項目的集成功能的增強 Kafka 集成示例
"""

import asyncio
import structlog
from fastapi import FastAPI, Depends
from contextlib import asynccontextmanager
from typing import Any, Dict

# Import enhanced GeneralOperate Kafka components
from general_operate.kafka.enhanced_client import (
    KafkaClientFactory, 
    EnhancedEventBus
)
from general_operate.kafka.service_initializer import (
    FlexibleEventBusInitializer,
    PreConfiguredInitializers
)
from general_operate.kafka.middleware_factory import (
    EventBusMiddlewareFactory,
    create_passwordless_auth_service_app,
    create_user_management_service_app,
    get_event_bus
)
from general_operate.kafka.models.event_message import EventMessage

logger = structlog.get_logger()


# Example 1: Basic event bus usage with factory pattern
async def basic_event_bus_example():
    """Basic event bus example using factory methods"""
    logger.info("=== Basic Event Bus Example ===")
    
    # Create event bus using factory
    event_bus = KafkaClientFactory.create_event_bus(
        bootstrap_servers="localhost:9092",
        service_name="example-service"
    )
    
    # Start event bus
    await event_bus.start()
    
    # Publish an event
    await event_bus.publish(
        topic="general-events",
        event_type="user.created",
        tenant_id="tenant-123",
        data={"user_id": "user-456", "email": "user@example.com"},
        metadata={"source": "registration"}
    )
    
    logger.info("Event published successfully")
    
    # Stop event bus
    await event_bus.stop()


# Example 2: Service-specific initialization
async def service_specific_example():
    """Example showing service-specific initialization"""
    logger.info("=== Service-Specific Initialization Example ===")
    
    # Initialize auth service
    auth_event_bus = await PreConfiguredInitializers.initialize_auth_service(
        bootstrap_servers="localhost:9092",
        service_name="auth-service-example"
    )
    
    # Publish auth event
    await auth_event_bus.publish(
        topic="auth-events",
        event_type="authentication.succeeded",
        tenant_id="tenant-123",
        data={"user_id": "user-456", "method": "passwordless"},
        user_id="user-456"
    )
    
    # Clean up
    await auth_event_bus.stop()


# Example 3: Flexible initialization with configuration
async def flexible_initialization_example():
    """Example showing flexible initialization patterns"""
    logger.info("=== Flexible Initialization Example ===")
    
    # Create flexible initializer
    initializer = FlexibleEventBusInitializer(
        bootstrap_servers="localhost:9092",
        config_path=None  # Can provide path to external config
    )
    
    # Initialize custom service
    custom_event_bus = await initializer.create_service_event_bus(
        service_name="custom-service",
        service_type="notification",  # Uses notification service topics
        consumer_handlers=None  # Can provide handlers here
    )
    
    # Use the event bus
    await custom_event_bus.publish(
        topic="notification-events",
        event_type="notification.send",
        tenant_id="tenant-123",
        data={"type": "email", "recipient": "user@example.com"}
    )
    
    await custom_event_bus.stop()


# Example 4: Consumer with event handler
async def consumer_example():
    """Example showing how to set up a consumer"""
    logger.info("=== Consumer Example ===")
    
    async def handle_user_event(event: EventMessage):
        """Handle user-related events"""
        logger.info(
            "Received user event",
            event_type=event.event_type,
            tenant_id=event.tenant_id,
            user_id=event.user_id,
            data=event.data
        )
        
        # Process the event here
        if event.event_type == "user.created":
            logger.info("Processing new user creation", user_id=event.user_id)
        elif event.event_type == "user.updated":
            logger.info("Processing user update", user_id=event.user_id)
    
    # Create consumer using factory
    consumer = KafkaClientFactory.create_async_consumer(
        topics=["user-events"],
        group_id="user-processor",
        message_handler=handle_user_event,
        bootstrap_servers="localhost:9092",
        service_name="consumer-example"
    )
    
    # Start consuming (this will run indefinitely)
    logger.info("Starting consumer - press Ctrl+C to stop")
    
    # Note: In real usage, you would handle this in a background task
    # Here we'll just demonstrate the setup
    await consumer.start()
    
    # Simulate some processing time
    await asyncio.sleep(2)
    
    await consumer.stop()


# Example 5: FastAPI integration with enhanced middleware
def create_example_auth_service() -> FastAPI:
    """Create an example auth service using enhanced middleware"""
    
    # Method 1: Using pre-configured function
    app = create_passwordless_auth_service_app(
        bootstrap_servers="localhost:9092"
    )
    
    @app.post("/login")
    async def login(
        request: Dict[str, Any],
        event_bus: EnhancedEventBus = Depends(get_event_bus)
    ):
        """Login endpoint that publishes events"""
        
        # Simulate login logic
        user_id = request.get("user_id")
        tenant_id = request.get("tenant_id", "default")
        
        # Publish login event
        await event_bus.publish(
            topic="auth-events",
            event_type="authentication.attempted",
            tenant_id=tenant_id,
            user_id=user_id,
            data={"method": "passwordless", "timestamp": "2024-01-01T00:00:00Z"}
        )
        
        return {"status": "success", "message": "Login attempted"}
    
    @app.get("/health")
    async def health_check(
        event_bus: EnhancedEventBus = Depends(get_event_bus)
    ):
        """Health check endpoint"""
        health_status = await event_bus.health_check()
        return {"status": "healthy", "kafka": health_status}
    
    return app


def create_example_user_service() -> FastAPI:
    """Create an example user service"""
    
    app = create_user_management_service_app(
        bootstrap_servers="localhost:9092"
    )
    
    @app.post("/users")
    async def create_user(
        request: Dict[str, Any],
        event_bus: EnhancedEventBus = Depends(get_event_bus)
    ):
        """Create user endpoint"""
        
        user_data = request.get("user", {})
        tenant_id = request.get("tenant_id", "default")
        
        # Simulate user creation
        user_id = f"user-{asyncio.get_running_loop().time()}"
        
        # Publish user created event
        await event_bus.publish(
            topic="user-events",
            event_type="user.created",
            tenant_id=tenant_id,
            user_id=user_id,
            data=user_data
        )
        
        return {"user_id": user_id, "status": "created"}
    
    return app


# Example 6: Multiple services initialization
async def multiple_services_example():
    """Example showing how to initialize multiple services at once"""
    logger.info("=== Multiple Services Example ===")
    
    initializer = FlexibleEventBusInitializer(
        bootstrap_servers="localhost:9092"
    )
    
    # Configure multiple services
    services_config = {
        "auth-service": {
            "type": "auth"
        },
        "user-service": {
            "type": "user"
        },
        "notification-service": {
            "type": "notification",
            "handlers": {}  # Could add consumer handlers here
        }
    }
    
    # Initialize all services
    event_buses = await initializer.initialize_multiple_services(services_config)
    
    # Use the services
    for service_name, event_bus in event_buses.items():
        logger.info(f"Service {service_name} initialized", health=await event_bus.health_check())
    
    # Clean up all services
    for event_bus in event_buses.values():
        await event_bus.stop()


# Main demonstration function
async def main():
    """Run all examples"""
    logger.info("Starting Enhanced Kafka Integration Examples")
    
    try:
        # Run basic examples
        await basic_event_bus_example()
        await asyncio.sleep(1)
        
        await service_specific_example()
        await asyncio.sleep(1)
        
        await flexible_initialization_example()
        await asyncio.sleep(1)
        
        await consumer_example()
        await asyncio.sleep(1)
        
        await multiple_services_example()
        
        logger.info("All examples completed successfully")
        
    except Exception as e:
        logger.error("Example failed", error=str(e))
        raise


if __name__ == "__main__":
    # Configure logging
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Run examples
    asyncio.run(main())