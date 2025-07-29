"""
Dynamic middleware factory for FastAPI integration
Generates service-specific event bus middleware for different service types
為 FastAPI 集成生成服務特定事件總線中間件的動態中間件工廠
"""

import structlog
from contextlib import asynccontextmanager
from typing import Any, Callable, Awaitable, Dict, Optional
from fastapi import FastAPI

from .service_initializer import (
    ServiceEventBusInitializer,
    PreConfiguredInitializers,
    FlexibleEventBusInitializer,
    cleanup_event_bus
)
from .enhanced_client import EnhancedEventBus, KafkaClientFactory
from .models.event_message import EventMessage

logger = structlog.get_logger()


class EventBusMiddlewareFactory:
    """Factory for creating service-specific FastAPI middleware"""
    
    @staticmethod
    def create_service_lifespan(
        service_type: str,
        bootstrap_servers: str | list[str],
        service_name: str | None = None,
        config: dict[str, Any] | None = None,
        consumer_handlers: dict[str, Callable[[EventMessage], Awaitable[None]]] | None = None,
        custom_topic_configs: list[dict[str, Any]] | None = None
    ):
        """
        Create a FastAPI lifespan context manager for a specific service type
        
        Args:
            service_type: Type of service (auth, user, notification, generic)
            bootstrap_servers: Kafka bootstrap servers
            service_name: Name of the service (optional, defaults based on type)
            config: Kafka configuration
            consumer_handlers: Event handlers for consumers
            custom_topic_configs: Custom topic configurations
        """
        
        # Set default service names
        if not service_name:
            service_name = f"{service_type}-service"
        
        @asynccontextmanager
        async def service_lifespan(app: FastAPI):
            """Service lifespan context manager"""
            
            # Store event bus in app state for access in routes
            event_bus = None
            
            try:
                logger.info(f"Starting {service_type} service with event bus", service=service_name)
                
                # Initialize based on service type
                if service_type == "auth":
                    event_bus = await PreConfiguredInitializers.initialize_auth_service(
                        bootstrap_servers, service_name, config
                    )
                elif service_type == "user":
                    event_bus = await PreConfiguredInitializers.initialize_user_service(
                        bootstrap_servers, service_name, config
                    )
                elif service_type == "notification":
                    event_bus = await PreConfiguredInitializers.initialize_notification_service(
                        bootstrap_servers, service_name, config, consumer_handlers
                    )
                else:  # generic
                    event_bus = await PreConfiguredInitializers.initialize_generic_service(
                        bootstrap_servers, service_name, custom_topic_configs, 
                        consumer_handlers, config
                    )
                
                # Store in app state
                app.state.event_bus = event_bus
                app.state.service_name = service_name
                app.state.service_type = service_type
                
                logger.info(f"{service_type} service event bus initialized", service=service_name)
                
                yield
                
            except Exception as e:
                logger.error(f"Failed to initialize {service_type} service", error=str(e))
                raise
            finally:
                # Cleanup
                logger.info(f"Shutting down {service_type} service event bus", service=service_name)
                if event_bus:
                    await cleanup_event_bus(event_bus)
        
        return service_lifespan
    
    @staticmethod
    def create_auth_service_app(
        bootstrap_servers: str | list[str],
        service_name: str = "auth-service",
        config: dict[str, Any] | None = None,
        app_config: dict[str, Any] | None = None
    ) -> FastAPI:
        """Create FastAPI app with auth service event bus integration"""
        
        app_config = app_config or {}
        
        app = FastAPI(
            title=app_config.get("title", "Authentication Service"),
            description=app_config.get("description", "Authentication service with Kafka event integration"),
            version=app_config.get("version", "1.0.0"),
            lifespan=EventBusMiddlewareFactory.create_service_lifespan(
                "auth", bootstrap_servers, service_name, config
            )
        )
        
        return app
    
    @staticmethod
    def create_user_service_app(
        bootstrap_servers: str | list[str],
        service_name: str = "user-service",
        config: dict[str, Any] | None = None,
        app_config: dict[str, Any] | None = None
    ) -> FastAPI:
        """Create FastAPI app with user service event bus integration"""
        
        app_config = app_config or {}
        
        app = FastAPI(
            title=app_config.get("title", "User Management Service"),
            description=app_config.get("description", "User management service with Kafka event integration"),
            version=app_config.get("version", "1.0.0"),
            lifespan=EventBusMiddlewareFactory.create_service_lifespan(
                "user", bootstrap_servers, service_name, config
            )
        )
        
        return app
    
    @staticmethod
    def create_notification_service_app(
        bootstrap_servers: str | list[str],
        service_name: str = "notification-service",
        config: dict[str, Any] | None = None,
        consumer_handlers: dict[str, Callable[[EventMessage], Awaitable[None]]] | None = None,
        app_config: dict[str, Any] | None = None
    ) -> FastAPI:
        """Create FastAPI app with notification service event bus integration"""
        
        app_config = app_config or {}
        
        app = FastAPI(
            title=app_config.get("title", "Notification Service"),
            description=app_config.get("description", "Notification service with Kafka event consumer"),
            version=app_config.get("version", "1.0.0"),
            lifespan=EventBusMiddlewareFactory.create_service_lifespan(
                "notification", bootstrap_servers, service_name, config, consumer_handlers
            )
        )
        
        return app
    
    @staticmethod
    def create_generic_service_app(
        service_type: str,
        bootstrap_servers: str | list[str],
        service_name: str | None = None,
        config: dict[str, Any] | None = None,
        consumer_handlers: dict[str, Callable[[EventMessage], Awaitable[None]]] | None = None,
        custom_topic_configs: list[dict[str, Any]] | None = None,
        app_config: dict[str, Any] | None = None
    ) -> FastAPI:
        """Create FastAPI app with generic service event bus integration"""
        
        app_config = app_config or {}
        service_name = service_name or f"{service_type}-service"
        
        app = FastAPI(
            title=app_config.get("title", f"{service_type.title()} Service"),
            description=app_config.get("description", f"{service_type} service with Kafka event integration"),
            version=app_config.get("version", "1.0.0"),
            lifespan=EventBusMiddlewareFactory.create_service_lifespan(
                service_type, bootstrap_servers, service_name, config, 
                consumer_handlers, custom_topic_configs
            )
        )
        
        return app
    
    @staticmethod
    def create_flexible_service_app(
        service_name: str,
        service_type: str = "default",
        bootstrap_servers: str | list[str] = "localhost:9092",
        config_path: Optional[str] = None,
        consumer_handlers: Optional[dict[str, Callable[[EventMessage], Awaitable[None]]]] = None,
        app_config: Optional[dict[str, Any]] = None
    ) -> FastAPI:
        """
        Create FastAPI app with flexible event bus integration
        創建具有靈活事件總線集成的 FastAPI 應用
        """
        
        app_config = app_config or {}
        
        @asynccontextmanager
        async def flexible_lifespan(app: FastAPI):
            """Flexible service lifespan with configuration support"""
            
            event_bus = None
            
            try:
                logger.info(f"Starting {service_name} with flexible event bus", service_type=service_type)
                
                # Initialize flexible event bus
                initializer = FlexibleEventBusInitializer(
                    bootstrap_servers=bootstrap_servers,
                    config_path=config_path
                )
                
                event_bus = await initializer.create_service_event_bus(
                    service_name=service_name,
                    service_type=service_type,
                    consumer_handlers=consumer_handlers
                )
                
                # Store in app state
                app.state.event_bus = event_bus
                app.state.service_name = service_name
                app.state.service_type = service_type
                
                logger.info(f"{service_name} event bus initialized successfully")
                
                yield
                
            except Exception as e:
                logger.error(f"Failed to initialize {service_name}", error=str(e))
                raise
            finally:
                # Cleanup
                logger.info(f"Shutting down {service_name} event bus")
                if event_bus:
                    await event_bus.stop()
        
        app = FastAPI(
            title=app_config.get("title", f"{service_name} Service"),
            description=app_config.get("description", f"{service_name} with Kafka event integration"),
            version=app_config.get("version", "1.0.0"),
            lifespan=flexible_lifespan
        )
        
        return app


class EventBusMiddleware:
    """Legacy middleware class for backward compatibility with account project"""
    
    @staticmethod
    @asynccontextmanager
    async def auth_service_lifespan(app: FastAPI):
        """Auth Service lifespan - backward compatibility"""
        bootstrap_servers = getattr(app.state, 'kafka_bootstrap_servers', 'localhost:9092')
        
        lifespan = EventBusMiddlewareFactory.create_service_lifespan(
            "auth", bootstrap_servers, "auth-service"
        )
        
        async with lifespan(app) as result:
            yield result
    
    @staticmethod
    @asynccontextmanager
    async def user_service_lifespan(app: FastAPI):
        """User Service lifespan - backward compatibility"""
        bootstrap_servers = getattr(app.state, 'kafka_bootstrap_servers', 'localhost:9092')
        
        lifespan = EventBusMiddlewareFactory.create_service_lifespan(
            "user", bootstrap_servers, "user-service"
        )
        
        async with lifespan(app) as result:
            yield result
    
    @staticmethod
    @asynccontextmanager
    async def notification_service_lifespan(app: FastAPI):
        """Notification Service lifespan - backward compatibility"""
        bootstrap_servers = getattr(app.state, 'kafka_bootstrap_servers', 'localhost:9092')
        
        lifespan = EventBusMiddlewareFactory.create_service_lifespan(
            "notification", bootstrap_servers, "notification-service"
        )
        
        async with lifespan(app) as result:
            yield result


# Backward compatible convenience functions
def create_auth_service_app(
    bootstrap_servers: str | list[str] = "localhost:9092",
    **kwargs
) -> FastAPI:
    """Create auth service app - backward compatible"""
    return EventBusMiddlewareFactory.create_auth_service_app(bootstrap_servers, **kwargs)


def create_user_service_app(
    bootstrap_servers: str | list[str] = "localhost:9092",  
    **kwargs
) -> FastAPI:
    """Create user service app - backward compatible"""
    return EventBusMiddlewareFactory.create_user_service_app(bootstrap_servers, **kwargs)


def create_notification_service_app(
    bootstrap_servers: str | list[str] = "localhost:9092",
    **kwargs
) -> FastAPI:
    """Create notification service app - backward compatible"""
    return EventBusMiddlewareFactory.create_notification_service_app(bootstrap_servers, **kwargs)


# Helper for accessing event bus in FastAPI routes
def get_event_bus(app: FastAPI) -> EnhancedEventBus:
    """Get event bus from FastAPI app state"""
    if not hasattr(app.state, 'event_bus') or app.state.event_bus is None:
        raise RuntimeError("Event bus not initialized. Make sure to use appropriate lifespan middleware.")
    
    return app.state.event_bus


def get_service_info(app: FastAPI) -> dict[str, str]:
    """Get service information from FastAPI app state"""
    return {
        "service_name": getattr(app.state, 'service_name', 'unknown'),
        "service_type": getattr(app.state, 'service_type', 'unknown')
    }


# Account project style factory functions - account 項目樣式的工廠函數
def create_passwordless_auth_service_app(
    bootstrap_servers: str | list[str] = "localhost:9092",
    **kwargs
) -> FastAPI:
    """
    Create passwordless auth service app - full account project compatibility
    創建無密碼認證服務應用 - 完全兼容 account 項目
    """
    return EventBusMiddlewareFactory.create_flexible_service_app(
        service_name="auth-service",
        service_type="auth",
        bootstrap_servers=bootstrap_servers,
        app_config={
            "title": "Passwordless Auth Service",
            "description": "Passwordless authentication service with Kafka event integration",
            "version": "1.0.0"
        },
        **kwargs
    )


def create_user_management_service_app(
    bootstrap_servers: str | list[str] = "localhost:9092",
    **kwargs
) -> FastAPI:
    """
    Create user management service app - account project compatibility
    創建用戶管理服務應用 - account 項目兼容性
    """
    return EventBusMiddlewareFactory.create_flexible_service_app(
        service_name="user-service", 
        service_type="user",
        bootstrap_servers=bootstrap_servers,
        **kwargs
    )


# Settings integration helper - 設置集成助手
def create_service_from_settings(
    service_name: str,
    service_type: str = "default",
    settings: Optional[Any] = None
) -> FastAPI:
    """
    Create service app using settings object (like account project)
    使用 settings 對象創建服務應用（如 account 項目）
    """
    bootstrap_servers = "localhost:9092"
    
    if settings:
        # Try to get bootstrap servers from settings
        bootstrap_servers = getattr(settings, 'kafka_bootstrap_servers', bootstrap_servers)
    
    return EventBusMiddlewareFactory.create_flexible_service_app(
        service_name=service_name,
        service_type=service_type,
        bootstrap_servers=bootstrap_servers
    )