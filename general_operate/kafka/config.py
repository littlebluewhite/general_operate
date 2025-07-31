"""
Enhanced Kafka configuration management system
Provides environment-specific configuration with validation and overrides
增強的Kafka配置管理系統，支持環境特定配置和驗證
"""

import os
import yaml
from typing import Dict, Any, Optional, List, Union
from pathlib import Path
from dataclasses import dataclass, field
import structlog

from .validation import (
    KafkaServiceConfig,
    TopicConfig, 
    SecurityConfig,
    ProducerConfig,
    ConsumerConfig,
    EnvironmentConfig,
    validate_kafka_config,
    validate_environment_config
)

logger = structlog.get_logger()


@dataclass
class KafkaConfigDefaults:
    """Default configuration values for different environments
    不同環境的默認配置值
    """
    
    # Development defaults
    DEVELOPMENT: Dict[str, Any] = field(default_factory=lambda: {
        "bootstrap_servers": ["localhost:9092"],
        "security": {
            "protocol": "PLAINTEXT"
        },
        "producer": {
            "acks": "1",
            "enable_idempotence": False,
            "compression_type": "none",
            "retries": 3
        },
        "consumer": {
            "auto_offset_reset": "latest",
            "enable_auto_commit": False,
            "session_timeout_ms": 30000
        },
        "circuit_breaker_enabled": True,
        "monitoring_enabled": True
    })
    
    # Production defaults
    PRODUCTION: Dict[str, Any] = field(default_factory=lambda: {
        "bootstrap_servers": ["kafka-1:9092", "kafka-2:9092", "kafka-3:9092"],
        "security": {
            "protocol": "SASL_SSL",
            "mechanism": "PLAIN"
        },
        "producer": {
            "acks": "all",
            "enable_idempotence": True,
            "compression_type": "gzip",
            "retries": 10,
            "retry_backoff_ms": 1000,
            "request_timeout_ms": 30000,
            "delivery_timeout_ms": 120000,
            "batch_size": 16384,
            "linger_ms": 5
        },
        "consumer": {
            "auto_offset_reset": "earliest",
            "enable_auto_commit": False,
            "max_poll_records": 500,
            "session_timeout_ms": 30000,
            "heartbeat_interval_ms": 3000
        },
        "circuit_breaker_enabled": True,
        "circuit_breaker_failure_threshold": 10,
        "circuit_breaker_recovery_timeout": 120,
        "monitoring_enabled": True,
        "retry_enabled": True,
        "max_retries": 5
    })
    
    # Staging defaults
    STAGING: Dict[str, Any] = field(default_factory=lambda: {
        "bootstrap_servers": ["kafka-staging:9092"],
        "security": {
            "protocol": "SASL_SSL",
            "mechanism": "PLAIN"
        },
        "producer": {
            "acks": "all",
            "enable_idempotence": True,
            "compression_type": "gzip",
            "retries": 5
        },
        "consumer": {
            "auto_offset_reset": "earliest",
            "enable_auto_commit": False
        },
        "circuit_breaker_enabled": True,
        "monitoring_enabled": True
    })
    
    # Test defaults
    TEST: Dict[str, Any] = field(default_factory=lambda: {
        "bootstrap_servers": ["localhost:9092"],
        "security": {
            "protocol": "PLAINTEXT"
        },
        "producer": {
            "acks": "1",
            "enable_idempotence": False,
            "compression_type": "none"
        },
        "consumer": {
            "auto_offset_reset": "earliest",
            "enable_auto_commit": True
        },
        "circuit_breaker_enabled": False,
        "monitoring_enabled": False
    })


class KafkaConfig:
    """
    Enhanced Kafka configuration management with environment support
    增強的Kafka配置管理，支持環境特定配置
    """
    
    def __init__(
        self,
        service_name: str,
        environment: Optional[str] = None,
        config_path: Optional[Union[str, Path]] = None,
        custom_config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize Kafka configuration
        
        Args:
            service_name: Name of the service
            environment: Environment name (development, staging, production, test)
            config_path: Path to configuration file
            custom_config: Custom configuration dictionary to override defaults
        """
        self.service_name = service_name
        self.environment = environment or os.getenv("ENVIRONMENT", "development")
        self.config_path = Path(config_path) if config_path else None
        self.custom_config = custom_config or {}
        
        # Load and validate configuration
        self._raw_config = self._load_configuration()
        self._validated_config = self._validate_configuration()
        
        logger.info(
            "Kafka configuration initialized",
            service_name=self.service_name,
            environment=self.environment,
            config_path=str(self.config_path) if self.config_path else None
        )
    
    def _load_configuration(self) -> Dict[str, Any]:
        """
        Load configuration from multiple sources with proper precedence
        從多個來源加載配置，按正確的優先級
        """
        # Start with environment defaults
        defaults = KafkaConfigDefaults()
        
        if self.environment.lower() == "production":
            config = defaults.PRODUCTION.copy()
        elif self.environment.lower() == "staging":
            config = defaults.STAGING.copy()
        elif self.environment.lower() == "test":
            config = defaults.TEST.copy()
        else:
            config = defaults.DEVELOPMENT.copy()
        
        # Add service metadata
        config.update({
            "service_name": self.service_name,
            "environment": self.environment,
            "service_type": "default"
        })
        
        # Load from configuration file if provided
        if self.config_path and self.config_path.exists():
            file_config = self._load_config_file()
            config = self._deep_merge(config, file_config)
        
        # Apply custom configuration overrides
        if self.custom_config:
            config = self._deep_merge(config, self.custom_config)
        
        # Apply environment variable overrides
        config = self._apply_env_overrides(config)
        
        return config
    
    def _load_config_file(self) -> Dict[str, Any]:
        """
        Load configuration from YAML file
        從YAML文件加載配置
        """
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                logger.info("Configuration file loaded", path=str(self.config_path))
                return config or {}
        except Exception as e:
            logger.error(
                "Failed to load configuration file",
                path=str(self.config_path),
                error=str(e)
            )
            return {}
    
    def _deep_merge(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """
        Deep merge two dictionaries
        深度合併兩個字典
        """
        result = base.copy()
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        
        return result
    
    def _apply_env_overrides(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply environment variable overrides to configuration
        將環境變量覆蓋應用到配置
        """
        # Bootstrap servers
        if os.getenv("KAFKA_BOOTSTRAP_SERVERS"):
            config["bootstrap_servers"] = os.getenv("KAFKA_BOOTSTRAP_SERVERS").split(",")
        
        # Security settings
        if os.getenv("KAFKA_SECURITY_PROTOCOL"):
            config.setdefault("security", {})["protocol"] = os.getenv("KAFKA_SECURITY_PROTOCOL")
        
        if os.getenv("KAFKA_SASL_MECHANISM"):
            config.setdefault("security", {})["mechanism"] = os.getenv("KAFKA_SASL_MECHANISM")
        
        if os.getenv("KAFKA_SASL_USERNAME"):
            config.setdefault("security", {})["username"] = os.getenv("KAFKA_SASL_USERNAME")
        
        if os.getenv("KAFKA_SASL_PASSWORD"):
            config.setdefault("security", {})["password"] = os.getenv("KAFKA_SASL_PASSWORD")
        
        # SSL settings
        if os.getenv("KAFKA_SSL_CA_LOCATION"):
            config.setdefault("security", {})["ssl_ca_location"] = os.getenv("KAFKA_SSL_CA_LOCATION")
        
        if os.getenv("KAFKA_SSL_CERT_LOCATION"):
            config.setdefault("security", {})["ssl_certificate_location"] = os.getenv("KAFKA_SSL_CERT_LOCATION")
        
        if os.getenv("KAFKA_SSL_KEY_LOCATION"):
            config.setdefault("security", {})["ssl_key_location"] = os.getenv("KAFKA_SSL_KEY_LOCATION")
        
        # Producer settings
        if os.getenv("KAFKA_PRODUCER_ACKS"):
            config.setdefault("producer", {})["acks"] = os.getenv("KAFKA_PRODUCER_ACKS")
        
        if os.getenv("KAFKA_PRODUCER_COMPRESSION"):
            config.setdefault("producer", {})["compression_type"] = os.getenv("KAFKA_PRODUCER_COMPRESSION")
        
        if os.getenv("KAFKA_ENABLE_IDEMPOTENCE"):
            config.setdefault("producer", {})["enable_idempotence"] = os.getenv("KAFKA_ENABLE_IDEMPOTENCE").lower() == "true"
        
        # Consumer settings
        if os.getenv("KAFKA_CONSUMER_GROUP_ID"):
            config.setdefault("consumer", {})["group_id"] = os.getenv("KAFKA_CONSUMER_GROUP_ID")
        
        if os.getenv("KAFKA_AUTO_OFFSET_RESET"):
            config.setdefault("consumer", {})["auto_offset_reset"] = os.getenv("KAFKA_AUTO_OFFSET_RESET")
        
        # Service settings
        service_name_env = os.getenv("KAFKA_SERVICE_NAME")
        if service_name_env:
            config["service_name"] = service_name_env
        
        # Circuit breaker settings
        if os.getenv("KAFKA_CIRCUIT_BREAKER_ENABLED"):
            config["circuit_breaker_enabled"] = os.getenv("KAFKA_CIRCUIT_BREAKER_ENABLED").lower() == "true"
        
        logger.debug("Environment variable overrides applied", overrides_count=len([
            key for key in os.environ if key.startswith("KAFKA_")
        ]))
        
        return config
    
    def _validate_configuration(self) -> KafkaServiceConfig:
        """
        Validate configuration using Pydantic models
        使用Pydantic模型驗證配置
        """
        try:
            return validate_kafka_config(self._raw_config)
        except Exception as e:
            logger.error(
                "Configuration validation failed",
                service_name=self.service_name,
                environment=self.environment,
                error=str(e)
            )
            raise
    
    @property
    def config(self) -> KafkaServiceConfig:
        """Get validated configuration"""
        return self._validated_config
    
    @property
    def raw_config(self) -> Dict[str, Any]:
        """Get raw configuration dictionary"""
        return self._raw_config.copy()
    
    def get_bootstrap_servers(self) -> List[str]:
        """Get Kafka bootstrap servers list"""
        return self.config.bootstrap_servers
    
    def get_producer_config(self) -> Dict[str, Any]:
        """Get producer configuration for aiokafka"""
        producer_config = {
            "bootstrap_servers": self.get_bootstrap_servers(),
            "acks": self.config.producer.acks,
            "enable_idempotence": self.config.producer.enable_idempotence,
            "compression_type": self.config.producer.compression_type,
            "retries": self.config.producer.retries,
            "retry_backoff_ms": self.config.producer.retry_backoff_ms,
            "request_timeout_ms": self.config.producer.request_timeout_ms,
            "delivery_timeout_ms": self.config.producer.delivery_timeout_ms,
            "batch_size": self.config.producer.batch_size,
            "linger_ms": self.config.producer.linger_ms,
            "buffer_memory": self.config.producer.buffer_memory,
        }
        
        # Add security configuration
        security_config = self._get_security_config()
        producer_config.update(security_config)
        
        return producer_config
    
    def get_consumer_config(self, group_id: Optional[str] = None) -> Dict[str, Any]:
        """Get consumer configuration for aiokafka"""
        consumer_config = {
            "bootstrap_servers": self.get_bootstrap_servers(),
            "auto_offset_reset": self.config.consumer.auto_offset_reset,
            "enable_auto_commit": self.config.consumer.enable_auto_commit,
            "max_poll_records": self.config.consumer.max_poll_records,
            "session_timeout_ms": self.config.consumer.session_timeout_ms,
            "heartbeat_interval_ms": self.config.consumer.heartbeat_interval_ms,
            "fetch_min_bytes": self.config.consumer.fetch_min_bytes,
            "fetch_max_wait_ms": self.config.consumer.fetch_max_wait_ms,
        }
        
        # Set group_id
        if group_id:
            consumer_config["group_id"] = group_id
        elif self.config.consumer.group_id:
            consumer_config["group_id"] = self.config.consumer.group_id
        else:
            consumer_config["group_id"] = f"{self.service_name}-consumer-group"
        
        # Add security configuration
        security_config = self._get_security_config()
        consumer_config.update(security_config)
        
        return consumer_config
    
    def get_admin_config(self) -> Dict[str, Any]:
        """Get admin client configuration"""
        admin_config = {
            "bootstrap_servers": self.get_bootstrap_servers(),
        }
        
        # Add security configuration
        security_config = self._get_security_config()
        admin_config.update(security_config)
        
        return admin_config
    
    def _get_security_config(self) -> Dict[str, Any]:
        """Get security configuration for Kafka clients"""
        security_config = {}
        security = self.config.security
        
        if security.protocol != "PLAINTEXT":
            security_config["security_protocol"] = security.protocol
        
        if security.protocol in ["SASL_PLAINTEXT", "SASL_SSL"]:
            if security.mechanism:
                security_config["sasl_mechanism"] = security.mechanism
            if security.username:
                security_config["sasl_plain_username"] = security.username
            if security.password:
                security_config["sasl_plain_password"] = security.password
        
        if security.protocol in ["SSL", "SASL_SSL"]:
            if security.ssl_ca_location:
                security_config["ssl_ca_location"] = security.ssl_ca_location
            if security.ssl_certificate_location:
                security_config["ssl_certificate_location"] = security.ssl_certificate_location
            if security.ssl_key_location:
                security_config["ssl_key_location"] = security.ssl_key_location
            security_config["ssl_check_hostname"] = security.ssl_check_hostname
        
        return security_config
    
    def get_topic_configs(self) -> List[TopicConfig]:
        """Get topic configurations"""
        return self.config.topics
    
    def get_circuit_breaker_config(self) -> Dict[str, Any]:
        """Get circuit breaker configuration"""
        return {
            "enabled": self.config.circuit_breaker_enabled,
            "failure_threshold": self.config.circuit_breaker_failure_threshold,
            "recovery_timeout": self.config.circuit_breaker_recovery_timeout
        }
    
    def get_monitoring_config(self) -> Dict[str, Any]:
        """Get monitoring configuration"""
        return {
            "enabled": self.config.monitoring_enabled,
            "health_check_enabled": self.config.health_check_enabled,
            "metrics_export_enabled": self.config.metrics_export_enabled
        }
    
    def get_retry_config(self) -> Dict[str, Any]:
        """Get retry configuration"""
        return {
            "enabled": self.config.retry_enabled,
            "max_retries": self.config.max_retries,
            "backoff_factor": self.config.retry_backoff_factor,
            "max_backoff": self.config.max_retry_backoff
        }
    
    def update_config(self, updates: Dict[str, Any]) -> None:
        """
        Update configuration with new values and re-validate
        使用新值更新配置並重新驗證
        """
        # Merge updates into raw config
        self._raw_config = self._deep_merge(self._raw_config, updates)
        
        # Re-validate
        self._validated_config = self._validate_configuration()
        
        logger.info(
            "Configuration updated",
            service_name=self.service_name,
            updates=list(updates.keys())
        )
    
    def export_config(self, export_path: Optional[Union[str, Path]] = None) -> Dict[str, Any]:
        """
        Export current configuration to file or return as dictionary
        將當前配置導出到文件或返回為字典
        """
        config_dict = self._raw_config.copy()
        
        if export_path:
            export_path = Path(export_path)
            export_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(export_path, 'w', encoding='utf-8') as f:
                yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)
            
            logger.info("Configuration exported", path=str(export_path))
        
        return config_dict


class KafkaConfigFactory:
    """
    Factory for creating Kafka configurations for different services and environments
    用於為不同服務和環境創建Kafka配置的工廠
    """
    
    @staticmethod
    def create_config(
        service_name: str,
        environment: Optional[str] = None,
        service_type: Optional[str] = None,
        config_path: Optional[Union[str, Path]] = None,
        **kwargs
    ) -> KafkaConfig:
        """
        Create a Kafka configuration instance
        
        Args:
            service_name: Name of the service
            environment: Environment name
            service_type: Type of service (auth, user, notification, etc.)
            config_path: Path to configuration file
            **kwargs: Additional configuration overrides
        """
        custom_config = {}
        
        if service_type:
            custom_config["service_type"] = service_type
        
        # Add any additional overrides
        custom_config.update(kwargs)
        
        return KafkaConfig(
            service_name=service_name,
            environment=environment,
            config_path=config_path,
            custom_config=custom_config
        )
    
    @staticmethod
    def create_development_config(service_name: str, **kwargs) -> KafkaConfig:
        """Create development environment configuration"""
        return KafkaConfigFactory.create_config(
            service_name=service_name,
            environment="development",
            **kwargs
        )
    
    @staticmethod
    def create_production_config(service_name: str, **kwargs) -> KafkaConfig:
        """Create production environment configuration"""
        return KafkaConfigFactory.create_config(
            service_name=service_name,
            environment="production",
            **kwargs
        )
    
    @staticmethod
    def create_test_config(service_name: str, **kwargs) -> KafkaConfig:
        """Create test environment configuration"""
        return KafkaConfigFactory.create_config(
            service_name=service_name,
            environment="test",
            **kwargs
        )


# Convenience functions
def create_kafka_config(
    service_name: str,
    environment: Optional[str] = None,
    **kwargs
) -> KafkaConfig:
    """
    Create a Kafka configuration instance
    創建Kafka配置實例
    """
    return KafkaConfigFactory.create_config(
        service_name=service_name,
        environment=environment,
        **kwargs
    )


def load_environment_config(
    environment: str,
    config_path: Union[str, Path]
) -> EnvironmentConfig:
    """
    Load and validate environment-specific configuration from file
    從文件加載並驗證環境特定配置
    """
    config_path = Path(config_path)
    
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)
        
        # Add environment name if not present
        config_data.setdefault("name", environment)
        
        return validate_environment_config(config_data)
    
    except Exception as e:
        logger.error(
            "Failed to load environment configuration",
            environment=environment,
            config_path=str(config_path),
            error=str(e)
        )
        raise