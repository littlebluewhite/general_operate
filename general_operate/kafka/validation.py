"""
Kafka configuration validation using Pydantic models
Provides validation for service configuration, topic configuration, and security settings
按照重構計劃提供的Pydantic驗證模型
"""

from typing import Dict, Any, List, Optional, Union
from pathlib import Path
import os
from pydantic import BaseModel, Field, validator, root_validator
import structlog

logger = structlog.get_logger()


class TopicConfig(BaseModel):
    """Topic configuration validation model
    主題配置驗證模型
    """
    name: str = Field(..., description="Topic name")
    partitions: int = Field(default=1, ge=1, le=100, description="Number of partitions")
    replication_factor: int = Field(default=1, ge=1, description="Replication factor")
    config: Dict[str, str] = Field(default_factory=dict, description="Topic-specific configuration")

    @validator('name')
    def validate_topic_name(cls, v):
        """Validate topic name format"""
        if not v or len(v.strip()) == 0:
            raise ValueError('Topic name cannot be empty')
        
        # Kafka topic naming rules
        if not all(c.isalnum() or c in '-._' for c in v):
            raise ValueError('Topic name must contain only alphanumeric characters, hyphens, dots, and underscores')
        
        if len(v) > 249:
            raise ValueError('Topic name must be less than 249 characters')
        
        return v.strip()

    @validator('partitions')
    def validate_partitions(cls, v):
        """Validate partition count"""
        if v > 50:
            logger.warning("High partition count detected", partitions=v)
        return v

    @validator('config')
    def validate_topic_config(cls, v):
        """Validate topic configuration parameters"""
        # Common topic configuration validations
        if 'retention.ms' in v:
            try:
                retention = int(v['retention.ms'])
                if retention < 1000:  # Less than 1 second
                    logger.warning("Very short retention period", retention_ms=retention)
            except ValueError:
                raise ValueError('retention.ms must be a valid integer')
        
        if 'cleanup.policy' in v:
            if v['cleanup.policy'] not in ['delete', 'compact', 'compact,delete']:
                raise ValueError('cleanup.policy must be one of: delete, compact, compact,delete')
        
        return v


class SecurityConfig(BaseModel):
    """Security configuration validation model
    安全配置驗證模型
    """
    protocol: str = Field(default="PLAINTEXT", description="Security protocol")
    mechanism: Optional[str] = Field(None, description="SASL mechanism")
    username: Optional[str] = Field(None, description="SASL username")
    password: Optional[str] = Field(None, description="SASL password")
    ssl_ca_location: Optional[str] = Field(None, description="SSL CA certificate location")
    ssl_certificate_location: Optional[str] = Field(None, description="SSL certificate location")
    ssl_key_location: Optional[str] = Field(None, description="SSL key location")
    ssl_check_hostname: bool = Field(default=True, description="Whether to check SSL hostname")

    @validator('protocol')
    def validate_protocol(cls, v):
        """Validate security protocol"""
        valid_protocols = ['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL']
        if v not in valid_protocols:
            raise ValueError(f'Security protocol must be one of: {", ".join(valid_protocols)}')
        return v

    @validator('mechanism')
    def validate_mechanism(cls, v, values):
        """Validate SASL mechanism"""
        protocol = values.get('protocol')
        if protocol in ['SASL_PLAINTEXT', 'SASL_SSL']:
            if not v:
                raise ValueError('SASL mechanism is required for SASL protocols')
            
            valid_mechanisms = ['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512', 'GSSAPI', 'OAUTHBEARER']
            if v not in valid_mechanisms:
                raise ValueError(f'SASL mechanism must be one of: {", ".join(valid_mechanisms)}')
        
        return v

    @root_validator
    def validate_ssl_files(cls, values):
        """Validate SSL file paths exist"""
        ssl_files = ['ssl_ca_location', 'ssl_certificate_location', 'ssl_key_location']
        
        for file_field in ssl_files:
            file_path = values.get(file_field)
            if file_path and not Path(file_path).exists():
                logger.warning(f"SSL file not found: {file_path}")
        
        return values


class ProducerConfig(BaseModel):
    """Producer configuration validation model
    生產者配置驗證模型
    """
    acks: Union[str, int] = Field(default="1", description="Acknowledgment level")
    enable_idempotence: bool = Field(default=False, description="Enable idempotent producer")
    compression_type: str = Field(default="none", description="Compression type")
    retries: int = Field(default=3, ge=0, description="Number of retries")
    retry_backoff_ms: int = Field(default=100, ge=0, description="Retry backoff in milliseconds")
    request_timeout_ms: int = Field(default=30000, ge=1000, description="Request timeout in milliseconds")
    delivery_timeout_ms: int = Field(default=120000, ge=1000, description="Delivery timeout in milliseconds")
    batch_size: int = Field(default=16384, ge=0, description="Batch size")
    linger_ms: int = Field(default=0, ge=0, description="Linger time in milliseconds")
    buffer_memory: int = Field(default=33554432, ge=1024, description="Buffer memory in bytes")

    @validator('acks')
    def validate_acks(cls, v):
        """Validate acknowledgment settings"""
        if isinstance(v, str):
            if v not in ['0', '1', 'all']:
                raise ValueError('acks must be "0", "1", or "all"')
        elif isinstance(v, int):
            if v not in [0, 1, -1]:
                raise ValueError('acks must be 0, 1, or -1')
        return v

    @validator('compression_type')
    def validate_compression(cls, v):
        """Validate compression type"""
        valid_types = ['none', 'gzip', 'snappy', 'lz4', 'zstd']
        if v not in valid_types:
            raise ValueError(f'compression_type must be one of: {", ".join(valid_types)}')
        return v

    @root_validator
    def validate_timeout_consistency(cls, values):
        """Validate timeout consistency"""
        request_timeout = values.get('request_timeout_ms', 30000)
        delivery_timeout = values.get('delivery_timeout_ms', 120000)
        
        if request_timeout >= delivery_timeout:
            raise ValueError('request_timeout_ms must be less than delivery_timeout_ms')
        
        return values


class ConsumerConfig(BaseModel):
    """Consumer configuration validation model
    消費者配置驗證模型
    """
    auto_offset_reset: str = Field(default="latest", description="Auto offset reset policy")
    enable_auto_commit: bool = Field(default=False, description="Enable auto commit")
    group_id: Optional[str] = Field(None, description="Consumer group ID")
    max_poll_records: int = Field(default=500, ge=1, description="Maximum records per poll")
    session_timeout_ms: int = Field(default=30000, ge=1000, description="Session timeout in milliseconds")
    heartbeat_interval_ms: int = Field(default=3000, ge=1000, description="Heartbeat interval in milliseconds") 
    fetch_min_bytes: int = Field(default=1, ge=1, description="Minimum fetch bytes")
    fetch_max_wait_ms: int = Field(default=500, ge=0, description="Maximum fetch wait time in milliseconds")
    auto_commit_interval_ms: int = Field(default=5000, ge=1000, description="Auto commit interval in milliseconds")

    @validator('auto_offset_reset')
    def validate_offset_reset(cls, v):
        """Validate auto offset reset policy"""
        valid_policies = ['earliest', 'latest', 'none']
        if v not in valid_policies:
            raise ValueError(f'auto_offset_reset must be one of: {", ".join(valid_policies)}')
        return v

    @validator('group_id')
    def validate_group_id(cls, v):
        """Validate consumer group ID"""
        if v is not None:
            if not v or len(v.strip()) == 0:
                raise ValueError('Consumer group ID cannot be empty string')
            
            # Kafka consumer group naming rules
            if not all(c.isalnum() or c in '-._' for c in v):
                raise ValueError('Consumer group ID must contain only alphanumeric characters, hyphens, dots, and underscores')
        
        return v.strip() if v else v

    @root_validator
    def validate_heartbeat_session_consistency(cls, values):
        """Validate heartbeat and session timeout consistency"""
        session_timeout = values.get('session_timeout_ms', 30000)
        heartbeat_interval = values.get('heartbeat_interval_ms', 3000)
        
        # Heartbeat should be less than 1/3 of session timeout
        if heartbeat_interval >= session_timeout / 3:
            raise ValueError('heartbeat_interval_ms should be less than session_timeout_ms / 3')
        
        return values


class EnvironmentConfig(BaseModel):
    """Environment-specific configuration validation model
    環境特定配置驗證模型
    """
    name: str = Field(..., description="Environment name")
    bootstrap_servers: List[str] = Field(..., description="Kafka bootstrap servers")
    security: SecurityConfig = Field(default_factory=SecurityConfig, description="Security configuration")
    producer: ProducerConfig = Field(default_factory=ProducerConfig, description="Producer configuration")
    consumer: ConsumerConfig = Field(default_factory=ConsumerConfig, description="Consumer configuration")
    topics: List[TopicConfig] = Field(default_factory=list, description="Topic configurations")
    monitoring_enabled: bool = Field(default=True, description="Enable monitoring")
    health_check_enabled: bool = Field(default=True, description="Enable health checks")

    @validator('name')
    def validate_environment_name(cls, v):
        """Validate environment name"""
        valid_environments = ['development', 'staging', 'production', 'test']
        if v not in valid_environments:
            logger.warning(f"Non-standard environment name: {v}")
        return v

    @validator('bootstrap_servers')
    def validate_bootstrap_servers(cls, v):
        """Validate bootstrap servers format"""
        if not v:
            raise ValueError('Bootstrap servers cannot be empty')
        
        for server in v:
            if ':' not in server:
                raise ValueError(f'Invalid server format: {server}. Expected format: host:port')
            
            host, port = server.rsplit(':', 1)
            try:
                port_num = int(port)
                if not (1 <= port_num <= 65535):
                    raise ValueError(f'Invalid port number: {port_num}')
            except ValueError:
                raise ValueError(f'Invalid port in server: {server}')
        
        return v

    @root_validator
    def validate_production_settings(cls, values):
        """Validate production environment specific settings"""
        env_name = values.get('name')
        
        if env_name == 'production':
            security = values.get('security')
            producer = values.get('producer')
            
            # Production should use secure protocols
            if security and security.protocol == 'PLAINTEXT':
                logger.warning("Production environment using PLAINTEXT protocol")
            
            # Production should have idempotence enabled
            if producer and not producer.enable_idempotence:
                logger.warning("Production environment should enable idempotence")
            
            # Production should use strong acknowledgment
            if producer and producer.acks not in ['all', -1]:
                logger.warning("Production environment should use acks='all'")
        
        return values


class KafkaServiceConfig(BaseModel):
    """Complete Kafka service configuration validation model
    完整的Kafka服務配置驗證模型
    """
    service_name: str = Field(..., description="Service name")
    service_type: str = Field(default="default", description="Service type")
    environment: str = Field(default="development", description="Environment name")
    
    # Connection settings
    bootstrap_servers: List[str] = Field(..., description="Kafka bootstrap servers")
    
    # Configuration objects
    security: SecurityConfig = Field(default_factory=SecurityConfig, description="Security configuration")
    producer: ProducerConfig = Field(default_factory=ProducerConfig, description="Producer configuration")
    consumer: ConsumerConfig = Field(default_factory=ConsumerConfig, description="Consumer configuration")
    topics: List[TopicConfig] = Field(default_factory=list, description="Topic configurations")
    
    # Service settings
    circuit_breaker_enabled: bool = Field(default=True, description="Enable circuit breaker")
    circuit_breaker_failure_threshold: int = Field(default=5, ge=1, description="Circuit breaker failure threshold")
    circuit_breaker_recovery_timeout: int = Field(default=60, ge=1, description="Circuit breaker recovery timeout")
    
    # Monitoring and health
    monitoring_enabled: bool = Field(default=True, description="Enable monitoring")
    health_check_enabled: bool = Field(default=True, description="Enable health checks")
    metrics_export_enabled: bool = Field(default=True, description="Enable metrics export")
    
    # Retry and resilience
    retry_enabled: bool = Field(default=True, description="Enable retry mechanism")
    max_retries: int = Field(default=3, ge=0, description="Maximum number of retries")
    retry_backoff_factor: float = Field(default=1.0, ge=0.1, description="Retry backoff factor")
    max_retry_backoff: float = Field(default=60.0, ge=1.0, description="Maximum retry backoff")

    @validator('service_name')
    def validate_service_name(cls, v):
        """Validate service name format"""
        if not v or len(v.strip()) == 0:
            raise ValueError('Service name cannot be empty')
        
        # Service name should follow naming conventions
        if not v.replace('-', '').replace('_', '').isalnum():
            raise ValueError('Service name must contain only alphanumeric characters, hyphens, and underscores')
        
        if len(v) > 50:
            raise ValueError('Service name must be less than 50 characters')
        
        return v.strip()

    @validator('service_type')
    def validate_service_type(cls, v):
        """Validate service type"""
        valid_types = ['auth', 'user', 'notification', 'audit', 'default', 'custom']
        if v not in valid_types:
            logger.info(f"Custom service type detected: {v}")
        return v

    @validator('bootstrap_servers')
    def validate_bootstrap_servers(cls, v):
        """Validate bootstrap servers"""
        if not v:
            raise ValueError('Bootstrap servers cannot be empty')
        
        for server in v:
            if ':' not in server:
                raise ValueError(f'Invalid server format: {server}')
        
        return v

    class Config:
        """Pydantic configuration"""
        validate_assignment = True
        extra = "forbid"
        use_enum_values = True


def validate_kafka_config(config_data: Dict[str, Any]) -> KafkaServiceConfig:
    """
    Validate Kafka configuration data using Pydantic models
    使用Pydantic模型驗證Kafka配置數據
    
    Args:
        config_data: Raw configuration dictionary
        
    Returns:
        Validated KafkaServiceConfig instance
        
    Raises:
        ValueError: If configuration is invalid
    """
    try:
        return KafkaServiceConfig(**config_data)
    except Exception as e:
        logger.error("Configuration validation failed", error=str(e), config_keys=list(config_data.keys()))
        raise ValueError(f"Configuration validation failed: {str(e)}")


def validate_topic_config(topic_data: Dict[str, Any]) -> TopicConfig:
    """
    Validate topic configuration data
    驗證主題配置數據
    
    Args:
        topic_data: Raw topic configuration dictionary
        
    Returns:
        Validated TopicConfig instance
    
    Raises:
        ValueError: If topic configuration is invalid
    """
    try:
        return TopicConfig(**topic_data)
    except Exception as e:
        logger.error("Topic configuration validation failed", error=str(e))
        raise ValueError(f"Topic configuration validation failed: {str(e)}")


def validate_environment_config(env_data: Dict[str, Any]) -> EnvironmentConfig:
    """
    Validate environment-specific configuration data  
    驗證環境特定配置數據
    
    Args:
        env_data: Raw environment configuration dictionary
        
    Returns:
        Validated EnvironmentConfig instance
        
    Raises:
        ValueError: If environment configuration is invalid
    """
    try:
        return EnvironmentConfig(**env_data)
    except Exception as e:
        logger.error("Environment configuration validation failed", error=str(e))
        raise ValueError(f"Environment configuration validation failed: {str(e)}")