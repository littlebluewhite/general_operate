"""
Kafka-specific exceptions for GeneralOperate
"""

from .. import GeneralOperateException


class KafkaOperateException(GeneralOperateException):
    """Base exception for Kafka operations"""
    pass


class KafkaConnectionException(KafkaOperateException):
    """Raised when Kafka connection fails"""
    pass


class KafkaProducerException(KafkaOperateException):
    """Raised when Kafka producer operations fail"""
    pass


class KafkaConsumerException(KafkaOperateException):
    """Raised when Kafka consumer operations fail"""
    pass


class KafkaSerializationException(KafkaOperateException):
    """Raised when message serialization/deserialization fails"""
    pass


class KafkaConfigurationException(KafkaOperateException):
    """Raised when Kafka configuration is invalid"""
    pass