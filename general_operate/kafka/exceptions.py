"""
Kafka-specific exceptions for GeneralOperate
Backwards compatibility aliases for the new unified exception system
"""

from ..core.exceptions import KafkaException

# Backwards compatibility aliases
KafkaOperateException = KafkaException
KafkaConnectionException = KafkaException  
KafkaProducerException = KafkaException
KafkaConsumerException = KafkaException
KafkaSerializationException = KafkaException
KafkaConfigurationException = KafkaException

# For clarity, also export the new unified exception
__all__ = [
    "KafkaOperateException",
    "KafkaConnectionException", 
    "KafkaProducerException",
    "KafkaConsumerException",
    "KafkaSerializationException",
    "KafkaConfigurationException",
]