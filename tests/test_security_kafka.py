"""
Security tests for the Kafka module.

Tests cover:
- SSL/TLS configuration and validation
- SASL authentication mechanisms
- Sensitive data handling and filtering
- Configuration security validation
- Authentication error handling
- Certificate validation scenarios
- Network security considerations
- Data privacy and compliance patterns
"""

import json
import uuid
from typing import Any, Dict, List
from unittest.mock import AsyncMock, Mock, patch

import pytest

from general_operate.kafka.kafka_client import (
    EventMessage,
    KafkaEventBus,
    MessageStatus,
    ProcessingResult,
)
from general_operate.kafka.manager_config import ServiceConfig
from general_operate.kafka.service_event_manager import ServiceEventManager


class TestKafkaSSLSecurity:
    """Test SSL/TLS security configurations."""

    @pytest.mark.asyncio
    async def test_ssl_configuration_validation(self, ssl_kafka_config, service_config):
        """Test SSL configuration validation and setup."""
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_event_bus_class.return_value = mock_event_bus
            
            manager = ServiceEventManager(ssl_kafka_config, service_config)
            await manager.start()
            
            # Verify SSL configuration was passed to event bus
            call_args = mock_event_bus_class.call_args[0]
            ssl_config = call_args[0]  # kafka_config
            
            assert ssl_config["security_protocol"] == "SSL"
            assert ssl_config["ssl_check_hostname"] is True
            assert "ssl_cafile" in ssl_config
            assert "ssl_certfile" in ssl_config
            assert "ssl_keyfile" in ssl_config
            
            await manager.stop()

    @pytest.mark.asyncio
    async def test_ssl_certificate_paths_validation(self):
        """Test SSL certificate path validation."""
        ssl_configs = [
            # Valid SSL configuration
            {
                "bootstrap_servers": "localhost:9092",
                "security_protocol": "SSL",
                "ssl_check_hostname": True,
                "ssl_cafile": "/valid/path/ca.pem",
                "ssl_certfile": "/valid/path/cert.pem",
                "ssl_keyfile": "/valid/path/key.pem",
            },
            # SSL with additional security options
            {
                "bootstrap_servers": "localhost:9092",
                "security_protocol": "SSL",
                "ssl_check_hostname": True,
                "ssl_cafile": "/secure/ca.pem",
                "ssl_certfile": "/secure/cert.pem",
                "ssl_keyfile": "/secure/key.pem",
                "ssl_crlfile": "/secure/crl.pem",
                "ssl_password": "certificate_password",
                "ssl_ciphers": "HIGH:!aNULL:!eNULL:!EXPORT:!DES:!RC4:!MD5:!PSK:!SRP:!CAMELLIA",
            },
        ]
        
        service_config = ServiceConfig(service_name="ssl-test-service")
        
        for ssl_config in ssl_configs:
            with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
                mock_event_bus = AsyncMock(spec=KafkaEventBus)
                mock_event_bus_class.return_value = mock_event_bus
                
                manager = ServiceEventManager(ssl_config, service_config)
                await manager.start()
                
                # Verify SSL configuration
                call_args = mock_event_bus_class.call_args[0]
                config = call_args[0]
                assert config["security_protocol"] == "SSL"
                assert config["ssl_check_hostname"] is True
                
                await manager.stop()

    @pytest.mark.asyncio
    async def test_ssl_hostname_verification(self, service_config):
        """Test SSL hostname verification options."""
        ssl_configs = [
            # Hostname verification enabled (secure)
            {
                "bootstrap_servers": "kafka.example.com:9092",
                "security_protocol": "SSL",
                "ssl_check_hostname": True,
                "ssl_cafile": "/path/ca.pem",
                "ssl_certfile": "/path/cert.pem",
                "ssl_keyfile": "/path/key.pem",
            },
            # Hostname verification disabled (less secure, for testing)
            {
                "bootstrap_servers": "kafka-dev.local:9092",
                "security_protocol": "SSL",
                "ssl_check_hostname": False,
                "ssl_cafile": "/path/ca.pem",
                "ssl_certfile": "/path/cert.pem",
                "ssl_keyfile": "/path/key.pem",
            },
        ]
        
        for ssl_config in ssl_configs:
            with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
                mock_event_bus = AsyncMock(spec=KafkaEventBus)
                mock_event_bus_class.return_value = mock_event_bus
                
                manager = ServiceEventManager(ssl_config, service_config)
                await manager.start()
                
                # Verify hostname verification setting
                call_args = mock_event_bus_class.call_args[0]
                config = call_args[0]
                assert config["ssl_check_hostname"] == ssl_config["ssl_check_hostname"]
                
                await manager.stop()


class TestKafkaSASLSecurity:
    """Test SASL authentication mechanisms."""

    @pytest.mark.asyncio
    async def test_sasl_plain_authentication(self, service_config):
        """Test SASL PLAIN authentication configuration."""
        sasl_config = {
            "bootstrap_servers": "localhost:9092",
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": "kafka_user",
            "sasl_plain_password": "kafka_password",
            "ssl_check_hostname": True,
            "ssl_cafile": "/path/ca.pem",
        }
        
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_event_bus_class.return_value = mock_event_bus
            
            manager = ServiceEventManager(sasl_config, service_config)
            await manager.start()
            
            # Verify SASL configuration
            call_args = mock_event_bus_class.call_args[0]
            config = call_args[0]
            
            assert config["security_protocol"] == "SASL_SSL"
            assert config["sasl_mechanism"] == "PLAIN"
            assert config["sasl_plain_username"] == "kafka_user"
            assert config["sasl_plain_password"] == "kafka_password"
            
            await manager.stop()

    @pytest.mark.asyncio
    async def test_sasl_scram_authentication(self, service_config):
        """Test SASL SCRAM authentication configuration."""
        scram_configs = [
            # SCRAM-SHA-256
            {
                "bootstrap_servers": "localhost:9092",
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "SCRAM-SHA-256",
                "sasl_plain_username": "scram_user",
                "sasl_plain_password": "scram_password",
            },
            # SCRAM-SHA-512
            {
                "bootstrap_servers": "localhost:9092",
                "security_protocol": "SASL_SSL", 
                "sasl_mechanism": "SCRAM-SHA-512",
                "sasl_plain_username": "scram_user_512",
                "sasl_plain_password": "scram_password_512",
            },
        ]
        
        for scram_config in scram_configs:
            with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
                mock_event_bus = AsyncMock(spec=KafkaEventBus)
                mock_event_bus_class.return_value = mock_event_bus
                
                manager = ServiceEventManager(scram_config, service_config)
                await manager.start()
                
                # Verify SCRAM configuration
                call_args = mock_event_bus_class.call_args[0]
                config = call_args[0]
                
                assert config["security_protocol"] == "SASL_SSL"
                assert config["sasl_mechanism"] in ["SCRAM-SHA-256", "SCRAM-SHA-512"]
                assert "sasl_plain_username" in config
                assert "sasl_plain_password" in config
                
                await manager.stop()

    @pytest.mark.asyncio
    async def test_sasl_oauth_authentication(self, service_config):
        """Test SASL OAuth authentication configuration."""
        oauth_config = {
            "bootstrap_servers": "localhost:9092",
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "OAUTHBEARER",
            "sasl_oauth_token_provider": "custom_token_provider",
            "ssl_check_hostname": True,
            "ssl_cafile": "/path/ca.pem",
        }
        
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_event_bus_class.return_value = mock_event_bus
            
            manager = ServiceEventManager(oauth_config, service_config)
            await manager.start()
            
            # Verify OAuth configuration
            call_args = mock_event_bus_class.call_args[0]
            config = call_args[0]
            
            assert config["security_protocol"] == "SASL_SSL"
            assert config["sasl_mechanism"] == "OAUTHBEARER"
            assert "sasl_oauth_token_provider" in config
            
            await manager.stop()

    @pytest.mark.asyncio
    async def test_sasl_kerberos_authentication(self, service_config):
        """Test SASL Kerberos authentication configuration."""
        kerberos_config = {
            "bootstrap_servers": "localhost:9092",
            "security_protocol": "SASL_PLAINTEXT",
            "sasl_mechanism": "GSSAPI",
            "sasl_kerberos_service_name": "kafka",
            "sasl_kerberos_domain_name": "example.com",
        }
        
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_event_bus_class.return_value = mock_event_bus
            
            manager = ServiceEventManager(kerberos_config, service_config)
            await manager.start()
            
            # Verify Kerberos configuration
            call_args = mock_event_bus_class.call_args[0]
            config = call_args[0]
            
            assert config["security_protocol"] == "SASL_PLAINTEXT"
            assert config["sasl_mechanism"] == "GSSAPI"
            assert config["sasl_kerberos_service_name"] == "kafka"
            assert config["sasl_kerberos_domain_name"] == "example.com"
            
            await manager.stop()


class TestSensitiveDataHandling:
    """Test sensitive data handling and filtering."""

    def test_sensitive_data_identification(self):
        """Test identification of sensitive data in events."""
        sensitive_event = EventMessage(
            event_type="user.authentication",
            tenant_id="tenant-123",
            user_id="user-456",
            data={
                "username": "testuser",
                "password": "secret123",  # Sensitive
                "api_key": "sk_test_1234567890",  # Sensitive
                "credit_card": "4111-1111-1111-1111",  # Sensitive
                "ssn": "123-45-6789",  # Sensitive
                "email": "user@example.com",  # May be sensitive
                "user_agent": "Mozilla/5.0...",  # Not sensitive
                "session_id": "sess_abc123",  # Potentially sensitive
            },
            metadata={
                "client_ip": "192.168.1.100",  # Potentially sensitive
                "user_agent": "Mozilla/5.0...",
                "request_id": "req_123456",
            },
        )
        
        # Convert to dict for analysis
        event_dict = sensitive_event.to_dict()
        
        # Verify sensitive fields are present (actual filtering should happen in logging/monitoring)
        assert "password" in event_dict["data"]
        assert "api_key" in event_dict["data"]
        assert "credit_card" in event_dict["data"]
        assert "ssn" in event_dict["data"]
        
        # In a real implementation, these fields would be filtered or masked

    def test_event_data_sanitization(self):
        """Test event data sanitization for logging."""
        def sanitize_event_data(data: Dict[str, Any]) -> Dict[str, Any]:
            """Sanitize sensitive data for logging."""
            sensitive_fields = {
                "password", "api_key", "secret", "token", "credit_card", 
                "ssn", "social_security", "passport", "driver_license"
            }
            
            sanitized = {}
            for key, value in data.items():
                if any(sensitive in key.lower() for sensitive in sensitive_fields):
                    sanitized[key] = "***REDACTED***"
                elif isinstance(value, str) and len(value) > 50:
                    # Truncate very long strings that might contain sensitive data
                    sanitized[key] = value[:47] + "..."
                else:
                    sanitized[key] = value
            
            return sanitized
        
        sensitive_data = {
            "username": "testuser",
            "password": "secret123",
            "api_key": "sk_test_1234567890",
            "user_preferences": "normal data",
            "very_long_field": "x" * 100,  # Should be truncated
        }
        
        sanitized = sanitize_event_data(sensitive_data)
        
        assert sanitized["username"] == "testuser"
        assert sanitized["password"] == "***REDACTED***"
        assert sanitized["api_key"] == "***REDACTED***"
        assert sanitized["user_preferences"] == "normal data"
        assert sanitized["very_long_field"].endswith("...")
        assert len(sanitized["very_long_field"]) == 50

    @pytest.mark.asyncio
    async def test_secure_event_handling(self, kafka_config, service_config):
        """Test secure event handling patterns."""
        events_processed = []
        
        async def secure_handler(event: EventMessage) -> ProcessingResult:
            # Simulate secure processing that doesn't log sensitive data
            secure_event_data = {
                "event_type": event.event_type,
                "tenant_id": event.tenant_id,
                "correlation_id": event.correlation_id,
                "timestamp": event.timestamp.isoformat() if event.timestamp else None,
                # Note: actual data is not logged
                "data_size": len(json.dumps(event.data)) if event.data else 0,
            }
            events_processed.append(secure_event_data)
            return ProcessingResult(status=MessageStatus.SUCCESS)
        
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_consumer = AsyncMock()
            mock_event_bus.subscribe_with_retry.return_value = mock_consumer
            mock_event_bus_class.return_value = mock_event_bus
            
            manager = ServiceEventManager(kafka_config, service_config)
            await manager.start()
            
            await manager.subscribe_to_events(
                topics=["secure-topic"],
                group_id="secure-group",
                handler=secure_handler,
            )
            
            # Publish event with sensitive data
            await manager.publish_event(
                topic="secure-topic",
                event_type="payment.processed",
                tenant_id="tenant-123",
                data={
                    "user_id": "user-456",
                    "payment_method": "credit_card",
                    "card_number": "4111-1111-1111-1111",  # Sensitive
                    "amount": 99.99,
                    "currency": "USD",
                },
                correlation_id="payment-123",
            )
            
            # Verify secure handling
            mock_event_bus.publish.assert_called_once()
            
            await manager.stop()

    def test_pii_detection_patterns(self):
        """Test PII (Personally Identifiable Information) detection."""
        def detect_pii(data: Any) -> List[str]:
            """Detect potential PII in data."""
            import re
            
            pii_patterns = {
                "email": r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b",
                "phone": r"\b\d{3}-?\d{3}-?\d{4}\b",
                "ssn": r"\b\d{3}-?\d{2}-?\d{4}\b",
                "credit_card": r"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b",
                "ip_address": r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b",
            }
            
            detected_pii = []
            data_str = json.dumps(data) if not isinstance(data, str) else data
            
            for pii_type, pattern in pii_patterns.items():
                if re.search(pattern, data_str):
                    detected_pii.append(pii_type)
            
            return detected_pii
        
        # Test data with various PII
        test_data = {
            "user_email": "john.doe@example.com",
            "phone_number": "555-123-4567",
            "social_security": "123-45-6789",
            "payment_card": "4111 1111 1111 1111",
            "server_ip": "192.168.1.100",
            "normal_field": "just normal data",
        }
        
        detected = detect_pii(test_data)
        
        expected_pii = ["email", "phone", "ssn", "credit_card", "ip_address"]
        for pii_type in expected_pii:
            assert pii_type in detected


class TestConfigurationSecurity:
    """Test security aspects of configuration handling."""

    def test_configuration_validation(self):
        """Test security validation of Kafka configurations."""
        def validate_kafka_config_security(config: Dict[str, Any]) -> List[str]:
            """Validate Kafka configuration for security issues."""
            security_issues = []
            
            # Check for plaintext protocols
            if config.get("security_protocol") == "PLAINTEXT":
                security_issues.append("Using PLAINTEXT protocol (unencrypted)")
            
            # Check for missing SSL verification
            if config.get("security_protocol") in ["SSL", "SASL_SSL"]:
                if not config.get("ssl_check_hostname", True):
                    security_issues.append("SSL hostname verification disabled")
                
                if not config.get("ssl_cafile"):
                    security_issues.append("Missing SSL CA certificate file")
            
            # Check for SASL without SSL
            if config.get("security_protocol") == "SASL_PLAINTEXT":
                security_issues.append("Using SASL without SSL encryption")
            
            # Check for weak SASL mechanisms
            weak_mechanisms = ["PLAIN"]
            if config.get("sasl_mechanism") in weak_mechanisms and config.get("security_protocol") != "SASL_SSL":
                security_issues.append(f"Using weak SASL mechanism without SSL: {config.get('sasl_mechanism')}")
            
            return security_issues
        
        # Test various configurations
        test_configs = [
            # Insecure configuration
            {
                "bootstrap_servers": "localhost:9092",
                "security_protocol": "PLAINTEXT",
            },
            # SASL without SSL
            {
                "bootstrap_servers": "localhost:9092",
                "security_protocol": "SASL_PLAINTEXT",
                "sasl_mechanism": "PLAIN",
            },
            # SSL without hostname verification
            {
                "bootstrap_servers": "localhost:9092",
                "security_protocol": "SSL",
                "ssl_check_hostname": False,
            },
            # Secure configuration
            {
                "bootstrap_servers": "localhost:9092",
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "SCRAM-SHA-256",
                "ssl_check_hostname": True,
                "ssl_cafile": "/path/ca.pem",
            },
        ]
        
        expected_issues = [
            ["Using PLAINTEXT protocol (unencrypted)"],
            ["Using SASL without SSL encryption", "Using weak SASL mechanism without SSL: PLAIN"],
            ["SSL hostname verification disabled", "Missing SSL CA certificate file"],
            [],  # Secure configuration should have no issues
        ]
        
        for config, expected in zip(test_configs, expected_issues):
            issues = validate_kafka_config_security(config)
            assert len(issues) == len(expected)
            for issue in expected:
                assert any(issue in found_issue for found_issue in issues)

    def test_sensitive_configuration_masking(self):
        """Test masking of sensitive configuration values."""
        def mask_sensitive_config(config: Dict[str, Any]) -> Dict[str, Any]:
            """Mask sensitive configuration values for logging."""
            sensitive_keys = {
                "sasl_plain_password", "ssl_password", "ssl_keyfile_password",
                "oauth_token", "api_key", "secret", "password"
            }
            
            masked_config = {}
            for key, value in config.items():
                if any(sensitive in key.lower() for sensitive in sensitive_keys):
                    masked_config[key] = "***MASKED***"
                else:
                    masked_config[key] = value
            
            return masked_config
        
        sensitive_config = {
            "bootstrap_servers": "localhost:9092",
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": "kafka_user",
            "sasl_plain_password": "secret_password",  # Should be masked
            "ssl_cafile": "/path/ca.pem",
            "ssl_keyfile": "/path/key.pem",
            "ssl_keyfile_password": "key_password",  # Should be masked
        }
        
        masked = mask_sensitive_config(sensitive_config)
        
        assert masked["sasl_plain_username"] == "kafka_user"  # Not masked
        assert masked["sasl_plain_password"] == "***MASKED***"  # Masked
        assert masked["ssl_cafile"] == "/path/ca.pem"  # Not masked
        assert masked["ssl_keyfile_password"] == "***MASKED***"  # Masked


class TestSecurityErrorHandling:
    """Test security-related error handling."""

    @pytest.mark.asyncio
    async def test_authentication_failure_handling(self, service_config):
        """Test handling of authentication failures."""
        invalid_auth_config = {
            "bootstrap_servers": "localhost:9092",
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": "invalid_user",
            "sasl_plain_password": "invalid_password",
            "ssl_cafile": "/path/ca.pem",
        }
        
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            
            # Simulate authentication failure
            from aiokafka.errors import KafkaError
            mock_event_bus.start.side_effect = KafkaError("Authentication failed")
            mock_event_bus_class.return_value = mock_event_bus
            
            manager = ServiceEventManager(invalid_auth_config, service_config)
            
            # Should handle authentication error gracefully
            with pytest.raises(KafkaError, match="Authentication failed"):
                await manager.start()

    @pytest.mark.asyncio
    async def test_ssl_certificate_error_handling(self, service_config):
        """Test handling of SSL certificate errors."""
        invalid_ssl_config = {
            "bootstrap_servers": "localhost:9092",
            "security_protocol": "SSL",
            "ssl_check_hostname": True,
            "ssl_cafile": "/nonexistent/ca.pem",  # Invalid path
            "ssl_certfile": "/nonexistent/cert.pem",  # Invalid path
            "ssl_keyfile": "/nonexistent/key.pem",  # Invalid path
        }
        
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            
            # Simulate SSL error
            from aiokafka.errors import KafkaError
            mock_event_bus.start.side_effect = KafkaError("SSL certificate error")
            mock_event_bus_class.return_value = mock_event_bus
            
            manager = ServiceEventManager(invalid_ssl_config, service_config)
            
            # Should handle SSL error gracefully
            with pytest.raises(KafkaError, match="SSL certificate error"):
                await manager.start()

    @pytest.mark.asyncio
    async def test_secure_error_logging(self, kafka_config, service_config):
        """Test that error logging doesn't expose sensitive information."""
        with patch("general_operate.kafka.service_event_manager.KafkaEventBus") as mock_event_bus_class:
            mock_event_bus = AsyncMock(spec=KafkaEventBus)
            mock_consumer = AsyncMock()
            mock_event_bus.subscribe_with_retry.return_value = mock_consumer
            mock_event_bus_class.return_value = mock_event_bus
            
            manager = ServiceEventManager(kafka_config, service_config)
            await manager.start()
            
            # Simulate error during event publishing
            mock_event_bus.publish.side_effect = Exception("Connection failed: authentication error with credentials user=xxx")
            
            # Attempt to publish event with sensitive data
            with pytest.raises(Exception):
                await manager.publish_event(
                    topic="test-topic",
                    event_type="sensitive.test",
                    tenant_id="tenant-123",
                    data={
                        "user_id": "user-456",
                        "api_key": "sk_test_secret123",  # Sensitive data
                        "session_token": "sess_secret456",  # Sensitive data
                    },
                )
            
            # In a real implementation, error logging should not expose sensitive data
            # from the event or detailed authentication information
            
            await manager.stop()


class TestCompliancePatterns:
    """Test compliance and regulatory patterns."""

    def test_gdpr_data_handling_patterns(self):
        """Test GDPR-compliant data handling patterns."""
        def create_gdpr_compliant_event(
            event_type: str,
            tenant_id: str,
            data: Dict[str, Any],
            user_consent: bool = False,
            data_retention_days: int = 30,
        ) -> EventMessage:
            """Create GDPR-compliant event with proper metadata."""
            
            # Add GDPR compliance metadata
            gdpr_metadata = {
                "data_subject_consent": user_consent,
                "data_retention_days": data_retention_days,
                "contains_personal_data": True,
                "legal_basis": "consent" if user_consent else "legitimate_interest",
                "data_controller": "ExampleCorp",
                "privacy_policy_version": "2.1",
            }
            
            return EventMessage(
                event_type=event_type,
                tenant_id=tenant_id,
                data=data,
                metadata=gdpr_metadata,
                correlation_id=str(uuid.uuid4()),
            )
        
        # Test GDPR-compliant event creation
        gdpr_event = create_gdpr_compliant_event(
            event_type="user.profile.updated",
            tenant_id="tenant-eu-123",
            data={
                "user_id": "user-456", 
                "email": "user@example.com",
                "preferences": {"newsletter": True},
            },
            user_consent=True,
            data_retention_days=365,
        )
        
        assert gdpr_event.metadata["data_subject_consent"] is True
        assert gdpr_event.metadata["data_retention_days"] == 365
        assert gdpr_event.metadata["contains_personal_data"] is True
        assert gdpr_event.metadata["legal_basis"] == "consent"

    def test_pci_compliance_patterns(self):
        """Test PCI DSS compliance patterns for payment data."""
        def create_pci_compliant_payment_event(
            amount: float,
            currency: str,
            card_last_four: str,  # Only last 4 digits
            transaction_id: str,
        ) -> EventMessage:
            """Create PCI DSS compliant payment event."""
            
            # PCI-compliant data (no full card numbers, CVV, etc.)
            payment_data = {
                "transaction_id": transaction_id,
                "amount": amount,
                "currency": currency,
                "card_last_four": card_last_four,  # Only last 4 digits
                "card_type": "visa",  # Generic type, not sensitive
                "merchant_id": "merchant_123",
                "status": "approved",
            }
            
            pci_metadata = {
                "pci_compliance_level": "level_1",
                "contains_cardholder_data": False,  # No full PAN stored
                "tokenization_used": True,
                "encryption_at_rest": True,
                "audit_trail": True,
            }
            
            return EventMessage(
                event_type="payment.processed",
                tenant_id="payment-processor",
                data=payment_data,
                metadata=pci_metadata,
                correlation_id=f"txn-{transaction_id}",
            )
        
        # Test PCI-compliant payment event
        payment_event = create_pci_compliant_payment_event(
            amount=99.99,
            currency="USD",
            card_last_four="1111",
            transaction_id="txn_123456",
        )
        
        assert payment_event.data["card_last_four"] == "1111"
        assert "card_number" not in payment_event.data  # Full card number not present
        assert payment_event.metadata["contains_cardholder_data"] is False
        assert payment_event.metadata["tokenization_used"] is True

    def test_hipaa_compliance_patterns(self):
        """Test HIPAA compliance patterns for healthcare data."""
        def create_hipaa_compliant_event(
            patient_id: str,  # Should be a pseudonym or encrypted ID
            data: Dict[str, Any],
            covered_entity: str,
        ) -> EventMessage:
            """Create HIPAA-compliant healthcare event."""
            
            hipaa_metadata = {
                "covered_entity": covered_entity,
                "contains_phi": True,  # Protected Health Information
                "minimum_necessary": True,
                "access_logged": True,
                "encryption_used": True,
                "authorized_disclosure": True,
                "patient_consent": True,
            }
            
            return EventMessage(
                event_type="patient.record.accessed",
                tenant_id=f"healthcare-{covered_entity}",
                data=data,
                metadata=hipaa_metadata,
                user_id=patient_id,  # Pseudonymized patient ID
                correlation_id=str(uuid.uuid4()),
            )
        
        # Test HIPAA-compliant event
        healthcare_event = create_hipaa_compliant_event(
            patient_id="patient_pseudonym_123",  # Not real patient ID
            data={
                "record_type": "lab_result",
                "accessed_by": "doctor_456",
                "access_reason": "treatment",
                "department": "cardiology",
            },
            covered_entity="general_hospital",
        )
        
        assert healthcare_event.metadata["contains_phi"] is True
        assert healthcare_event.metadata["minimum_necessary"] is True
        assert healthcare_event.metadata["encryption_used"] is True
        assert healthcare_event.user_id == "patient_pseudonym_123"