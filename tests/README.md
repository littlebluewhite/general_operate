# Kafka Module Test Suite

This directory contains comprehensive tests for the Kafka module in the `general_operate` project.

## Test Structure

### Test Files

- **`conftest.py`** - Shared fixtures, mocks, and test utilities
- **`test_kafka_client.py`** - Unit tests for core Kafka client components
- **`test_dlq_handler.py`** - Unit tests for Dead Letter Queue handling
- **`test_manager_config.py`** - Unit tests for configuration management
- **`test_service_event_manager.py`** - Unit tests for service event management
- **`test_integration_kafka.py`** - Integration tests for end-to-end workflows
- **`test_performance_kafka.py`** - Performance and load testing
- **`test_security_kafka.py`** - Security and authentication testing
- **`run_kafka_tests.py`** - Test runner script with various options

### Test Categories

#### Unit Tests
- **EventMessage** creation, validation, and serialization
- **RetryConfig** functionality and validation
- **CircuitBreaker** state management and behavior
- **KafkaAsyncProducer/Consumer** operations and error handling
- **KafkaEventBus** integration and lifecycle
- **DLQHandler** event processing and recovery
- **Configuration** validation and edge cases

#### Integration Tests
- End-to-end message flow from producer to consumer
- Event bus integration with DLQ handling
- Multi-service coordination scenarios
- Error propagation and recovery patterns
- Resource lifecycle management

#### Performance Tests
- Throughput benchmarks for event publishing
- Consumer processing performance
- Memory usage and resource efficiency
- Concurrent operation scalability
- Large message handling

#### Security Tests
- SSL/TLS configuration validation
- SASL authentication mechanisms
- Sensitive data handling and filtering
- Configuration security validation
- Compliance patterns (GDPR, PCI, HIPAA)

## Running Tests

### Prerequisites

Ensure you have the following installed:
- Python 3.13+
- `uv` package manager
- Project dependencies installed via `uv sync`

### Quick Start

```bash
# Run all tests
uv run python tests/run_kafka_tests.py

# Run specific categories
uv run python tests/run_kafka_tests.py --category unit
uv run python tests/run_kafka_tests.py --category integration
uv run python tests/run_kafka_tests.py --category performance
uv run python tests/run_kafka_tests.py --category security

# Run with coverage
uv run python tests/run_kafka_tests.py --coverage

# Run specific test file
uv run python tests/run_kafka_tests.py --file test_kafka_client.py
```

### Direct pytest Usage

```bash
# Run all Kafka tests
uv run python -m pytest tests/test_*kafka*.py -v

# Run with coverage
uv run python -m pytest tests/test_*kafka*.py --cov=general_operate.kafka --cov-report=html

# Run specific test class
uv run python -m pytest tests/test_kafka_client.py::TestEventMessage -v

# Run with specific markers
uv run python -m pytest tests/ -m "asyncio" -v
```

### Test Runner Options

The `run_kafka_tests.py` script provides several convenient options:

```bash
# List available tests
uv run python tests/run_kafka_tests.py --list

# Check test environment
uv run python tests/run_kafka_tests.py --check-env

# Generate comprehensive report
uv run python tests/run_kafka_tests.py --report

# Run with specific markers
uv run python tests/run_kafka_tests.py --markers "asyncio" "integration"

# Run tests in parallel
uv run python tests/run_kafka_tests.py --parallel

# Verbose output
uv run python tests/run_kafka_tests.py --verbose
```

## Test Configuration

### Pytest Configuration

The project's `pyproject.toml` includes pytest configuration:

```toml
[tool.pytest.ini_options]
minversion = "8.0"
addopts = "-ra -q --strict-markers --strict-config"
testpaths = ["tests"]
markers = [
    "asyncio: marks tests as async",
    "integration: marks tests as integration tests"
]
```

### Test Markers

- `@pytest.mark.asyncio` - Async tests requiring event loop
- `@pytest.mark.integration` - Integration tests that test multiple components
- Custom markers can be added for specific test categorization

### Fixtures and Mocks

The test suite uses extensive mocking to avoid dependencies on real Kafka infrastructure:

- **Mock Kafka Clients** - `AIOKafkaProducer` and `AIOKafkaConsumer` are mocked
- **Event Fixtures** - Sample events, configurations, and handlers
- **Performance Timers** - Utilities for measuring test execution time
- **Error Simulators** - Tools for testing error scenarios

## Test Coverage

### Coverage Goals

- **Unit Tests**: >95% line coverage for individual components
- **Integration Tests**: Complete workflow coverage
- **Error Paths**: All error handling paths tested
- **Edge Cases**: Boundary conditions and invalid inputs

### Coverage Reporting

```bash
# Generate HTML coverage report
uv run python tests/run_kafka_tests.py --coverage

# View coverage report
open htmlcov/index.html
```

### Coverage Exclusions

The following are excluded from coverage:
- Debug-only code paths
- Abstract methods and protocols
- Type checking blocks (`if TYPE_CHECKING:`)

## Writing New Tests

### Test Naming Conventions

- Test files: `test_<module_name>.py`
- Test classes: `Test<ComponentName>`
- Test methods: `test_<functionality>_<scenario>`

### Example Test Structure

```python
import pytest
from unittest.mock import AsyncMock, patch

from general_operate.kafka.kafka_client import EventMessage


class TestEventMessage:
    """Test EventMessage functionality."""
    
    def test_event_message_creation(self):
        """Test basic EventMessage creation."""
        event = EventMessage(
            event_type="test.event",
            tenant_id="tenant-123",
            data={"key": "value"},
        )
        
        assert event.event_type == "test.event"
        assert event.tenant_id == "tenant-123"
        assert event.data == {"key": "value"}
    
    @pytest.mark.asyncio
    async def test_async_functionality(self, sample_event):
        """Test async functionality with fixtures."""
        # Test implementation
        pass
```

### Best Practices

1. **Use Fixtures** - Leverage shared fixtures from `conftest.py`
2. **Mock External Dependencies** - Don't rely on real Kafka infrastructure
3. **Test Error Paths** - Include negative test cases
4. **Descriptive Names** - Test names should describe what is being tested
5. **Arrange-Act-Assert** - Follow the AAA pattern
6. **Async Tests** - Use `@pytest.mark.asyncio` for async tests
7. **Parameterized Tests** - Use `@pytest.mark.parametrize` for multiple scenarios

### Adding Integration Tests

Integration tests should:
- Test complete workflows end-to-end
- Use realistic data and scenarios
- Verify cross-component interactions
- Include error recovery testing

### Adding Performance Tests

Performance tests should:
- Measure specific metrics (throughput, latency, memory)
- Use realistic load patterns
- Include assertions on performance characteristics
- Test resource cleanup

## Troubleshooting

### Common Issues

1. **Import Errors**
   ```bash
   # Ensure dependencies are installed
   uv sync
   ```

2. **Async Test Issues**
   ```bash
   # Ensure pytest-asyncio is installed
   uv add --dev pytest-asyncio
   ```

3. **Coverage Issues**
   ```bash
   # Ensure pytest-cov is installed
   uv add --dev pytest-cov
   ```

4. **Mock Issues**
   - Verify mock patches are applied correctly
   - Check that mocks match the expected interface
   - Ensure async mocks are used for async code

### Debug Tips

1. **Verbose Output**: Use `-v` flag for detailed test output
2. **Print Debugging**: Add print statements in tests (remember to remove)
3. **Breakpoints**: Use `pytest --pdb` to drop into debugger on failures
4. **Isolated Tests**: Run single tests to isolate issues

## CI/CD Integration

### GitHub Actions

Example workflow for running tests in CI:

```yaml
name: Test Kafka Module

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    
    - name: Install uv
      uses: astral-sh/setup-uv@v1
      
    - name: Set up Python
      run: uv python install 3.13
      
    - name: Install dependencies
      run: uv sync
      
    - name: Run tests
      run: uv run python tests/run_kafka_tests.py --coverage
      
    - name: Upload coverage
      uses: codecov/codecov-action@v3
      with:
        file: ./coverage.xml
```

### Pre-commit Hooks

Add Kafka tests to pre-commit hooks:

```yaml
# .pre-commit-config.yaml
repos:
  - repo: local
    hooks:
      - id: kafka-tests
        name: Run Kafka tests
        entry: uv run python tests/run_kafka_tests.py --category unit
        language: system
        pass_filenames: false
```

## Contributing

When contributing to the Kafka test suite:

1. **Write Tests First** - Follow TDD principles
2. **Maintain Coverage** - Ensure new code has test coverage
3. **Update Documentation** - Update this README for new test categories
4. **Review Test Output** - Ensure tests provide meaningful feedback
5. **Consider Performance** - Keep test execution time reasonable

## Resources

- [pytest Documentation](https://docs.pytest.org/)
- [pytest-asyncio Documentation](https://pytest-asyncio.readthedocs.io/)
- [aiokafka Documentation](https://aiokafka.readthedocs.io/)
- [Python unittest.mock Documentation](https://docs.python.org/3/library/unittest.mock.html)