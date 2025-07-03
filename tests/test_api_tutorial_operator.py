"""
Test suite for APITutorialOperator refactored implementation.
"""
import os
import sys

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from unittest.mock import AsyncMock, MagicMock

import pytest

from tutorial.routers.operator import api_schemas
from tutorial.routers.operator.api_schemas import (
    APISubtableCreate,
    APITutorialCreate,
    APITutorialUpdate,
)
from tutorial.routers.operator.API_tutorial import APITutorialOperator


# Fixtures
@pytest.fixture
def mock_api_schema():
    """Mock api_schema with the required attributes"""
    mock_schema = MagicMock()
    mock_schema.create_schemas = api_schemas.create_schemas
    mock_schema.main_schemas = api_schemas.main_schemas
    mock_schema.update_schemas = api_schemas.update_schemas
    return mock_schema


@pytest.fixture
async def mock_tutorial_operate():
    """Mock GeneralOperate instance for tutorial operations."""
    tutorial_op = AsyncMock()
    tutorial_op.table_name = "tutorial"
    # High-level methods (used after refactoring)
    tutorial_op.create_data = AsyncMock()
    tutorial_op.read_data = AsyncMock()
    tutorial_op.read_data_by_id = AsyncMock()
    tutorial_op.read_data_by_filter = AsyncMock()
    tutorial_op.update_data = AsyncMock()
    tutorial_op.delete_data = AsyncMock()
    # Transaction support - return synchronously, not as coroutine
    tutorial_op.create_external_session = MagicMock()
    # Keep SQL methods for fallback scenarios
    tutorial_op.read_sql = AsyncMock()
    return tutorial_op


@pytest.fixture
async def mock_subtable_operate():
    """Mock GeneralOperate instance for subtable operations."""
    subtable_op = AsyncMock()
    subtable_op.table_name = "subtable"
    # High-level methods (used after refactoring)
    subtable_op.create_data = AsyncMock()
    subtable_op.read_data = AsyncMock()
    subtable_op.read_data_by_id = AsyncMock()
    subtable_op.read_data_by_filter = AsyncMock()
    subtable_op.update_data = AsyncMock()
    subtable_op.delete_data = AsyncMock()
    subtable_op.delete_filter = AsyncMock()
    subtable_op.delete_filter_data = AsyncMock()
    subtable_op.create_by_foreign_key = AsyncMock()
    subtable_op.update_by_foreign_key = AsyncMock()
    # Keep SQL methods for fallback scenarios
    subtable_op.read_sql = AsyncMock()
    return subtable_op


@pytest.fixture
async def api_tutorial_operator(mock_api_schema, mock_db, mock_redis, mock_influxdb):
    """Create APITutorialOperator instance with mocked dependencies."""
    operator = APITutorialOperator(mock_api_schema, mock_db, mock_redis, mock_influxdb)
    return operator


@pytest.fixture
async def mock_session():
    """Mock database session for transaction testing."""
    from unittest.mock import AsyncMock, MagicMock
    
    # Create a mock async context manager for session.begin()
    class MockTransaction:
        async def __aenter__(self):
            return self
        
        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return None
    
    # Create a mock session with proper async context manager support
    session = AsyncMock()
    session.begin = MagicMock(return_value=MockTransaction())
    session.close = AsyncMock()
    return session


@pytest.fixture
async def api_tutorial_operator_with_mocks(
    api_tutorial_operator, mock_tutorial_operate, mock_subtable_operate, mock_session
):
    """APITutorialOperator with mocked GeneralOperate instances."""
    api_tutorial_operator.tutorial_operate = mock_tutorial_operate
    api_tutorial_operator.subtable_operate = mock_subtable_operate
    
    # Mock the transaction context manager
    mock_tutorial_operate.create_external_session.return_value = mock_session
    
    return api_tutorial_operator


@pytest.fixture
def sample_tutorial_create_data():
    """Sample tutorial data for testing."""
    return {
        "name": "Python Basics",
        "title": "Introduction to Python Programming",
        "tags": ["python", "programming", "beginner"],
        "enable": True,
        "subtables": [
            {
                "description": "Variables and Data Types",
                "author": "John Doe"
            },
            {
                "description": "Control Flow",
                "author": "Jane Smith"
            }
        ]
    }


class TestAPITutorialOperator:
    """Test suite for APITutorialOperator CRUD operations."""

    @pytest.mark.asyncio
    async def test_create_single_tutorial_with_subtables(
        self, api_tutorial_operator_with_mocks, sample_tutorial_create_data
    ):
        """Test creating a single tutorial with subtables."""
        # Setup mocks
        created_tutorial = {"id": 1, "name": "Python Basics", "title": "Introduction to Python Programming", "tags": ["python"], "enable": True}
        created_subtables = [
            {"id": 1, "tutorial_id": 1, "description": "Variables and Data Types", "author": "John Doe"},
            {"id": 2, "tutorial_id": 1, "description": "Control Flow", "author": "Jane Smith"}
        ]

        # Mock create_data to return Pydantic models (simulated with mock objects)
        from unittest.mock import MagicMock
        created_tutorial_model = MagicMock()
        created_tutorial_model.id = 1
        created_tutorial_model.model_dump.return_value = created_tutorial
        
        created_subtable_models = []
        for sub in created_subtables:
            sub_model = MagicMock()
            sub_model.model_dump.return_value = sub
            created_subtable_models.append(sub_model)
        
        api_tutorial_operator_with_mocks.tutorial_operate.create_data.return_value = [created_tutorial_model]
        api_tutorial_operator_with_mocks.subtable_operate.create_by_foreign_key.return_value = created_subtable_models

        # Create input data
        tutorial_create = APITutorialCreate(**sample_tutorial_create_data)

        # Execute
        result = await api_tutorial_operator_with_mocks.create([tutorial_create])

        # Verify
        assert len(result) == 1
        assert result[0].id == 1
        assert result[0].name == "Python Basics"
        assert len(result[0].subtables) == 2
        assert result[0].subtables[0].description == "Variables and Data Types"

    @pytest.mark.asyncio
    async def test_create_multiple_tutorials(self, api_tutorial_operator_with_mocks):
        """Test batch creation of multiple tutorials."""
        # Setup data
        tutorial_data_1 = APITutorialCreate(
            name="Python Basics",
            title="Introduction to Python",
            tags=["python"],
            subtables=[APISubtableCreate(description="Variables", author="Author1")]
        )
        tutorial_data_2 = APITutorialCreate(
            name="JavaScript Basics",
            title="Introduction to JavaScript",
            tags=["javascript"],
            subtables=[]
        )

        # Setup mocks
        created_tutorials = [
            {"id": 1, "name": "Python Basics", "title": "Introduction to Python", "tags": ["python"], "enable": True},
            {"id": 2, "name": "JavaScript Basics", "title": "Introduction to JavaScript", "tags": ["javascript"], "enable": True}
        ]
        created_subtables = [{"id": 1, "tutorial_id": 1, "description": "Variables", "author": "Author1"}]

        # Mock create_data to return Pydantic models
        created_tutorial_models = []
        for tut in created_tutorials:
            tut_model = MagicMock()
            tut_model.id = tut["id"]
            tut_model.model_dump.return_value = tut
            created_tutorial_models.append(tut_model)
        
        created_subtable_models = []
        for sub in created_subtables:
            sub_model = MagicMock()
            sub_model.model_dump.return_value = sub
            created_subtable_models.append(sub_model)
        
        api_tutorial_operator_with_mocks.tutorial_operate.create_data.return_value = created_tutorial_models
        api_tutorial_operator_with_mocks.subtable_operate.create_by_foreign_key.return_value = created_subtable_models

        # Execute
        result = await api_tutorial_operator_with_mocks.create([tutorial_data_1, tutorial_data_2])

        # Verify
        assert len(result) == 2
        assert result[0].name == "Python Basics"
        assert len(result[0].subtables) == 1
        assert result[1].name == "JavaScript Basics"
        assert len(result[1].subtables) == 0

    @pytest.mark.asyncio
    async def test_read_tutorials_with_subtables(self, api_tutorial_operator_with_mocks):
        """Test reading tutorials with their associated subtables."""
        # Setup mocks
        tutorials = [
            {"id": 1, "name": "Python Basics", "title": "Introduction to Python", "tags": ["python"], "enable": True},
            {"id": 2, "name": "JavaScript Basics", "title": "Introduction to JavaScript", "tags": ["javascript"], "enable": True}
        ]
        subtables = [
            {"id": 1, "tutorial_id": 1, "description": "Variables", "author": "Author1"},
            {"id": 2, "tutorial_id": 1, "description": "Functions", "author": "Author2"},
            {"id": 3, "tutorial_id": 2, "description": "DOM", "author": "Author3"}
        ]

        # For read without specific IDs, it will use read_data_by_filter
        tutorial_models = []
        for tut in tutorials:
            tut_model = MagicMock()
            tut_model.model_dump.return_value = tut
            tutorial_models.append(tut_model)
        
        subtable_models = []
        for sub in subtables:
            sub_model = MagicMock()
            sub_model.model_dump.return_value = sub
            subtable_models.append(sub_model)
        
        api_tutorial_operator_with_mocks.tutorial_operate.read_data_by_filter.return_value = tutorial_models
        api_tutorial_operator_with_mocks.subtable_operate.read_data_by_filter.return_value = subtable_models

        # Execute
        result = await api_tutorial_operator_with_mocks.read()

        # Verify
        assert len(result) == 2
        assert result[0].id == 1
        assert len(result[0].subtables) == 2
        assert result[1].id == 2
        assert len(result[1].subtables) == 1

    @pytest.mark.asyncio
    async def test_read_with_filters(self, api_tutorial_operator_with_mocks):
        """Test reading tutorials with filters."""
        # Setup mocks
        tutorials = [{"id": 1, "name": "Python Basics", "title": "Introduction to Python", "tags": ["python"], "enable": True}]
        subtables = [{"id": 1, "tutorial_id": 1, "description": "Variables", "author": "Author1"}]

        # Mock read_data_by_id for ID-based reads
        tutorial_model = MagicMock()
        tutorial_model.model_dump.return_value = tutorials[0]
        api_tutorial_operator_with_mocks.tutorial_operate.read_data_by_id.return_value = [tutorial_model]
        
        subtable_models = []
        for sub in subtables:
            sub_model = MagicMock()
            sub_model.model_dump.return_value = sub
            subtable_models.append(sub_model)
        api_tutorial_operator_with_mocks.subtable_operate.read_data_by_filter.return_value = subtable_models

        # Execute
        result = await api_tutorial_operator_with_mocks.read(
            tutorial_ids=[1],
            filters={"enable": True},
            limit=10,
            offset=0
        )

        # Verify
        assert len(result) == 1
        assert result[0].id == 1
        # Verify the correct parameters were passed to read_data_by_id method
        api_tutorial_operator_with_mocks.tutorial_operate.read_data_by_id.assert_called_once_with({1})

    @pytest.mark.asyncio
    async def test_update_tutorials(self, api_tutorial_operator_with_mocks):
        """Test updating tutorials."""
        # Setup data
        update_data = APITutorialUpdate(
            id=1,
            name="Advanced Python",
            title="Advanced Python Programming"
        )

        # Setup mocks
        api_tutorial_operator_with_mocks.tutorial_operate.update_data.return_value = []
        
        # Mock read_data_by_id for the final read call in update method
        tutorial_model = MagicMock()
        tutorial_model.model_dump.return_value = {"id": 1, "name": "Advanced Python", "title": "Advanced Python Programming", "tags": ["python"], "enable": True}
        api_tutorial_operator_with_mocks.tutorial_operate.read_data_by_id.return_value = [tutorial_model]
        
        # Mock read_data_by_filter for subtables
        api_tutorial_operator_with_mocks.subtable_operate.read_data_by_filter.return_value = []

        # Execute
        result = await api_tutorial_operator_with_mocks.update([update_data])

        # Verify
        assert len(result) == 1
        assert result[0].name == "Advanced Python"

    @pytest.mark.asyncio
    async def test_delete_tutorials_with_cascade(self, api_tutorial_operator_with_mocks):
        """Test deleting tutorials with cascade deletion of subtables."""
        # Setup mocks
        api_tutorial_operator_with_mocks.subtable_operate.delete_filter_data.return_value = []
        api_tutorial_operator_with_mocks.tutorial_operate.delete_data.return_value = [1, 2]

        # Execute
        result = await api_tutorial_operator_with_mocks.delete([1, 2])

        # Verify
        assert result == [1, 2]
        api_tutorial_operator_with_mocks.subtable_operate.delete_filter_data.assert_called_once()
        api_tutorial_operator_with_mocks.tutorial_operate.delete_data.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_empty_list(self, api_tutorial_operator_with_mocks):
        """Test creating with empty list returns empty result."""
        result = await api_tutorial_operator_with_mocks.create([])
        assert result == []

    @pytest.mark.asyncio
    async def test_update_empty_list(self, api_tutorial_operator_with_mocks):
        """Test updating with empty list returns empty result."""
        result = await api_tutorial_operator_with_mocks.update([])
        assert result == []

    @pytest.mark.asyncio
    async def test_delete_empty_list(self, api_tutorial_operator_with_mocks):
        """Test deleting with empty list returns empty result."""
        result = await api_tutorial_operator_with_mocks.delete([])
        assert result == []

    @pytest.mark.asyncio
    async def test_read_no_tutorials_found(self, api_tutorial_operator_with_mocks):
        """Test reading when no tutorials are found."""
        api_tutorial_operator_with_mocks.tutorial_operate.read_sql.return_value = []

        result = await api_tutorial_operator_with_mocks.read()
        assert result == []

    # New comprehensive tests for subtable update functionality
    @pytest.mark.asyncio
    async def test_update_tutorial_create_new_subtables(self, api_tutorial_operator_with_mocks):
        """Test updating tutorial with new subtables (id=None)."""
        from tutorial.routers.operator.api_schemas import (
            APISubtableUpdate,
            APITutorialUpdate,
        )

        # Setup data - tutorial update with new subtables
        update_data = APITutorialUpdate(
            id=1,
            name="Updated Tutorial",
            subtables=[
                APISubtableUpdate(id=0, description="New Subtable 1", author="Author1"),
                APISubtableUpdate(id=0, description="New Subtable 2", author="Author2")
            ]
        )

        # Setup mocks
        api_tutorial_operator_with_mocks.tutorial_operate.update_data.return_value = []
        
        # Mock update_by_foreign_key to handle subtable creation
        api_tutorial_operator_with_mocks.subtable_operate.update_by_foreign_key.return_value = None
        
        # Mock final read for update method's read call
        tutorial_model = MagicMock()
        tutorial_model.model_dump.return_value = {"id": 1, "name": "Updated Tutorial", "title": "Test", "tags": [], "enable": True}
        api_tutorial_operator_with_mocks.tutorial_operate.read_data_by_id.return_value = [tutorial_model]
        
        # Mock subtables read
        api_tutorial_operator_with_mocks.subtable_operate.read_data_by_filter.return_value = [
            MagicMock(model_dump=lambda: {"id": 1, "tutorial_id": 1, "description": "New Subtable 1", "author": "Author1"}),
            MagicMock(model_dump=lambda: {"id": 2, "tutorial_id": 1, "description": "New Subtable 2", "author": "Author2"})
        ]

        # Execute
        result = await api_tutorial_operator_with_mocks.update([update_data])

        # Verify
        assert len(result) == 1
        assert result[0].name == "Updated Tutorial"
        assert len(result[0].subtables) == 2

        # Verify update_by_foreign_key was called with correct data including session
        api_tutorial_operator_with_mocks.subtable_operate.update_by_foreign_key.assert_called_once()
        update_call_args = api_tutorial_operator_with_mocks.subtable_operate.update_by_foreign_key.call_args
        
        # Check that session parameter was passed
        assert "session" in update_call_args[1]
        assert update_call_args[1]["foreign_key_field"] == "tutorial_id"
        assert update_call_args[1]["foreign_key_value"] == 1
        assert update_call_args[1]["data"] == update_data.subtables

    @pytest.mark.asyncio
    async def test_update_tutorial_update_existing_subtables(self, api_tutorial_operator_with_mocks):
        """Test updating existing subtables."""
        from tutorial.routers.operator.api_schemas import (
            APISubtableUpdate,
            APITutorialUpdate,
        )

        # Setup data - update existing subtables
        update_data = APITutorialUpdate(
            id=1,
            subtables=[
                APISubtableUpdate(id=1, description="Updated Description 1", author="Updated Author 1"),
                APISubtableUpdate(id=2, description="Updated Description 2")  # Only description changed
            ]
        )

        # Setup mocks
        api_tutorial_operator_with_mocks.tutorial_operate.update_data.return_value = []
        
        # Mock update_by_foreign_key to handle subtable updates
        api_tutorial_operator_with_mocks.subtable_operate.update_by_foreign_key.return_value = None
        
        # Mock final read for update method's read call
        tutorial_model = MagicMock()
        tutorial_model.model_dump.return_value = {"id": 1, "name": "Tutorial", "title": "Test", "tags": [], "enable": True}
        api_tutorial_operator_with_mocks.tutorial_operate.read_data_by_id.return_value = [tutorial_model]
        
        # Mock subtables read - return updated subtables
        api_tutorial_operator_with_mocks.subtable_operate.read_data_by_filter.return_value = [
            MagicMock(model_dump=lambda: {"id": 1, "tutorial_id": 1, "description": "Updated Description 1", "author": "Updated Author 1"}),
            MagicMock(model_dump=lambda: {"id": 2, "tutorial_id": 1, "description": "Updated Description 2", "author": "Author 2"})
        ]

        # Execute
        result = await api_tutorial_operator_with_mocks.update([update_data])

        # Verify
        assert len(result) == 1
        assert len(result[0].subtables) == 2

        # Verify update_by_foreign_key was called with correct data including session
        api_tutorial_operator_with_mocks.subtable_operate.update_by_foreign_key.assert_called_once()
        update_call_args = api_tutorial_operator_with_mocks.subtable_operate.update_by_foreign_key.call_args
        
        # Check that session parameter was passed
        assert "session" in update_call_args[1]
        assert update_call_args[1]["foreign_key_field"] == "tutorial_id"
        assert update_call_args[1]["foreign_key_value"] == 1
        assert update_call_args[1]["data"] == update_data.subtables

    @pytest.mark.asyncio
    async def test_update_tutorial_delete_subtables(self, api_tutorial_operator_with_mocks):
        """Test deleting subtables by omitting them from update."""
        from tutorial.routers.operator.api_schemas import (
            APISubtableUpdate,
            APITutorialUpdate,
        )

        # Setup data - only keep subtable id=1, delete id=2,3
        update_data = APITutorialUpdate(
            id=1,
            subtables=[
                APISubtableUpdate(id=1, description="Kept Subtable")
            ]
        )

        # Setup mocks - existing subtables (first call for _update_tutorial_subtables)
        # Then final call for read() method
        api_tutorial_operator_with_mocks.subtable_operate.read_sql.side_effect = [
            # First call: get existing subtables for comparison
            [
                {"id": 1, "tutorial_id": 1, "description": "Kept Subtable", "author": "Author1"},
                {"id": 2, "tutorial_id": 1, "description": "To Delete 1", "author": "Author2"},
                {"id": 3, "tutorial_id": 1, "description": "To Delete 2", "author": "Author3"}
            ],
            # Second call: final read for result
            [
                {"id": 1, "tutorial_id": 1, "description": "Kept Subtable", "author": "Author1"}
            ]
        ]

        # Mock final read for update method's read call
        tutorial_model = MagicMock()
        tutorial_model.model_dump.return_value = {"id": 1, "name": "Tutorial", "title": "Test", "tags": [], "enable": True}
        api_tutorial_operator_with_mocks.tutorial_operate.read_data_by_id.return_value = [tutorial_model]
        
        # Mock subtables read
        api_tutorial_operator_with_mocks.subtable_operate.read_data_by_filter.return_value = [
            MagicMock(model_dump=lambda: {"id": 1, "tutorial_id": 1, "description": "Kept Subtable", "author": "Author1"})
        ]

        # Execute
        result = await api_tutorial_operator_with_mocks.update([update_data])

        # Verify
        assert len(result) == 1
        assert len(result[0].subtables) == 1

        # Verify update_by_foreign_key was called with subtables to keep
        api_tutorial_operator_with_mocks.subtable_operate.update_by_foreign_key.assert_called_once()
        update_call_args = api_tutorial_operator_with_mocks.subtable_operate.update_by_foreign_key.call_args
        
        # Check that session parameter was passed
        assert "session" in update_call_args[1]
        assert update_call_args[1]["foreign_key_field"] == "tutorial_id"
        assert update_call_args[1]["foreign_key_value"] == 1
        assert update_call_args[1]["data"] == update_data.subtables

    @pytest.mark.asyncio
    async def test_update_tutorial_mixed_crud_operations(self, api_tutorial_operator_with_mocks):
        """Test complex scenario with create, update, and delete operations."""
        from tutorial.routers.operator.api_schemas import (
            APISubtableUpdate,
            APITutorialUpdate,
        )

        # Setup data - mixed operations
        update_data = APITutorialUpdate(
            id=1,
            subtables=[
                # Keep and update existing subtable
                APISubtableUpdate(id=1, description="Updated Existing", author="Updated Author"),
                # Create new subtable
                APISubtableUpdate(id=0, description="New Subtable", author="New Author"),
                # Keep existing subtable unchanged (id=3 will be deleted)
                APISubtableUpdate(id=2, description="Unchanged")
            ]
        )

        # Setup mocks - existing subtables (first call for _update_tutorial_subtables)
        # Then final call for read() method
        api_tutorial_operator_with_mocks.subtable_operate.read_sql.side_effect = [
            # First call: get existing subtables for comparison
            [
                {"id": 1, "tutorial_id": 1, "description": "Old Description", "author": "Old Author"},
                {"id": 2, "tutorial_id": 1, "description": "Unchanged", "author": "Author2"},
                {"id": 3, "tutorial_id": 1, "description": "To Delete", "author": "Author3"}
            ],
            # Second call: final read for result
            [
                {"id": 1, "tutorial_id": 1, "description": "Updated Existing", "author": "Updated Author"},
                {"id": 2, "tutorial_id": 1, "description": "Unchanged", "author": "Author2"},
                {"id": 4, "tutorial_id": 1, "description": "New Subtable", "author": "New Author"}
            ]
        ]

        # Mock create operation (create_data returns Pydantic models)
        created_model = MagicMock()
        created_model.model_dump.return_value = {"id": 4, "tutorial_id": 1, "description": "New Subtable", "author": "New Author"}
        api_tutorial_operator_with_mocks.subtable_operate.create_data.return_value = [created_model]

        # Mock final read for update method's read call
        tutorial_model = MagicMock()
        tutorial_model.model_dump.return_value = {"id": 1, "name": "Tutorial", "title": "Test", "tags": [], "enable": True}
        api_tutorial_operator_with_mocks.tutorial_operate.read_data_by_id.return_value = [tutorial_model]
        
        # Mock subtables read
        api_tutorial_operator_with_mocks.subtable_operate.read_data_by_filter.return_value = [
            MagicMock(model_dump=lambda: {"id": 1, "tutorial_id": 1, "description": "Updated Existing", "author": "Updated Author"}),
            MagicMock(model_dump=lambda: {"id": 2, "tutorial_id": 1, "description": "Unchanged", "author": "Author2"}),
            MagicMock(model_dump=lambda: {"id": 4, "tutorial_id": 1, "description": "New Subtable", "author": "New Author"})
        ]

        # Execute
        result = await api_tutorial_operator_with_mocks.update([update_data])

        # Verify
        assert len(result) == 1
        assert len(result[0].subtables) == 3

        # Verify update_by_foreign_key was called with mixed CRUD data
        api_tutorial_operator_with_mocks.subtable_operate.update_by_foreign_key.assert_called_once()
        update_call_args = api_tutorial_operator_with_mocks.subtable_operate.update_by_foreign_key.call_args
        
        # Check that session parameter was passed
        assert "session" in update_call_args[1]
        assert update_call_args[1]["foreign_key_field"] == "tutorial_id"
        assert update_call_args[1]["foreign_key_value"] == 1
        assert update_call_args[1]["data"] == update_data.subtables

    @pytest.mark.asyncio
    async def test_update_tutorial_no_subtable_changes(self, api_tutorial_operator_with_mocks):
        """Test update with no actual subtable changes."""
        from tutorial.routers.operator.api_schemas import (
            APISubtableUpdate,
            APITutorialUpdate,
        )

        # Setup data - same as existing
        update_data = APITutorialUpdate(
            id=1,
            subtables=[
                APISubtableUpdate(id=1, description="Same Description", author="Same Author")
            ]
        )

        # Setup mocks - identical existing subtable
        api_tutorial_operator_with_mocks.subtable_operate.read_sql.return_value = [
            {"id": 1, "tutorial_id": 1, "description": "Same Description", "author": "Same Author"}
        ]

        # Mock final read for update method's read call
        tutorial_model = MagicMock()
        tutorial_model.model_dump.return_value = {"id": 1, "name": "Tutorial", "title": "Test", "tags": [], "enable": True}
        api_tutorial_operator_with_mocks.tutorial_operate.read_data.return_value = [tutorial_model]
        api_tutorial_operator_with_mocks.subtable_operate.read_sql.return_value = [
            {"id": 1, "tutorial_id": 1, "description": "Same Description", "author": "Same Author"}
        ]

        # Execute
        await api_tutorial_operator_with_mocks.update([update_data])

        # Verify update_by_foreign_key was still called (it handles the comparison internally)
        api_tutorial_operator_with_mocks.subtable_operate.update_by_foreign_key.assert_called_once()
        update_call_args = api_tutorial_operator_with_mocks.subtable_operate.update_by_foreign_key.call_args
        
        # Check that session parameter was passed
        assert "session" in update_call_args[1]
        assert update_call_args[1]["foreign_key_field"] == "tutorial_id"
        assert update_call_args[1]["foreign_key_value"] == 1
        assert update_call_args[1]["data"] == update_data.subtables

    @pytest.mark.asyncio
    async def test_update_tutorial_empty_subtables_deletes_all(self, api_tutorial_operator_with_mocks):
        """Test updating with empty subtables list deletes all existing."""
        from tutorial.routers.operator.api_schemas import APITutorialUpdate

        # Setup data - empty subtables
        update_data = APITutorialUpdate(
            id=1,
            subtables=[]
        )

        # Setup mocks - existing subtables to delete (first call for _update_tutorial_subtables)
        # Then final call for read() method
        api_tutorial_operator_with_mocks.subtable_operate.read_sql.side_effect = [
            # First call: get existing subtables for comparison
            [
                {"id": 1, "tutorial_id": 1, "description": "Delete me", "author": "Author1"},
                {"id": 2, "tutorial_id": 1, "description": "Delete me too", "author": "Author2"}
            ],
            # Second call: final read for result (empty after deletion)
            []
        ]

        # Mock final read for update method's read call
        tutorial_model = MagicMock()
        tutorial_model.model_dump.return_value = {"id": 1, "name": "Tutorial", "title": "Test", "tags": [], "enable": True}
        api_tutorial_operator_with_mocks.tutorial_operate.read_data.return_value = [tutorial_model]

        # Execute
        await api_tutorial_operator_with_mocks.update([update_data])

        # Verify update_by_foreign_key was called with empty subtables list
        api_tutorial_operator_with_mocks.subtable_operate.update_by_foreign_key.assert_called_once()
        update_call_args = api_tutorial_operator_with_mocks.subtable_operate.update_by_foreign_key.call_args
        
        # Check that session parameter was passed
        assert "session" in update_call_args[1]
        assert update_call_args[1]["foreign_key_field"] == "tutorial_id"
        assert update_call_args[1]["foreign_key_value"] == 1
        assert update_call_args[1]["data"] == []

    @pytest.mark.asyncio
    async def test_update_tutorial_subtables_none_skips_processing(self, api_tutorial_operator_with_mocks):
        """Test update with subtables=None skips subtable processing."""
        from tutorial.routers.operator.api_schemas import APITutorialUpdate

        # Setup data - subtables=None
        update_data = APITutorialUpdate(
            id=1,
            name="Updated Name",
            subtables=None  # This should skip subtable processing
        )

        # Setup mocks
        api_tutorial_operator_with_mocks.tutorial_operate.update_sql.return_value = []
        api_tutorial_operator_with_mocks.tutorial_operate.read_sql.return_value = [
            {"id": 1, "name": "Updated Name", "title": "Test", "tags": [], "enable": True}
        ]
        api_tutorial_operator_with_mocks.subtable_operate.read_sql.return_value = [
            {"id": 1, "tutorial_id": 1, "description": "Existing", "author": "Author"}
        ]

        # Execute
        await api_tutorial_operator_with_mocks.update([update_data])

        # Verify subtable operations were not called
        api_tutorial_operator_with_mocks.subtable_operate.delete_data.assert_not_called()
        api_tutorial_operator_with_mocks.subtable_operate.update_data.assert_not_called()
        api_tutorial_operator_with_mocks.subtable_operate.create_data.assert_not_called()
        # But tutorial update should have happened
        api_tutorial_operator_with_mocks.tutorial_operate.update_data.assert_called_once()

    # Transaction-specific tests
    @pytest.mark.asyncio
    async def test_create_uses_transaction(self, api_tutorial_operator_with_mocks, sample_tutorial_create_data, mock_session):
        """Test that create method uses transaction context manager."""
        # Setup mocks
        created_tutorial = {"id": 1, "name": "Python Basics", "title": "Introduction", "tags": ["python"], "enable": True}
        created_tutorial_model = MagicMock()
        created_tutorial_model.id = 1
        created_tutorial_model.model_dump.return_value = created_tutorial
        
        api_tutorial_operator_with_mocks.tutorial_operate.create_data.return_value = [created_tutorial_model]
        api_tutorial_operator_with_mocks.subtable_operate.create_by_foreign_key.return_value = []
        
        # Execute
        tutorial_create = APITutorialCreate(**sample_tutorial_create_data)
        await api_tutorial_operator_with_mocks.create([tutorial_create])
        
        # Verify transaction was used
        api_tutorial_operator_with_mocks.tutorial_operate.create_external_session.assert_called_once()
        mock_session.begin.assert_called_once()
        mock_session.close.assert_called_once()
        
        # Verify session was passed to operations
        api_tutorial_operator_with_mocks.tutorial_operate.create_data.assert_called_once()
        create_call_kwargs = api_tutorial_operator_with_mocks.tutorial_operate.create_data.call_args[1]
        assert 'session' in create_call_kwargs
        assert create_call_kwargs['session'] == mock_session

    @pytest.mark.asyncio
    async def test_update_uses_transaction(self, api_tutorial_operator_with_mocks, mock_session):
        """Test that update method uses transaction context manager."""
        from tutorial.routers.operator.api_schemas import APITutorialUpdate
        
        # Setup data
        update_data = APITutorialUpdate(id=1, name="Updated Tutorial")
        
        # Setup mocks
        tutorial_model = MagicMock()
        tutorial_model.model_dump.return_value = {"id": 1, "name": "Updated Tutorial", "title": "Test", "tags": [], "enable": True}
        api_tutorial_operator_with_mocks.tutorial_operate.read_data_by_id.return_value = [tutorial_model]
        api_tutorial_operator_with_mocks.subtable_operate.read_data_by_filter.return_value = []
        
        # Execute
        await api_tutorial_operator_with_mocks.update([update_data])
        
        # Verify transaction was used
        api_tutorial_operator_with_mocks.tutorial_operate.create_external_session.assert_called_once()
        mock_session.begin.assert_called_once()
        mock_session.close.assert_called_once()
        
        # Verify session was passed to operations
        if api_tutorial_operator_with_mocks.tutorial_operate.update_data.called:
            update_call_kwargs = api_tutorial_operator_with_mocks.tutorial_operate.update_data.call_args[1]
            assert 'session' in update_call_kwargs
            assert update_call_kwargs['session'] == mock_session

    @pytest.mark.asyncio
    async def test_delete_uses_transaction(self, api_tutorial_operator_with_mocks, mock_session):
        """Test that delete method uses transaction context manager."""
        # Setup mocks
        api_tutorial_operator_with_mocks.tutorial_operate.delete_data.return_value = [1, 2]
        api_tutorial_operator_with_mocks.subtable_operate.delete_filter_data.return_value = []
        
        # Execute
        await api_tutorial_operator_with_mocks.delete([1, 2])
        
        # Verify transaction was used
        api_tutorial_operator_with_mocks.tutorial_operate.create_external_session.assert_called_once()
        mock_session.begin.assert_called_once()
        mock_session.close.assert_called_once()
        
        # Verify session was passed to operations
        delete_call_kwargs = api_tutorial_operator_with_mocks.tutorial_operate.delete_data.call_args[1]
        assert 'session' in delete_call_kwargs
        assert delete_call_kwargs['session'] == mock_session
        
        delete_filter_call_kwargs = api_tutorial_operator_with_mocks.subtable_operate.delete_filter_data.call_args[1]
        assert 'session' in delete_filter_call_kwargs
        assert delete_filter_call_kwargs['session'] == mock_session

    @pytest.mark.asyncio
    async def test_transaction_rollback_on_error(self, api_tutorial_operator_with_mocks, sample_tutorial_create_data, mock_session):
        """Test that transaction rolls back on error."""
        # Setup mocks to raise an exception
        api_tutorial_operator_with_mocks.tutorial_operate.create_data.side_effect = Exception("Database error")
        
        # Execute and expect exception
        tutorial_create = APITutorialCreate(**sample_tutorial_create_data)
        with pytest.raises(Exception, match="Database error"):
            await api_tutorial_operator_with_mocks.create([tutorial_create])
        
        # Verify transaction was used and session was closed
        api_tutorial_operator_with_mocks.tutorial_operate.create_external_session.assert_called_once()
        mock_session.close.assert_called_once()

    @pytest.mark.asyncio 
    async def test_create_with_subtables_in_transaction(self, api_tutorial_operator_with_mocks, sample_tutorial_create_data, mock_session):
        """Test that both tutorial and subtable creation happens in same transaction."""
        # Setup mocks
        created_tutorial = {"id": 1, "name": "Python Basics", "title": "Introduction", "tags": ["python"], "enable": True}
        created_tutorial_model = MagicMock()
        created_tutorial_model.id = 1
        created_tutorial_model.model_dump.return_value = created_tutorial
        
        created_subtables = [
            {"id": 1, "tutorial_id": 1, "description": "Variables", "author": "John Doe"},
            {"id": 2, "tutorial_id": 1, "description": "Control Flow", "author": "Jane Smith"}
        ]
        created_subtable_models = []
        for sub in created_subtables:
            sub_model = MagicMock()
            sub_model.model_dump.return_value = sub
            created_subtable_models.append(sub_model)
        
        api_tutorial_operator_with_mocks.tutorial_operate.create_data.return_value = [created_tutorial_model]
        api_tutorial_operator_with_mocks.subtable_operate.create_by_foreign_key.return_value = created_subtable_models
        
        # Execute
        tutorial_create = APITutorialCreate(**sample_tutorial_create_data)
        result = await api_tutorial_operator_with_mocks.create([tutorial_create])
        
        # Verify both operations used the same session
        tutorial_call_kwargs = api_tutorial_operator_with_mocks.tutorial_operate.create_data.call_args[1]
        subtable_call_kwargs = api_tutorial_operator_with_mocks.subtable_operate.create_by_foreign_key.call_args[1]
        
        assert tutorial_call_kwargs['session'] == mock_session
        assert subtable_call_kwargs['session'] == mock_session
        
        # Verify result
        assert len(result) == 1
        assert result[0].id == 1
        assert len(result[0].subtables) == 2
