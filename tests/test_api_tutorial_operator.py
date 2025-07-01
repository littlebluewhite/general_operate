"""
Test suite for APITutorialOperator refactored implementation.
"""
import pytest

from tutorial.routers.operator.api_schemas import (
    APISubtableCreate,
    APITutorialCreate,
    APITutorialUpdate,
)


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

        api_tutorial_operator_with_mocks.tutorial_operate.create_sql.return_value = [created_tutorial]
        api_tutorial_operator_with_mocks.subtable_operate.create_sql.return_value = created_subtables

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

        api_tutorial_operator_with_mocks.tutorial_operate.create_sql.return_value = created_tutorials
        api_tutorial_operator_with_mocks.subtable_operate.create_sql.return_value = created_subtables

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

        api_tutorial_operator_with_mocks.tutorial_operate.read_sql.return_value = tutorials
        api_tutorial_operator_with_mocks.subtable_operate.read_sql.return_value = subtables

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

        api_tutorial_operator_with_mocks.tutorial_operate.read_sql.return_value = tutorials
        api_tutorial_operator_with_mocks.subtable_operate.read_sql.return_value = subtables

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
        # Verify the correct parameters were passed to read method
        api_tutorial_operator_with_mocks.tutorial_operate.read_sql.assert_called_once()

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
        api_tutorial_operator_with_mocks.tutorial_operate.update_sql.return_value = []
        api_tutorial_operator_with_mocks.tutorial_operate.read_sql.return_value = [
            {"id": 1, "name": "Advanced Python", "title": "Advanced Python Programming", "tags": ["python"], "enable": True}
        ]
        api_tutorial_operator_with_mocks.subtable_operate.read_sql.return_value = []

        # Execute
        result = await api_tutorial_operator_with_mocks.update([update_data])

        # Verify
        assert len(result) == 1
        assert result[0].name == "Advanced Python"

    @pytest.mark.asyncio
    async def test_delete_tutorials_with_cascade(self, api_tutorial_operator_with_mocks):
        """Test deleting tutorials with cascade deletion of subtables."""
        # Setup mocks
        api_tutorial_operator_with_mocks.subtable_operate.delete_filter.return_value = 2
        api_tutorial_operator_with_mocks.tutorial_operate.delete_data.return_value = [1, 2]

        # Execute
        result = await api_tutorial_operator_with_mocks.delete([1, 2])

        # Verify
        assert result == [1, 2]
        api_tutorial_operator_with_mocks.subtable_operate.delete_filter.assert_called_once()
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
                APISubtableUpdate(id=None, description="New Subtable 1", author="Author1"),
                APISubtableUpdate(id=None, description="New Subtable 2", author="Author2")
            ]
        )

        # Setup mocks
        api_tutorial_operator_with_mocks.tutorial_operate.update_sql.return_value = []
        # No existing subtables for this tutorial
        api_tutorial_operator_with_mocks.subtable_operate.read_sql.return_value = []
        # Mock created subtables
        api_tutorial_operator_with_mocks.subtable_operate.create_sql.return_value = [
            {"id": 1, "tutorial_id": 1, "description": "New Subtable 1", "author": "Author1"},
            {"id": 2, "tutorial_id": 1, "description": "New Subtable 2", "author": "Author2"}
        ]
        # Mock final read
        api_tutorial_operator_with_mocks.tutorial_operate.read_sql.return_value = [
            {"id": 1, "name": "Updated Tutorial", "title": "Test", "tags": [], "enable": True}
        ]
        api_tutorial_operator_with_mocks.subtable_operate.read_sql.return_value = [
            {"id": 1, "tutorial_id": 1, "description": "New Subtable 1", "author": "Author1"},
            {"id": 2, "tutorial_id": 1, "description": "New Subtable 2", "author": "Author2"}
        ]

        # Execute
        result = await api_tutorial_operator_with_mocks.update([update_data])

        # Verify
        assert len(result) == 1
        assert result[0].name == "Updated Tutorial"
        assert len(result[0].subtables) == 2

        # Verify create was called with correct data
        api_tutorial_operator_with_mocks.subtable_operate.create_sql.assert_called_once()
        create_call_args = api_tutorial_operator_with_mocks.subtable_operate.create_sql.call_args[1]['data']
        assert len(create_call_args) == 2
        assert all(item['tutorial_id'] == 1 for item in create_call_args)

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

        # Setup mocks - existing subtables (first call for _update_tutorial_subtables)
        # Then final call for read() method
        api_tutorial_operator_with_mocks.subtable_operate.read_sql.side_effect = [
            # First call: get existing subtables for comparison
            [
                {"id": 1, "tutorial_id": 1, "description": "Old Description 1", "author": "Old Author 1"},
                {"id": 2, "tutorial_id": 1, "description": "Old Description 2", "author": "Author 2"}
            ],
            # Second call: final read for result
            [
                {"id": 1, "tutorial_id": 1, "description": "Updated Description 1", "author": "Updated Author 1"},
                {"id": 2, "tutorial_id": 1, "description": "Updated Description 2", "author": "Author 2"}
            ]
        ]

        # Mock final read
        api_tutorial_operator_with_mocks.tutorial_operate.read_sql.return_value = [
            {"id": 1, "name": "Tutorial", "title": "Test", "tags": [], "enable": True}
        ]

        # Execute
        result = await api_tutorial_operator_with_mocks.update([update_data])

        # Verify
        assert len(result) == 1
        assert len(result[0].subtables) == 2

        # Verify update was called with correct data
        api_tutorial_operator_with_mocks.subtable_operate.update_sql.assert_called_once()
        update_call_args = api_tutorial_operator_with_mocks.subtable_operate.update_sql.call_args[1]['update_data']
        assert 1 in update_call_args
        assert 2 in update_call_args
        assert update_call_args[1]['description'] == "Updated Description 1"
        assert update_call_args[1]['author'] == "Updated Author 1"
        assert update_call_args[2]['description'] == "Updated Description 2"
        assert 'author' not in update_call_args[2]  # Should not include unchanged fields

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

        # Mock final read
        api_tutorial_operator_with_mocks.tutorial_operate.read_sql.return_value = [
            {"id": 1, "name": "Tutorial", "title": "Test", "tags": [], "enable": True}
        ]

        # Execute
        result = await api_tutorial_operator_with_mocks.update([update_data])

        # Verify
        assert len(result) == 1
        assert len(result[0].subtables) == 1

        # Verify delete was called with correct IDs
        api_tutorial_operator_with_mocks.subtable_operate.delete_data.assert_called_once()
        delete_call_args = api_tutorial_operator_with_mocks.subtable_operate.delete_data.call_args[1]['id_value']
        assert delete_call_args == {2, 3}

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
                APISubtableUpdate(id=None, description="New Subtable", author="New Author"),
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

        # Mock create operation
        api_tutorial_operator_with_mocks.subtable_operate.create_sql.return_value = [
            {"id": 4, "tutorial_id": 1, "description": "New Subtable", "author": "New Author"}
        ]

        # Mock final read
        api_tutorial_operator_with_mocks.tutorial_operate.read_sql.return_value = [
            {"id": 1, "name": "Tutorial", "title": "Test", "tags": [], "enable": True}
        ]

        # Execute
        result = await api_tutorial_operator_with_mocks.update([update_data])

        # Verify
        assert len(result) == 1
        assert len(result[0].subtables) == 3

        # Verify all operations were called
        api_tutorial_operator_with_mocks.subtable_operate.delete_data.assert_called_once_with(id_value={3})
        api_tutorial_operator_with_mocks.subtable_operate.update_sql.assert_called_once()
        api_tutorial_operator_with_mocks.subtable_operate.create_sql.assert_called_once()

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

        # Mock final read
        api_tutorial_operator_with_mocks.tutorial_operate.read_sql.return_value = [
            {"id": 1, "name": "Tutorial", "title": "Test", "tags": [], "enable": True}
        ]
        api_tutorial_operator_with_mocks.subtable_operate.read_sql.return_value = [
            {"id": 1, "tutorial_id": 1, "description": "Same Description", "author": "Same Author"}
        ]

        # Execute
        await api_tutorial_operator_with_mocks.update([update_data])

        # Verify no CRUD operations were performed
        api_tutorial_operator_with_mocks.subtable_operate.delete_data.assert_not_called()
        api_tutorial_operator_with_mocks.subtable_operate.update_sql.assert_not_called()
        api_tutorial_operator_with_mocks.subtable_operate.create_sql.assert_not_called()

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

        # Mock final read
        api_tutorial_operator_with_mocks.tutorial_operate.read_sql.return_value = [
            {"id": 1, "name": "Tutorial", "title": "Test", "tags": [], "enable": True}
        ]

        # Execute
        await api_tutorial_operator_with_mocks.update([update_data])

        # Verify all subtables were deleted
        api_tutorial_operator_with_mocks.subtable_operate.delete_data.assert_called_once_with(id_value={1, 2})
        api_tutorial_operator_with_mocks.subtable_operate.update_sql.assert_not_called()
        api_tutorial_operator_with_mocks.subtable_operate.create_sql.assert_not_called()

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
        api_tutorial_operator_with_mocks.subtable_operate.update_sql.assert_not_called()
        api_tutorial_operator_with_mocks.subtable_operate.create_sql.assert_not_called()
        # But tutorial update should have happened
        api_tutorial_operator_with_mocks.tutorial_operate.update_sql.assert_called_once()
