"""
Test suite for schema validation and functionality.
"""
import pytest
from pydantic import ValidationError
from tutorial.routers.operator.api_schemas import APITutorial, APITutorialCreate, APITutorialUpdate, APISubtable, APISubtableCreate, APISubtableUpdate


class TestAPITutorialSchemas:
    """Test suite for APITutorial schema models."""

    def test_api_tutorial_create_valid_data(self):
        """Test creating APITutorialCreate with valid data."""
        data = {
            "name": "Python Basics",
            "title": "Introduction to Python",
            "tags": ["python", "programming"],
            "enable": True,
            "subtables": [
                {"description": "Variables", "author": "John Doe"}
            ]
        }
        tutorial = APITutorialCreate(**data)
        assert tutorial.name == "Python Basics"
        assert tutorial.tags == ["python", "programming"]
        assert len(tutorial.subtables) == 1

    def test_api_tutorial_create_defaults(self):
        """Test APITutorialCreate with default values."""
        tutorial = APITutorialCreate(name="Test Tutorial")
        assert tutorial.title == ""
        assert tutorial.tags == []
        assert tutorial.enable is True
        assert tutorial.subtables == []

    def test_api_tutorial_create_tags_validation(self):
        """Test tags validation removes empty strings."""
        tutorial = APITutorialCreate(
            name="Test", 
            tags=["python", "", "  ", "programming", ""]
        )
        assert tutorial.tags == ["python", "programming"]

    def test_api_tutorial_create_name_validation(self):
        """Test name field validation."""
        with pytest.raises(ValidationError) as exc_info:
            APITutorialCreate(name="")
        assert "at least 1 character" in str(exc_info.value)

    def test_api_tutorial_with_subtables(self):
        """Test APITutorial with nested subtables."""
        data = {
            "id": 1,
            "name": "Python Basics",
            "title": "Introduction to Python",
            "tags": ["python"],
            "enable": True,
            "subtables": [
                {
                    "id": 1,
                    "tutorial_id": 1,
                    "description": "Variables",
                    "author": "John Doe"
                }
            ]
        }
        tutorial = APITutorial(**data)
        assert tutorial.id == 1
        assert len(tutorial.subtables) == 1
        assert tutorial.subtables[0].description == "Variables"

    def test_api_tutorial_update_partial_fields(self):
        """Test APITutorialUpdate with partial field updates."""
        update = APITutorialUpdate(
            id=1,
            name="Updated Name",
            tags=["updated", "tags"]
        )
        assert update.id == 1
        assert update.name == "Updated Name"
        assert update.title is None
        assert update.enable is None
        assert update.tags == ["updated", "tags"]

    def test_api_tutorial_update_invalid_id(self):
        """Test APITutorialUpdate with invalid ID."""
        with pytest.raises(ValidationError) as exc_info:
            APITutorialUpdate(id=0, name="Test")
        assert "greater than 0" in str(exc_info.value)


class TestAPISubtableSchemas:
    """Test suite for APISubtable schema models."""

    def test_api_subtable_create_valid_data(self):
        """Test creating APISubtableCreate with valid data."""
        subtable = APISubtableCreate(
            description="Test description",
            author="Test Author"
        )
        assert subtable.description == "Test description"
        assert subtable.author == "Test Author"

    def test_api_subtable_create_defaults(self):
        """Test APISubtableCreate with default values."""
        subtable = APISubtableCreate()
        assert subtable.description == ""
        assert subtable.author == ""

    def test_api_subtable_validation(self):
        """Test APISubtable field validation."""
        data = {
            "id": 1,
            "tutorial_id": 1,
            "description": "Test description",
            "author": "Test Author"
        }
        subtable = APISubtable(**data)
        assert subtable.id == 1
        assert subtable.tutorial_id == 1

    def test_api_subtable_invalid_ids(self):
        """Test APISubtable with invalid IDs."""
        with pytest.raises(ValidationError) as exc_info:
            APISubtable(
                id=0,
                tutorial_id=1,
                description="Test",
                author="Author"
            )
        assert "greater than 0" in str(exc_info.value)

        with pytest.raises(ValidationError) as exc_info:
            APISubtable(
                id=1,
                tutorial_id=0,
                description="Test",
                author="Author"
            )
        assert "greater than 0" in str(exc_info.value)

    def test_api_subtable_update_partial_fields(self):
        """Test APISubtableUpdate with partial field updates."""
        update = APISubtableUpdate(
            id=1,
            description="Updated description"
        )
        assert update.id == 1
        assert update.description == "Updated description"
        assert update.author is None
        assert update.tutorial_id is None

    def test_api_subtable_update_validation(self):
        """Test APISubtableUpdate field validation."""
        with pytest.raises(ValidationError) as exc_info:
            APISubtableUpdate(id=1, description="")
        assert "at least 1 character" in str(exc_info.value)

        with pytest.raises(ValidationError) as exc_info:
            APISubtableUpdate(id=1, author="")
        assert "at least 1 character" in str(exc_info.value)


class TestSchemaInteroperability:
    """Test schema interoperability and edge cases."""

    def test_nested_schema_creation(self):
        """Test creating nested structures with schemas."""
        tutorial_data = APITutorialCreate(
            name="Nested Test",
            subtables=[
                APISubtableCreate(description="Sub 1", author="Author 1"),
                APISubtableCreate(description="Sub 2", author="Author 2")
            ]
        )
        assert len(tutorial_data.subtables) == 2
        assert all(isinstance(sub, APISubtableCreate) for sub in tutorial_data.subtables)

    def test_model_config_from_attributes(self):
        """Test that models can be created from ORM attributes."""
        # Simulate ORM object with attribute access
        class MockORM:
            id = 1
            name = "Test"
            title = "Test Title"
            tags = ["test"]
            enable = True
            
        mock_orm = MockORM()
        tutorial = APITutorial.model_validate(mock_orm)
        assert tutorial.id == 1
        assert tutorial.name == "Test"

    def test_schema_serialization(self):
        """Test schema serialization to dict."""
        tutorial = APITutorialCreate(
            name="Test",
            title="Test Title",
            subtables=[
                APISubtableCreate(description="Sub", author="Author")
            ]
        )
        data = tutorial.model_dump()
        assert isinstance(data, dict)
        assert data["name"] == "Test"
        assert len(data["subtables"]) == 1