
from pydantic import BaseModel, ConfigDict, Field, field_validator



# API-specific models with enhanced validation
class APISubtableBasic(BaseModel):
    description: str = Field(min_length=0, description="Description of the subtable content")
    author: str = Field(min_length=0, description="Author of the subtable")

    model_config = ConfigDict(from_attributes=True)


class APISubtable(APISubtableBasic):
    id: int = Field(description="Unique identifier for the subtable")
    model_config = ConfigDict(from_attributes=True)


class APISubtableCreate(BaseModel):
    description: str = Field(default="", min_length=0, description="Description of the subtable content")
    author: str = Field(default="", min_length=0, description="Author of the subtable")

    model_config = ConfigDict(from_attributes=True)


class APISubtableUpdate(BaseModel):
    id: int = Field(0, description="Unique identifier for the subtable to update")
    description: str | None = Field(None, min_length=1, description="Updated description")
    author: str | None = Field(None, min_length=1, description="Updated author")

    model_config = ConfigDict(from_attributes=True)



# API-specific models with enhanced validation and bidirectional relationships
class APITutorialBasic(BaseModel):
    name: str = Field(min_length=1, description="Name of the tutorial")
    title: str = Field(description="Title of the tutorial")
    tags: list[str] = Field(default_factory=list, description="List of tags for categorization")
    enable: bool = Field(default=True, description="Whether the tutorial is enabled")

    @classmethod
    @field_validator('tags')
    def validate_tags(cls, v):
        if not isinstance(v, list):
            raise ValueError('Tags must be a list')
        return [str(tag).strip() for tag in v if str(tag).strip()]

    model_config = ConfigDict(from_attributes=True)


class APITutorial(APITutorialBasic):
    """
    API model for Tutorial with enhanced features and bidirectional relationships.

    This model represents a complete tutorial with its associated subtables.
    Used for API responses and complex operations involving nested data.

    Attributes:
        id (int): Unique identifier for the tutorial (must be positive)
        name (str): Name of the tutorial (minimum length 1)
        title (str): Title of the tutorial
        tags (list[str]): List of tags for categorization (automatically cleaned)
        enable (bool): Whether the tutorial is enabled (default: True)
        subtables (list[APISubtable]): List of associated subtables (default: empty list)

    Example:
        >>> tutorial = APITutorial(
        ...     id=1,
        ...     name="Python Basics",
        ...     title="Introduction to Python",
        ...     tags=["python", "programming"],
        ...     enable=True,
        ...     subtables=[
        ...         APISubtable(
        ...             id=1,
        ...             tutorial_id=1,
        ...             description="Variables",
        ...             author="John Doe"
        ...         )
        ...     ]
        ... )
    """
    id: int = Field(gt=0, description="Unique identifier for the tutorial")
    subtables: list[APISubtable] = Field(default_factory=list, description="List of subtables associated with this tutorial")

    model_config = ConfigDict(from_attributes=True)


class APITutorialCreate(APITutorialBasic):
    title: str = Field(default="", description="Title of the tutorial")
    tags: list[str] = Field(default_factory=list, description="List of tags for categorization")
    enable: bool = Field(default=True, description="Whether the tutorial is enabled")
    subtables: list[APISubtableCreate] = Field(default_factory=list, description="List of subtables to create with this tutorial")

    model_config = ConfigDict(from_attributes=True)


class APITutorialUpdate(BaseModel):
    id: int | None = Field(None, description="Unique identifier for the tutorial to update")
    name: str | None = Field(None, description="Updated name")
    title: str | None = Field(None, description="Updated title")
    tags: list[str] | None = Field(None, description="Updated tags list")
    enable: bool | None = Field(None, description="Updated enable status")
    subtables: list[APISubtableUpdate] | None = Field(None, description="Updated subtables list")

    @classmethod
    @field_validator('tags')
    def validate_tags(cls, v):
        if v is None:
            return v
        if not isinstance(v, list):
            raise ValueError('Tags must be a list')
        return [str(tag).strip() for tag in v if str(tag).strip()]

    model_config = ConfigDict(from_attributes=True)


main_schemas = APITutorial
create_schemas = APITutorialCreate
update_schemas = APITutorialUpdate
