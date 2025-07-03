from pydantic import BaseModel, ConfigDict, Field, field_validator


# Original models for database operations
class TutorialBasic(BaseModel):
    name: str = Field(min_length=1, description="Name of the tutorial")
    title: str = Field(description="Title of the tutorial")
    tags: list[str] = Field(default_factory=list, description="List of tags for categorization")
    enable: bool = Field(default=True, description="Whether the tutorial is enabled")

    @field_validator('tags')
    @classmethod
    def validate_tags(cls, v):
        if not isinstance(v, list):
            raise ValueError('Tags must be a list')
        return [str(tag).strip() for tag in v if str(tag).strip()]

    model_config = ConfigDict(from_attributes=True)


class Tutorial(TutorialBasic):
    id: int = Field(gt=0, description="Unique identifier for the tutorial")

    model_config = ConfigDict(from_attributes=True)


class TutorialCreate(TutorialBasic):
    title: str = Field(default="", description="Title of the tutorial")
    tags: list[str] = Field(default_factory=list, description="List of tags for categorization")
    enable: bool = Field(default=True, description="Whether the tutorial is enabled")

    model_config = ConfigDict(from_attributes=True)


class TutorialUpdate(BaseModel):
    id: int | None = Field(None, description="Unique identifier for the tutorial to update")
    name: str | None = Field(None, description="Updated name")
    title: str | None = Field(None, description="Updated title")
    tags: list[str] | None = Field(None, description="Updated tags list")
    enable: bool | None = Field(None, description="Updated enable status")

    @field_validator('tags')
    @classmethod
    def validate_tags(cls, v):
        if v is None:
            return v
        if not isinstance(v, list):
            raise ValueError('Tags must be a list')
        return [str(tag).strip() for tag in v if str(tag).strip()]

    model_config = ConfigDict(from_attributes=True)



# Database configuration settings
table_name = "tutorial"
main_schemas = Tutorial
create_schemas = TutorialCreate
update_schemas = TutorialUpdate
