from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


# Original models for database operations
class SubtableBasic(BaseModel):
    description: str = Field(min_length=0, description="Description of the subtable content")
    tutorial_id: int = Field(gt=0, description="ID of the parent tutorial")
    author: str = Field(min_length=0, description="Author of the subtable")

    model_config = ConfigDict(from_attributes=True)


class Subtable(SubtableBasic):
    id: int = Field(gt=0, description="Unique identifier for the subtable")

    model_config = ConfigDict(from_attributes=True)


class SubtableCreate(BaseModel):
    description: str = Field(default="", min_length=0, description="Description of the subtable content")
    author: str = Field(default="", min_length=0, description="Author of the subtable")
    tutorial_id: int = Field(gt=0, description="ID of the parent tutorial")

    model_config = ConfigDict(from_attributes=True)


class SubtableUpdate(BaseModel):
    id: int = Field(gt=0, description="Unique identifier for the subtable to update")
    description: Optional[str] = Field(None, min_length=1, description="Updated description")
    tutorial_id: Optional[int] = Field(None, gt=0, description="Updated parent tutorial ID")
    author: Optional[str] = Field(None, min_length=1, description="Updated author")

    model_config = ConfigDict(from_attributes=True)




# Database configuration settings
table_name = "subtable"
main_schemas = Subtable
create_schemas = SubtableCreate
update_schemas = SubtableUpdate