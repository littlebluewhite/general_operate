from typing import Any, get_args
import sys
from pathlib import Path

# Add project root to Python path for imports
project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from general_operate import GeneralOperate
from general_operate.utils.build_data import build_create_data, extract_object_fields
from tutorial.schemas import subtable, tutorial


# Create specific operator classes that implement get_module
class TutorialOperator(GeneralOperate):
    def get_module(self):
        return tutorial


class SubtableOperator(GeneralOperate):
    def get_module(self):
        return subtable


class APITutorialOperator:
    def __init__(self, api_schema, db, redis, influxdb=None):
        self.create_schemas = api_schema.create_schemas
        self.main_schemas = api_schema.main_schemas
        self.update_schemas = api_schema.update_schemas
        self.tutorial_operate = TutorialOperator(database_client=db, redis_client=redis, influxdb=influxdb)
        self.subtable_operate = SubtableOperator(database_client=db, redis_client=redis, influxdb=influxdb)

    def get_subtable_model_from_main_model(self) -> type:
        """
        Get the subtable model class from the main model's type annotations.

        This function extracts the subtable class from the main_schemas
        subtables field annotation, which is typed as list[subtable_type].
        The extracted class can be used for dynamic instantiation of subtable objects.

        Returns:
            type: The subtable class that can be used for instantiation

        Raises:
            AttributeError: If the main_schemas doesn't have a subtables field
            TypeError: If the subtables field annotation is not a list type
            IndexError: If the list type annotation doesn't have a type argument

        Example:
            >>> SubtableClass = self.get_subtable_model_from_main_model()
            >>> instance = SubtableClass(**dict)
        """
        try:
            # Get the subtables field from main_schemas
            subtables_field = self.main_schemas.model_fields['subtables']

            # Extract the type annotation (should be list[APISubtable])
            annotation = subtables_field.annotation

            # Get the type arguments from the list type

            type_args = get_args(annotation)

            if not type_args:
                # If no type args, import and use APISubtable from api_schemas
                from .api_schemas import APISubtable
                return APISubtable

            # Return the first type argument (should be APISubtable)
            return type_args[0]

        except (AttributeError, KeyError, IndexError, TypeError):
            # Fallback to import APISubtable class if extraction fails
            from .api_schemas import APISubtable
            return APISubtable

    async def create(self, data: list[Any]) -> list[Any]:
        """
        Create tutorials with their subtables in a single transaction
        Supports batch creation of multiple tutorials with their associated subtables

        Args:
            data: List of tutorial create objects, each containing tutorial data and subtables

        Returns:
            List of tutorial objects with their associated subtables
        """
        if not data:
            return []

        async with self.tutorial_operate.transaction() as session:
            result_tutorials = []
            
            # Create tutorials first
            tutorial_create_data = [build_create_data(tutorial_data) for tutorial_data in data]

            # Batch create tutorials using transactional session
            created_tutorials = await self.tutorial_operate.create_data(tutorial_create_data, session=session)

            # Create subtables for each tutorial within the same transaction
            for _idx, (created_tutorial, tutorial_data) in enumerate(zip(created_tutorials, data, strict=False)):
                tutorial_id = created_tutorial.id

                # Prepare subtables data
                created_subtables = []
                if tutorial_data.subtables:
                    # Create subtables using foreign key method with transaction session
                    created_subtables_raw = await self.subtable_operate.create_by_foreign_key(
                        foreign_key_field="tutorial_id",
                        foreign_key_value=tutorial_id,
                        data=tutorial_data.subtables,
                        session=session
                    )

                    # Convert to APISubtable objects (create_data returns Pydantic models)
                    subtable_class = self.get_subtable_model_from_main_model()
                    created_subtables = [subtable_class(**sub.model_dump()) for sub in created_subtables_raw]

                # Build result with nested structure (create_data returns Pydantic models)
                tutorial_with_subtables = self.main_schemas(
                    **created_tutorial.model_dump(),
                    subtables=created_subtables
                )
                result_tutorials.append(tutorial_with_subtables)

            return result_tutorials

    async def read(self,
                   tutorial_ids: list[int] | None = None,
                   filters: dict[str, Any] | None = None,
                   limit: int | None = None,
                   offset: int = 0) -> list[Any]:
        """
        Read tutorials with their associated subtables

        Args:
            tutorial_ids: Optional list of tutorial IDs to filter by
            filters: Optional dictionary of filters for tutorial query
            limit: Optional limit for number of tutorials to return
            offset: Offset for pagination

        Returns:
            List of tutorial objects with their associated subtables
        """
        # Build filters for tutorial query
        tutorial_filters = filters or {}
        if tutorial_ids:
            tutorial_filters["id"] = tutorial_ids

        # Read tutorials using high-level method with caching
        if tutorial_ids:
            # For ID-based reads, use cached read_data
            tutorials_models = await self.tutorial_operate.read_data_by_id(set(tutorial_ids))
            tutorials = [tut_model.model_dump() for tut_model in tutorials_models]
        else:
            # For filter-based reads, use read_data_by_filter
            tutorials_models = await self.tutorial_operate.read_data_by_filter(
                filters=tutorial_filters,
                limit=limit,
                offset=offset
            )
            tutorials = [tut_model.model_dump() for tut_model in tutorials_models]

        if not tutorials:
            return []

        # Extract tutorial IDs for subtable query
        tutorial_id_list = [t["id"] for t in tutorials]

        # Batch read all subtables for these tutorials
        subtables_models = await self.subtable_operate.read_data_by_filter(
            filters={"tutorial_id": tutorial_id_list}
        )
        subtables = [sub_model.model_dump() for sub_model in subtables_models]

        # Group subtables by tutorial_id
        subtables_by_tutorial: dict[int, list] = {}
        subtable_class = self.get_subtable_model_from_main_model()
        for sub in subtables:
            tutorial_id = sub["tutorial_id"]
            if tutorial_id not in subtables_by_tutorial:
                subtables_by_tutorial[tutorial_id] = []
            subtables_by_tutorial[tutorial_id].append(subtable_class(**sub))

        # Build result with nested structure
        result = []
        for tutorial_data in tutorials:
            tutorial_id = tutorial_data["id"]
            tutorial_subtables = subtables_by_tutorial.get(tutorial_id, [])

            tutorial_with_subtables = self.main_schemas(
                **tutorial_data,
                subtables=tutorial_subtables
            )
            result.append(tutorial_with_subtables)

        return result

    async def update(self, update_data: list[Any], where_field: str = "id") -> list[Any]:
        """
        Update tutorials and their subtables with full CRUD support in a single transaction.

        This method handles comprehensive updates including:
        - Tutorial field updates
        - Subtable creation, updates, and deletion
        - Referential integrity maintenance
        - ACID compliance with automatic rollback on failures

        Args:
            update_data: List of tutorial update objects with tutorial IDs and updated fields
            where_field: Field name to use in WHERE clause (default: "id")

        Returns:
            List of updated tutorial objects with their associated subtables

        Raises:
            Exception: If update operations fail or referential integrity is violated
        """
        if not update_data:
            return []

        async with self.tutorial_operate.transaction() as session:
            # Process tutorial updates
            tutorial_update_list = []
            tutorial_ids = []

            for update_item in update_data:
                # Get WHERE value for this item
                where_value = getattr(update_item, where_field, None)
                if where_value is None:
                    continue  # Skip items without WHERE field value
                
                tutorial_ids.append(where_value)

                # Build tutorial update data dict, including where_field but excluding None values
                update_fields = extract_object_fields(
                    obj=update_item,
                    exclude_fields=[],  # 不排除任何字段
                    exclude_none=True   # 排除 None 值
                )

                # Only update if there are fields to update besides the where_field
                if len(update_fields) > 1 or (len(update_fields) == 1 and where_field not in update_fields):
                    tutorial_update_list.append(update_fields)

            # Update tutorials if there are changes using transactional session
            if tutorial_update_list:
                await self.tutorial_operate.update_data(tutorial_update_list, where_field, session=session)

            # Process subtable updates for each tutorial within the same transaction
            for update_item in update_data:
                where_value = getattr(update_item, where_field, None)
                if where_value is not None and update_item.subtables is not None:
                    # For subtables, we still use tutorial_id as foreign key, 
                    # but the value comes from the where_field
                    await self.subtable_operate.update_by_foreign_key(
                        foreign_key_field="tutorial_id",
                        foreign_key_value=where_value,
                        data=update_item.subtables,
                        session=session
                    )

        # Read and return updated tutorials with subtables (outside transaction for consistency)
        # Note: Reading after transaction ensures we get the committed data
        return await self.read(tutorial_ids=tutorial_ids)

    async def delete(self, tutorial_ids: list[int]) -> list[int]:
        """
        Delete tutorials and their associated subtables in a single transaction

        Args:
            tutorial_ids: List of tutorial IDs to delete

        Returns:
            List of successfully deleted tutorial IDs
        """
        if not tutorial_ids:
            return []

        async with self.tutorial_operate.transaction() as session:
            # First, delete all subtables associated with these tutorials within transaction
            await self.subtable_operate.delete_filter_data(
                filters={"tutorial_id": tutorial_ids}, session=session
            )

            # Then delete the tutorials within the same transaction
            deleted_ids = await self.tutorial_operate.delete_data(
                id_value=set(tutorial_ids), session=session
            )

            return deleted_ids
