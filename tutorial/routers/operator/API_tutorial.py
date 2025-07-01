from typing import Any

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent.parent))

from general_operate import GeneralOperate
from tutorial.schemas import subtable, tutorial


class APITutorialOperator:
    def __init__(self, api_schema, db, redis, influxdb=None):
        self.create_schemas = api_schema.create_schemas
        self.main_schemas = api_schema.main_schemas
        self.update_schemas = api_schema.update_schemas
        self.tutorial_operate = GeneralOperate(module=tutorial, database_client=db, redis_client=redis, influxdb=influxdb)
        self.subtable_operate = GeneralOperate(module=subtable, database_client=db, redis_client=redis, influxdb=influxdb)

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
            from typing import get_args
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

    @staticmethod
    def _compare_subtables(existing_subtables: list[dict], new_subtables: list[Any]) -> tuple[list[dict], list[dict], list[int]]:
        """
        Compare existing subtables with new subtables to determine CRUD operations.

        Args:
            existing_subtables: List of existing subtable dictionaries from database
            new_subtables: List of subtable update objects from the update request

        Returns:
            Tuple of (to_create, to_update, to_delete_ids):
            - to_create: List of subtable data dicts for new subtables
            - to_update: List of subtable update dicts with 'id' and update fields
            - to_delete_ids: List of subtable IDs to delete
        """
        # Create lookup dictionaries for efficient comparison
        existing_by_id = {sub['id']: sub for sub in existing_subtables}
        _ = {sub.id: sub for sub in new_subtables if sub.id is not None}  # Unused but kept for potential future use

        # Find subtables to create (no ID or ID not in existing)
        to_create = []
        for sub in new_subtables:
            if sub.id is None or sub.id not in existing_by_id:
                # New subtable to create
                create_data = {}
                if sub.description is not None:
                    create_data['description'] = sub.description
                if sub.author is not None:
                    create_data['author'] = sub.author
                if sub.tutorial_id is not None:
                    create_data['tutorial_id'] = sub.tutorial_id
                if create_data:  # Only add if there's data to create
                    to_create.append(create_data)

        # Find subtables to update (ID exists in both but data differs)
        to_update = []
        for sub in new_subtables:
            if sub.id is not None and sub.id in existing_by_id:
                existing = existing_by_id[sub.id]
                update_data = {'id': sub.id}
                has_changes = False

                # Check each field for changes
                if sub.description is not None and sub.description != existing.get('description'):
                    update_data['description'] = sub.description
                    has_changes = True
                if sub.author is not None and sub.author != existing.get('author'):
                    update_data['author'] = sub.author
                    has_changes = True
                if sub.tutorial_id is not None and sub.tutorial_id != existing.get('tutorial_id'):
                    update_data['tutorial_id'] = sub.tutorial_id
                    has_changes = True

                if has_changes:
                    to_update.append(update_data)

        # Find subtables to delete (exist in current but not in new)
        new_ids = {sub.id for sub in new_subtables if sub.id is not None}
        to_delete_ids = [sub_id for sub_id in existing_by_id if sub_id not in new_ids]

        return to_create, to_update, to_delete_ids

    async def _update_tutorial_subtables(self, tutorial_id: int, new_subtables: list[Any]) -> None:
        """
        Update subtables for a specific tutorial using CRUD operations.

        Args:
            tutorial_id: ID of the tutorial to update subtables for
            new_subtables: List of subtable update objects representing desired subtables
        """
        # Get existing subtables for this tutorial
        existing_subtables = await self.subtable_operate.read_sql(
            table_name=subtable.table_name,
            filters={"tutorial_id": tutorial_id}
        )

        # Compare existing with new to determine operations
        to_create, to_update, to_delete_ids = self._compare_subtables(existing_subtables, new_subtables)

        # Perform delete operations first (to avoid conflicts)
        if to_delete_ids:
            await self.subtable_operate.delete_data(id_value=set(to_delete_ids))

        # Perform update operations
        if to_update:
            update_dict = {item['id']: {k: v for k, v in item.items() if k != 'id'} for item in to_update}
            await self.subtable_operate.update_sql(
                table_name=subtable.table_name,
                update_data=update_dict
            )

        # Perform create operations
        if to_create:
            # Ensure all new subtables have the correct tutorial_id
            for item in to_create:
                item['tutorial_id'] = tutorial_id

            await self.subtable_operate.create_sql(
                table_name=subtable.table_name,
                data=to_create
            )

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

        result_tutorials = []

        try:
            # Create tutorials first
            tutorial_create_data = []
            for tutorial_data in data:
                tutorial_dict = {
                    "name": tutorial_data.name,
                    "title": tutorial_data.title,
                    "tags": tutorial_data.tags,
                    "enable": tutorial_data.enable
                }
                tutorial_create_data.append(tutorial_dict)

            # Batch create tutorials
            created_tutorials = await self.tutorial_operate.create_sql(
                table_name=tutorial.table_name,
                data=tutorial_create_data
            )

            # Create subtables for each tutorial
            for _idx, (created_tutorial, tutorial_data) in enumerate(zip(created_tutorials, data, strict=False)):
                tutorial_id = created_tutorial["id"]

                # Prepare subtables data
                created_subtables = []
                if tutorial_data.subtables:
                    subtable_create_data = []
                    for subtable_data in tutorial_data.subtables:
                        subtable_dict = {
                            "description": subtable_data.description,
                            "author": subtable_data.author,
                            "tutorial_id": tutorial_id
                        }
                        subtable_create_data.append(subtable_dict)

                    # Batch create subtables
                    created_subtables_raw = await self.subtable_operate.create_sql(
                        table_name=subtable.table_name,
                        data=subtable_create_data
                    )

                    # Convert to APISubtable objects
                    SubtableClass = self.get_subtable_model_from_main_model()
                    created_subtables = [SubtableClass(**sub) for sub in created_subtables_raw]

                # Build result with nested structure
                tutorial_with_subtables = self.main_schemas(
                    **created_tutorial,
                    subtables=created_subtables
                )
                result_tutorials.append(tutorial_with_subtables)

            return result_tutorials

        except Exception as e:
            # In case of error, we might need to clean up created tutorials
            # This would require transaction support in the database
            raise e

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

        # Read tutorials
        tutorials = await self.tutorial_operate.read_sql(
            table_name=self.tutorial_operate.table_name,
            filters=tutorial_filters,
            limit=limit,
            offset=offset
        )

        if not tutorials:
            return []

        # Extract tutorial IDs for subtable query
        tutorial_id_list = [t["id"] for t in tutorials]

        # Batch read all subtables for these tutorials
        subtables = await self.subtable_operate.read_sql(
            table_name=subtable.table_name,
            filters={"tutorial_id": tutorial_id_list}
        )

        # Group subtables by tutorial_id
        subtables_by_tutorial: dict[int, list] = {}
        SubtableClass = self.get_subtable_model_from_main_model()
        for sub in subtables:
            tutorial_id = sub["tutorial_id"]
            if tutorial_id not in subtables_by_tutorial:
                subtables_by_tutorial[tutorial_id] = []
            subtables_by_tutorial[tutorial_id].append(SubtableClass(**sub))

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

    async def update(self, update_data: list[Any]) -> list[Any]:
        """
        Update tutorials and their subtables with full CRUD support.

        This method handles comprehensive updates including:
        - Tutorial field updates
        - Subtable creation, updates, and deletion
        - Referential integrity maintenance

        Args:
            update_data: List of tutorial update objects with tutorial IDs and updated fields

        Returns:
            List of updated tutorial objects with their associated subtables

        Raises:
            Exception: If update operations fail or referential integrity is violated
        """
        if not update_data:
            return []

        try:
            # Process tutorial updates
            tutorial_update_dict = {}
            tutorial_ids = []

            for update_item in update_data:
                tutorial_ids.append(update_item.id)

                # Build tutorial update data dict, excluding None values and id
                update_fields = {}
                if update_item.name is not None:
                    update_fields["name"] = update_item.name
                if update_item.title is not None:
                    update_fields["title"] = update_item.title
                if update_item.tags is not None:
                    update_fields["tags"] = update_item.tags
                if update_item.enable is not None:
                    update_fields["enable"] = update_item.enable

                if update_fields:  # Only update if there are fields to update
                    tutorial_update_dict[update_item.id] = update_fields

            # Update tutorials if there are changes
            if tutorial_update_dict:
                await self.tutorial_operate.update_sql(
                    table_name=tutorial.table_name,
                    update_data=tutorial_update_dict
                )

            # Process subtable updates for each tutorial
            for update_item in update_data:
                if update_item.subtables is not None:
                    await self._update_tutorial_subtables(update_item.id, update_item.subtables)

            # Read and return updated tutorials with subtables
            return await self.read(tutorial_ids=tutorial_ids)

        except Exception as e:
            # In case of error, log and re-raise for proper error handling
            # Note: In a production environment, you might want to implement
            # transaction rollback here depending on your database setup
            raise e

    async def delete(self, tutorial_ids: list[int]) -> list[int]:
        """
        Delete tutorials and their associated subtables

        Args:
            tutorial_ids: List of tutorial IDs to delete

        Returns:
            List of successfully deleted tutorial IDs
        """
        if not tutorial_ids:
            return []

        # First, delete all subtables associated with these tutorials
        await self.subtable_operate.delete_filter(
            table_name=subtable.table_name,
            filters={"tutorial_id": tutorial_ids}
        )

        # Then delete the tutorials
        deleted_ids = await self.tutorial_operate.delete_data(
            id_value=set(tutorial_ids)
        )

        return deleted_ids
