from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from general_operate import GeneralOperateException
from tutorial.routers.operator import api_schemas
from tutorial.routers.operator.API_tutorial import APITutorialOperator


class APITutorialRouter(APITutorialOperator):
    def __init__(self, db, redis, exc=GeneralOperateException):
        self.__exc = exc
        APITutorialOperator.__init__(self, api_schema=api_schemas, db=db, redis=redis)

    def create_router(self):
        create_schemas = self.create_schemas
        update_schemas = self.update_schemas
        main_schemas = self.main_schemas
        router = APIRouter(prefix="/tutorial", tags=["tutorial"], dependencies=[])

        @router.get("", tags=[])
        async def get_tutorials():
            """
            Get all tutorials with their associated subtables.
            
            Returns:
                list[dict]: List of all tutorial objects with nested subtables
            """
            try:
                tutorials = await self.read()
                # Convert Pydantic models to dictionaries for JSON response
                return [tutorial.model_dump() if hasattr(tutorial, 'model_dump') else tutorial.dict() for tutorial in tutorials]
            except self.__exc as e:
                raise e
            except Exception as e:
                # Log error and return empty list for graceful degradation
                print(f"Error retrieving tutorials: {e}")
                return []

        @router.get("/id", tags=[])
        async def get_tutorial_by_id(id_set: set[int] = Query(...)):
            """
            Get tutorials by their IDs.
            
            Args:
                id_set: Set of tutorial IDs to retrieve
                
            Returns:
                list[dict]: List of tutorial objects matching the provided IDs
            """
            try:
                if not id_set:
                    return JSONResponse(content=[], status_code=200)
                
                tutorials = await self.read(tutorial_ids=list(id_set))
                result = [tutorial.model_dump() if hasattr(tutorial, 'model_dump') else tutorial.dict() for tutorial in tutorials]
                return JSONResponse(content=result, status_code=200)
            except self.__exc as e:
                raise e
            except Exception as e:
                print(f"Error retrieving tutorials by ID: {e}")
                return JSONResponse(content={"error": "Failed to retrieve tutorials"}, status_code=500)

        @router.post("", tags=[])
        async def create_tutorials(create_data: list[create_schemas]):
            """
            Create new tutorials with their subtables.
            
            Args:
                create_data: List of tutorial creation objects
                
            Returns:
                list[dict]: List of created tutorial objects with generated IDs
            """
            try:
                if not create_data:
                    return JSONResponse(content=[], status_code=201)
                
                created_tutorials = await self.create(create_data)
                result = [tutorial.model_dump() if hasattr(tutorial, 'model_dump') else tutorial.dict() for tutorial in created_tutorials]
                return JSONResponse(content=result, status_code=201)
            except self.__exc as e:
                raise e
            except Exception as e:
                print(f"Error creating tutorials: {e}")
                return JSONResponse(content={"error": "Failed to create tutorials"}, status_code=500)

        @router.patch("", tags=[])
        async def update_tutorials(update_data: list[update_schemas]):
            """
            Update existing tutorials with partial data.
            
            Args:
                update_data: List of tutorial update objects with IDs and updated fields
                
            Returns:
                list[dict]: List of updated tutorial objects
            """
            try:
                if not update_data:
                    return JSONResponse(content=[], status_code=200)
                
                updated_tutorials = await self.update(update_data)
                result = [tutorial.model_dump() if hasattr(tutorial, 'model_dump') else tutorial.dict() for tutorial in updated_tutorials]
                return JSONResponse(content=result, status_code=200)
            except self.__exc as e:
                raise e
            except Exception as e:
                print(f"Error updating tutorials: {e}")
                return JSONResponse(content={"error": "Failed to update tutorials"}, status_code=500)

        @router.delete("", tags=[])
        async def delete_tutorials(id_set: set[int] = Query(...)):
            """
            Delete tutorials by their IDs.
            
            Args:
                id_set: Set of tutorial IDs to delete
                
            Returns:
                list[dict]: List of successfully deleted tutorial IDs
            """
            try:
                if not id_set:
                    return JSONResponse(content=[], status_code=200)
                
                deleted_ids = await self.delete(list(id_set))
                result = [{"id": tutorial_id} for tutorial_id in deleted_ids]
                return JSONResponse(content=result, status_code=200)
            except self.__exc as e:
                raise e
            except Exception as e:
                print(f"Error deleting tutorials: {e}")
                return JSONResponse(content={"error": "Failed to delete tutorials"}, status_code=500)

        return router
