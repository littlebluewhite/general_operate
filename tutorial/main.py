import asyncio
import sys
from pathlib import Path
import logfire

# Add project root to Python path first
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from tutorial.SQL.models import Base

from general_operate import GeneralOperateException
from general_operate.app.client.database import SQLClient
from general_operate.app.client.redis_db import RedisDB
from tutorial.routers.API_tutorial import APITutorialRouter

sql_config = {
    "host": "127.0.0.1",
    "port": 5432,
    "user": "root",
    "password": "123456",
    "db": "generaloperate",
    "engine": "postgresql",
}

redis_config = {"host": "127.0.0.1:6379", "db": 13, "user": None, "password": None}

# influxdb_config = {
#     "host": "127.0.0.1",
#     "port": 8086,
#     "org": "my-org",
#     "token": "my-super-influxdb-auth-token",
#     "bucket": "general_operator",
# }

db = SQLClient(sql_config)

# for create sql data
async def init_db():
    async with db.get_engine().begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

redis_db = RedisDB(redis_config).redis_client()
# influxdb_client = InfluxDB(influx_config=influxdb_config, timeout=12000_000)

app = FastAPI(
    version="0.0.1", title="general_operator", description="general_operator tutorial"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(APITutorialRouter(db=db, redis=redis_db).create_router())

@app.exception_handler(GeneralOperateException)
async def operate_exception_handler(request: Request, exc: GeneralOperateException):
    return JSONResponse(
        status_code=500,  # Use 500 as default HTTP status code
        content={
            "error": {
                "code": exc.code.value,
                "name": exc.code.name,
                "message": exc.message,
                "context": exc.context.operation if exc.context else None
            }
        },
        headers={
            "X-Error-Code": str(exc.code.value),
            "X-Error-Name": exc.code.name,
        },
    )
logfire.configure(token="pylf_v1_us_Pm9s7JZVRGvtyVdBbjVxyFKL98nK17k1gCWfBlKzss5M")
logfire.instrument_redis(capture_statement=True)
logfire.instrument_fastapi(app)
logfire.instrument_sqlalchemy(engine=db.get_engine(), enable_commenter=True)

from pydantic import BaseModel
class Item(BaseModel):
    name: str
    description: str | None = None
    price: float
    tax: float | None = None

@app.post("/test")
async def test(body: list[Item]):
    return body

if __name__ == "__main__":
    asyncio.run(init_db())
    uvicorn.run("main:app", host="0.0.0.0", port=9000)
