import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

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
# Note: Table creation should be handled separately for async engines
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
async def alarm_exception_handler(request: Request, exc: GeneralOperateException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"message": f"{exc.message}", "message_code": f"{exc.message_code}"},
        headers={
            "message": f"{exc.message}".replace("\n", " "),
            "message_code": f"{exc.message_code}",
        },
    )


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000)
