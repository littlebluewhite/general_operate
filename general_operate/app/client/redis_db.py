import asyncio

import redis.asyncio as redis
from redis.cluster import ClusterNode

class RedisDB:
    def __init__(self, redis_config, decode_responses=False):
        self.host = redis_config["host"]
        self.db = redis_config["db"]
        self.username = redis_config["user"]
        self.password = redis_config["password"]
        self.decode_responses = decode_responses
        self.is_cluster = self.__is_redis_cluster()

    def redis_client(self):
        if self.is_cluster:
            return self.__new_cluster()
        else:
            return self.__new_single()

    def __new_cluster(self):
        nodes = [
            ClusterNode(s.strip().split(":")[0], s.strip().split(":")[1])
            for s in self.host.split(",")
        ]
        return redis.RedisCluster(
            startup_nodes=nodes,
            username=self.username,
            password=self.password,
            socket_timeout=5,
            decode_responses=self.decode_responses,
        )

    def __new_single(self):
        host_parts = self.host.strip().split(":")

        return redis.Redis(
            host=host_parts[0],  # Hostname or IP address
            port=int(host_parts[1])
            if len(host_parts) > 1
            else 6379,  # Port or default 6379
            db=self.db,  # Database number (0-15)
            username=self.username,  # Optional username for auth
            password=self.password,  # Optional password for auth
            decode_responses=self.decode_responses,  # Automatic response decoding
            socket_timeout=5,  # 5-second connection timeout
        )

    def __is_redis_cluster(self):
        host = self.host.split(",")
        if len(host) > 1:
            return True
        else:
            return False

# async def wilson():
#     redis_config = {
#         "host": "127.0.0.1:6379",
#         "db": 0,
#         "user": "",
#         "password": "",
#         "decode_responses": "True"
#     }
#     r =CacheOperate(RedisDB(redis_config).redis_client())
#     await r.redis.hsetex("test", mapping=redis_config)
#     await r.redis.expire("test", 10)
#     a = await r.redis.hgetall("test")
#     print(a)

if __name__ == "__main__":
    pass
    # asyncio.run(wilson())