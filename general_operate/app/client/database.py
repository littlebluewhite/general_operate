from sqlalchemy.ext.asyncio.engine import create_async_engine


class SQLClient:
    def __init__(self, db_config: dict, echo=True):
        self.engine_type = db_config["engine"]
        self.host = db_config["host"]
        self.port = db_config["port"]
        self.db = db_config["db"]
        self.user = db_config["user"]
        self.password = db_config["password"]
        self.pool_recycle = db_config.get("pool_recycle", 3600)

        self.url = (
            f"mysql+pymysql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.db}"
        )

        if self.engine_type.lower() == "postgresql":
            # PostgreSQL with asyncpg driver for optimal performance
            self.url = f"postgresql+asyncpg://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"
        else:  # Default to MySQL
            # MySQL with aiomysql driver for async support
            self.url = f"mysql+aiomysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"

        print(self.url)
        self.__engine = create_async_engine(
            self.url,  # Database connection URL
            echo=echo,  # SQL query logging (False for production)
            pool_size=10,  # Base connection pool size
            max_overflow=20,  # Additional connections during high load
            pool_recycle=self.pool_recycle,  # Connection recycling interval
        )

    def get_engine(self):
        return self.__engine

if __name__ == "__main__":
    pass
