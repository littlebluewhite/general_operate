import functools

import influxdb_client
from influxdb_client.client.write_api import WriteOptions
from influxdb_client.rest import ApiException
from urllib3.exceptions import NewConnectionError

from ..core.exceptions import InfluxDBException, ErrorCode, ErrorContext
from .client.influxdb import InfluxDB


class InfluxOperate:
    def __init__(self, influxdb: InfluxDB = None):
        self.influxdb = influxdb
        self.__exc = InfluxDBException

        if influxdb is not None:
            self.__bucket = influxdb.bucket
            self.__org = influxdb.org
            self.writer = self.influxdb.client.write_api(
                write_options=WriteOptions(
                    batch_size=1000,
                    flush_interval=10_000,
                    jitter_interval=2_000,
                    retry_interval=5_000,
                    max_retries=5,
                    max_retry_delay=30_000,
                    max_close_wait=300_000,
                    exponential_base=2,
                )
            )
            self.reader = self.influxdb.client.query_api()
        else:
            self.__bucket = None
            self.__org = None
            self.writer = None
            self.reader = None

    @staticmethod
    def exception_handler(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except NewConnectionError as e:
                raise self.__exc(
                    code=ErrorCode.INFLUXDB_CONNECTION_ERROR,
                    message=f"InfluxDB connection failed: {str(e)}",
                    context=ErrorContext(operation=func.__name__, resource="influxdb"),
                    cause=e
                )
            except ApiException as e:
                raise self.__exc(
                    code=ErrorCode.INFLUXDB_QUERY_ERROR,
                    message=str(e.message).replace("\n", ""),
                    context=ErrorContext(operation=func.__name__, resource="influxdb", details={"api_status": e.status}),
                    cause=e
                )
            except (ValueError, TypeError) as e:
                # Data validation errors
                raise self.__exc(
                    code=ErrorCode.VALIDATION_ERROR,
                    message=f"InfluxDB data validation error: {str(e)}",
                    context=ErrorContext(operation=func.__name__, resource="influxdb"),
                    cause=e
                )
            except AttributeError as e:
                # Attribute access errors (missing client, etc)
                raise self.__exc(
                    code=ErrorCode.CONFIGURATION_ERROR,
                    message=f"InfluxDB configuration error: {str(e)}",
                    context=ErrorContext(operation=func.__name__, resource="influxdb"),
                    cause=e
                )
            except Exception as e:
                # Log unexpected errors with details
                import logging
                logger = logging.getLogger(__name__)
                logger.error(f"Unexpected InfluxDB error: {type(e).__name__}: {str(e)}", exc_info=True)
                raise self.__exc(
                    code=ErrorCode.UNKNOWN_ERROR,
                    message=f"InfluxDB error: {type(e).__name__}: {str(e)}",
                    context=ErrorContext(operation=func.__name__, resource="influxdb"),
                    cause=e
                )

        return wrapper

    def change_bucket(self, bucket: str):
        self.__bucket = bucket

    def change_org(self, org: str):
        self.__org = org

    def show_bucket(self):
        return self.__bucket

    def show_org(self):
        return self.__org

    @exception_handler
    def write(self, p: influxdb_client.Point | list[influxdb_client.Point]):
        """_summary_

        Args:
            p (influxdb_client.Point | list[influxdb_client.Point]): _description_
            
        ex:
        p = influxdb_client.Point(
            "object_value").tag("id", str(_id)) \
            .tag("uid", str(uid)) \
            .field("value", str(value))
        """
        if self.writer is None:
            raise self.__exc(
                code=ErrorCode.CONFIGURATION_ERROR,
                message="InfluxDB is not configured",
                context=ErrorContext(operation="write", resource="influxdb", details={"missing": "writer"})
            )

        self.writer.write(bucket=self.__bucket, org=self.__org, record=p)

    @exception_handler
    def query(self, q: str):
        """_summary_

        Args:
            q (str): _description_

        ex:
        query = f'''from(bucket:"node_object")
        |> range(start: {start}, stop: {end})
        |> filter(fn:(r) => r._measurement == "object_value")
        |> filter(fn:(r) => r.id == "{_id}")
        |> filter(fn:(r) => r._field == "value")'''
        """
        if self.reader is None:
            raise self.__exc(
                code=ErrorCode.CONFIGURATION_ERROR,
                message="InfluxDB is not configured",
                context=ErrorContext(operation="query", resource="influxdb", details={"missing": "reader"})
            )

        data = self.reader.query(org=self.__org, query=q)
        return data
