import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS


class InfluxDB:
    def __init__(self, influx_config, timeout=10_000):
        self.host = influx_config["host"]
        self.port = influx_config["port"]
        self.org = influx_config["org"]
        self.token = influx_config["token"]
        self.bucket = influx_config["bucket"]
        self.url = f"http://{self.host}:{self.port}"
        self.client = influxdb_client.InfluxDBClient(
            url=self.url, token=self.token, org=self.org, timeout=timeout
        )
        self.write = self.client.write_api(write_options=SYNCHRONOUS)
        self.query = self.client.query_api()
