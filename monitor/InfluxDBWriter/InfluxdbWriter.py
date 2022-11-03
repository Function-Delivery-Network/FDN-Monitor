#!/usr/bin/env python
import yaml
from flask import Flask
from decouple import config
from influxdb_client import InfluxDBClient
from influxdb_client.extras import pd, np
import logging
from datetime import datetime
"""
Enable logging for DataFrame serializer
"""
loggerSerializer = logging.getLogger('influxdb_client.client.write.dataframe_serializer')
loggerSerializer.setLevel(level=logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s | %(message)s'))
loggerSerializer.addHandler(handler)

app = Flask(__name__)


class InfluxDBWriter:
    def __init__(self, data: object) -> None:
        
        self.url = 'http://' + data['host'] + ':' + str(data['port'])
        self.token = data['token']
        self.org = data['org']
        self.bucket = data['bucket']
        self.table_functions = data['table_functions']
        self.table_infra = data['table_infra']

    def write_dataframe_influxdb(self, df, data_category):

        """
        Ingest DataFrame
        """
        print()
        print("=== Ingesting DataFrame via batching API ===")
        print()
        startTime = datetime.now()

        with InfluxDBClient(url=self.url, token=self.token, org=self.org) as client:

            """
            Use batching API
            """
            if data_category == "functions_usage":
                with client.write_api() as write_api:
                    write_api.write(bucket=self.bucket, record=df,
                                    data_frame_tag_columns=['cluster_name', 'function_name'],
                                    data_frame_measurement_name=self.table_functions, write_precision='ms')
                    print()
                    print("Wait to finishing ingesting DataFrame...")
                    print()
            elif data_category == "system_usage":
                with client.write_api() as write_api:
                    write_api.write(bucket=self.bucket, record=df,
                                    data_frame_tag_columns=['cluster_name'],
                                    data_frame_measurement_name=self.table_infra, write_precision='ms')
                    print()
                    print("Wait to finishing ingesting DataFrame...")
                    print()
        print()
        print(f'Import finished in: {datetime.now() - startTime}')
        print()
