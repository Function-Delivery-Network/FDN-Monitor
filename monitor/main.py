#!/usr/bin/env python
from pickle import TRUE
from flask import Flask, Response, request
from typing import List
import asyncio
import requests
from decouple import config
from Clusters import BaseCollector
from Clusters import OpenFaasCollector
from Clusters import OpenWhiskCollector
from Clusters import GCFCollector
from Clusters import AWSCollector
from datetime import datetime
from PeriodicAsync import PeriodicAsyncThread
from InfluxDBWriter import InfluxDBWriter
import logging
from logging.handlers import RotatingFileHandler
import sys
from minio import Minio
import pandas as pd
import json
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        RotatingFileHandler("Logs/log.log", maxBytes=1e9, backupCount=2),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

app = Flask(__name__)
loop = asyncio.get_event_loop()
apt = PeriodicAsyncThread(int(config('DEFAULT_LOGGING_PERIOD')))

default_config = {
    'step': 60,
    'interval': '1m',
}


class CollectData:
    def __init__(self) -> None:

        self.step = default_config["step"]
        self.interval = default_config["interval"]

        # INFLUXDB configuration
        self.influx_config = {
            "host": config('INFLUXDB_HOST'),
            "port": int(config('INFLUXDB_PORT')),
            "token": config('INFLUXDB_ADMIN_TOKEN'),
            "org": config('INFLUXDB_ORG'),
            "bucket": config('INFLUXDB_BUCKET'),
            "table_functions": config('INFLUXDB_TABLE_FUNCTIONS'),
            "table_infra": config('INFLUXDB_TABLE_INFRA')
        }

        self.influx_db_writer_obj = InfluxDBWriter(self.influx_config)

        # Cluster Configuration
        self.cluster_type = config('CLUSTER_TYPE')
        self.cluster_name = config('CLUSTER_NAME')
        self.power_collection = False

        if config('POWER_COLLECTION') == "True":
            self.power_collection = True

        if self.cluster_type == "OPENFAAS":
            self.cluster_auth = config('CLUSTER_AUTH')
            self.cluster_host = config('CLUSTER_HOST')
            self.cluster_username = config('CLUSTER_USERNAME')
            self.cluster_apigw_access_token = config(
                'CLUSTER_API_GW_ACCESS_TOKEN')
            self.cluster_gateway_port = config('CLUSTER_GATEWAY_PORT')
            self.cluster_serverless_platform_prometheus_port = config(
                'CLUSTER_SERVERLESS_PLATFROM_PROMETHEUS_PORT')
            self.cluster_kubernetes_prometheus_port = config(
                'CLUSTER_KUBERNETES_PROMETHEUS_PORT')
            self.cluster_collector_obj = OpenFaasCollector("http://" + self.cluster_host + ':' + str(self.cluster_serverless_platform_prometheus_port),
                                                           "http://" + self.cluster_host + ':' +
                                                           str(self.cluster_kubernetes_prometheus_port),
                                                           self.power_collection,
                                                           self.step,
                                                           self.interval)

        elif self.cluster_type == "OPENWHISK":
            self.cluster_auth = config('CLUSTER_AUTH')
            self.cluster_host = config('CLUSTER_HOST')
            self.cluster_username = config('CLUSTER_USERNAME')
            self.cluster_apigw_access_token = config(
                'CLUSTER_API_GW_ACCESS_TOKEN')
            self.cluster_gateway_port = config('CLUSTER_GATEWAY_PORT')
            self.cluster_api_host_port = self.cluster_host + \
                ':' + str(self.cluster_gateway_port)
            self.cluster_serverless_platform_prometheus_port = config(
                'CLUSTER_SERVERLESS_PLATFROM_PROMETHEUS_PORT')
            self.cluster_kubernetes_prometheus_port = config(
                'CLUSTER_KUBERNETES_PROMETHEUS_PORT')
            self.cluster_collector_obj = OpenWhiskCollector("http://" + self.cluster_host + ':' + str(self.cluster_serverless_platform_prometheus_port),
                                                            "http://" + self.cluster_host + ':' + str(self.cluster_kubernetes_prometheus_port), 
                                                            self.power_collection,
                                                            self.step,
                                                            self.interval)

        elif self.cluster_type == "GCF":
            self.minio_host = config('MINIO_ENDPOINT')
            self.minio_access_key = config('MINIO_ACCESS_KEY')
            self.minio_secret_key = config('MINIO_SECRET_KEY')
            self.cluster_config_bucket = config('CLUSTER_CONFIG_BUCKET')
            self.cluster_config_object = config('CLUSTER_CONFIG_OBJECT')

            self.MINIO_CLIENT = Minio(self.minio_host,
                                      self.minio_access_key,
                                      self.minio_secret_key,
                                      secure=False)

            get = self.MINIO_CLIENT.fget_object(
                self.cluster_config_bucket, self.cluster_config_object, '/tmp/' + self.cluster_config_object)

            # Opening JSON file
            f = open('/tmp/' + self.cluster_config_object)

            # returns JSON object as
            # a dictionary
            config_object = json.load(f)
            logging.debug('project_id %s', config_object['project_id'])
            self.cluster_collector_obj = GCFCollector(
                config_object, '/tmp/' + self.cluster_config_object, False)

        elif self.cluster_type == "AWS":
            self.aws_secret_access_key = config('AWS_SECRET_ACCESS_KEY')
            self.aws_access_key_id = config('AWS_ACCESS_KEY_ID')
            self.cluster_region = config('CLUSTER_REGION')
            self.cluster_collector_obj = AWSCollector(self.aws_access_key_id, self.aws_secret_access_key, self.cluster_region)

    async def collect_from_clusters(self) -> None:

        dt = datetime.now()
        seconds = int(dt.strftime('%s'))
        
        if self.cluster_type == "OPENFAAS":
            logging.debug('cluster_host %s', self.cluster_host)
            logging.debug('cluster_host %s', self.cluster_host)
            logging.debug('seconds %s', str(seconds))
            logging.debug('cluster_auth %s', self.cluster_auth)
            logging.debug('cluster_username %s', self.cluster_username)
            logging.debug('cluster_gateway_port %s', self.cluster_gateway_port)
            logging.debug('cluster_serverless_platform_prometheus_port %s',
                          self.cluster_serverless_platform_prometheus_port)
            logging.debug('cluster_kubernetes_prometheus_port %s',
                          self.cluster_kubernetes_prometheus_port)

        elif self.cluster_type == "OPENWHISK":
            logging.debug('cluster_host %s', self.cluster_host)
            logging.debug('cluster_host %s', self.cluster_host)
            logging.debug('seconds %s', str(seconds))
            logging.debug('cluster_auth %s', self.cluster_auth)
            logging.debug('cluster_apigw_access_token %s',
                          self.cluster_apigw_access_token)
            logging.debug('cluster_gateway_port %s', self.cluster_gateway_port)
            logging.debug('cluster_serverless_platform_prometheus_port %s',
                          self.cluster_serverless_platform_prometheus_port)
            logging.debug('cluster_kubernetes_prometheus_port %s',
                          self.cluster_kubernetes_prometheus_port)

        elif self.cluster_type == "GCF":
            logging.debug('minio_host %s', self.minio_host)
            logging.debug('seconds %s', str(seconds))
            logging.debug('minio_access_key %s', self.minio_access_key)
            logging.debug('minio_secret_key %s', self.minio_secret_key)

        elif self.cluster_type == "AWS":
            logging.debug('aws_access_key_id %s', self.aws_access_key_id)
            logging.debug('seconds %s', str(seconds))
            logging.debug('aws_secret_access_key %s', self.aws_secret_access_key)
            logging.debug('cluster_region %s', self.cluster_region)
            
        data_list = await self.cluster_collector_obj.collect(self.cluster_name, seconds - 5*60, seconds)
        logging.debug("cluster_type %s", self.cluster_type)
        if len(data_list) > 0:
            if "functions_usage" in data_list:
                logging.debug("functions_usage")
                logging.debug("%s", data_list["functions_usage"])
            if "system_usage" in data_list:
                logging.debug("system_usage")
                logging.debug("%s", data_list["system_usage"])
            if "functions_usage" in data_list and not data_list["functions_usage"].empty:
                self.influx_db_writer_obj.write_dataframe_influxdb(
                    data_list["functions_usage"], "functions_usage")
            if "system_usage" in data_list and not data_list["system_usage"].empty:
                self.influx_db_writer_obj.write_dataframe_influxdb(
                    data_list["system_usage"], "system_usage")

    async def collect_data(self) -> None:
        await self.collect_from_clusters()
        logging.debug("All deployment/removal finished")


async def collect_data_interface():
    collect_data_obj = CollectData()
    await collect_data_obj.collect_data()


@app.route('/start')
def start_collection():
    response = Response("Data collection has been started")

    @response.call_on_close
    def on_close():
        loop.create_task(apt.invoke_forever(collect_data_interface))
        loop.run_forever()

    return response


@app.route('/stop')
def stop_collection():
    loop.stop()
    return {
        "message": "data collection has been stopped",
    }


if __name__ == '__main__':
    print("Logging metrics")

    SEND_CLUSTER_INFO = False
    if SEND_CLUSTER_INFO:
        url = 'http://' + \
            config('FDN_HOST') + ':' + \
            config('CLUSTER_REGISTER_SERVICE_PORT')+'/api/clusters'
        myobj = {'name': config('CLUSTER_NAME'), 'platform': config('CLUSTER_TYPE').lower(), 'host': config('CLUSTER_HOST'),
                 'auth': config('CLUSTER_AUTH'), 'port': str(config('CLUSTER_GATEWAY_PORT'))}

        x = requests.post(url, data=myobj, auth=(config(
            'FDN_CLUSTER_SERVICE_ADMIN_USERNAME'), 'FDN_CLUSTER_SERVICE_ADMIN_PASSWORD'))

        print(x.text)

    loop.create_task(apt.invoke_forever(collect_data_interface))
    loop.run_forever()
    app.run(debug=True, host='0.0.0.0', port=3005)
