#!/usr/bin/env python
import subprocess
import sys
import os
sys.path.append(os.path.abspath('../'))
from Clusters import BaseCollector
import logging
import json
import yaml

logging.basicConfig(filename='Logs/log.log', format='%(message)s', filemode='w', level=logging.DEBUG)
logger = logging.getLogger(__name__)

from pandas import DataFrame
import pandas as pd
from google.cloud import monitoring_v3
from google.oauth2 import service_account
from typing import List, Tuple

import traceback
import asyncio
import os
import time


class GCFCollector(BaseCollector):

    def __init__(self, config_object: str = None, config_path: str = None, power_collection: bool = False):
        self.config_object = config_object
        self.config_path = config_path
        self.power_collection = power_collection


    async def get_and_convert_data_frame(self, start: int, end: int,
                                   feature_col_name: str) -> DataFrame:
        """ Get and Convert the timeseries data values to a dataframe .
        Args:
            config_object:
                config_object - The GoogleCloudTarget, for which the collector should use
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
           feature_col_name:
               String - name of the feature column name
        Returns:
           DataFrame - Query result as DataFrame - with columns: 'timestamp', 'function_name', 'region', 'memory',
           'feature_col_name'
        """

        credentials = service_account.Credentials.from_service_account_file(self.config_path)
        client = monitoring_v3.services.metric_service.MetricServiceAsyncClient(credentials=credentials)
        interval = monitoring_v3.TimeInterval(
            {
                "end_time": {"seconds": end, "nanos": 0},
                "start_time": {"seconds": start, "nanos": 0},
            }
        )

        aggregation = monitoring_v3.Aggregation(
            {
                "alignment_period": {"seconds": 60},  # cannnot be  less than 60
                "per_series_aligner": monitoring_v3.Aggregation.Aligner.ALIGN_SUM,
            }
        )
        ts_results = await client.list_time_series(
            request={
                "name": f"projects/{ self.config_object['project_id']}",
                "filter": 'metric.type = "cloudfunctions.googleapis.com/function/' + feature_col_name + '"',
                "interval": interval,
                "view": "FULL",
                "aggregation": aggregation,
            }
        )
        values = []
        #logging.debug("ts_results, %s", ts_results)
        async for ts in ts_results:
            name = ts.resource.labels['function_name']
            region = ts.resource.labels['region']

            if feature_col_name == 'execution_times':
                memory = ts.metric.labels['memory']
                for p in ts.points:
                    t = int(p.interval.start_time.timestamp())
                    execution_time = p.value.distribution_value.mean / 10**9
                    invocations = p.value.distribution_value.count
                    dic = {'timestamp': t, 'function_name': name, 'region': region, 'pods-mem-limits': float(memory),
                           feature_col_name: float(execution_time), 'success_invocations': float(invocations)}
                    values.append(dic)
            elif feature_col_name == 'user_memory_bytes':
                for p in ts.points:
                    t = int(p.interval.start_time.timestamp())
                    mem_usage = p.value.distribution_value.mean # /(1024*1024)
                    dic = {'timestamp': t, 'function_name': name, 'region': region,
                           feature_col_name: float(mem_usage)}
                    values.append(dic)
                #logging.debug("frame, %s", feature_col_name)
                #logging.debug(values)
            elif feature_col_name == 'active_instances':
                for p in ts.points:
                    t = int(p.interval.start_time.timestamp()) - 60
                    v = p.value.int64_value
                    dic = {'timestamp': t, 'function_name': name, 'region': region, feature_col_name: float(v)}
                    values.append(dic)
                #logging.debug("frame, %s", feature_col_name)
                #logging.debug(values)
            else:
                for p in ts.points:
                    t = int(p.interval.start_time.timestamp())
                    v = p.value.int64_value
                    dic = {'timestamp': t, 'function_name': name, 'region': region, feature_col_name: float(v)}
                    values.append(dic)
                #logging.debug("frame, %s", feature_col_name)
                #logging.debug(values)

        return DataFrame(values)

    async def collect_active_instances(self, start: int, end: int) -> DataFrame:
        """ Collects the number of active instances for GCF Function.
        Args:
            config_object:
                config_object - The GoogleCloudTarget, for which the collector should use
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
        Returns:
            DataFrame - Query result as DataFrame - with columns: 'timestamp', 'function_name', 'region', 'memory',
            'active_instances'
        """

        result_df = await self.get_and_convert_data_frame(start, end, 'active_instances')
        result_df.rename(columns = {'active_instances':'replicas'}, inplace = True)
        if len(result_df.values) > 0:
            result_df['replicas'] = result_df['replicas'].astype('float')
        logging.debug("frame, %s", 'active_instances')
        logging.debug(result_df)
        
        return result_df

    async def collect_network_egress(self, start: int, end: int) -> DataFrame:
        """ Collects the network egress usage from GCF Function.
        Args:
            target:
                Target - The GoogleCloudTarget, for which the collector should use
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
        Returns:
            DataFrame - Query result as DataFrame - with columns: 'timestamp', 'function_name', 'region', 'memory',
            'network_egress'
        """

        result_df = await self.get_and_convert_data_frame(start, end, 'network_egress')
        result_df.rename(columns = {'network_egress':'pods-network-transmit-bytes'}, inplace = True)
        logging.debug("frame, %s", 'pods-network-transmit-bytes')
        logging.debug(result_df)        
        return result_df

    async def collect_execution_times(self, start: int, end: int) -> DataFrame:
        """ Collects the execution times of GCF Functions.
        Args:
            target:
                Target - The GoogleCloudTarget, for which the collector should use
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
        Returns:
            DataFrame - Query result as DataFrame - with columns: 'timestamp', 'function_name', 'region', 'memory',
            'execution_times'
        """

        result_df = await self.get_and_convert_data_frame(start, end, 'execution_times')
        result_df.rename(columns = {'execution_times':'average_execution_time'}, inplace = True)
        logging.debug("frame, %s", 'average_execution_time')
        logging.debug(result_df)

        return result_df

    async def collect_user_memory_bytes(self, start: int, end: int) -> DataFrame:
        """ Collects the memory usage from GCF Function.
        Args:
            target:
                Target - The GoogleCloudTarget, for which the collector should use
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
        Returns:
            DataFrame - Query result as DataFrame - with columns: 'timestamp', 'function_name', 'region', 'memory',
            'user_memory_bytes'
        """

        result_df = await self.get_and_convert_data_frame(start, end, 'user_memory_bytes')
        result_df.rename(columns = {'user_memory_bytes':'pods-mem-sum-bytes'}, inplace = True)
        logging.debug("frame, %s", 'pods-mem-sum-bytes')
        logging.debug(result_df)

        return result_df

    def change_function_name(self, name):
        name = name.replace("-", "")
        return name
    
    def do_frame_postprocessing(self, frame: DataFrame, cluster_name: str, measurement_category: str) -> DataFrame:
        """ Performs postprocessing on dataframes.
        These are:
            - apply the target name
            - fill N/A values with 0
            - set timestamp as index
            - set the measurement_category (e.g. system resource, function usage)
        Args:
            frame:
                DataFrame - The frame which should be processed
            cluster_name:
                String - Name of the CLUSTER
            measurement_category:
                String - Measurement category

        Returns:
            DataFrame - The processed DataFrame with the columns 'timestamp', 'cluster_name', 'measurement_category' and measurement fields(s) (and 'function_name' if applicable)
        """
        if frame.empty:
            return frame

        frame.reset_index(inplace=True, drop=True)
        frame.set_index("timestamp", inplace=True)
        frame.index = pd.to_datetime(frame.index, unit='s')
        frame['cluster_name'] = cluster_name
        frame['measurement_category'] = measurement_category
        frame['function_name'] = frame.apply(lambda x: self.change_function_name(x['function_name']),
                                            axis=1)
        # frame.fillna(0, inplace=True)
        logging.debug("frame, %s", measurement_category)
        logging.debug(frame)
        return frame

    async def collect(self, cluster_name: str, start: int, end: int) -> DataFrame:
        """ Collects function active_instances, network_egress, and execution_times for a GoogleCloudTarget.
        Args:
            config_object:
                object - The Target, for which the prometheus should be queried
            cluster_name:
                String - The name of the cluster
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
        Returns:
            DataFrame - Query result as DataFrame - with columns: 'timestamp', 'target', 'function_name' and measurement fields(s)
        """

        # start each worker
        tasks: List[asyncio.Task] = [
            asyncio.create_task(self.collect_network_egress(start, end)),
            asyncio.create_task(self.collect_execution_times(start, end)),
            asyncio.create_task(self.collect_user_memory_bytes(start, end)),
            asyncio.create_task(self.collect_active_instances(start, end))
        ]

        # wait for all workers
        combined_frame = DataFrame()
        try:
            # wait for max 45 seconds
            for result in asyncio.as_completed(tasks, timeout=45.0):
                frame = await result

                if combined_frame.empty and len(frame.values)>0 and "timestamp" in frame.columns and "function_name" in frame.columns and "region" in frame.columns:
                    combined_frame = frame
                else:
                    #frame = frame[frame["function_name"] == "mytestservicegcf1-dev-nodeinfo"]
                    if len(frame.values)>0 and "timestamp" in frame.columns and "function_name" in frame.columns and "region" in frame.columns:
                        combined_frame = pd.merge(combined_frame, frame, on=['timestamp', 'function_name', 'region'])

        except Exception as e:
            print("Exception when tyring to query data")
            print(e)
            traceback.print_exc()

        combined_frame_functions_usage =  self.do_frame_postprocessing(combined_frame, cluster_name, "function_usage")
        result_dict = {'functions_usage': combined_frame_functions_usage}

        return result_dict
