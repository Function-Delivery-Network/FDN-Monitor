import sys
import os

import pandas
sys.path.append(os.path.abspath('../'))
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError
import pandas as pd

from Clusters import BaseCollector
import logging

logging.basicConfig(filename='Logs/log.log', format='%(message)s', filemode='w', level=logging.DEBUG)
logger = logging.getLogger(__name__)

from pandas import DataFrame
from typing import List, Tuple

import traceback
import asyncio
import time


class AWSCollector(BaseCollector):

    def __init__(self, aws_access_key_id : str = None, aws_secret_access_key: str = None, region_name: str = None):
        self.aws_access_key_id  = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region = region_name
        self.cloudwatch_client = boto3.client('cloudwatch', aws_access_key_id=self.aws_access_key_id, 
                                              aws_secret_access_key=self.aws_secret_access_key, 
                                              region_name=self.region)
        self.logs_client = boto3.client('logs', aws_access_key_id=self.aws_access_key_id, 
                                              aws_secret_access_key=self.aws_secret_access_key, 
                                              region_name=self.region)
        self.metric_namespace = "AWS/Lambda"
        self.period = 60

# TODO: update column name 
    async def get_and_convert_data_frame(self, start: int, end: int, stat_type: str,
                                         feature_col_name: str, save_feature_col_name: str) -> DataFrame:
        """ Get and Convert the timeseries data values to a dataframe .
        Args:
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
        "SELECT AVG(Duration) FROM \"AWS/Lambda\" GROUP BY FunctionName"
        
        search_query = "SEARCH('{" + self.metric_namespace+",FunctionName} MetricName=\"" + feature_col_name+"\"', '"+stat_type+"', "+str(self.period)+")"
        search_query =  'SELECT ' + stat_type + '(' + feature_col_name + ') FROM SCHEMA("AWS/Lambda", FunctionName) GROUP BY FunctionName ORDER BY '+stat_type+'() DESC'
        
        #search_query =  'SELECT SUM(Invocations) FROM SCHEMA("AWS/Lambda", FunctionName) GROUP BY FunctionName ORDER BY SUM() DESC '
        
        logging.debug("search_query, %s", search_query)
        stats = self.cloudwatch_client.get_metric_data(
            MetricDataQueries=[
                {
                    'Id': 'metric_data',
                    'Expression': search_query,
                    'ReturnData': True,
                    'Period': self.period
                },
            ],
            StartTime=start,
            EndTime=end,
            ScanBy='TimestampDescending'
        )
        values = []
        logging.debug("feature_col_name, %s", feature_col_name)
        #logging.debug("stats, %s", stats)
        
        for record in stats['MetricDataResults']:
            for idx, inner_record in enumerate(record['Timestamps']):
                values.append(
                    {"function_name": record['Label'].split(" ")[-1], "timestamp": inner_record, save_feature_col_name: record['Values'][idx]})

        return DataFrame(values)

    async def collect_invocations(self, start: int, end: int) -> DataFrame:
        """ Collects the number of active instances for GCF Function.
        Args:
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
        Returns:
            DataFrame - Query result as DataFrame - with columns: 'timestamp', 'function_name', 'region', 'memory',
            'active_instances'
        """

        result_df = await self.get_and_convert_data_frame(start, end, 'SUM',
                                                          'Invocations', 'success_invocations')
        logging.debug("frame, %s", 'success_invocations')
        logging.debug(result_df)
        return result_df
    
    async def collect_concurrency_invocations(self, start: int, end: int) -> DataFrame:
        """ Collects the number of active instances for GCF Function.
        Args:
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
        Returns:
            DataFrame - Query result as DataFrame - with columns: 'timestamp', 'function_name', 'region', 'memory',
            'active_instances'
        """

        result_df = await self.get_and_convert_data_frame(start, end, 'AVG',
                                                          'ConcurrentExecutions', 'replicas')
        
        
        if len(result_df.values) > 0:
            result_df['replicas'] = result_df['replicas'].astype('float')
        logging.debug("frame, %s", 'replicas')
        logging.debug(result_df)
        return result_df

    async def collect_post_runtime_duration(self, start: int, end: int) -> DataFrame:
        """ Collects the number of active instances for GCF Function.
        Args:
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
        Returns:
            DataFrame - Query result as DataFrame - with columns: 'timestamp', 'function_name', 'region', 'memory',
            'active_instances'
        """

        result_df = await self.get_and_convert_data_frame(start, end, 'AVG',
                                                          'PostRuntimeExtensionsDuration', 'post_runtime_duration')

        return result_df

    async def collect_execution_times(self, start: int, end: int) -> DataFrame:
        """ Collects the execution times of AWS Lambda functions.
        Args:
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
        Returns:
            DataFrame - Query result as DataFrame - with columns: 'timestamp', 'function_name', 'region', 'memory',
            'execution_times'
        """

        result_df = await self.get_and_convert_data_frame(start, end, 'AVG',
                                                          'Duration', 'average_execution_time')
        if len(result_df.values) > 0:
            result_df["average_execution_time"] = result_df["average_execution_time"]/1000
            
        logging.debug("frame, %s", 'average_execution_time')
        logging.debug(result_df)
        return result_df

    def collect_data_from_logs(self, func_name: str, start: int, end: int):
        
        query = "fields @timestamp, @billedDuration, @duration, @ingestionTime, @maxMemoryUsed, @memorySize"

        log_group = '/aws/lambda/' + func_name

        start_query_response = self.logs_client.start_query(
            logGroupName=log_group,
            startTime=start,
            endTime=end,
            queryString=query,
        )

        query_id = start_query_response['queryId']

        response = None

        while response == None or response['status'] == 'Running':
            print('Waiting for query to complete ...')
            time.sleep(1)
            response = self.logs_client.get_query_results(
                queryId=query_id
            )
        #logging.debug("collect_data_from_logs, %s", response)
        values = []
        if response["results"]:
            for log in response["results"]:
                value = {
                    "timestamp": "",
                    "billed_duration": "",
                    "pods-mem-sum-bytes": "",
                    "pods-mem-limits": ""
                }
                for record in log:

                    if "@timestamp" in record['field']:
                        value["function_name"] = func_name
                        value["timestamp"] = record['value']

                    elif "@billedDuration" in record['field']:
                        value["billed_duration"] = float(record['value'])

                    elif "@maxMemoryUsed" in record['field']:
                        value["pods-mem-sum-bytes"] = float(record['value'])

                    elif "@memorySize" in record['field']:
                        value["pods-mem-limits"] = float(record['value']) 

                if value["billed_duration"]:
                    values.append(value)

            return values
        else:
            values
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
        logging.debug("frame, %s", 'do_frame_postprocessing')
        logging.debug(frame)
        return frame

    async def collect(self, cluster_name: str, start: int, end: int) -> DataFrame:
        """ Collects function active_instances, network_egress, and execution_times for a GoogleCloudTarget.
        Args:
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
            asyncio.create_task(self.collect_execution_times(start, end)),
            asyncio.create_task(self.collect_invocations(start, end)),
            asyncio.create_task(self.collect_concurrency_invocations(start, end)),
            asyncio.create_task(self.collect_post_runtime_duration(start, end)),
        ]
        # wait for all workers
        combined_frame = DataFrame()
        try:
            # wait for max 45 seconds
            for result in asyncio.as_completed(tasks, timeout=45.0):
                frame = await result
                print("Frame collected")
                if frame.empty:
                    continue
                elif combined_frame.empty:
                    combined_frame = frame
                else:
                    combined_frame = pd.merge(combined_frame, frame, on=['timestamp', 'function_name'])

        except Exception as e:
            print("Exception when tyring to query data")
            print(e)
            traceback.print_exc()

        if(len(combined_frame.values) > 0):
            logging.debug("combined_frame_before, %s", combined_frame)
            #combined_frame['timestamp'] = pd.to_datetime(combined_frame['timestamp'], utc=True)
            logging.debug("combined_frame, %s", combined_frame)
            values = []
            try:
                for func_name in combined_frame["function_name"].unique():
                    logging.debug("func_name, %s", func_name)
                    processed_values = self.collect_data_from_logs(func_name, start, end)
                    df = pd.DataFrame(processed_values)
                    df.set_index("timestamp", inplace=True)
                    df.index = pd.to_datetime(df.index, unit='ns')
                    df = df.resample(str(self.period)+'s').mean()
                    df.reset_index(inplace=True)
                    df = df.dropna(how='any')
                    df.reset_index(inplace=True, drop=True)
                    df["function_name"]= func_name
                    logging.debug("frame, %s", 'collect_data_from_logs')
                    logging.debug(df)
                    processed_values = df.to_dict('records')
                    values = values + processed_values
            except Exception as e:
                logging.error(e)
                print("something when wrong", e)
            df = pd.DataFrame(values)
            logging.debug("frame, %s", df)
            logging.debug(df)
            if(len(df.values) > 0 and "timestamp" in df.columns and "function_name" in df.columns):
                df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
                combined_frame = pd.merge(left=combined_frame, right=df, how='inner', right_on=['timestamp', 'function_name'], left_on=['timestamp', 'function_name'])

            combined_frame_functions_usage = self.do_frame_postprocessing(combined_frame, cluster_name, "functions_usage")
            #updated_df = updated_df.rename(columns={"function_name": "function_name", "pods-mem-avg": "pods-mem-sum"})
            result_dict = {'functions_usage': combined_frame_functions_usage}
        else:
            result_dict = {'functions_usage': pd.DataFrame()}
        return result_dict
