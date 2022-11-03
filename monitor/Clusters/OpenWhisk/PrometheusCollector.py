#!/usr/bin/env python
from abc import abstractmethod
from aiohttp_requests import requests
import pandas as pd
from pandas import DataFrame
from Clusters import BaseCollector
import logging

logging.basicConfig(filename='Logs/log.log',
                    format='%(message)s', filemode='w', level=logging.DEBUG)
logger = logging.getLogger(__name__)


class PrometheusCollector(BaseCollector):

    def __init__(self, prometheus_url: str):
        """ Collects usage data from the openwhisk cluster
        Args:
            prometheus_url:
                String - the url where we can find the prometheus instnace
        """

        self.prometheus_url = prometheus_url

        self.query_base = "/api/v1/query_range?query="
        self.query_base_without_ts = "/api/v1/query?query="
        
    @staticmethod
    def parse_result_to_dataframe_without_ts(measurement_category: str, measurement_field_name: str, response,
                                  multiple_actions: bool = False,
                                  action_field: str = 'action') -> DataFrame:
        """ Parses a result from the prometheus instance to a DataFrame.
        Args:
            measurement_category:
                String - Name of the measurement category (=functions_usage, system_usage
            measurement_field_name:
                String - Name of the measurement field (=values from query result)
            response:
                Object - Parsed from the prometheus reponse
            multiple_actions:
                Boolean, optional - Indicates if the result contains measurement fields from different actions.
                Default: False
            action_field:
                String - action_field

        Returns:
            DataFrame - A DataFrame with the columns 'timestamp' and measurement_name (and action if
            multiple_actions is set)
        """
        result = response['data']['result']
        
        if len(result) == 0:
            return DataFrame()

        frame = DataFrame()

        if measurement_category == 'system_usage':
            frames = []
            for metric in result:
                frame = DataFrame(columns=[measurement_field_name])
                frame.loc[0] = [metric['value'][1]]

                frame["node"] = "test"
                if "node" in metric["metric"]:
                    frame["node"] = metric["metric"]["node"]
                        
                frame[measurement_field_name] = frame[measurement_field_name].apply(
                    lambda v: float(v))
                
                frames.append(frame)

            frame = pd.concat(frames, join="inner")
        else:
            frames = []

            for metric in result:
                new_frame = DataFrame(columns=[measurement_field_name])
                new_frame.loc[0] = [metric['value'][1]]
                if "pod" in metric['metric'].keys():
                    fun_name_arr = metric['metric']["pod"].split("-")
                    new_frame['function_name'] = '-'.join(
                        fun_name_arr[:-2]) + '.openfaas-fn'
                else:
                    new_frame['function_name'] = "None"

                frames.append(new_frame)

            frame = pd.concat(frames, join="inner")
        frame[measurement_field_name] = frame[measurement_field_name].apply(lambda v: float(v))
        return frame
    
    @staticmethod
    def parse_result_to_dataframe(measurement_category: str, measurement_field_name: str, response,
                                  multiple_actions: bool = False,
                                  action_field: str = 'action') -> DataFrame:
        """ Parses a result from the prometheus instance to a DataFrame.
        Args:
            measurement_category:
                String - Name of the measurement category (=functions_usage, system_usage
            measurement_field_name:
                String - Name of the measurement field (=values from query result)
            response:
                Object - Parsed from the prometheus reponse
            multiple_actions:
                Boolean, optional - Indicates if the result contains measurement fields from different actions.
                Default: False
            action_field:
                String - action_field

        Returns:
            DataFrame - A DataFrame with the columns 'timestamp' and measurement_name (and action if
            multiple_actions is set)
        """

        result = response['data']['result']
        if len(result) == 0:
            return DataFrame()

        frame = DataFrame()
        
        if measurement_category == 'system_usage':
            frames = []
            for metric in result:
                frame = DataFrame(metric['values'], columns=['timestamp', measurement_field_name])

                frame["node"] = "test"
                if "instance" in metric["metric"]:
                    frame["node"] = metric["metric"]["instance"]
                        
                frame[measurement_field_name] = frame[measurement_field_name].apply(
                    lambda v: float(v))
                
                frames.append(frame)

            frame = pd.concat(frames, join="inner")
        else:
            frames = []

            for metric in result:
                new_frame = DataFrame(metric['values'], columns=[
                                        'timestamp', measurement_field_name])
                new_frame["node"] = "test"
                
                if "node" in metric["metric"]:
                    new_frame["node"] = metric["metric"]["node"]
                if measurement_field_name == 'replicas' or measurement_field_name == 'pods-cpu-sum' or \
                    measurement_field_name == 'pods-mem-sum-bytes' or measurement_field_name == 'pods-file-descp-sum' or \
                    measurement_field_name == 'pods-cpu-requests' or measurement_field_name == 'pods-cpu-limits' or\
                    measurement_field_name == 'pods-iops-reads-sum' or measurement_field_name == 'pods-iops-writes-sum' or\
                    measurement_field_name == 'pods-network-transmit-bytes' or measurement_field_name == 'pods-network-receive-bytes' or \
                    measurement_field_name == 'pods-fs-write-bytes' or measurement_field_name == 'pods-fs-read-bytes':
                    if "pod" in metric['metric'].keys():
                        fun_name_arr = metric['metric']["pod"].split("-guest-")
                        new_frame['function_name'] = fun_name_arr[-1].replace('-', '')
                    else:
                        new_frame['function_name'] = "None" 
                        
                else:   
                    func_name  = metric['metric'][action_field].replace('-', '')
                    new_frame['function_name'] = func_name

                # if "kubernetes_pod_name" in metric['metric'].keys():
                #     new_frame['kubernetes_pod_name'] = metric['metric']["kubernetes_pod_name"]
                
                
                # elif "kubernetes_pod_name" not in metric['metric'].keys() and "pod" in metric['metric'].keys():
                #     new_frame['kubernetes_pod_name'] = metric['metric']["pod"]

                frames.append(new_frame)

            frame = pd.concat(frames, join="inner")
            
        frame[measurement_field_name] = frame[measurement_field_name].apply(lambda v: float(v))
        logging.debug("frame, %s", measurement_field_name)
        logging.debug(frame)
        return frame

    def do_frame_postprocessing(self, frame: DataFrame, target_name: str, measurement_category: str) -> DataFrame:
        """ Performs postprocessing on dataframes.
        These are:
            - apply the target name
            - fill N/A values with 0
            - set timestamp as index
            - set the measurement_category (e.g. system resource, function usage)
        Args:
            frame:
                DataFrame - The frame which should be processed
            target_name:
                String - Name of the target
            measurement_category:
                String - Measurement category

        Returns:
            DataFrame - The processed DataFrame with the columns 'timestamp', 'target', 'measurement_category'
            and measurement fields(s) (and 'action' if applicable)
        """
        if frame.empty:
            return frame

        if "timestamp" in frame.columns or "timestamp" in frame.index.names:
            if measurement_category == 'system_usage':
                if "timestamp" in frame.columns:
                    frame.reset_index(inplace=True, drop=True)
                if "timestamp" not in frame.index.names:
                    frame.set_index("timestamp", inplace=True)
                    
                #frame.drop_duplicates(subset=['timestamp'], keep='first', inplace=True)
                frame.index = pd.to_datetime(frame.index, unit='s')
                frame['cluster_name'] = target_name
                frame['measurement_category'] = measurement_category
                

                # frame.fillna(0, inplace=True)
            else:
                frame = frame[frame.function_name != 'None'].copy()
                if "timestamp" in frame.columns:
                    frame.reset_index(inplace=True, drop=True)
                #frame.drop_duplicates(subset=['timestamp', 'function_name'], keep='first', inplace=True)
                if "timestamp" not in frame.index.names:
                    frame.set_index("timestamp", inplace=True)
                    
                frame.index = pd.to_datetime(frame.index, unit='s')
                frame['cluster_name'] = target_name
                frame['measurement_category'] = measurement_category
                frame.fillna(0, inplace=True)

            return frame
        else:
            return DataFrame()
        
    async def query_prometheus_without_ts(self, query: str, measurement_category: str, measurement_field_name: str,
                               multiple_actions: bool = False, action_field: str = "action") -> DataFrame:
        """ Queries the configured Prometheus instance with a given query.
        Args:
            query:
                String - The query that should be executed
            measurement_category:
                String - measurement_category
            measurement_field_name:
                String - Name of the measurement (=values from query result)
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
            step:
                Integer - step
            multiple_actions:
                Boolean, Optional - Indicates if the query returns measurement fields for multiple actions.
                Default: False
            action_field:
                String, Optional - The field that carries the action name. Default: "action"

        Returns:
            DataFrame - The processed DataFrame with the columns 'timestamp', 'target' and measurement_field_name(s)
            (and 'action' if multiple_actions is set)
        """
        url = self.prometheus_url + self.query_base_without_ts + query

        prometheus_request = await requests.get(url)

        # get result and parse
        if prometheus_request.status == 200:
            response = await prometheus_request.json()
            return self.parse_result_to_dataframe_without_ts(measurement_category, measurement_field_name, response,
                                                  multiple_actions, action_field)

        return DataFrame()        

    async def query_prometheus(self, query: str, measurement_category: str, measurement_field_name: str, start: int,
                               end: int, step: int,
                               multiple_actions: bool = False, action_field: str = "action") -> DataFrame:
        """ Queries the configured Prometheus instance with a given query.
        Args:
            query:
                String - The query that should be executed
            measurement_category:
                String - measurement_category
            measurement_field_name:
                String - Name of the measurement (=values from query result)
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
            step:
                Integer - step
            multiple_actions:
                Boolean, Optional - Indicates if the query returns measurement fields for multiple actions.
                Default: False
            action_field:
                String, Optional - The field that carries the action name. Default: "action"

        Returns:
            DataFrame - The processed DataFrame with the columns 'timestamp', 'target' and measurement_field_name(s)
            (and 'action' if multiple_actions is set)
        """
        url = self.prometheus_url + self.query_base + query + \
            "&start={}&end={}&step={}".format(start, end, step)
            
        logging.debug("measurement_field_name url %s", url)


        prometheus_request = await requests.get(url)
        # get result and parse
        if prometheus_request.status == 200:
            response = await prometheus_request.json()
            return self.parse_result_to_dataframe(measurement_category, measurement_field_name, response,
                                                  multiple_actions, action_field)

        return DataFrame()

    @abstractmethod
    async def collect(self, config_object: object, cluster_name: str, start: int, end: int) -> DataFrame:
        """ Collects one or more measurements for a Target from the configured Prometheus instance.

        Args:
            config_object:
                Object - The Target, for which the prometheus should be queried
            cluster_name:
                str - cluster_name

            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end

        Returns:
            DataFrame - Query result as DataFrame - with columns: 'timestamp', 'target', 'measurement_category',
            measurement fields(s) (+ 'action' if applicable)
        """
        pass
