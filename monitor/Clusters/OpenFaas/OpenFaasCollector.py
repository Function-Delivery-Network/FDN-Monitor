#!/usr/bin/env python
from abc import abstractmethod
import asyncio
import traceback
from typing import List, Tuple
import pandas as pd
from pandas import DataFrame
import logging
from .KubernetesCollector import KubernetesCollector
from .PrometheusCollector import PrometheusCollector
from Clusters import BaseCollector
import sys
import os
sys.path.append(os.path.abspath('../'))

logging.basicConfig(filename='Logs/log.log',
                    format='%(message)s', filemode='w', level=logging.DEBUG)
logger = logging.getLogger(__name__)


class OpenFaasCollector(BaseCollector):
    def __init__(self, of_prometheus_url: str = None, cluster_kube_prom_url: str = None, power_collection: bool = False,
                 step: int=60, interval: str = "1m"):
        self.of_prom_obj = PrometheusCollector(of_prometheus_url)    
        self.step = step
        self.interval = interval
        self.cluster_kube_prom_obj = KubernetesCollector(cluster_kube_prom_url, self.step, self.interval)
        self.power_collection = power_collection


    async def collect_average_execution_time(self, start: int, end: int, measurement_category: str) -> DataFrame:
        """ Collects cold starts for FaaS functions.
        It uses PrometheusCollector's query_prometheus function.
        Args:
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
            measurement_category:
                String - measurement_category
        Returns:
            DataFrame - The result as Dataframe with the columns: 'timestamp', 'action' and 'cold_starts'
        """
        query = "rate(gateway_functions_seconds_sum[{0}]) / rate(gateway_functions_seconds_count[{0}])".format(
            self.interval)
        frame = await self.of_prom_obj.query_prometheus(query, measurement_category, "average_execution_time", start,
                                                        end, self.step)
        if len(frame.values) > 0:
            frame = frame[["timestamp","function_name", "average_execution_time"]]
            frame.set_index("timestamp", inplace=True)
        return frame

    async def collect_percentile(self, start: int, end: int, measurement_category: str) -> DataFrame:
        """ Collects cold starts for FaaS functions.
        It uses PrometheusCollector's query_prometheus function.
        Args:
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
            measurement_category:
                String - measurement_category
        Returns:
            DataFrame - The result as Dataframe with the columns: 'timestamp', 'action' and 'cold_starts'
        """

        query = "histogram_quantile(0.9,rate(gateway_functions_seconds_bucket[{}]))".format(
            self.interval)
        frame = await self.of_prom_obj.query_prometheus(query, measurement_category, "percentile_90_exec_time", start,
                                                        end, self.step)
        if len(frame.values) > 0:
            frame = frame[["timestamp","function_name", "percentile_90_exec_time"]]
            frame.set_index("timestamp", inplace=True)
        return frame

    async def collect_function_invocations(self, start: int, end: int, measurement_category: str) -> DataFrame:
        """ Collects function invocations for FaaS functions.
        It uses PrometheusCollector's query_prometheus function.
        Args:
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
            measurement_category:
                String - measurement_category
        Returns:
            DataFrame - The result as Dataframe with the columns: 'timestamp', 'action' and 'invocations'
        """

        query = "increase(gateway_function_invocation_total{code='200'}["+ self.interval +"])"
        frame = await self.of_prom_obj.query_prometheus(query, measurement_category, "success_invocations", start, end, self.step)
        if len(frame.values) > 0:
            frame = frame.groupby(['timestamp', 'function_name'])[
                'success_invocations'].mean().reset_index()
            frame = frame[["timestamp","function_name", "success_invocations"]]
            frame.set_index("timestamp", inplace=True)
        return frame

    async def collect_500_errors(self, start: int, end: int, measurement_category: str) -> DataFrame:
        """ Collects function invocations for FaaS functions.
        It uses PrometheusCollector's query_prometheus function.
        Args:
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
            measurement_category:
                String - measurement_category
        Returns:
            DataFrame - The result as Dataframe with the columns: 'timestamp', 'action' and 'invocations'
        """

        query = "increase(gateway_function_invocation_total{code='500'}["+ self.interval +"])"
        frame = await self.of_prom_obj.query_prometheus(query, measurement_category, "500_error_invocations", start, end, self.step)
        if len(frame.values) > 0:
            frame = frame[["timestamp","function_name", "500_error_invocations"]]
            frame.set_index("timestamp", inplace=True)
        return frame
    
    async def collect_502_errors(self, start: int, end: int, measurement_category: str) -> DataFrame:
        """ Collects function invocations for FaaS functions.
        It uses PrometheusCollector's query_prometheus function.
        Args:
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
            measurement_category:
                String - measurement_category
        Returns:
            DataFrame - The result as Dataframe with the columns: 'timestamp', 'action' and 'invocations'
        """

        query = "increase(gateway_function_invocation_total{code='502'}["+ self.interval +"])"
        frame = await self.of_prom_obj.query_prometheus(query, measurement_category, "502_error_invocations", start, end, self.step)
        if len(frame.values) > 0:
            frame = frame[["timestamp","function_name", "502_error_invocations"]]
            frame.set_index("timestamp", inplace=True)
        return frame

    @staticmethod
    def postprocess_relative_to_invocations(frame: DataFrame, measurement: str) -> DataFrame:
        """ Divides the values by invocations.
        If values are missing, the last valid value is used.
        If no values exist at all, this function does nothing.
        Args:
            frame:
                DataFrame - The frame that holds the data
            measurement:
                String - name of the measurement that should be processed

        Returns:
            DataFrame - The processed frame
        """
        if measurement in frame.columns:
            if not frame[measurement].empty:
                # Divide and fill
                frame[[measurement]] = frame[[measurement]].divide(
                    frame['success_invocations'], axis='index')
                frame[[measurement]] = frame[[
                    measurement]].fillna(method='ffill')

        return frame

    async def collect(self, cluster_name: str, start: int, end: int) -> dict:
        """ Collects function cold starts, invocations, initialization time and runtime for a Target
        from the configured prometheus instance.

        Args:
            cluster_name:
                String - Name of cluster
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end

        Returns:
            DataFrame - Query result as DataFrame - with columns: 'timestamp', 'target', 'action' and measurement fields(s)
        """

        # start each worker
        
        if self.power_collection: 
            tasks_functions_usage: List[asyncio.Task] = [
                asyncio.create_task(self.collect_average_execution_time(
                    start, end, "function_usage")),
                asyncio.create_task(self.collect_percentile(
                    start, end, "function_usage")),
                asyncio.create_task(self.collect_function_invocations(
                    start, end, "function_usage")),
                asyncio.create_task(self.collect_500_errors(
                    start, end, "function_usage")),
                asyncio.create_task(self.collect_502_errors(
                    start, end, "function_usage")),
                asyncio.create_task(self.cluster_kube_prom_obj.collect_pods_cpu_sum(
                    start, end, "function_usage")),
                asyncio.create_task(self.cluster_kube_prom_obj.collect_pods_power_sum(
                    start, end, "function_usage")),
                asyncio.create_task(self.cluster_kube_prom_obj.collect_pod_cpu_requests(
                    start, end, "function_usage")),
                asyncio.create_task(self.cluster_kube_prom_obj.collect_pod_cpu_limits(
                    start, end, "function_usage")),
                asyncio.create_task(self.cluster_kube_prom_obj.collect_replicas(
                    start, end, "function_usage")),
                asyncio.create_task(self.cluster_kube_prom_obj.collect_pods_mem_sum(
                    start, end, "function_usage")),
                asyncio.create_task(self.cluster_kube_prom_obj.collect_pods_file_descp_sum(
                    start, end, "function_usage")),
                asyncio.create_task(self.cluster_kube_prom_obj.collect_pods_iops_read_sum(
                    start, end, "function_usage")),
                asyncio.create_task(self.cluster_kube_prom_obj.collect_pods_iops_write_sum(
                    start, end, "function_usage")),
                asyncio.create_task(self.cluster_kube_prom_obj.collect_pods_fs_read_bytes(
                    start, end, "function_usage")),
                asyncio.create_task(self.cluster_kube_prom_obj.collect_pods_fs_write_bytes(
                    start, end, "function_usage")),
                asyncio.create_task(self.cluster_kube_prom_obj.collect_pods_network_receive_bytes(
                    start, end, "function_usage")),
                asyncio.create_task(self.cluster_kube_prom_obj.collect_pods_network_trasmit_bytes(
                    start, end, "function_usage"))
            ]
        else:
            tasks_functions_usage: List[asyncio.Task] = [
                asyncio.create_task(self.collect_average_execution_time(
                    start, end, "function_usage")),
                asyncio.create_task(self.collect_percentile(
                    start, end, "function_usage")),
                asyncio.create_task(self.collect_function_invocations(
                    start, end, "function_usage")),
                asyncio.create_task(self.collect_500_errors(
                    start, end, "function_usage")),
                asyncio.create_task(self.collect_502_errors(
                    start, end, "function_usage")),
                asyncio.create_task(self.cluster_kube_prom_obj.collect_pods_cpu_sum(
                    start, end, "function_usage")),
                asyncio.create_task(self.cluster_kube_prom_obj.collect_replicas(
                    start, end, "function_usage")),
                asyncio.create_task(self.cluster_kube_prom_obj.collect_pods_mem_sum(
                    start, end, "function_usage")),
                asyncio.create_task(self.cluster_kube_prom_obj.collect_pod_cpu_requests(
                    start, end, "function_usage")),
                asyncio.create_task(self.cluster_kube_prom_obj.collect_pod_cpu_limits(
                    start, end, "function_usage")),
                asyncio.create_task(self.cluster_kube_prom_obj.collect_pods_file_descp_sum(
                    start, end, "function_usage")),
                asyncio.create_task(self.cluster_kube_prom_obj.collect_pods_fs_read_bytes(
                    start, end, "function_usage")),
                asyncio.create_task(self.cluster_kube_prom_obj.collect_pods_fs_write_bytes(
                    start, end, "function_usage")),
                asyncio.create_task(self.cluster_kube_prom_obj.collect_pods_network_receive_bytes(
                    start, end, "function_usage")),
                asyncio.create_task(self.cluster_kube_prom_obj.collect_pods_network_trasmit_bytes(
                    start, end, "function_usage"))
            ]
        # start each worker
        tasks_system_usage: List[asyncio.Task] = [
            asyncio.create_task(self.cluster_kube_prom_obj.collect_bytes_transmitted(
                start, end, "system_usage")),
            # asyncio.create_task(self.collect_500_errors(start, end, step)),
            asyncio.create_task(
                self.cluster_kube_prom_obj.collect_nodes_avg_cpu_usage_system(start, end, "system_usage")),
            asyncio.create_task(
                self.cluster_kube_prom_obj.collect_nodes_avg_cpu_usage_user(start, end, "system_usage")),
            asyncio.create_task(
                self.cluster_kube_prom_obj.collect_nodes_avg_cpu_usage_iowait(start, end, "system_usage")),
            asyncio.create_task(
                self.cluster_kube_prom_obj.collect_nodes_avg_cpu_usage_idle(start, end, "system_usage")),
            asyncio.create_task(self.cluster_kube_prom_obj.collect_avg_total_memory(
                start, end, "system_usage")),
            asyncio.create_task(self.cluster_kube_prom_obj.collect_memory_avg_usage(
                start, end, "system_usage")),
            asyncio.create_task(self.cluster_kube_prom_obj.collect_disk_written_bytes(
                start, end, "system_usage")),
            asyncio.create_task(self.cluster_kube_prom_obj.collect_disk_read_iops(
                start, end, "system_usage")),
            asyncio.create_task(self.cluster_kube_prom_obj.collect_disk_write_iops(
                start, end, "system_usage")),
            asyncio.create_task(self.cluster_kube_prom_obj.collect_nodes_avg_power_consumption(
                start, end, "system_usage")),
            asyncio.create_task(self.cluster_kube_prom_obj.collect_nodes_avg_current_usage(
                start, end, "system_usage"))
        ]

        # wait for all workers
        combined_frame_functions_usage = DataFrame()
        combined_frame_systems_usage = DataFrame()
        try:
            # wait for max 45 seconds
            for result in asyncio.as_completed(tasks_functions_usage, timeout=30.0):
                frame = await result

                if combined_frame_functions_usage.empty and len(frame.values)>0:
                    combined_frame_functions_usage = frame
                else:
                    logging.debug("frame combined_frame_functions_usagecombined_frame_functions_usage, %s", frame)
                    if len(frame.values)>0:
                        combined_frame_functions_usage = pd.merge(combined_frame_functions_usage,
                                                                frame, on=['timestamp', 'function_name'], how="outer")
                logging.debug("after combined_frame_functions_usagecombined_frame_functions_usage, %s", combined_frame_functions_usage)
                

            # wait for max 45 seconds
            for result in asyncio.as_completed(tasks_system_usage, timeout=30.0):
                frame = await result

                if combined_frame_systems_usage.empty:
                    combined_frame_systems_usage = frame
                else:
                    if len(frame.values)>0:
                        combined_frame_systems_usage = pd.merge(combined_frame_systems_usage, frame,
                                                                on=['timestamp'])

            # combined_frame_functions_usage = self.postprocess_relative_to_invocations(combined_frame_functions_usage, '500_error_invocations')
            # combined_frame_functions_usage = self.postprocess_relative_to_invocations(combined_frame_functions_usage, '502_error_invocations')
            
            # Divide by invocations & interpolate
            # If no value exists -> just insert empty values
            # combined_frame = self.postprocess_relative_to_invocations(combined_frame, 'inittime')
            # combined_frame = self.postprocess_relative_to_invocations(combined_frame, 'runtime')

        except Exception as e:
            print("Exception when tyring to query data")
            print(e)
            traceback.print_exc()

        #print('combined_frame_systems_usage', combined_frame_systems_usage)

        combined_frame_systems_usage = self.of_prom_obj.do_frame_postprocessing(combined_frame_systems_usage,
                                                                                str(cluster_name), "system_usage")
        combined_frame_functions_usage = self.of_prom_obj.do_frame_postprocessing(combined_frame_functions_usage,
                                                                                  str(cluster_name), "function_usage")

        result_dict = {'functions_usage': combined_frame_functions_usage,
                       'system_usage': combined_frame_systems_usage}

        return result_dict

    @abstractmethod
    async def do_frame_postprocessing(self, frame: DataFrame, target_name: str, measurement_category: str) -> DataFrame:
        pass
