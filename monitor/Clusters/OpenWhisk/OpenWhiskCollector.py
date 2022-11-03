#!/usr/bin/env python
import sys
import os
sys.path.append(os.path.abspath('../'))
from Clusters import BaseCollector
from .PrometheusCollector import PrometheusCollector
from .KubernetesCollector import KubernetesCollector
import logging

logging.basicConfig(filename='Logs/log.log', format='%(message)s', filemode='w', level=logging.DEBUG)
logger = logging.getLogger(__name__)

from pandas import DataFrame
import pandas as pd
from typing import List, Tuple
import traceback
import asyncio
from abc import abstractmethod


class OpenWhiskCollector(BaseCollector):
    def __init__(self, ow_prometheus_url: str = None, kubernetes_prom_url: str = None, power_collection: bool = False,
                 step: int=60, interval: str = "1m"):
        self.ow_prom_obj = PrometheusCollector(ow_prometheus_url)
        self.step = step
        self.interval = interval
        self.cluster_kube_prom_obj = KubernetesCollector(kubernetes_prom_url, self.step, self.interval)
        self.power_collection = power_collection


    async def collect_cold_starts(self, start: int, end: int, measurement_category: str) -> DataFrame:
        """ Collects cold starts for FaaS functions.
        It uses PrometheusCollector's query_prometheus function.
        Args:
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
        Returns:
            DataFrame - The result as Dataframe with the columns: 'timestamp', 'action' and 'cold_starts'
        """

        query = "increase(openwhisk_action_coldStarts_total[{0}])".format(self.interval)
        frame = await self.ow_prom_obj.query_prometheus(query, measurement_category, "cold_starts", start, end, self.step, True)
        if len(frame.values)>0:
            frame = frame.groupby(['timestamp', 'function_name'])[
                'cold_starts'].mean().reset_index()
            frame.reset_index(inplace=True, drop=True)
            frame = frame[["timestamp","function_name", "cold_starts"]]
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
        Returns:
            DataFrame - The result as Dataframe with the columns: 'timestamp', 'action' and 'invocations'
        """

        query = "increase(openwhisk_action_activations_total[{0}])".format(self.interval)
        frame = await self.ow_prom_obj.query_prometheus(query, measurement_category, "success_invocations", start, end, self.step, True)
        if len(frame.values)>0:
            frame = frame.groupby(['timestamp', 'function_name'])[
                'success_invocations'].mean().reset_index()
            
            frame.reset_index(inplace=True, drop=True)
            frame = frame[["timestamp","function_name", "success_invocations"]]
            frame.set_index("timestamp", inplace=True)

        #frame = frame[frame["action"]=="myservice-dev-nodeinfo"]
        #print(frame.action.values)
        return frame

    async def collect_function_memory(self, start: int, end: int, measurement_category: str) -> DataFrame:
        """ Collects function invocations for FaaS functions.
        It uses PrometheusCollector's query_prometheus function.
        Args:
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
        Returns:
            DataFrame - The result as Dataframe with the columns: 'timestamp', 'action' and 'invocations'
        """

        query = "openwhisk_action_memory"
        frame = await self.ow_prom_obj.query_prometheus(query, measurement_category, "pod-mem-limits", start, end, self.step, True)
        if len(frame.values)>0:
            frame = frame.groupby(['timestamp', 'function_name'])[
                'pod-mem-limits'].mean().reset_index()
            
            frame.reset_index(inplace=True, drop=True)
            frame = frame[["timestamp","function_name", "pod-mem-limits"]]
            frame["pod-mem-limits"] = frame["pod-mem-limits"]*1024*1024
            frame.set_index("timestamp", inplace=True)
            
        return frame


    async def collect_function_runtimes(self, start: int, end: int, measurement_category: str) -> DataFrame:
        """ Collects function runtimes for FaaS functions.
        It uses PrometheusCollector's query_prometheus function.
        Args:
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
        Returns:
            DataFrame - The result as Dataframe with the columns: 'timestamp', 'action' and 'runtime'
        """

        query = "rate(openwhisk_action_duration_seconds_sum[{0}]) /  rate(openwhisk_action_duration_seconds_count[{0}])".format(self.interval)
        frame = await self.ow_prom_obj.query_prometheus(query, measurement_category, "average_execution_time", start, end, self.step, True)
        if len(frame.values)>0:
            frame = frame.groupby(['timestamp', 'function_name'])[
                'average_execution_time'].mean().reset_index()
            
            frame.reset_index(inplace=True, drop=True)
            frame = frame[["timestamp","function_name", "average_execution_time"]]
            frame.set_index("timestamp", inplace=True)
        return frame

    async def collect_function_initialization_times(self, start: int, end: int, measurement_category: str) -> DataFrame:
        """ Collects function initialization times for FaaS functions.
        It uses PrometheusCollector's query_prometheus function.
        Args:
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
        Returns:
            DataFrame - The result as Dataframe with the columns: 'timestamp', 'action' and 'inittime'
        """

        # TODO: maybe find sth better than average init time; Maybe bucket in combination with coldstarts?
        query = "rate(openwhisk_action_initTime_seconds_sum[{0}]) /  rate(openwhisk_action_duration_seconds_count[{0}])".format(self.interval)
        frame = await self.ow_prom_obj.query_prometheus(query, measurement_category, "init_time", start, end, self.step, True)
        if len(frame.values)>0:
            frame = frame.groupby(['timestamp', 'function_name'])[
                'init_time'].mean().reset_index()
            
            frame.reset_index(inplace=True, drop=True)
            frame = frame[["timestamp","function_name", "init_time"]]
            frame.set_index("timestamp", inplace=True)
        return frame

    async def collect_function_wait_times(self, start: int, end: int, measurement_category: str) -> DataFrame:
        """ Collects function wait times for FaaS functions.
        It uses PrometheusCollector's query_prometheus function.
        Args:
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
        Returns:
            DataFrame - The result as Dataframe with the columns: 'timestamp', 'action' and 'waittime'
        """

        # TODO: maybe find sth better than average init time; Maybe bucket in combination with coldstarts?
        query = "rate(openwhisk_action_waitTime_seconds_sum[{0}])/  rate(openwhisk_action_duration_seconds_count[{0}])".format(self.interval)
        frame = await self.ow_prom_obj.query_prometheus(query, measurement_category, "wait_time", start, end, self.step, True)
        if len(frame.values)>0:
            frame = frame.groupby(['timestamp', 'function_name'])[
                'wait_time'].mean().reset_index()
            
            frame.reset_index(inplace=True, drop=True)
            frame = frame[["timestamp","function_name", "wait_time"]]
            frame.set_index("timestamp", inplace=True)
        return frame

    def postprocess_relative_to_invocations(self, frame: DataFrame, measurement: str) -> DataFrame:
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
                frame[[measurement]] = frame[[measurement]].divide(frame['success_invocations'], axis='index')
                frame[[measurement]] = frame[[measurement]].fillna(method='ffill')

        return frame

    async def collect(self, cluster_name: str, start: int, end: int) -> DataFrame:
        """ Collects function cold starts, invocations, initialization time and runtime for a Target from the configured prometheus instance.

        Args:
            config_object:
                Object - The Target, for which the prometheus should be queried
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end

        Returns:
            DataFrame - Query result as DataFrame - with columns: 'timestamp', 'target', 'action' and measurement fields(s)
        """
        
        # start each worker
        tasks_functions_usage: List[asyncio.Task] = [
            asyncio.create_task(self.collect_cold_starts(
                start, end, "function_usage")),
            asyncio.create_task(self.collect_function_runtimes(
                start, end, "function_usage")),
            asyncio.create_task(self.collect_function_invocations(
                start, end, "function_usage")),
            asyncio.create_task(self.collect_function_initialization_times(
                start, end, "function_usage")),
            asyncio.create_task(self.collect_function_wait_times(
                start, end, "function_usage")),
            asyncio.create_task(self.collect_function_memory(
                start, end, "function_usage")),
            asyncio.create_task(self.cluster_kube_prom_obj.collect_replicas(
                start, end, "function_usage")),
            asyncio.create_task(self.cluster_kube_prom_obj.collect_pods_cpu_sum(
                start, end, "function_usage")),
            asyncio.create_task(self.cluster_kube_prom_obj.collect_pod_cpu_requests(
                start, end, "function_usage")),
            asyncio.create_task(self.cluster_kube_prom_obj.collect_pod_cpu_limits(
                start, end, "function_usage")),
            asyncio.create_task(self.cluster_kube_prom_obj.collect_pods_mem_sum(
                start, end, "function_usage")),
            asyncio.create_task(self.cluster_kube_prom_obj.collect_pods_file_descp_sum(
                start, end, "function_usage")),
            asyncio.create_task(self.cluster_kube_prom_obj.collect_pods_fs_read_bytes(
                start, end, "function_usage")),
            asyncio.create_task(self.cluster_kube_prom_obj.collect_pods_fs_write_bytes(
                start, end, "function_usage")),
            asyncio.create_task(self.cluster_kube_prom_obj.collect_pods_iops_read_sum(
                start, end, "function_usage")),
            asyncio.create_task(self.cluster_kube_prom_obj.collect_pods_iops_write_sum(
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
            # asyncio.create_task(self.collect_500_errors(start, end, self.step)),
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
            # Divide by invocations & interpolate
            # If no value exists -> just insert empty values
            # combined_frame_functions_usage = self.postprocess_relative_to_invocations(combined_frame_functions_usage, 'init_time')
            # combined_frame_functions_usage = self.postprocess_relative_to_invocations(combined_frame_functions_usage, 'wait_time')
            # combined_frame_functions_usage = self.postprocess_relative_to_invocations(combined_frame_functions_usage, 'average_execution_time')
        
            # combined_frame = self.postprocess_relative_to_invocations(combined_frame, 'inittime')
            # combined_frame = self.postprocess_relative_to_invocations(combined_frame, 'runtime')

        except Exception as e:
            print("Exception when tyring to query data")
            print(e)
            traceback.print_exc()

        #print('combined_frame_systems_usage', combined_frame_systems_usage)

        combined_frame_systems_usage = self.ow_prom_obj.do_frame_postprocessing(combined_frame_systems_usage,
                                                                                str(cluster_name), "system_usage")
        combined_frame_functions_usage = self.ow_prom_obj.do_frame_postprocessing(combined_frame_functions_usage,
                                                                                  str(cluster_name), "function_usage")
            
        result_dict = {'functions_usage': combined_frame_functions_usage,
                       'system_usage': combined_frame_systems_usage}

        return result_dict

        # # start each worker
        # tasks: List[asyncio.Task] = [
        #     asyncio.create_task(self.collect_cold_starts(start, end)),
        #     asyncio.create_task(self.collect_function_invocations(start, end)),
        #     asyncio.create_task(self.collect_function_runtimes(start, end)),
        #     asyncio.create_task(self.collect_function_initialization_times(start, end)),
        #     asyncio.create_task(self.collect_function_memory(start, end))
        #     #asyncio.create_task(self.kube_prom_obj.collect_replicas(start, end))
        # ]

        # # wait for all workers
        # combined_frame = DataFrame()
        # try:
        #     # wait for max 45 seconds
        #     for result in asyncio.as_completed(tasks, timeout=10.0):
        #         frame = await result

        #         if combined_frame.empty:
        #             combined_frame = frame
        #         else:
        #             combined_frame = pd.merge(combined_frame, frame, on=['timestamp', 'action'])
        #             #print(combined_frame)

        #     # Divide by invocations & interpolate
        #     # If no value exists -> just insert empty values
        #     combined_frame = self.postprocess_relative_to_invocations(combined_frame, 'inittime')
        #     combined_frame = self.postprocess_relative_to_invocations(combined_frame, 'runtime')

        # except Exception as e:
        #     print("Exception when tyring to query data")
        #     print(e)
        #     traceback.print_exc()

        # return self.ow_prom_obj.do_frame_postprocessing(combined_frame, cluster_name, "function_usage")