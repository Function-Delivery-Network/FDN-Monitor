#!/usr/bin/env python
from Clusters import BaseCollector
import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
from pandas import DataFrame
from .PrometheusCollector import PrometheusCollector
from abc import abstractmethod
import sys

logging.basicConfig(format='%(message)s', level=logging.DEBUG,
                    handlers=[
                        RotatingFileHandler("Logs/log.log", maxBytes=1e9, backupCount=2),
                        logging.StreamHandler(sys.stdout)
                        ]
                    )
logger = logging.getLogger(__name__)


class KubernetesCollector(BaseCollector):

    def __init__(self, prometheus_url: str, step: int, interval: str):
        self.prom_obj = PrometheusCollector(prometheus_url)
        self.step = step
        self.interval = interval

    async def collect_replicas(self, start: int, end: int, measurement_category: str) -> DataFrame:
        """ Collects the amount of active replicas per function.
        Args:
            start:
                Integer - timestamp of start
            end:
                Integer - timestamp of end
            measurement_category:
                String - measurement_category
        Returns:
            DataFrame - a Pandas Dataframe with the result.
        """
        query = "count(kube_pod_info{pod=~\".*.*\", namespace=~\".*openfaas-fn.*\"}) by (pod)"
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "replicas", start, end, self.step, True,
                                                     "pod")
        if len(frame.values) > 0:
            frame = frame.groupby(['timestamp', 'function_name'])[
                'replicas'].sum().reset_index()

            
            frame.reset_index(inplace=True, drop=True)
            frame.set_index("timestamp", inplace=True)
            frame['replicas'] = frame['replicas'].astype('float')
            

        return frame

    async def collect_pods_cpu_sum(self, start: int, end: int, measurement_category: str) -> DataFrame:
        """ Collects the amount of active replicas per function.
        Args:
            start:
                Integer - timestamp of start
            end:
                Integer - timestamp of end
            measurement_category:
                String - measurement_category
        Returns:
            DataFrame - a Pandas Dataframe with the result.
        """
        query = "sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{pod=~\".*.*\", namespace=~\".*openfaas-fn.*\"})by (pod,node)"
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "pods-cpu-sum", start, end, self.step, True,
                                                     "pod")
        if len(frame.values) > 0:
            frame = frame.groupby(['timestamp', 'function_name'])[
                'pods-cpu-sum'].mean().reset_index()
            frame.reset_index(inplace=True, drop=True)
            frame = frame[["timestamp","function_name", "pods-cpu-sum"]]
            frame.set_index("timestamp", inplace=True)

        return frame

    async def collect_nodes_avg_cpu_usage_user_per_node(self, start: int, end: int, measurement_category: str) -> DataFrame:
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
        query = 'irate(node_cpu_seconds_total{instance=~\".*.*\", mode!=\"idle\"}[1m]) '
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "avg_cpu_user", start, end, self.step)

        logging.debug("node_cpu_seconds_total %s", frame)

        if len(frame.values) > 0:
            frame = frame.groupby(['timestamp', 'node', 'mode'])[
                'avg_cpu_user'].sum().reset_index()
            logging.debug("node_cpu_seconds_totaldfdsfds %s", frame)
            frame = frame.groupby(['timestamp', 'node'])[
                'avg_cpu_user'].sum().reset_index()
            logging.debug("node_cpu_seconds_totdaldfdsfdsdsfsdf %s", frame)
            frame.reset_index(inplace=True, drop=True)
            frame.set_index("timestamp", inplace=True)

        return frame

    async def collect_pods_cpu_per_node_sum(self, start: int, end: int, measurement_category: str) -> DataFrame:
        """ Collects the amount of active replicas per function.
        Args:
            start:
                Integer - timestamp of start
            end:
                Integer - timestamp of end
            measurement_category:
                String - measurement_category
        Returns:
            DataFrame - a Pandas Dataframe with the result.
        """
        query = "sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{pod=~\".*.*\", namespace=~\".*openfaas-fn.*\"})by (pod,node)"
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "pods-cpu-sum", start, end, self.step, True,
                                                     "pod")

        if len(frame.values) > 0:
            frame = frame.groupby(['timestamp', 'function_name', 'node'])[
                'pods-cpu-sum'].sum().reset_index()

            frame.reset_index(inplace=True, drop=True)
            frame.set_index("timestamp", inplace=True)

        return frame

    async def collect_power_usage_per_node_sum(self, start: int, end: int, measurement_category: str) -> DataFrame:
        """ Collects the amount of active replicas per function.
        Args:
            start:
                Integer - timestamp of start
            end:
                Integer - timestamp of end
            measurement_category:
                String - measurement_category
        Returns:
            DataFrame - a Pandas Dataframe with the result.
        """
        query = "idelta(powerexporter_power_consumption_ampere_seconds_total{instance=~\".*.*\"}[1m:10s])"
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "avg_power_consumption", start, end, self.step, True,
                                                     "pod")
        if len(frame.values) > 0:
            frame = frame.groupby(['timestamp', 'node'])[
                'avg_power_consumption'].mean().reset_index()
            frame.reset_index(inplace=True, drop=True)
            frame.set_index("timestamp", inplace=True)

        return frame
    

    async def collect_pod_cpu_requests(self, start: int, end: int, measurement_category: str) -> DataFrame:
        """ Collects the amount of active replicas per function.
        Args:
            start:
                Integer - timestamp of start
            end:
                Integer - timestamp of end
            measurement_category:
                String - measurement_category
        Returns:
            DataFrame - a Pandas Dataframe with the result.
        """
        query = "sum(kube_pod_container_resource_requests{namespace=~\".*openfaas-fn.*\", resource=~\"cpu\"}) by (pod,node)"
        logging.debug("collect_pod_cpu_requests query %s", query)

        frame = await self.prom_obj.query_prometheus(query, measurement_category, "pods-cpu-requests", start, end, self.step, True,
                                                     "pod")
        if len(frame.values) > 0:
            frame = frame.groupby(['timestamp', 'function_name'])[
                'pods-cpu-requests'].mean().reset_index()
            
            
            frame.reset_index(inplace=True, drop=True)
            frame = frame[["timestamp","function_name", "pods-cpu-requests"]]
            frame.set_index("timestamp", inplace=True)

        return frame

    async def collect_pod_cpu_limits(self, start: int, end: int, measurement_category: str) -> DataFrame:
        """ Collects the amount of active replicas per function.
        Args:
            start:
                Integer - timestamp of start
            end:
                Integer - timestamp of end
            measurement_category:
                String - measurement_category
        Returns:
            DataFrame - a Pandas Dataframe with the result.
        """
        query = "sum(kube_pod_container_resource_limits{namespace=~\".*openfaas-fn.*\", resource=~\"cpu\"}) by (pod,node)"
        logging.debug("kube_pod_container_resource_limits query %s", query)
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "pods-cpu-limits", start, end, self.step, True,
                                                     "pod")
        if len(frame.values) > 0:
            frame = frame.groupby(['timestamp', 'function_name'])[
                'pods-cpu-limits'].mean().reset_index()
            
            frame.reset_index(inplace=True, drop=True)
            frame = frame[["timestamp","function_name", "pods-cpu-limits"]]
            frame.set_index("timestamp", inplace=True)

        return frame

    async def collect_machine_cpu_cores(self, measurement_category: str) -> DataFrame:
        """ Collects the amount of active replicas per function.
        Args:
            start:
                Integer - timestamp of start
            end:
                Integer - timestamp of end
            measurement_category:
                String - measurement_category
        Returns:
            DataFrame - a Pandas Dataframe with the result.
        """
        query = 'machine_cpu_cores'
        frame = await self.prom_obj.query_prometheus_without_ts(query, measurement_category, "machine_cpu_cores")
        frame.reset_index(inplace=True, drop=True)
        
        return frame

    async def collect_pods_power_sum(self, start: int, end: int, measurement_category: str) -> DataFrame:
        """ Collects the amount of active replicas per function.
        Args:
            start:
                Integer - timestamp of start
            end:
                Integer - timestamp of end
            measurement_category:
                String - measurement_category
        Returns:
            DataFrame - a Pandas Dataframe with the result.
        """
        frame = pd.DataFrame()
        frame_cpu_usage = await self.collect_pods_cpu_per_node_sum(start, end, measurement_category)
        frame_power_consumption = await self.collect_power_usage_per_node_sum(start, end, "system_usage")
        # frame_pods_limits = await self.collect_pod_container_resource_limits(measurement_category)
        # frame_machine_cpu_cores = await self.collect_machine_cpu_cores("system_usage")
        frame_cpu_usage_per_node = await self.collect_nodes_avg_cpu_usage_user_per_node(start, end, "system_usage")

        if len(frame_cpu_usage) > 0 and len(frame_power_consumption) > 0 and len(frame_cpu_usage_per_node) > 0:
            result1 = pd.merge(frame_cpu_usage, frame_cpu_usage_per_node, on=[
                               'timestamp', 'node'])

            logging.debug(
                "frame_cpu_usageframe_cpu_usageframe_cpu_usage %s", result1)

            result = pd.merge(result1, frame_power_consumption,
                              on=['timestamp', 'node'])

            logging.debug(
                "frame_power_consumptionframe_power_consumptionframe_power_consumption %s", result)

            result["pods-power-usage-sum"] = (
                result["pods-cpu-sum"]/result["avg_cpu_user"])*result["avg_power_consumption"]

            logging.debug("result pods-power-usage-sum %s", result)

            # funct_to_limits_dict = {}
            # for index, val in frame_pods_limits.iterrows():
            #     func = {}
            #     func[val["function_name"]] = val["pods-cpu-limits"]
            #     funct_to_limits_dict.update(func)

            # code_cores_dict = {}
            # for index, val in frame_machine_cpu_cores.iterrows():
            #     nodde = {}
            #     nodde[val["node"]] = val["machine_cpu_cores"]
            #     code_cores_dict.update(nodde)

            # print("funct_to_limits_dict", funct_to_limits_dict)
            # print("code_cores_dict", code_cores_dict)
            # result['func_limits'] = result['function_name'].map(funct_to_limits_dict)

            # result['node_cores'] = result['node'].map(code_cores_dict)
            # #testtt = pd.concat([frame, frame2], axis=1, join='outer')

            # print("frame_pods_limits", frame_pods_limits)
            # print("frame_machine_cpu_cores", frame_machine_cpu_cores)
            # frame.reset_index(inplace=True, drop=True)
            # frame.set_index("timestamp", inplace=True)
            frame = result.groupby(['timestamp', 'function_name'])[
                'pods-power-usage-sum'].sum().reset_index()
            frame = frame[['timestamp', 'function_name',
                           'pods-power-usage-sum']]
            
            

            frame.reset_index(inplace=True, drop=True)
            frame = frame[["timestamp","function_name", "pods-power-usage-sum"]]
            frame.set_index("timestamp", inplace=True)
            #print("frame", frame)
            #print("result.pod", result["pods-cpu-sum"].values)
            #print("result.node", result["avg_cpu_user"].values)

        return frame

    async def collect_pods_mem_sum(self, start: int, end: int, measurement_category: str) -> DataFrame:
        """ Collects the amount of active replicas per function.
        Args:
            start:
                Integer - timestamp of start
            end:
                Integer - timestamp of end
            measurement_category:
                String - measurement_category
        Returns:
            DataFrame - a Pandas Dataframe with the result.
        """
        query = "sum(node_namespace_pod_container:container_memory_working_set_bytes{container!=\"\", namespace=~\".*openfaas-fn.*\"}) by (pod,node)"
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "pods-mem-sum-bytes", start, end, self.step, True,
                                                     "pod")
        if len(frame.values) > 0:
            frame = frame.groupby(['timestamp', 'function_name'])[
                'pods-mem-sum-bytes'].mean().reset_index()
            
            #frame= frame[frame['pods-mem-sum-mega-bytes'] != 0]
            #frame['pods-mem-sum-mega-bytes'] = frame['pods-mem-sum-bytes']  / 1048576
            frame.reset_index(inplace=True, drop=True)
            frame = frame[["timestamp","function_name", "pods-mem-sum-bytes"]]
            frame.set_index("timestamp", inplace=True)

        return frame

    async def collect_pods_file_descp_sum(self, start: int, end: int, measurement_category: str) -> DataFrame:
        """ Collects the amount of active replicas per function.
        Args:
            start:
                Integer - timestamp of start
            end:
                Integer - timestamp of end
            measurement_category:
                String - measurement_category
        Returns:
            DataFrame - a Pandas Dataframe with the result.
        """
        query = "irate(container_file_descriptors{pod=~\".*.*\", namespace=~\".*openfaas-fn.*\"}[1m])"
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "pods-file-descp-sum", start, end, self.step,
                                                     True,
                                                     "pod")
        if len(frame.values) > 0:
            frame = frame.groupby(['timestamp', 'function_name'])[
                'pods-file-descp-sum'].mean().reset_index()
            
            frame.reset_index(inplace=True, drop=True)
            frame = frame[["timestamp","function_name", "pods-file-descp-sum"]]
            frame.set_index("timestamp", inplace=True)

        return frame

    async def collect_pods_iops_read_sum(self, start: int, end: int, measurement_category: str) -> DataFrame:
        """ Collects the amount of active replicas per function.
        Args:
            start:
                Integer - timestamp of start
            end:
                Integer - timestamp of end
            measurement_category:
                String - measurement_category
        Returns:
            DataFrame - a Pandas Dataframe with the result.
        """
        query = "sum(irate(container_fs_reads_total{job=~\"kubelet\", metrics_path=~\"/metrics/cadvisor\", namespace=~\".*openfaas-fn.*\"}[1m])) by (pod,node)"
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "pods-iops-reads-sum", start, end, self.step,
                                                     True,
                                                     "pod")
        if len(frame.values) > 0:
            frame = frame.groupby(['timestamp', 'function_name'])[
                'pods-iops-reads-sum'].mean().reset_index()
            
            frame.reset_index(inplace=True, drop=True)
            frame = frame[["timestamp","function_name", "pods-iops-reads-sum"]]
            frame.set_index("timestamp", inplace=True)

        return frame

    async def collect_pods_iops_write_sum(self, start: int, end: int, measurement_category: str) -> DataFrame:
        """ Collects the amount of active replicas per function.
        Args:
            start:
                Integer - timestamp of start
            end:
                Integer - timestamp of end
            measurement_category:
                String - measurement_category
        Returns:
            DataFrame - a Pandas Dataframe with the result.
        """
        query = "sum(irate(container_fs_writes_total{job=~\"kubelet\", metrics_path=~\"/metrics/cadvisor\", namespace=~\".*openfaas-fn.*\"}[1m])) by (pod,node)"
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "pods-iops-writes-sum", start, end, self.step,
                                                     True,
                                                     "pod")
        if len(frame.values) > 0:
            frame = frame.groupby(['timestamp', 'function_name'])[
                'pods-iops-writes-sum'].mean().reset_index()
            
            frame.reset_index(inplace=True, drop=True)
            frame = frame[["timestamp","function_name", "pods-iops-writes-sum"]]
            frame.set_index("timestamp", inplace=True)

        return frame

    async def collect_pods_network_trasmit_bytes(self, start: int, end: int, measurement_category: str) -> DataFrame:
        """ Collects the amount of active replicas per function.
        Args:
            start:
                Integer - timestamp of start
            end:
                Integer - timestamp of end
            measurement_category:
                String - measurement_category
        Returns:
            DataFrame - a Pandas Dataframe with the result.
        """
        query = "sum(irate(container_network_transmit_bytes_total{job=~\"kubelet\", metrics_path=~\"/metrics/cadvisor\", namespace=~\".*openfaas-fn.*\"}[1m])) by (pod,node)"
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "pods-network-transmit-bytes", start, end, self.step,
                                                     True,
                                                     "pod")
        if len(frame.values) > 0:
            frame = frame.groupby(['timestamp', 'function_name'])[
                'pods-network-transmit-bytes'].mean().reset_index()
            
            #frame['pods-network-transmit-mega-bytes'] = frame['pods-network-transmit-bytes']  / 1048576
            frame.reset_index(inplace=True, drop=True)
            frame = frame[["timestamp","function_name", "pods-network-transmit-bytes"]]
            frame.set_index("timestamp", inplace=True)

        return frame

    async def collect_pods_network_receive_bytes(self, start: int, end: int, measurement_category: str) -> DataFrame:
        """ Collects the amount of active replicas per function.
        Args:
            start:
                Integer - timestamp of start
            end:
                Integer - timestamp of end
            measurement_category:
                String - measurement_category
        Returns:
            DataFrame - a Pandas Dataframe with the result.
        """
        query = "sum(irate(container_network_receive_bytes_total{job=~\"kubelet\", metrics_path=~\"/metrics/cadvisor\", namespace=~\".*openfaas-fn.*\"}[1m])) by (pod,node)"
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "pods-network-receive-bytes", start, end, self.step,
                                                     True,
                                                     "pod")
        if len(frame.values) > 0:
            frame = frame.groupby(['timestamp', 'function_name'])[
                'pods-network-receive-bytes'].mean().reset_index()
            
            #frame['pods-network-receive-mega-bytes'] = frame['pods-network-receive-bytes']  / 1048576
            frame.reset_index(inplace=True, drop=True)
            frame = frame[["timestamp","function_name", "pods-network-receive-bytes"]]
            frame.set_index("timestamp", inplace=True)

        return frame

    async def collect_pods_fs_write_bytes(self, start: int, end: int, measurement_category: str) -> DataFrame:
        """ Collects the amount of active replicas per function.
        Args:
            start:
                Integer - timestamp of start
            end:
                Integer - timestamp of end
            measurement_category:
                String - measurement_category
        Returns:
            DataFrame - a Pandas Dataframe with the result.
        """
        query = "sum(irate(container_fs_writes_bytes_total{job=~\"kubelet\", metrics_path=~\"/metrics/cadvisor\", container!=\"\", pod=~\".*.*\", namespace=~\".*openfaas-fn.*\"}[1m])) by (pod,node)"
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "pods-fs-write-bytes", start, end, self.step,
                                                     True,
                                                     "pod")
        if len(frame.values) > 0:
            frame = frame.groupby(['timestamp', 'function_name'])[
                'pods-fs-write-bytes'].mean().reset_index()
            
            frame['pods-fs-write-mega-bytes'] = frame['pods-fs-write-bytes'] / 1048576
            frame.reset_index(inplace=True, drop=True)
            frame = frame[["timestamp","function_name", "pods-fs-write-mega-bytes"]]
            frame.set_index("timestamp", inplace=True)

        return frame

    async def collect_pods_fs_read_bytes(self, start: int, end: int, measurement_category: str) -> DataFrame:
        """ Collects the amount of active replicas per function.
        Args:
            start:
                Integer - timestamp of start
            end:
                Integer - timestamp of end
            measurement_category:
                String - measurement_category
        Returns:
            DataFrame - a Pandas Dataframe with the result.
        """
        query = "sum(irate(container_fs_reads_bytes_total{job=~\"kubelet\", metrics_path=~\"/metrics/cadvisor\", container!=\"\", pod=~\".*.*\", namespace=~\".*openfaas-fn.*\"}[1m])) by (pod,node)"
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "pods-fs-read-bytes", start, end, self.step,
                                                     True,
                                                     "pod")
        if len(frame.values) > 0:
            frame = frame.groupby(['timestamp', 'function_name'])[
                'pods-fs-read-bytes'].mean().reset_index()
            
            frame['pods-fs-read-mega-bytes'] = frame['pods-fs-read-bytes'] / 1048576
            frame.reset_index(inplace=True, drop=True)
            frame = frame[["timestamp","function_name", "pods-fs-read-bytes"]]
            frame.set_index("timestamp", inplace=True)

        return frame

    async def collect_nodes_avg_power_consumption(self, start: int, end: int, measurement_category: str) -> DataFrame:
        """ Collects power consumption from nodes
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
        query = 'irate(powerexporter_power_consumption_ampere_seconds_total{instance=~\".*.*\"}[1m]) '
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "avg_power_consumption", start, end, self.step)
        if len(frame.values) > 0:
            frame = frame.groupby(['timestamp'])[
                'avg_power_consumption'].mean().reset_index()
            frame.reset_index(inplace=True, drop=True)
            frame.set_index("timestamp", inplace=True)

        return frame

    async def collect_nodes_avg_current_usage(self, start: int, end: int, measurement_category: str) -> DataFrame:
        """ Collects current usage  from nodes
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
        query = 'irate(powerexporter_current_ampere{instance=~\".*.*\"}[1m]) '
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "avg_current_usage", start, end, self.step)

        if len(frame.values) > 0:
            frame = frame.groupby(['timestamp'])[
                'avg_current_usage'].mean().reset_index()
            frame.reset_index(inplace=True, drop=True)
            frame.set_index("timestamp", inplace=True)

        return frame

    async def collect_nodes_avg_cpu_usage_system(self, start: int, end: int, measurement_category: str) -> DataFrame:
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
        query = 'irate(node_cpu_seconds_total{instance=~\".*.*\", mode=\"system\"}[1m]) '
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "avg_cpu_system", start, end, self.step)

        if len(frame.values) > 0:
            frame = frame.groupby(['timestamp'])[
                'avg_cpu_system'].mean().reset_index()
            frame.reset_index(inplace=True, drop=True)
            frame.set_index("timestamp", inplace=True)

        return frame

    async def collect_nodes_avg_cpu_usage_user(self, start: int, end: int, measurement_category: str) -> DataFrame:
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
        query = 'irate(node_cpu_seconds_total{instance=~\".*.*\", mode=\"user\"}[1m]) '
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "avg_cpu_user", start, end, self.step)

        if len(frame.values) > 0:
            frame = frame.groupby(['timestamp'])[
                'avg_cpu_user'].mean().reset_index()

            frame.reset_index(inplace=True, drop=True)
            frame.set_index("timestamp", inplace=True)

        return frame

    async def collect_nodes_avg_cpu_usage_iowait(self, start: int, end: int, measurement_category: str) -> DataFrame:
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
        query = 'irate(node_cpu_seconds_total{instance=~\".*.*\", mode=\"iowait\"}[1m]) '
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "avg_cpu_iowait", start, end, self.step)

        if len(frame.values) > 0:
            frame = frame.groupby(['timestamp'])[
                'avg_cpu_iowait'].mean().reset_index()

            frame.reset_index(inplace=True, drop=True)
            frame.set_index("timestamp", inplace=True)

        return frame

    async def collect_nodes_avg_cpu_usage_idle(self, start: int, end: int, measurement_category: str) -> DataFrame:
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
        query = 'irate(node_cpu_seconds_total{instance=~\".*.*\", mode=\"idle\"}[1m]) '
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "avg_cpu_idle", start, end, self.step)
        if len(frame.values) > 0:
            frame = frame.groupby(['timestamp'])[
                'avg_cpu_idle'].mean().reset_index()

            frame.reset_index(inplace=True, drop=True)
            frame.set_index("timestamp", inplace=True)

        return frame

    async def collect_avg_total_memory(self, start: int, end: int, measurement_category: str) -> DataFrame:
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

        query = 'node_memory_MemTotal_bytes{instance=~\".*.*\"}'
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "total_avg_mem_per_node", start, end, self.step)

        return frame

    async def collect_memory_avg_usage(self, start: int, end: int, measurement_category: str) -> DataFrame:
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
            DataFrame - The result as df with the columns: 'timestamp', 'action' and 'invocations'
        """

        query = '(1 - (node_memory_MemAvailable_bytes{instance=~\".*.*\"} / ' \
                '(node_memory_MemTotal_bytes{instance=~\".*.*\"})))* 100'
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "avg_memory_usage_percent", start,
                                                     end, self.step)
        if len(frame.values) > 0:
            frame = frame.groupby(['timestamp'])[
                'avg_memory_usage_percent'].mean().reset_index()

            frame.reset_index(inplace=True, drop=True)
            frame.set_index("timestamp", inplace=True)

        return frame

    async def collect_bytes_transmitted(self, start: int, end: int, measurement_category: str) -> DataFrame:
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
        
        query = 'irate(container_network_transmit_bytes_total[{}])'.format(
            self.interval)

        frame = await self.prom_obj.query_prometheus(query, measurement_category, "network_bytes_transmitted", start,
                                                     end, self.step)
        if len(frame.values) > 0:
            frame = frame.groupby(['timestamp'])[
                'network_bytes_transmitted'].mean().reset_index()

            frame.reset_index(inplace=True, drop=True)
            frame.set_index("timestamp", inplace=True)

        return frame

    async def collect_disk_written_bytes(self, start: int, end: int, measurement_category: str) -> DataFrame:
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
        
        query = 'irate(node_disk_written_bytes_total[{}])'.format(self.interval)
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "disk_writes_bytes", start, end, self.step)
        if len(frame.values) > 0:
            frame = frame.groupby(['timestamp'])[
                'disk_writes_bytes'].mean().reset_index()

            frame.reset_index(inplace=True, drop=True)
            frame.set_index("timestamp", inplace=True)

        return frame

    async def collect_disk_read_bytes(self, start: int, end: int, measurement_category: str) -> DataFrame:
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
        
        query = 'irate(node_disk_read_bytes_total[{}])'.format(self.interval)
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "disk_read_bytes", start, end, self.step)
        if len(frame.values) > 0:
            frame = frame.groupby(['timestamp'])[
                'disk_read_bytes'].mean().reset_index()

            frame.reset_index(inplace=True, drop=True)
            frame.set_index("timestamp", inplace=True)
        return frame

    async def collect_disk_read_iops(self, start: int, end: int, measurement_category: str) -> DataFrame:
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
        
        query = 'irate(node_disk_reads_completed_total[{}])'.format(self.interval)
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "disk_read_iops", start, end, self.step)
        if len(frame.values) > 0:
            frame = frame.groupby(['timestamp'])[
                'disk_read_iops'].mean().reset_index()

            frame.reset_index(inplace=True, drop=True)
            frame.set_index("timestamp", inplace=True)
        return frame

    async def collect_disk_write_iops(self, start: int, end: int, measurement_category: str) -> DataFrame:
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
        
        query = 'irate(node_disk_writes_completed_total[{}])'.format(self.interval)
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "disk_write_iops", start, end, self.step)
        if len(frame.values) > 0:
            frame = frame.groupby(['timestamp'])[
                'disk_write_iops'].mean().reset_index()

            frame.reset_index(inplace=True, drop=True)
            frame.set_index("timestamp", inplace=True)
        return frame

    @abstractmethod
    async def do_frame_postprocessing(self, frame: DataFrame, target_name: str, measurement_category: str) -> DataFrame:
        pass

    @abstractmethod
    async def collect(self, config_object: object, cluster_name: str, start: int, end: int) -> DataFrame:
        pass
