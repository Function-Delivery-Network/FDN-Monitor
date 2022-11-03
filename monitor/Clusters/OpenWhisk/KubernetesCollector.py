#!/usr/bin/env python
from Clusters import BaseCollector
import logging
from pandas import DataFrame
from .PrometheusCollector import PrometheusCollector
from abc import abstractmethod
logging.basicConfig(filename='Logs/log.log',
                    format='%(message)s', filemode='w', level=logging.DEBUG)
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
        Returns:
            DataFrame - a Pandas Dataframe with the result.
        """
        query = "count(kube_pod_info{pod=~\".*guest.*\"}) by (pod)"
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "replicas", start, end, self.step, True, "pod")

        #frame["action"] = frame["action"].map(lambda e: re.sub(r'^.*?-user-events-', '', e))

        if len(frame.values)>0:
            frame = frame.groupby(['timestamp', 'function_name'])['replicas'].sum().reset_index()
            frame.reset_index(inplace=True, drop=True)
            frame = frame[["timestamp","function_name", "replicas"]]
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
        query = "sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{pod=~\".*guest.*\", container=~\".*user-action.*\"})by (pod,node)"
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "pods-cpu-sum", start, end, self.step, True,
                                                     "pod")
        if len(frame.values)>0:
            frame = frame.groupby(['timestamp', 'function_name'])[
                'pods-cpu-sum'].mean().reset_index()

            frame.reset_index(inplace=True, drop=True)
            frame = frame[["timestamp","function_name", "pods-cpu-sum"]]
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
        query = "sum(kube_pod_container_resource_requests{pod=~\".*guest.*\", container=~\".*user-action.*\", resource=~\"cpu\"}) by (pod,node)"
        logging.debug("collect_pod_cpu_requests query %s", query)
        
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "pods-cpu-requests", start, end, self.step, True,
                                                     "pod")
        if len(frame.values)>0:
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
        query = "sum(kube_pod_container_resource_limits{pod=~\".*guest.*\", container=~\".*user-action.*\", resource=~\"cpu\"}) by (pod,node)"
        logging.debug("kube_pod_container_resource_limits query %s", query)
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "pods-cpu-limits", start, end, self.step, True,
                                                     "pod")
        if len(frame.values)>0:
            frame = frame.groupby(['timestamp', 'function_name'])[
                'pods-cpu-limits'].mean().reset_index()
            frame.reset_index(inplace=True, drop=True)
            frame = frame[["timestamp","function_name", "pods-cpu-limits"]]
            frame.set_index("timestamp", inplace=True)

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
        query = "sum(node_namespace_pod_container:container_memory_working_set_bytes{pod=~\".*guest.*\", container=~\".*user-action.*\"}) by (pod,node)"
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "pods-mem-sum-bytes", start, end, self.step, True,
                                                     "pod")
        if len(frame.values)>0:
            frame = frame.groupby(['timestamp', 'function_name'])[
                'pods-mem-sum-bytes'].mean().reset_index()
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
        query = "irate(container_file_descriptors{pod=~\".*guest.*\", container=~\".*user-action.*\"}[1m])"
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "pods-file-descp-sum", start, end, self.step,
                                                     True,
                                                     "pod")
        if len(frame.values)>0:
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
        query = "sum(irate(container_fs_reads_total{job=~\"kubelet\", metrics_path=~\"/metrics/cadvisor\", pod=~\".*guest.*\"}[1m])) by (pod,node)"
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "pods-iops-reads-sum", start, end, self.step,
                                                     True,
                                                     "pod")
        if len(frame.values)>0:
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
        query = "sum(irate(container_fs_writes_total{job=~\"kubelet\", metrics_path=~\"/metrics/cadvisor\", pod=~\".*guest.*\"}[1m])) by (pod,node)"
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "pods-iops-writes-sum", start, end, self.step,
                                                     True,
                                                     "pod")
        if len(frame.values)>0:
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
        query = "sum(irate(container_network_transmit_bytes_total{job=~\"kubelet\", metrics_path=~\"/metrics/cadvisor\", pod=~\".*guest.*\"}[1m])) by (pod,node)"
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "pods-network-transmit-bytes", start, end, self.step,
                                                     True,
                                                     "pod")
        if len(frame.values)>0:
            frame = frame.groupby(['timestamp', 'function_name'])[
                'pods-network-transmit-bytes'].mean().reset_index()
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
        query = "sum(irate(container_network_receive_bytes_total{job=~\"kubelet\", metrics_path=~\"/metrics/cadvisor\", pod=~\".*guest.*\"}[1m])) by (pod,node)"
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "pods-network-receive-bytes", start, end, self.step,
                                                     True,
                                                     "pod")
        if len(frame.values)>0:
            frame = frame.groupby(['timestamp', 'function_name'])[
                'pods-network-receive-bytes'].mean().reset_index()
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
        query = "sum(irate(container_fs_writes_bytes_total{job=~\"kubelet\", metrics_path=~\"/metrics/cadvisor\", pod=~\".*guest.*\", container=~\".*user-action.*\"}[1m])) by (pod,node)"
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "pods-fs-write-bytes", start, end, self.step,
                                                     True,
                                                     "pod")
        if len(frame.values)>0:
            frame = frame.groupby(['timestamp', 'function_name'])[
                'pods-fs-write-bytes'].mean().reset_index()
            frame.reset_index(inplace=True, drop=True)
            frame = frame[["timestamp","function_name", "pods-fs-write-bytes"]]
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
        query = "sum(irate(container_fs_reads_bytes_total{job=~\"kubelet\", metrics_path=~\"/metrics/cadvisor\", pod=~\".*guest.*\", container=~\".*user-action.*\"}[1m])) by (pod,node)"
        frame = await self.prom_obj.query_prometheus(query, measurement_category, "pods-fs-read-bytes", start, end, self.step,
                                                     True,
                                                     "pod")
        if len(frame.values)>0:
            frame = frame.groupby(['timestamp', 'function_name'])[
                'pods-fs-read-bytes'].mean().reset_index()
            frame['pods-fs-read-mega-bytes'] = frame['pods-fs-read-bytes']  / 1048576
            frame.reset_index(inplace=True, drop=True)
            frame = frame[["timestamp","function_name", "pods-fs-read-mega-bytes", "pods-fs-read-bytes"]]
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

        if len(frame.values)>0:
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

        if len(frame.values)>0:
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

        if len(frame.values)>0:
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
        if len(frame.values)>0:
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
        if len(frame.values)>0:
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
        if len(frame.values)>0:
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
        if len(frame.values)>0:
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
        if len(frame.values)>0:
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
        if len(frame.values)>0:
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
        if len(frame.values)>0:
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
