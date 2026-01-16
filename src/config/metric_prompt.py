node_metric_prompt = """
## Node Metrics Explanation:
- **node_cpu_usage_rate**: CPU usage rate
- **node_memory_usage_rate**: Memory usage rate
- **node_memory_MemAvailable_bytes**: Available memory size
- **node_filesystem_usage_rate**: Disk usage rate
- **node_disk_read_bytes_total**: Disk read bytes
- **node_disk_read_time_seconds_total**: Disk read time(second)
- **node_network_receive_packets_total**: Total number of received packets per second per interface
- **node_network_transmit_packets_total**: Total number of transmitted packets per second per interface 
"""

service_metric_prompt = """
## Service Metrics Explanation:
- **client_error_ratio**: Client error ratio
- **error_ratio**: Error ratio
- **server_error_ratio**: Server error ratio
- **request**: Request count
- **response**: Response count
- **rrt**: Average response time
- **rrt_max**: Max response time
- **timeout**: Timeout count
"""

pod_metric_prompt = """
## Pod Metrics Explanation:
- **pod_cpu_usage**: Pod CPU usage rate
- **pod_memory_working_set_bytes**: Pod working set memory usage
- **pod_processes**: Number of processes running inside the Pod
- **pod_fs_writes_bytes**: Pod filesystem write bytes
- **pod_network_receive_packets**: Pod network receive packets
- **pod_network_transmit_packets**: Pod network transmit packets
"""

tidb_metric_prompt = """
## TiDB Metrics Explanation:
### TiDB-TiDB Metrics:
- **failed_query_ops**: Failed queries.
- **connection_count**: Connection count
- **cpu_usage**: CPU usage rate
- **memory_usage**: Memory usage
- **uptime**: Service uptime
- **block_cache_size**: Block cache size
- **qps**: Queries per second
### TiDB-TIKV Metrics:
- **cpu_usage**: CPU usage rate
- **grpc_qps**: GRPC queries per second
- **io_util**: IO utilization
- **memory_usage**: Memory usage
- **qps**: Queries per second
- **raft_propose_wait**: Raft propose wait latency (P99)
- **raft_apply_wait**: Raft apply wait latency (P99)
- **region_pending**: Number of pending regions
- **snapshot_apply_count**: Number of applied snapshots
- **store_size**: Storage size
### TiDB-PD Metrics:
- **abnormal_region_count**: Number of abnormal regions
- **cpu_usage**: CPU usage rate
- **leader_count**: Leader count
- **memory_usage**: Memory usage
- **region_health**: Health status of regions
"""