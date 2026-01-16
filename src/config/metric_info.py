node_metric = [
    "node_cpu_usage_rate",
    "node_disk_read_bytes_total",
    "node_disk_read_time_seconds_total",
    "node_filesystem_usage_rate",
    "node_memory_MemAvailable_bytes",
    "node_memory_usage_rate",
    "node_network_receive_packets_total",
    "node_network_transmit_packets_total"
]
service_metric = [
    'client_error_ratio', 
    'error_ratio',
    'request', 
    'response', 
    'rrt', 
    'rrt_max', 
    'server_error_ratio', 
    'timeout'
]
pod_metric = [
    "pod_cpu_usage",
    "pod_fs_writes_bytes",
    "pod_memory_working_set_bytes",
    "pod_network_receive_packets",
    "pod_network_transmit_packets",
    "pod_processes"
]
tidb_tipd_metric = [
    "abnormal_region_count",
    "cpu_usage",
    "leader_count",
    "memory_usage",
    "region_health"
]
tidb_tikv_metric = [
    "cpu_usage",
    "grpc_qps",
    "io_util",
    "memory_usage",
    "qps",
    "raft_apply_wait",
    "raft_propose_wait",
    "region_pending",
    "snapshot_apply_count",
    "store_size"
]
tidb_tidb_metric = [
    "block_cache_size",
    "connection_count",
    "cpu_usage",
    "failed_query_ops",
    "memory_usage",
    "qps",
    "uptime"
]
