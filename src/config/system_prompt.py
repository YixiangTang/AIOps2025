topological_prompt = """
## The topological relations of the system (→ represents the dependencies among services):
User → frontend
frontend → ad, recommendation, productcatalog, shipping, currency, cart, checkout
recommendation → productcatalog
ad → tidb
productcatalog → tidb-tidb, tidb-tipd, tidb-tikv
cart → redis-cart
checkout → productcatalog, shipping, currency, payment, email
"""

component_prompt = """
## Microservices System Components
- Cluster Nodes: aiops-k8s-01 to aiops-k8s-08
- Single-Pod Services: redis-cart, tidb-pd, tidb-tikv, tidb-tidb
    - Pods in Single-Pod Services are named the same as it's service (e.g., redis-cart, tidb-pd, tidb-tikv, tidb-tidb).
- Triple-Pod Services: adservice, cartservice, checkoutservice, currencyservice, emailservice, frontend, paymentservice, productcatalogservice, recommendationservice, shippingservice
    - Pods in Triple-Pod Services are named with numeric suffixes (e.g., adservice-0, adservice-1, adservice-2).
"""

call_path_prompt = """
## Main call path :
1. User request entry: User → frontend (unified entry point for all user requests. Will not shown in trace data.)
2. Shopping core process: frontend → checkoutservice → (paymentservice, emailservice, shippingservice, Current Service)
3. Product browsing related: frontend → (adservice, recommendationservice, productcatalogservice, cartservice)
4. Inter-service dependencies: recommendationservice → productcatalogservice
5. Data Storage layer:
- adservice/productcatalogservice → tidb (goods advertising and data storage.)
- cartservice → redis-cart (cart cache)
- tidb cluster internal: tidb → (tidb-tidb, tidB-tikv, tidb-tipd)
"""

infra_metric_description_prompt = """
## Infrastructure Metrics Explanation
### Compute Resource Metrics(kpi_key):
- **node_cpu_usage_rate**: CPU usage rate
- **node_memory_usage_rate**: Memory usage rate
- **pod_cpu_usage**: Pod CPU usage rate  
- **pod_memory_working_set_bytes**: Pod working set memory usage
- **pod_processes**: Number of processes running inside the Pod 

### Storage Resource Metrics(kpi_key):
- **node_filesystem_free_bytes**: Free disk size
- **node_filesystem_usage_rate**: Disk usage rate
- **node_disk_read_bytes_total**: Disk read bytes
- **node_disk_read_time_seconds_total**: Disk read time(second)
- **node_disk_written_bytes_total**: Disk write bytes
- **node_disk_write_time_seconds_total**: Disk write time(second)
- **pod_fs_reads_bytes**: Pod filesystem read bytes
- **pod_fs_writes_bytes**: Pod filesystem write bytes 

### Network Resource Metrics(kpi_key):
- **node_network_receive_bytes_total**: Network receive bytes
- **node_network_transmit_bytes_total**: Network transmit bytes  
- **node_network_receive_packets_total**: Total number of received packets per second per interface
- **node_network_transmit_packets_total**: Total number of transmitted packets per second per interface  
- **node_sockstat_TCP_inuse**: Number of TCP connections
- **pod_network_receive_bytes**: Pod network receive bytes
- **pod_network_receive_packets**: Pod network receive packets
- **pod_network_transmit_bytes**: Pod network transmit bytes
- **pod_network_transmit_packets**: Pod network transmit packets
"""

apm_metric_description_prompt = """
## Application Performance Metrics
### Request & Response Metrics:
- `request`: Request count.
- `response`: Response count.
- `rrt`: Average response time.

### Exception Metrics
- `timeout`: Timeout count.
- `error_ratio`: Error ratio.
- `client_error_ratio`: Client error ratio.
- `server_error_ratio`: Server error ratio.
"""

tidb_metric_description_prompt = """
## Database(TiDB) Metrics
### TiDB-TiDB Metrics:
- `failed_query_ops`: Failed queries.
- `duration_99th`: 99th percentile request latency
- `connection_count`: Connection count
- `server_is_up`: Number of active service nodes
- `cpu_usage`: CPU usage rate
- `memory_usage`: Memory usage

### TiDB-TIKV Metrics:
- `cpu_usage`: CPU usage rate
- `memory_usage`: Memory usage
- `server_is_up`: Number of active service nodes
- `available_size`: Available storage capacity
- `raft_propose_wait`: Raft propose wait latency (P99)
- `raft_apply_wait`: Raft apply wait latency (P99)
- `rocksdb_write_stall`: RocksDB write stall count

### TiDB-TIKV Metrics:
- `store_up_count`: Number of healthy stores
- `store_down_count`: Number of down stores
- `store_unhealth_count`: Number of unhealthy stores
- `storage_used_ratio`: Storage usage ratio
- `cpu_usage`: CPU usage rate
- `memory_usage`: Memory usage
"""

