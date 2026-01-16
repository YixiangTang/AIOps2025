import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import os

base_dir = "dataset"
start_date = datetime(2025, 6, 6)
end_date = datetime(2025, 6, 29)
exclude_dates = {
    "2025-06-15", "2025-06-16", "2025-06-22",
    "2025-06-23", "2025-06-25", "2025-06-26"
}
date_range = [
    (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
    for i in range((end_date - start_date).days + 1)
    if (start_date + timedelta(days=i)).strftime("%Y-%m-%d") not in exclude_dates
]

def infra_pod_metric():  
    OUTPUT_DIR = "processed_data/metric/infra/pod"

    pod_list = [
        "adservice-0", "adservice-1", "adservice-2",
        "cartservice-0", "cartservice-1", "cartservice-2",
        "checkoutservice-0", "checkoutservice-1", "checkoutservice-2",
        "currencyservice-0", "currencyservice-1", "currencyservice-2",
        "emailservice-0", "emailservice-1", "emailservice-2",
        "frontend-0", "frontend-1", "frontend-2",
        "paymentservice-0", "paymentservice-1", "paymentservice-2",
        "productcatalogservice-0", "productcatalogservice-1", "productcatalogservice-2",
        "recommendationservice-0", "recommendationservice-1", "recommendationservice-2",
        "redis-cart-0",
        "shippingservice-0", "shippingservice-1", "shippingservice-2"
        ]

    infra_pod_metric_keep_list =[
        "pod_cpu_usage",
        "pod_fs_writes_bytes",
        "pod_memory_working_set_bytes",
        "pod_network_receive_packets",
        "pod_network_transmit_packets",
        "pod_processes"
    ]

    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    metric_df_dict = {}

    for metric in infra_pod_metric_keep_list:
        dfs = []
        for date in date_range:
            filename = f"infra_pod_{metric}_{date}.parquet"
            file_path = os.path.join(f"{base_dir}/{date}/metric-parquet/infra/infra_pod", filename)

            if os.path.exists(file_path):
                df = pd.read_parquet(file_path)
                metric_column = df["kpi_key"].iloc[0]
                df = df[["time", "pod", metric_column]]
                df = df.rename(columns={metric_column: metric})
                dfs.append(df)
            else:
                print(f"文件不存在: {file_path}")

        if dfs:
            merged_df = pd.concat(dfs, ignore_index=True)
            metric_df_dict[metric] = merged_df
        else:
            print(f"{metric}: 没有数据")

    # 对每个 node 构造最终表格
    for pod in pod_list:
        pod_df = None

        for metric, df in metric_df_dict.items():
            # 筛选当前 node 的数据并按时间分组计算平均值
            tmp_df = df[df["pod"] == pod][["time", metric]].copy()
            
            # 修改这里：按时间分组并计算平均值
            tmp_df = tmp_df.groupby("time", as_index=False).mean()
            
            tmp_df["time"] = pd.to_datetime(tmp_df["time"])
            tmp_df["time"] = tmp_df["time"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            
            # 按 time 合并
            if pod_df is None:
                pod_df = tmp_df
            else:
                pod_df = pd.merge(pod_df, tmp_df, on="time", how="outer")

        if pod_df is not None and not pod_df.empty:
            pod_df = pod_df.sort_values(by="time")
            pod_df = pod_df.fillna(-1)
            output_file = Path(f"{OUTPUT_DIR}/{pod}.csv")
            pod_df.to_csv(output_file, index=False)
            print(f"已保存: {output_file}")
        else:
            print(f"{pod}: 没有任何指标数据")
     
def infra_node_metric():
    OUTPUT_DIR = "processed_data/metric/infra/node"

    node_list = [
        'aiops-k8s-04', 'aiops-k8s-05', 'aiops-k8s-06',
        'aiops-k8s-07', 'aiops-k8s-08', 'aiops-k8s-01',
        'aiops-k8s-02', 'aiops-k8s-03'
    ]

    infra_node_metric_keep_list = [
        "node_cpu_usage_rate",
        "node_disk_read_bytes_total",
        "node_disk_read_time_seconds_total",
        "node_filesystem_usage_rate",
        "node_memory_MemAvailable_bytes",
        "node_memory_usage_rate",
        "node_network_receive_packets_total",
        "node_network_transmit_packets_total"
    ]

    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    # 存储每个metric的完整df
    metric_df_dict = {}

    for metric in infra_node_metric_keep_list:
        dfs = []
        for date in date_range:
            filename = f"infra_node_{metric}_{date}.parquet"
            file_path = os.path.join(f"{base_dir}/{date}/metric-parquet/infra/infra_node", filename)

            if os.path.exists(file_path):
                df = pd.read_parquet(file_path)
                metric_column = df["kpi_key"].iloc[0]
                df = df[["time", "kubernetes_node", metric_column]]
                df = df.rename(columns={metric_column: metric})
                dfs.append(df)
            else:
                print(f"文件不存在: {file_path}")

        if dfs:
            merged_df = pd.concat(dfs, ignore_index=True)
            metric_df_dict[metric] = merged_df
        else:
            print(f"{metric}: 没有数据")

    # 对每个 node 构造最终表格
    for node in node_list:
        node_df = None

        for metric, df in metric_df_dict.items():
            # 筛选当前 node 的数据并按时间分组计算平均值
            tmp_df = df[df["kubernetes_node"] == node][["time", metric]].copy()
            
            # 修改这里：按时间分组并计算平均值
            tmp_df = tmp_df.groupby("time", as_index=False).mean()
            
            tmp_df["time"] = pd.to_datetime(tmp_df["time"])
            tmp_df["time"] = tmp_df["time"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            
            # 按 time 合并
            if node_df is None:
                node_df = tmp_df
            else:
                node_df = pd.merge(node_df, tmp_df, on="time", how="outer")

        if node_df is not None and not node_df.empty:
            node_df = node_df.sort_values(by="time")
            node_df = node_df.fillna(-1) 
            output_file = Path(f"{OUTPUT_DIR}/{node}.csv")
            node_df.to_csv(output_file, index=False)
            print(f"已保存: {output_file}")
        else:
            print(f"{node}: 没有任何指标数据")
     
def infra_tidb_metric():
    OUTPUT_DIR = "processed_data/metric/infra/tidb"
    OUTPUT_FILE = "tidb-tidb.csv"  # 最终输出文件名
    
    infra_tidb_metric_keep_list =tidb_tidb_metric = [
        "block_cache_size",
        "connection_count",
        "cpu_usage",
        "failed_query_ops",
        "memory_usage",
        "qps",
        "uptime"
    ]

    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    # 存储每个metric的完整df
    metric_df_dict = {}

    for metric in infra_tidb_metric_keep_list:
        dfs = []
        for date in date_range:
            filename = f"infra_tidb_{metric}_{date}.parquet"
            file_path = os.path.join(f"{base_dir}/{date}/metric-parquet/infra/infra_tidb", filename)

            if os.path.exists(file_path):
                df = pd.read_parquet(file_path)
                metric_column = df["kpi_key"].iloc[0]
                df = df[["time", metric_column]]
                df = df.rename(columns={metric_column: metric})
                dfs.append(df)
            else:
                print(f"文件不存在: {file_path}")

        if dfs:
            merged_df = pd.concat(dfs, ignore_index=True)
            metric_df_dict[metric] = merged_df
        else:
            print(f"{metric}: 没有数据")

    # 构造最终表格
    tidb_df = None

    for metric, df in metric_df_dict.items():
        # 按时间分组计算平均值
        tmp_df = df.groupby("time", as_index=False).mean()
        tmp_df["time"] = pd.to_datetime(tmp_df["time"])
        tmp_df["time"] = tmp_df["time"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # 按 time 合并
        if tidb_df is None:
            tidb_df = tmp_df
        else:
            tidb_df = pd.merge(tidb_df, tmp_df, on="time", how="outer")

    if tidb_df is not None and not tidb_df.empty:
        tidb_df = tidb_df.sort_values(by="time")
        tidb_df = tidb_df.fillna(-1) 
        output_path = Path(f"{OUTPUT_DIR}/{OUTPUT_FILE}")
        tidb_df.to_csv(output_path, index=False)
        print(f"已保存: {output_path}")
    else:
        print("TiDB: 没有任何指标数据")
     
def infra_tipd_metric():
    OUTPUT_DIR = "processed_data/metric/infra/tidb"
    OUTPUT_FILE = "tidb-tipd.csv"  # 最终输出文件名
    
    infra_tipd_metric_keep_list = [
        "abnormal_region_count",
        "cpu_usage",
        "leader_count",
        "memory_usage",
        "region_health"
    ]

    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    # 存储每个metric的完整df
    metric_df_dict = {}

    for metric in infra_tipd_metric_keep_list:
        dfs = []
        for date in date_range:
            filename = f"infra_pd_{metric}_{date}.parquet"
            file_path = os.path.join(f"{base_dir}/{date}/metric-parquet/other", filename)

            if os.path.exists(file_path):
                df = pd.read_parquet(file_path)
                metric_column = df["kpi_key"].iloc[0]
                df = df[["time", metric_column]]
                df = df.rename(columns={metric_column: metric})
                dfs.append(df)
            else:
                print(f"文件不存在: {file_path}")

        if dfs:
            merged_df = pd.concat(dfs, ignore_index=True)
            metric_df_dict[metric] = merged_df
        else:
            print(f"{metric}: 没有数据")

    # 构造最终表格
    tipd_df = None

    for metric, df in metric_df_dict.items():
        # 按时间分组计算平均值
        tmp_df = df.groupby("time", as_index=False).mean()
        tmp_df["time"] = pd.to_datetime(tmp_df["time"])
        tmp_df["time"] = tmp_df["time"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # 按 time 合并
        if tipd_df is None:
            tipd_df = tmp_df
        else:
            tipd_df = pd.merge(tipd_df, tmp_df, on="time", how="outer")

    if tipd_df is not None and not tipd_df.empty:
        tipd_df = tipd_df.sort_values(by="time")
        tipd_df = tipd_df.fillna(-1) 
        output_path = Path(f"{OUTPUT_DIR}/{OUTPUT_FILE}")
        tipd_df.to_csv(output_path, index=False)
        print(f"已保存: {output_path}")
    else:
        print("TiPD: 没有任何指标数据")
        
def infra_tikv_metric():
    OUTPUT_DIR = "processed_data/metric/infra/tidb"
    OUTPUT_FILE = "tidb-tikv.csv"  # 最终输出文件名
    
    infra_tikv_metric_keep_list = [
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

    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    # 存储每个metric的完整df
    metric_df_dict = {}

    for metric in infra_tikv_metric_keep_list:
        dfs = []
        for date in date_range:
            filename = f"infra_tikv_{metric}_{date}.parquet"
            file_path = os.path.join(f"{base_dir}/{date}/metric-parquet/other", filename)

            if os.path.exists(file_path):
                df = pd.read_parquet(file_path)
                metric_column = df["kpi_key"].iloc[0]
                df = df[["time", metric_column]]
                df = df.rename(columns={metric_column: metric})
                dfs.append(df)
            else:
                print(f"文件不存在: {file_path}")

        if dfs:
            merged_df = pd.concat(dfs, ignore_index=True)
            metric_df_dict[metric] = merged_df
        else:
            print(f"{metric}: 没有数据")

    # 构造最终表格
    tikv_df = None

    for metric, df in metric_df_dict.items():
        # 按时间分组计算平均值
        tmp_df = df.groupby("time", as_index=False).mean()
        tmp_df["time"] = pd.to_datetime(tmp_df["time"])
        tmp_df["time"] = tmp_df["time"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # 按 time 合并
        if tikv_df is None:
            tikv_df = tmp_df
        else:
            tikv_df = pd.merge(tikv_df, tmp_df, on="time", how="outer")

    if tikv_df is not None and not tikv_df.empty:
        tikv_df = tikv_df.sort_values(by="time")
        tikv_df = tikv_df.fillna(-1) 
        output_path = Path(f"{OUTPUT_DIR}/{OUTPUT_FILE}")
        tikv_df.to_csv(output_path, index=False)
        print(f"已保存: {output_path}")
    else:
        print("TiKV: 没有任何指标数据")      
        
infra_pod_metric()
infra_node_metric()
infra_tidb_metric()
infra_tipd_metric()
infra_tikv_metric()