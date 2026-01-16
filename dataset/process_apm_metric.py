import pandas as pd
from pathlib import Path

apm_metric  = [
    'time',
    'client_error_ratio', 
    'error_ratio',
    'request', 
    'response', 
    'rrt', 
    'rrt_max', 
    'server_error_ratio', 
    'timeout'
]

skip_dates = {
    "2025-06-15", "2025-06-16", 
    "2025-06-22", "2025-06-23",
    "2025-06-25", "2025-06-26"
}

dates = pd.date_range("2025-06-06", "2025-06-29").strftime("%Y-%m-%d")
dates = [d for d in dates if d not in skip_dates]

input_base = Path("dataset")
service_output = Path("processed_data/metric/apm/service")
pod_output = Path("processed_data/metric/apm/pod")
service_output.mkdir(parents=True, exist_ok=True)
pod_output.mkdir(parents=True, exist_ok=True)

services = [ 
    "adservice", "cartservice", "checkoutservice", "currencyservice",
    "emailservice", "frontend", "paymentservice", "productcatalogservice",
    "recommendationservice", "redis-cart", "shippingservice"
]

pods = [
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

def process_service_metrics():
    for service in services:
        dfs = []
        for date in dates:
            filepath = input_base / date / "metric-parquet" / "apm" / "service" / f"service_{service}_{date}.parquet"
            if filepath.exists():
                df = pd.read_parquet(filepath)
                df = df[apm_metric]
                dfs.append(df)
            else:
                print(f"[Warning] Missing file: {filepath}")
        
        if dfs:
            merged_df = pd.concat(dfs, ignore_index=True)
            merged_df = merged_df.set_index("time")
            merged_df = merged_df.fillna(-1) 
            merged_df.to_csv(service_output / f"{service}.csv")
            print(f"[Success] Saved service: {service}")
        else:
            print(f"[Skip] No data for service: {service}")


def process_pod_metrics():
    for pod in pods:
        dfs = []
        for date in dates:
            filepath = input_base / date / "metric-parquet" / "apm" / "pod" / f"pod_{pod}_{date}.parquet"
            if filepath.exists():
                df = pd.read_parquet(filepath)
                df = df[apm_metric]
                dfs.append(df)
            else:
                print(f"[Warning] Missing file: {filepath}")
        
        if dfs:
            merged_df = pd.concat(dfs, ignore_index=True)
            merged_df = merged_df.set_index("time")
            merged_df = merged_df.fillna(-1) 
            merged_df.to_csv(pod_output / f"{pod}.csv")
            print(f"[Success] Saved pod: {pod}")
        else:
            print(f"[Skip] No data for pod: {pod}")

process_service_metrics()
process_pod_metrics()
