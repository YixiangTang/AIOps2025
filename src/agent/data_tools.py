import pandas as pd
from config.components_info import service_list, pod_list, node_list
from config.llm_config import get_openai_lm, get_gemini_lm
from config.system_prompt import *
from config.metric_description import metric_description
import os
from datetime import datetime, timedelta
import json
import re
from sklearn.cluster import DBSCAN
import numpy as np
from langchain.schema import HumanMessage, SystemMessage
from itertools import chain

def load_metric(file_path, start_time, end_time, normal_offset_min=30):
    df = pd.read_csv(file_path)
    df['time'] = pd.to_datetime(df['time'])

    fault_mask = (df['time'] >= pd.to_datetime(start_time)) & (df['time'] <= pd.to_datetime(end_time))
    fault_df = df.loc[fault_mask]

    normal_mask = (df['time'] >= pd.to_datetime(start_time) - pd.Timedelta(minutes=normal_offset_min)) & \
                  (df['time'] <= pd.to_datetime(start_time))
    normal_df = df.loc[normal_mask]

    return {'normal': normal_df, 'fault': fault_df}

def load_all_metrics(start_time, end_time):
    metric_service_apm_data = {
        service: load_metric(f"processed_data/metric/apm/service/{service}.csv", start_time, end_time)
        for service in service_list
    }
    metric_pod_apm_data = {
        pod: load_metric(f"processed_data/metric/apm/pod/{pod}.csv", start_time, end_time)
        for pod in pod_list
    }
    metric_node_infra_data = {
        node: load_metric(f"processed_data/metric/infra/node/{node}.csv", start_time, end_time)
        for node in node_list
    }
    metric_pod_infra_data = {
        pod: load_metric(f"processed_data/metric/infra/pod/{pod}.csv", start_time, end_time)
        for pod in pod_list
    }
    tidb_pods = ["tidb-tidb", "tidb-tipd", "tidb-tikv"]
    metric_tidb_data = {
        pod: load_metric(f"processed_data/metric/infra/tidb/{pod}.csv", start_time, end_time)
        for pod in tidb_pods
    }

    return metric_service_apm_data, metric_pod_apm_data, metric_pod_infra_data, metric_node_infra_data, metric_tidb_data

def metrics_anomaly_detection(metric_data, level="service", type="apm", sigma_k=3, iqr_k=1.5):
    anomalies = []
    metrics = []
    if level == "tidb":
        tikv_mean_std = pd.read_csv("processed_data/metric/statistic/tidb-tikv_mean_std.csv")
        tipd_mean_std = pd.read_csv("processed_data/metric/statistic/tidb-tipd_mean_std.csv")
        tidb_mean_std = pd.read_csv("processed_data/metric/statistic/tidb-tidb_mean_std.csv")

        tikv_median_iqr = pd.read_csv("processed_data/metric/statistic/tidb-tikv_median_iqr.csv")
        tipd_median_iqr = pd.read_csv("processed_data/metric/statistic/tidb-tipd_median_iqr.csv")
        tidb_median_iqr = pd.read_csv("processed_data/metric/statistic/tidb-tidb_median_iqr.csv")

        for component, data in metric_data.items():
            fault_df = data['fault']
            normal_df = data['normal']
            if component == "tidb-tikv":
                mean_std, median_iqr = tikv_mean_std, tikv_median_iqr
            elif component == "tidb-tipd":
                mean_std, median_iqr = tipd_mean_std, tipd_median_iqr
            elif component == "tidb-tidb":
                mean_std, median_iqr = tidb_mean_std, tidb_median_iqr
            else:
                continue  

            for col in fault_df.columns:
                if col == "time":
                    continue
                mean_col, std_col = f"{col}_mean", f"{col}_std"
                median_col, iqr_col = f"{col}_median", f"{col}_iqr"

                col_anomalies = []
                col_metrics = []
                
                if (mean_col not in mean_std or std_col not in mean_std or
                    median_col not in median_iqr or iqr_col not in median_iqr):
                    continue  

                mean_val = mean_std[mean_col].values[0]
                std_val = mean_std[std_col].values[0]
                sigma_upper, sigma_lower = mean_val + sigma_k * std_val, mean_val - sigma_k * std_val

                median_val = median_iqr[median_col].values[0]
                iqr_val = median_iqr[iqr_col].values[0]
                iqr_upper, iqr_lower = median_val + iqr_k * iqr_val, median_val - iqr_k * iqr_val

                low_outliers = fault_df[(fault_df[col] != 0) & (fault_df[col] != -1) &
                                    (fault_df[col] < sigma_lower) & (fault_df[col] < iqr_lower)]
                if not low_outliers.empty:
                    col_anomalies.append(
                        f"{component}: Metric({col}), "
                        f"Duration({low_outliers.shape[0]}min), "
                        f"Min({low_outliers[col].min():.4f}), "
                        f"Normal average({mean_val:.4f}), Normal median({median_val:.4f})."
                    )
                    col_metrics.append(col)

                high_outliers = fault_df[(fault_df[col] != 0) & (fault_df[col] != -1) &
                                     (fault_df[col] > sigma_upper) & (fault_df[col] > iqr_upper)]
                if not high_outliers.empty:
                    col_anomalies.append(
                        f"{component}: Metric({col}), "
                        f"Duration({high_outliers.shape[0]}min), "
                        f"Max({high_outliers[col].max():.4f}), "
                        f"Normal average({mean_val:.4f}), Normal median({median_val:.4f})."
                    )
                    col_metrics.append(col)
                # if len(col_anomalies) == 0:
                #     lower = normal_df[col].quantile(0.05)
                #     upper = normal_df[col].quantile(0.95)
                #     filtered_normal_df = normal_df[(normal_df[col] >= lower) & (normal_df[col] <= upper)]
                #     filtered_normal_df = filtered_normal_df[filtered_normal_df[col] != 0]
                    
                #     if filtered_normal_df.empty or filtered_normal_df.shape[0] < 10:
                #         continue
                    
                #     min_val = filtered_normal_df[col].min()
                #     max_val = filtered_normal_df[col].max()
                #     lower_bound = min_val / 2
                #     upper_bound = max_val * 2
                    
                #     below_half_min = fault_df[(fault_df[col] < lower_bound)]
                #     above_double_max = fault_df[(fault_df[col] > upper_bound)]
                #     if not below_half_min.empty and below_half_min.shape[0] > fault_df.shape[0] * 0.1:
                #         if not (fault_df[fault_df[col] < lower_bound][col].min() == 0 or fault_df[fault_df[col] < lower_bound][col].min() == -1):  
                #             col_anomalies.append(
                #                 f"{component}: Metric({col}), "
                #                 f"Duration({below_half_min.shape[0]}min), "
                #                 f"Min({fault_df[fault_df[col] < lower_bound][col].min():.4f}), "
                #                 f"Normal range([{min_val:.4f}, {max_val:.4f}])."
                #             )
                #             col_metrics.append(col)
                #     if not above_double_max.empty and above_double_max.shape[0] > fault_df.shape[0] * 0.1:
                #         if not (fault_df[fault_df[col] < lower_bound][col].min() == 0 or fault_df[fault_df[col] < lower_bound][col].min() == -1):
                #             col_anomalies.append(
                #                 f"{component}: Metric({col}), "
                #                 f"Duration({above_double_max.shape[0]}min), "
                #                 f"Max({fault_df[fault_df[col] > upper_bound][col].max()}), "
                #                 f"Normal range([{min_val:.4f}, {max_val:.4f}])."
                #             )
                #             col_metrics.append(col)
                anomalies += col_anomalies
                metrics += col_metrics
    else:
        mean_std = pd.read_csv(f"processed_data/metric/statistic/{level}_{type}_mean_std.csv")
        median_iqr = pd.read_csv(f"processed_data/metric/statistic/{level}_{type}_median_iqr.csv")

        for component, data in metric_data.items():
            fault_df = data['fault']
            normal_df = data['normal']
            selected_mean_std = mean_std[mean_std[level] == component]
            selected_median_iqr = median_iqr[median_iqr[level] == component]

            for col in fault_df.columns:
                if col == "time":
                    continue
                mean_col, std_col = f"{col}_mean", f"{col}_std"
                median_col, iqr_col = f"{col}_median", f"{col}_iqr"
                col_anomalies = []
                col_metrics = []
                if (mean_col not in selected_mean_std or std_col not in selected_mean_std or
                    median_col not in selected_median_iqr or iqr_col not in selected_median_iqr):
                    continue  

                mean_val = selected_mean_std[mean_col].values[0]
                std_val = selected_mean_std[std_col].values[0]
                sigma_upper, sigma_lower = mean_val + sigma_k * std_val, mean_val - sigma_k * std_val

                median_val = selected_median_iqr[median_col].values[0]
                iqr_val = selected_median_iqr[iqr_col].values[0]
                iqr_upper, iqr_lower = median_val + iqr_k * iqr_val, median_val - iqr_k * iqr_val

                low_outliers = fault_df[(fault_df[col] != 0) & (fault_df[col] != -1) &
                                    (fault_df[col] < sigma_lower) & (fault_df[col] < iqr_lower)]
                if (not low_outliers.empty):
                    col_anomalies.append(
                        f"{component}: Metric({col}), "
                        f"Duration({low_outliers.shape[0]}min), "
                        f"Min({low_outliers[col].min():.4f}), "
                        f"Normal average({mean_val:.4f}), Normal median({median_val:.4f})."
                    )
                    col_metrics.append(col)
                    
                high_outliers = fault_df[(fault_df[col] != 0) & (fault_df[col] != -1) &
                                     (fault_df[col] > sigma_upper) & (fault_df[col] > iqr_upper)]
                if (not high_outliers.empty):
                    col_anomalies.append(
                        f"{component}: Metric({col}), "
                        f"Duration({high_outliers.shape[0]}min), "
                        f"Max({high_outliers[col].max():.4f}), "
                        f"Normal average({mean_val:.4f}), Normal median({median_val:.4f})."
                    )
                    col_metrics.append(col)
                    
                # if len(col_anomalies) == 0:
                #     lower = normal_df[col].quantile(0.05)
                #     upper = normal_df[col].quantile(0.95)
                #     filtered_normal_df = normal_df[(normal_df[col] >= lower) & (normal_df[col] <= upper)]
                #     filtered_normal_df = filtered_normal_df[filtered_normal_df[col] != 0]
                    
                #     if filtered_normal_df.empty or filtered_normal_df.shape[0] < 10:
                #         continue
                    
                #     min_val = filtered_normal_df[col].min()
                #     max_val = filtered_normal_df[col].max()
                #     lower_bound = min_val / 2
                #     upper_bound = max_val * 2
                    
                #     below_half_min = fault_df[(fault_df[col] < lower_bound)]
                #     above_double_max = fault_df[(fault_df[col] > upper_bound)]
                #     if not below_half_min.empty and below_half_min.shape[0] > fault_df.shape[0] * 0.5:
                #         if not (fault_df[fault_df[col] < lower_bound][col].min() == 0 or fault_df[fault_df[col] < lower_bound][col].min() == -1):  
                #             col_anomalies.append(
                #                 f"{component}: Metric({col}), "
                #                 f"Duration({below_half_min.shape[0]}min), "
                #                 f"Min({fault_df[fault_df[col] < lower_bound][col].min():.4f}), "
                #                 f"Normal range([{min_val:.4f}, {max_val:.4f}])."
                #             )
                #             col_metrics.append(col)
                #     if not above_double_max.empty and above_double_max.shape[0] > fault_df.shape[0] * 0.5:
                #         if not (fault_df[fault_df[col] < lower_bound][col].min() == 0 or fault_df[fault_df[col] < lower_bound][col].min() == -1):
                #             col_anomalies.append(
                #                 f"{component}: Metric({col}), "
                #                 f"Duration({above_double_max.shape[0]}min), "
                #                 f"Max({fault_df[fault_df[col] > upper_bound][col].max():.4f}), "
                #                 f"Normal range([{min_val:.4f}, {max_val:.4f}])."
                #             )
                #             col_metrics.append(col)
                
                anomalies += col_anomalies
                metrics += col_metrics
    return anomalies, metrics

def metrics_jump_anomaly_detection(metric_data):
    anomalies = []
    metrics = []
    for component, data in metric_data.items():
        fault_df = data['fault']
        normal_df = data['normal'] 
        for col in normal_df.columns:
            if col == "time":
                continue
            lower = normal_df[col].quantile(0.05)
            upper = normal_df[col].quantile(0.95)
            filtered_normal_df = normal_df[(normal_df[col] >= lower) & (normal_df[col] <= upper)]
            filtered_normal_df = filtered_normal_df[filtered_normal_df[col] != 0]
            
            if filtered_normal_df.empty or filtered_normal_df.shape[0] < 10:
                continue
            
            min_val = filtered_normal_df[col].min()
            max_val = filtered_normal_df[col].max()
            lower_bound = min_val / 2
            upper_bound = max_val * 2
            
            below_half_min = fault_df[(fault_df[col] < lower_bound)]
            above_double_max = fault_df[(fault_df[col] > upper_bound)]
            if not below_half_min.empty and below_half_min.shape[0] > fault_df.shape[0] * 0.1:
                if not (fault_df[fault_df[col] < lower_bound][col].min() == 0 or fault_df[fault_df[col] < lower_bound][col].min() == -1):  
                    anomalies.append(
                        f"{component}: Metric({col}), "
                        f"Duration({below_half_min.shape[0]}min), "
                        f"Min({fault_df[fault_df[col] < lower_bound][col].min()}), "
                        f"Normal range([{min_val}, {max_val}])."
                    )
                    metrics.append(col)
            if not above_double_max.empty and above_double_max.shape[0] > fault_df.shape[0] * 0.1:
                if not (fault_df[fault_df[col] < lower_bound][col].min() == 0 or fault_df[fault_df[col] < lower_bound][col].min() == -1):
                    anomalies.append(
                        f"{component}: Metric({col}), "
                        f"Duration({above_double_max.shape[0]}min), "
                        f"Max({fault_df[fault_df[col] > upper_bound][col].max()}), "
                        f"Normal range([{min_val}, {max_val}])."
                    )
                    metrics.append(col)
    return anomalies, metrics

def metric_description_generation(metrics):
    description_text = ""
    for metric in metrics:
        description = metric_description.get(metric)
        description_text += "- " + metric + ": " + description + "\n"
    return description_text

def missing_metric_detection(data):
    missing_metric = []
    for component, dataframe in data.items():
        data = dataframe['fault']
        if data.empty:
            # missing_metric.append(f"{component} metrics missing.")
            continue
        else:
            start_dt = data['time'].iloc[0]
            end_dt = data['time'].iloc[-1]
            full_time_range = pd.date_range(start=start_dt, end=end_dt, freq='1min')
            full_df = pd.DataFrame({'time': full_time_range})
            merged_df = pd.merge(full_df, data, on='time', how='left')
            merged_df.fillna(-1, inplace=True)
            value_columns = [col for col in merged_df.columns if col != 'time']
            total_values = len(value_columns) * len(merged_df)
            minus_one_total = (merged_df[value_columns] == -1).sum().sum() 
            if minus_one_total > total_values / 2:
                missing_metric.append(f"{component} metrics missing.")
    return missing_metric

def metric_data(start_time, end_time):
    metric_service_apm_data, metric_pod_apm_data, metric_pod_infra_data, metric_node_infra_data, metric_tidb_data = load_all_metrics(start_time, end_time)
    service_missing = missing_metric_detection(metric_service_apm_data)
    pod_apm_missing = missing_metric_detection(metric_pod_apm_data)
    pod_infra_missing = missing_metric_detection(metric_pod_infra_data)
    node_missing = missing_metric_detection(metric_node_infra_data)
    tidb_missing = missing_metric_detection(metric_tidb_data)
    
    missing = service_missing + pod_apm_missing + pod_infra_missing + node_missing + tidb_missing
    missing = list(set(missing))
    missing.sort()
    missing_anomaly = '\n'.join(missing) if len(missing) != 0 else 'No missing metrics detected.'
    
    sigma_k, iqr_k = 3, 1.5
   
    # service_jump_anomaly, service_jump_metrics = metrics_jump_anomaly_detection(metric_service_apm_data)
    # pod_jump_apm_anomaly, pod_jump_apm_metrics = metrics_jump_anomaly_detection(metric_pod_apm_data)
    # pod_jump_infra_anomaly, pod_jump_infra_metrics = metrics_jump_anomaly_detection(metric_pod_infra_data)
    # node_jump_infra_anomaly, node_jump_infra_metrics = metrics_jump_anomaly_detection(metric_node_infra_data)
    # tidb_jump_anomaly, tidb_jump_metrics = metrics_jump_anomaly_detection(metric_tidb_data) 

    service_anomaly, service_metrics = metrics_anomaly_detection(metric_service_apm_data, "service", "apm", sigma_k=sigma_k, iqr_k=iqr_k)
    pod_apm_anomaly, pod_apm_metrics = metrics_anomaly_detection(metric_pod_apm_data, "pod", "apm", sigma_k=sigma_k, iqr_k=iqr_k)
    pod_infra_anomaly, pod_infra_metrics = metrics_anomaly_detection(metric_pod_infra_data, "pod", "infra", sigma_k=sigma_k, iqr_k=iqr_k)
    node_infra_anomaly, node_infra_metrics = metrics_anomaly_detection(metric_node_infra_data, "node", "infra", sigma_k=sigma_k, iqr_k=iqr_k)
    tidb_anomaly, tidb_metrics = metrics_anomaly_detection(metric_tidb_data, "tidb", "infra", sigma_k=sigma_k, iqr_k=iqr_k)  

    metrics = service_metrics + pod_apm_metrics + pod_infra_metrics + node_infra_metrics + tidb_metrics
    # metrics += service_jump_metrics + pod_jump_apm_metrics + pod_jump_infra_metrics + node_jump_infra_metrics + tidb_jump_metrics
    metrics = list(set(metrics))
    description_text = metric_description_generation(metrics)
    
    # service_anomaly += service_jump_anomaly
    # pod_apm_anomaly += pod_jump_apm_anomaly
    # pod_infra_anomaly += pod_jump_infra_anomaly
    # node_infra_anomaly += node_jump_infra_anomaly
    # tidb_anomaly += tidb_jump_anomaly
    def list2text(anomalies):
        if len(anomalies) != 0:
            anomalies = [f"{i+1}. {s}" for i, s in enumerate(anomalies)]
            anomalies = '\n'.join(anomalies)
        else:
            anomalies = 'No anomalies detected.'
        return anomalies
    
    service_anomaly = list2text(service_anomaly)
    pod_apm_anomaly = list2text(pod_apm_anomaly)
    pod_infra_anomaly = list2text(pod_infra_anomaly)
    node_infra_anomaly = list2text(node_infra_anomaly)
    tidb_anomaly = list2text(tidb_anomaly)
    
    
    metric_data = f"""
## Metric Anomalies Summary
### Missing Metrics
{missing_anomaly}
### Node-level infrastructure metric anomalies
{node_infra_anomaly}
### TiDB metric anomalies
{tidb_anomaly}
### Service-level application performance metric anomalies
{service_anomaly}
### Pod-level application performance metric anomalies
{pod_apm_anomaly}
### Pod-level infrastructure metric anomalies
{pod_infra_anomaly}
### Metric Description
{description_text}
"""
    return metric_data

def load_logs(start_time, end_time):
    start_dt = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ") + timedelta(hours=8)
    end_dt = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%SZ") + timedelta(hours=8)
    file_paths = []
    current = start_dt.replace(minute=0, second=0, microsecond=0)
    while current <= end_dt:
        date_str = current.strftime("%Y-%m-%d")
        hour_str = current.strftime("%H-00-00")
        path = f"dataset/{date_str}/log-parquet/log_filebeat-server_{date_str}_{hour_str}.parquet"
        file_paths.append(path)
        current += timedelta(hours=1)
    dfs = []
    for path in file_paths:
        if os.path.exists(path):
            df = pd.read_parquet(path)
            dfs.append(df)

    if len(dfs) == 0:
        merged_df = pd.DataFrame()
    else:
        merged_df = pd.concat(dfs, ignore_index=True)

    if not merged_df.empty:
        log_df = merged_df[
            (merged_df["@timestamp"] >= start_time) &
            (merged_df["@timestamp"] <= end_time)
        ].copy()
    else:
        log_df = merged_df
    return log_df


def log_dbscan_clustering(log_df):
    def tokenizer(text):
        placeholders = re.findall(r'<\*[a-zA-Z]+\*>', text)
        words = re.findall(r'\b[a-z]{2,}\b', text)
        tokens = set(placeholders + words)
        
        return tokens
    def jaccard_distance(set1, set2):
        if not set1 and not set2:
            return 0.0
        return 1 - len(set1 & set2) / len(set1 | set2)

    log_df = log_df.reset_index(drop=True)
    tokenized = log_df['message'].apply(tokenizer)
    n = len(tokenized)
    
    distance_matrix = np.zeros((n, n))
    for i in range(n):
        for j in range(i, n):
            dist = jaccard_distance(tokenized[i], tokenized[j])
            distance_matrix[i, j] = dist
            distance_matrix[j, i] = dist

    db = DBSCAN(eps=0.5, min_samples=2, metric='precomputed')
    labels = db.fit_predict(distance_matrix)

    log_df['cluster'] = labels
    
    summary = log_df.groupby('cluster').agg(
        representative_message=('message', 'first'),  
        unique_pods=('k8_pod', lambda x: list(pd.unique(x))),
        cluster_size=('message', 'count')                
    ).reset_index()
    
    return summary

def log_hash_clustering(log_df):
    log_df = log_df.reset_index(drop=True)
    log_df["message_hash"] = log_df["message"].apply(hash)
    hash_clustered = (
        log_df.groupby("message_hash")
        .agg(
            unique_pods=("k8_pod", lambda x: list(sorted(set(x)))),
            representative_message=("message", "first"),
            cluster_size=("message", "size"),
        )
        .reset_index(drop=True)
    )
    if hash_clustered.shape[0] <= 10:
        return hash_clustered
    
    def text_to_wordset(text):
        return set(text.lower().split())
    hash_clustered['wordset'] = hash_clustered['representative_message'].apply(text_to_wordset)
    def jaccard_distance(set1, set2):
        if not set1 and not set2:
            return 0.0
        return 1 - len(set1 & set2) / len(set1 | set2)
    wordsets = hash_clustered['wordset'].tolist()
    n = len(wordsets)
    dist_matrix = np.zeros((n, n))
    for i in range(n):
        for j in range(i + 1, n):
            d = jaccard_distance(wordsets[i], wordsets[j])
            dist_matrix[i, j] = d
            dist_matrix[j, i] = d
    dbscan = DBSCAN(metric='precomputed', eps=0.2, min_samples=2)
    labels = dbscan.fit_predict(dist_matrix)
    hash_clustered['dbscan_label'] = labels
    def aggregate_group(group):
        all_pods = list(chain.from_iterable(group['unique_pods']))
        unique_pods = list(dict.fromkeys(all_pods))
        return pd.Series({
            'unique_pods': unique_pods,
            'representative_message': group['representative_message'].iloc[0],
            'cluster_size': group['cluster_size'].sum()
        })
    dbscan_clustered = (
        hash_clustered
        .groupby('dbscan_label', group_keys=False)
        .apply(aggregate_group, include_groups=False)
        .reset_index(drop=True)
    )
    dbscan_clustered = dbscan_clustered[['unique_pods', 'representative_message', 'cluster_size']]
    return dbscan_clustered

def log_data(start_time, end_time):
    log_df = load_logs(start_time, end_time)
    if log_df.empty:
        return """## Log Clustering Results:
No logs in the fault time."""   

    error_keywords = [
        'error', 'fail', 'failure', 'fatal', 'critical', 
        'warning', 'warn', 'alert', 'caution', 
        'exception', 'traceback', 'stacktrace',
        'denied', 'unauthorized', 'forbidden', 'rejected',
        'timeout', 'refused', 'unreachable', 'disconnect',
        'abort', 'crash', 'panic',
        'corrupt', 'invalid', 'mismatch',
        'cannot', 'unable', 'not found', 'broken',  
        'init', 'kill', "gc", "dns", "internal"
    ]
    error_df = log_df[log_df["message"].str.contains('|'.join(error_keywords), case=False, na=False)].copy()
    
    if error_df.empty:
        return """## Log Clustering Results:
No error logs found in the fault time."""   
    
    def extract_error(msg):
        try:
            data = json.loads(msg)       
            return data.get("error", data.get("message", msg)) 
        except Exception:
            return msg         
    error_df["message"] = error_df["message"].apply(extract_error)

    patterns = {
        '<IP>': r'\b(?:\d{1,3}\.){3}\d{1,3}\b',
        '<PORT>': r'\b\d{1,5}\b', 
        '<URL>': r'https?://[^\s]+',
        '<PATH>': r'(/[A-Za-z0-9_\-\.]+)+',
        '<HEX>': r'\b0x[0-9a-fA-F]+\b',
    }
    def sanitize_message(msg):
        sanitized = msg
        for placeholder, pattern in patterns.items():
            sanitized = re.sub(pattern, placeholder, sanitized)
        return sanitized

    error_df['message'] = error_df['message'].apply(sanitize_message)

    # summary = log_dbscan_clustering(error_df)
    summary = log_hash_clustering(error_df)
    
    log_abstract = []
    for index, row in summary.iterrows():
        log_abstract.append(f'{index+1}. In Pod(s) {row["unique_pods"]}, the following log appeared {row["cluster_size"]} times:\n "{row["representative_message"]}".')
    log_abstract = "\n".join(log_abstract)
    return f"""## Log Clustering Results:
{log_abstract}"""   

def load_trace(start_time, end_time):
    start_dt = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ") + timedelta(hours=8)
    end_dt = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%SZ") + timedelta(hours=8)

    start_ts_us = int(start_dt.timestamp() * 1e6)
    end_ts_us = int(end_dt.timestamp() * 1e6)

    file_paths = []
    current = start_dt.replace(minute=0, second=0, microsecond=0)
    while current <= end_dt:
        date_str = current.strftime("%Y-%m-%d")
        hour_str = current.strftime("%H-00-00")
        path = f"dataset/{date_str}/trace-parquet/trace_jaeger-span_{date_str}_{hour_str}.parquet"
        file_paths.append(path)
        current += timedelta(hours=1)

    dfs = []
    for path in file_paths:
        if os.path.exists(path):
            df = pd.read_parquet(path)
            dfs.append(df)

    if len(dfs) == 0:
        trace_df = pd.DataFrame()
        return trace_df
    else:
        trace_df = pd.concat(dfs, ignore_index=True)

    trace_df["startTime"] = trace_df["startTime"].astype("int64")
    trace_df["duration"] = trace_df["duration"].astype("int64")

    trace_df = trace_df[
        (trace_df["startTime"] >= start_ts_us) &
        (trace_df["startTime"] <= end_ts_us)
    ].copy()

    return trace_df

def process_trace(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df['startTime'] = df['startTime'].apply(
        lambda ts: (datetime.utcfromtimestamp(ts / 1e6) - timedelta(hours=8)).strftime("%Y-%m-%dT%H:%M:%SZ")
    )

    def extract_tag_value(proc, keys):
        if isinstance(proc, dict):
            for tag in proc.get("tags", []):
                if tag.get("key") in keys:
                    return tag.get("value")
        return None
    
    def extrace_parent_spanID(refs):
        if len(refs) == 0:
            return "root"
        else:
            for ref in refs:
                if ref.get("refType") == "CHILD_OF":
                    return ref.get("spanID")
        return None

    df['pod_name'] = df['process'].apply(lambda x: extract_tag_value(x, ["name", "podName"]))
    df['node_name'] = df['process'].apply(lambda x: extract_tag_value(x, ["nodeName", "node_name"]))
    df['parent_spanID'] = df['references'].apply(extrace_parent_spanID)
    span_to_pod = df.set_index("spanID")["pod_name"]
    span_to_pod.loc["root"] = "root"
    df["child_pod"] = df["spanID"].map(span_to_pod)
    df["parent_pod"] = df["parent_spanID"].map(span_to_pod)
    df = df[df['parent_pod'].notna()]
    
    df["child_service"] = df["child_pod"].str[:-2]
    df["parent_service"] = df["parent_pod"].str[:-2]
    df.loc[df["child_pod"] == "root", "child_service"] = "root"
    df.loc[df["parent_pod"] == "root", "parent_service"] = "root"
    return df

def trace_anomaly_detection(trace_df):
    def extract_error_codes(tags):
        codes = []
        for tag in tags:
            key = tag.get('key', '').lower()
            value = tag.get('value')
            if key == 'http.status_code':
                try:
                    status_code = int(value)
                    if status_code >= 400:
                        codes.append("HTTP status code:"+str(status_code))
                except ValueError:
                    return codes
            if key == 'status.code':
                try:
                    status_code = int(value)
                    if status_code >= 2:
                        codes.append("RPC status code:"+str(status_code))
                except (ValueError, TypeError):
                    return codes
        return codes

    trace_df['error_codes'] = trace_df['tags'].apply(extract_error_codes)
    trace_df['is_anomaly'] = trace_df['error_codes'].apply(lambda x: 1 if len(x) > 0 else 0)
    
    threshold_df = pd.read_csv("processed_data/trace/operation_duration.csv", usecols=["operationName", "p99"])
    trace_df = trace_df.merge(threshold_df, on="operationName", how="left")
    duration_anomaly_count = trace_df["duration"] > trace_df["p99"]
    trace_df["is_anomaly"] = trace_df["is_anomaly"] + duration_anomaly_count
    return trace_df

def trace_intergrity_check(trace_df):
    invalid_traces = set()
    for trace_id, group in trace_df.groupby("traceID"):
        span_ids = set(group["spanID"].tolist())
        for refs in group["references"]:
            if refs is not None:  
                for ref in refs:
                    parent_id = ref.get("spanID")
                    if parent_id not in span_ids:
                        invalid_traces.add(trace_id)
                        break
            if trace_id in invalid_traces:
                break

    return trace_df[~trace_df["traceID"].isin(invalid_traces)].copy()

def trace_top_k(trace_df, k):
    anomaly_sums = trace_df.groupby('traceID')['is_anomaly'].sum()
    sorted_traces = anomaly_sums.sort_values(ascending=False)
    top_k_traces = sorted_traces.head(k).index.tolist()
    def build_tree(df):
        tree = {}
        for index, row in df.iterrows():
            spanID = row['spanID']
            parent_spanID = row['parent_spanID']
            pod_name = row['pod_name']
            if len(row['error_codes']) == 0:
                error_codes = ""
            else:  
                error_codes = " ".join(row['error_codes'])
            duration_info = f"Duration: {row['duration']}μs exceed threshold {row['p99']}μs" if row['duration'] > row['p99'] else ""
            if duration_info and error_codes:
                span_info = f"{error_codes} {duration_info}"
            elif duration_info:
                span_info = f"{duration_info}"
            elif error_codes:
                span_info = f"{error_codes}"
            else:
                span_info = ""
            if parent_spanID not in tree:
                tree[parent_spanID] = []
            tree[parent_spanID].append({
                'spanID': spanID,
                'pod_name': pod_name,
                'span_info': span_info
            })
        return tree

    def prune_tree(tree):
        change = False
        for parent_id, child_spans in tree.items():
            for child_span in child_spans:
                if child_span.get("span_info", "") == "" and child_span["spanID"] not in tree:
                    child_spans.remove(child_span)
                    change = True
        for parent_id, child_spans in tree.copy().items():
            if child_spans == []:
                del tree[parent_id]
                change = True
        if change:
            prune_tree(tree)
        return tree

    def get_paths(tree, parent_id="root", current_path=""):
        paths = []
        if parent_id not in tree or not tree[parent_id]:
            paths.append(current_path.strip(" -> "))
            return paths
        for node in tree[parent_id]:
            pod_name = node.get('pod_name', '')
            span_id = node.get('spanID', '')
            span_info = node.get('span_info', '')
            node_description = pod_name
            if span_info:
                node_description += f"({span_info})"
            new_path = f"{current_path} -> {node_description}"
            paths.extend(get_paths(tree, span_id, new_path))
        return paths  
    
    def tree_to_indented_text(tree, parent_id="root", indent=0):
        text = ""
        if parent_id not in tree:
            return ""
        for node in tree[parent_id]:
            indent_str = "  " * indent 
            pod_name = node.get('pod_name', 'Unnamed')
            span_info = node.get('span_info', '')
            node_text = f"{indent_str}{pod_name}"
            if span_info: 
                node_text += f" ({span_info})"
            text += node_text + "\n"
            child_text = tree_to_indented_text(tree, parent_id=node['spanID'], indent=indent + 1)
            text += child_text
        return text
    
    top_k_trace_text = ""
    for index, trace_id in enumerate(top_k_traces):
        trace_rows = trace_df[trace_df['traceID'] == trace_id]   
        tree = build_tree(trace_rows)
        pruned_tree = prune_tree(tree)
        indented_text = tree_to_indented_text(pruned_tree)
        top_k_trace_text += f"Trace {index+1}:\n{indented_text}\n" 
    return top_k_trace_text

def trace_status_code_analysis(trace_df):
    def extract_http_error_codes(tags):
        codes = None
        for tag in tags:
            key = tag.get('key', '').lower()
            value = tag.get('value')
            if key == 'http.status_code':
                try:
                    status_code = int(value)
                    if status_code >= 400:
                        codes = status_code
                except ValueError:
                    return None
        return codes
    def extract_rpc_error_codes(tags):
        codes = None
        for tag in tags:
            key = tag.get('key', '').lower()
            value = tag.get('value')
            if key == 'status.code':
                try:
                    status_code = int(value)
                    if status_code >= 1:
                        codes = status_code
                except ValueError:
                    return None
        return codes
    trace_df['http_error_codes'] = trace_df['tags'].apply(extract_http_error_codes)
    trace_df['rpc_error_codes'] = trace_df['tags'].apply(extract_rpc_error_codes)
    
    http_counts = trace_df["http_error_codes"].value_counts()
    if http_counts.empty:
        return "No HTTP error status codes detected."
    else:
        http_text = [f"'{int(code)}' occure in {int(count)} traces." for code, count in http_counts.items()]
        http_text = [f"{i+1}. {s}" for i, s in enumerate(http_text)]
        http_text = "HTTP status code:\n" + "\n".join(http_text)
    
    grouped_counts = (
        trace_df.groupby("pod_name")["rpc_error_codes"]
        .value_counts()
        .reset_index(name="count")
    )
    rpc_texts = []
    for pod, sub_df in grouped_counts.groupby("pod_name"):
        lines = [
            f"'{int(code)}' occurred {int(cnt)} times in {pod}"
            for code, cnt in zip(sub_df["rpc_error_codes"], sub_df["count"])
        ]
        rpc_texts.extend(lines)
    if grouped_counts.empty:
        return "No RPC error status codes."
    else:
        rpc_texts = [f"{i+1}. {s}" for i, s in enumerate(rpc_texts)]
        rpc_text = "RPC status code:\n" + "\n".join(rpc_texts)
    
    return http_text + '\n' + rpc_text

def trace_latency_analysis(trace_df):
    threshold_df = pd.read_csv("processed_data/trace/service_relation_operation_duration.csv")

    trace_df = (
        trace_df.groupby(["parent_service", "child_service", "operationName"])["duration"]
        .quantile([0.95, 0.99])
        .unstack()
        .reset_index()
        .rename(columns={0.95: "df_p95", 0.99: "df_p99"})
    )

    trace_df = trace_df.merge(
        threshold_df,
        on=["parent_service", "child_service", "operationName"],
        how="inner"
    )

    mask = (trace_df["df_p95"] > 1.2 * trace_df["p95"]) & (trace_df["df_p99"] > 1.2 * trace_df["p99"])

    anomalies = []

    for _, row in trace_df.loc[mask].iterrows():
        anomalies.append(
            f"{row['parent_service']}->{row['child_service']} (operationName:{row['operationName']}): "
            f"P99 {row['df_p99']:.0f}µs > threshold {row['p99']:.0f}µs"
        )
    
    if len(anomalies)==0:
        return "No latency anomalies detected."
    else:
        anomalies = "\n".join([f"{i+1}. {s}" for i, s in enumerate(anomalies)])
    return anomalies

def trace_data(start_time, end_time):
    trace_df = load_trace(start_time, end_time)
    if trace_df.empty:
        return """## Trace Analysis
Traces data lost in the fault time.
"""
    trace_df = process_trace(trace_df)
    if trace_df.empty:
        return """## Trace Analysis
Traces data lost in the fault time.
"""
    # trace_df = trace_intergrity_check(trace_df)
    # trace_df = trace_anomaly_detection(trace_df)
    # top_k_trace_text = trace_top_k(trace_df, k=3)
    trace_status_code_text = trace_status_code_analysis(trace_df)
    trace_latency_text = trace_latency_analysis(trace_df)
    trace_abstract_prompt = """
## Trace Analysis
### Status code distribution
{status_code}
### Span duration anomaly detection    
{latency}
"""
    return trace_abstract_prompt.format(status_code=trace_status_code_text, latency=trace_latency_text)

