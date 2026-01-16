import os
import pandas as pd
import numpy as np
import json
import re
from collections import defaultdict
from datetime import datetime, timedelta


def load_exclude_ranges(json_file):
    """
    从 input.json 读取异常时间范围，转为北京时间
    返回: [(start, end), ...]
    """
    with open(json_file, "r", encoding="utf-8") as f:
        anomalies = json.load(f)

    ranges = []
    time_pattern = re.compile(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)")
    for item in anomalies:
        desc = item["Anomaly Description"]
        times = time_pattern.findall(desc)
        if len(times) >= 2:
            start_utc = pd.to_datetime(times[0], utc=True)
            end_utc = pd.to_datetime(times[1], utc=True)
            # 转北京时间
            start = start_utc.tz_convert("Asia/Shanghai")
            end = end_utc.tz_convert("Asia/Shanghai")
            ranges.append((start, end))
    return ranges


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

    span_to_pod = df.set_index("spanID")["pod_name"].to_dict()
    span_to_pod["root"] = "root"

    df["child_pod"] = df["spanID"].map(span_to_pod)
    df["parent_pod"] = df["parent_spanID"].map(span_to_pod)
    
    df["child_service"] = df["child_pod"].str[:-2]
    df["parent_service"] = df["parent_pod"].str[:-2]
    df.loc[df["child_pod"] == "root", "child_service"] = "root"
    df.loc[df["parent_pod"] == "root", "parent_service"] = "root"
    return df


def analyze_trace_files(root_dir, exclude_ranges=None, start_date="2025-06-06", end_date="2025-06-29"):
    counts = defaultdict(int)
    duration_sums = defaultdict(float)
    durations = defaultdict(list)

    date_range = pd.date_range(start=start_date, end=end_date, freq="D")

    for date in date_range:
        date_str = date.strftime("%Y-%m-%d")
        date_dir = os.path.join(root_dir, date_str, "trace-parquet")

        if not os.path.exists(date_dir):
            print(f"⚠️ 跳过缺失日期目录: {date_dir}")
            continue

        for hour in range(24):
            hour_str = f"{hour:02d}-00-00"
            filename = f"trace_jaeger-span_{date_str}_{hour_str}.parquet"
            file_path = os.path.join(date_dir, filename)

            if not os.path.exists(file_path):
                continue

            try:
                df = pd.read_parquet(file_path)
            except Exception as e:
                print(f"⚠️ 读取失败 {file_path}: {e}")
                continue

            if df.empty:
                continue

            # 处理 trace，得到 parent_pod / child_pod
            df = process_trace(df)

            # 将 startTime 转换为 pandas 时间戳（便于过滤异常范围）
            df["timestamp"] = pd.to_datetime(df["startTime"], utc=True)

            # 剔除异常时间范围
            if exclude_ranges:
                mask = pd.Series(True, index=df.index)
                for start, end in exclude_ranges:
                    mask &= ~df["timestamp"].between(start, end)
                df = df[mask]

            if df.empty:
                continue

            grouped = df.groupby(["parent_service", "child_service", "operationName"])["duration"].agg(["count", "sum", list])
            for key, row in grouped.iterrows():
                counts[key] += row["count"]
                duration_sums[key] += row["sum"]
                durations[key].extend(row["list"])
            print(f"✅ 处理完成 {file_path}")

    results = []
    for key in counts:
        parent_pod, child_pod, op = key
        avg_duration = duration_sums[key] / counts[key]
        arr = np.array(durations[key])
        p95 = np.percentile(arr, 95)
        p99 = np.percentile(arr, 99)
        results.append((parent_pod, child_pod, op, counts[key], avg_duration, p95, p99))

    result_df = pd.DataFrame(
        results,
        columns=["parent_service", "child_service", "operationName", "total_count", "avg_duration", "p95", "p99"]
    )
    return result_df.sort_values("total_count", ascending=False)


if __name__ == "__main__":
    root_dir = "dataset"
    exclude_ranges = load_exclude_ranges("dataset/input.json")

    result = analyze_trace_files(root_dir, exclude_ranges=exclude_ranges, start_date="2025-06-06", end_date="2025-06-29")

    print("Top 20 (parent_pod, child_pod, operationName) with P95/P99 (excluding anomaly ranges):")
    print(result.head(20))

    output_file = "processed_data/trace/service_relation_operation_duration.csv"
    result.to_csv(output_file, index=False, encoding="utf-8")
    print(f"统计结果已保存到 {output_file}")
