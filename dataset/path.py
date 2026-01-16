import os
import pandas as pd
import json
from datetime import datetime, timedelta
from collections import defaultdict
import numpy as np

def build_tree(spans_df):
    id_to_span = {row['spanID']: row for _, row in spans_df.iterrows()}
    children_map = defaultdict(list)

    for _, row in spans_df.iterrows():
        refs = row['references']
        parent_id = None
        for ref in refs:
            if ref.get('refType') == 'CHILD_OF':
                parent_id = ref.get('spanID')
        if parent_id in id_to_span:
            children_map[parent_id].append(row['spanID'])
    
    root_span = None
    for _, row in spans_df.iterrows():
        if row['spanID'] == row['traceID']:
            root_span = row['spanID']
            break
    if not root_span:
        all_children = set([cid for clist in children_map.values() for cid in clist])
        possible_roots = set(id_to_span.keys()) - all_children
        if possible_roots:
            root_span = possible_roots.pop()
        else:
            return None

    return root_span, id_to_span, children_map

def extract_paths(span_id, id_to_span, children_map, current_path, all_paths):
    if span_id == "Unknown":
        service_name = "Unknown"
    else:
        service_name = id_to_span[span_id]['process'].get('serviceName', 'Unknown')
    current_path.append(service_name)

    if not children_map.get(span_id):
        all_paths.append(list(current_path))
    else:
        for child_id in children_map.get(span_id, []):
            extract_paths(child_id, id_to_span, children_map, current_path, all_paths)
    
    current_path.pop()
    
def build_forest(spans_df):
    id_to_span = {row['spanID']: row for _, row in spans_df.iterrows()}
    children_map = defaultdict(list)

    trace_break = False
    for _, row in spans_df.iterrows():
        refs = row['references']
        parent_id = None
        span_id = row['spanID']
        for ref in refs:
            if ref.get('refType') == 'CHILD_OF':
                parent_id = ref.get('spanID')
        if parent_id in id_to_span:
            children_map[parent_id].append(span_id)
        elif parent_id is None:
            continue
        else:
            trace_break = True
            children_map["Unknown"].append(span_id)

    if trace_break:
        all_span_ids = set(id_to_span.keys())|{"Unknown"} 
    else:
        all_span_ids = set(id_to_span.keys())
    all_children_ids = set(cid for clist in children_map.values() for cid in clist)
    root_spans = list(all_span_ids - all_children_ids)

    return root_spans, id_to_span, children_map



def path_stastsitic():
    path_summary = defaultdict(int)

    start_date = datetime(2025, 6, 6)
    end_date = datetime(2025, 6, 29)

    cur_date = start_date
    while cur_date <= end_date:
        date_str = cur_date.strftime("%Y-%m-%d")
        for hour in range(24):
            hour_str = f"{hour:02d}-00-00"
            parquet_path = f"phaseone/{date_str}/trace-parquet/trace_jaeger-span_{date_str}_{hour_str}.parquet"
            if not os.path.exists(parquet_path):
                continue

            print(f"Processing {parquet_path}")

            df = pd.read_parquet(parquet_path)
            if isinstance(df['references'].iloc[0], str):
                df['references'] = df['references'].apply(lambda x: json.loads(x) if pd.notnull(x) else [])

            grouped = df.groupby('traceID')
            for trace_id, trace_df in grouped:
                tree = build_forest(trace_df)
                if not tree:
                    continue
                root_spans, id_to_span, children_map = tree

                all_paths = []
                for root_id in root_spans:
                    extract_paths(root_id, id_to_span, children_map, [], all_paths)

                for path in all_paths:
                    path_key = json.dumps(path)
                    path_summary[path_key] += 1

        cur_date += timedelta(days=1)

    # 转换为列表形式保存（更直观）
    result_list = []
    for path_str, count in path_summary.items():
        result_list.append({
            "path": json.loads(path_str),
            "count": count
        })

    # 排序（比如按 count 降序）
    result_list.sort(key=lambda x: x["count"], reverse=True)

    # 保存
    with open("tyx/processed_data/trace/path_summary.json", "w") as f:
        json.dump(result_list, f, indent=2)

    print("Finished. Unique paths found:", len(result_list))

def latency_stastistic():
    # 初始化调用对的统计结构
    call_edges = defaultdict(list)

    start_date = datetime(2025, 6, 6)
    end_date = datetime(2025, 6, 29)

    cur_date = start_date
    while cur_date <= end_date:
        date_str = cur_date.strftime("%Y-%m-%d")
        for hour in range(24):
            hour_str = f"{hour:02d}-00-00"
            parquet_path = f"phaseone/{date_str}/trace-parquet/trace_jaeger-span_{date_str}_{hour_str}.parquet"
            if not os.path.exists(parquet_path):
                continue

            print(f"Processing {parquet_path}")

            df = pd.read_parquet(parquet_path)
            df = df.sample(frac=0.05, random_state=42)
            if isinstance(df['references'].iloc[0], str):
                df['references'] = df['references'].apply(lambda x: json.loads(x) if pd.notnull(x) else [])

            grouped = df.groupby('traceID')
            for trace_id, trace_df in grouped:
                tree = build_tree(trace_df)
                if not tree:
                    continue
                root_span_id, id_to_span, children_map = tree

                for _, row in trace_df.iterrows():
                    child_service = row['process'].get('serviceName', 'Unknown')
                    duration = row.get('duration', None)
                    if duration is None:
                        continue

                    refs = row['references']
                    for ref in refs:
                        if ref.get('refType') == 'CHILD_OF':
                            parent_id = ref.get('spanID')
                            if parent_id in id_to_span:
                                parent_service = id_to_span[parent_id]['process'].get('serviceName', 'Unknown')
                                if parent_service != "Unknown" and child_service != "Unknown":
                                    call_edges[(parent_service, child_service)].append(duration)

        cur_date += timedelta(days=1)

    # 计算每个服务对的P99
    result_list = []
    for (parent, child), durations in call_edges.items():
        if durations:
            p99 = float(np.percentile(durations, 99))
            result_list.append({
                "service": child,
                "parent_service": parent,
                "P99duration": p99
            })

    # 排序（按 P99duration 降序）
    result_list.sort(key=lambda x: x["P99duration"], reverse=True)

    # 保存
    os.makedirs("tyx/processed_data/trace", exist_ok=True)
    with open("tyx/processed_data/trace/service_calls_p99.json", "w") as f:
        json.dump(result_list, f, indent=2)

    print("Finished. Unique service calls found:", len(result_list))
    
path_stastsitic()