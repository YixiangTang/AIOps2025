import os
import pandas as pd
from datetime import datetime, timedelta

# 原始目录和输出目录
output_dir = "processed_data/log"

# 日期范围
start_date = datetime(2025, 6, 6)
end_date   = datetime(2025, 6, 29)

# 排除的日期
exclude_dates = {"2025-06-15", "2025-06-16", "2025-06-22", 
                 "2025-06-23", "2025-06-25", "2025-06-26"}

# 需要的列
keep_cols = ["k8_pod", "@timestamp", "k8_node_name", "message"]

# 遍历日期
date = start_date
while date <= end_date:
    date_str = date.strftime("%Y-%m-%d")
    if date_str not in exclude_dates:
        input_dir = "dataset"
        input_dir = os.path.join(input_dir, date_str)
        input_dir = os.path.join(input_dir, "log-parquet")
        if os.path.exists(input_dir):
            for fname in os.listdir(input_dir):
                if fname.endswith(".parquet"):
                    # 读取 parquet
                    fpath = os.path.join(input_dir, fname)
                    try:
                        df = pd.read_parquet(fpath, engine="pyarrow")
                    except:
                        df = pd.read_parquet(fpath, engine="fastparquet")

                    # 保留列
                    df = df[keep_cols]

                    # 输出目录
                    output_date_dir = os.path.join(output_dir, date_str)
                    os.makedirs(output_date_dir, exist_ok=True)

                    # 输出文件名（parquet → csv）
                    out_name = fname.replace("log_filebeat-server_", "").replace(".parquet", ".csv")
                    out_path = os.path.join(output_date_dir, out_name)

                    # 保存
                    df.to_csv(out_path, index=False)
                    print(f"✔ Saved {out_path}")
    date += timedelta(days=1)
