import pandas as pd
from datetime import datetime, timedelta
import os


file_path = r"dataset\2025-06-20\log-parquet\log_filebeat-server_2025-06-20_17-00-00.parquet"
df = pd.read_parquet(file_path)
print(df.columns)

pd.set_option('display.max_rows', None)       # 显示所有行
pd.set_option('display.max_columns', None)    # 显示所有列
pd.set_option('display.max_colwidth', None)   # 每个单元格的字符串不截断
pd.set_option('display.width', None)          # 自动适应控制台宽度

base_dir = r"dataset"
start_date = datetime(2025, 6, 6)
end_date = datetime(2025, 6, 29)

unique_pods = {
    "aiops-k8s-01": [],
    "aiops-k8s-02": [],
    "aiops-k8s-03": [],
    "aiops-k8s-04": [],
    "aiops-k8s-05": [],
    "aiops-k8s-06": [],
    "aiops-k8s-07": [],
    "aiops-k8s-08": []
}

for i in range((end_date - start_date).days + 1):
    date_str = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
    
    for hour in range(24):
        hour_str = f"{hour:02d}-00-00"
        file_path = os.path.join(base_dir, date_str, "log-parquet",
                                 f"log_filebeat-server_{date_str}_{hour_str}.parquet")
        
        if os.path.exists(file_path):
            try:
                df = pd.read_parquet(file_path)
                print(f"读取成功: {file_path}, 行数: {len(df)}")
                
                unique_pods["aiops-k8s-01"].extend(df[df['k8_node_name']=='aiops-k8s-01']['k8_pod'].unique().tolist())
                unique_pods["aiops-k8s-02"].extend(df[df['k8_node_name']=='aiops-k8s-02']['k8_pod'].unique().tolist())
                unique_pods["aiops-k8s-03"].extend(df[df['k8_node_name']=='aiops-k8s-03']['k8_pod'].unique().tolist())
                unique_pods["aiops-k8s-04"].extend(df[df['k8_node_name']=='aiops-k8s-04']['k8_pod'].unique().tolist())
                unique_pods["aiops-k8s-05"].extend(df[df['k8_node_name']=='aiops-k8s-05']['k8_pod'].unique().tolist())
                unique_pods["aiops-k8s-06"].extend(df[df['k8_node_name']=='aiops-k8s-06']['k8_pod'].unique().tolist())
                unique_pods["aiops-k8s-07"].extend(df[df['k8_node_name']=='aiops-k8s-07']['k8_pod'].unique().tolist())
                unique_pods["aiops-k8s-08"].extend(df[df['k8_node_name']=='aiops-k8s-08']['k8_pod'].unique().tolist())
                
                
            except Exception as e:
                print(f"读取失败: {file_path}, 错误: {e}")
        else:
            print(f"文件不存在: {file_path}")

unique_pods["aiops-k8s-01"] = list(set(unique_pods["aiops-k8s-01"]))
unique_pods["aiops-k8s-02"] = list(set(unique_pods["aiops-k8s-02"]))
unique_pods["aiops-k8s-03"] = list(set(unique_pods["aiops-k8s-03"]))
unique_pods["aiops-k8s-04"] = list(set(unique_pods["aiops-k8s-04"]))
unique_pods["aiops-k8s-05"] = list(set(unique_pods["aiops-k8s-05"]))
unique_pods["aiops-k8s-06"] = list(set(unique_pods["aiops-k8s-06"]))
unique_pods["aiops-k8s-07"] = list(set(unique_pods["aiops-k8s-07"]))
unique_pods["aiops-k8s-08"] = list(set(unique_pods["aiops-k8s-08"]))

print(unique_pods)
            
        