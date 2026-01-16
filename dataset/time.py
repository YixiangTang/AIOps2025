import json
import re
import csv
from datetime import datetime

time_pattern = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")

with open("dataset/input.json", "r", encoding="utf-8") as f:
    data = json.load(f)

time_records = []

for item in data:
    desc = item.get("Anomaly Description", "")
    uuid = item.get("uuid")

    times = time_pattern.findall(desc)
    if len(times) >= 2: 
        start_str, end_str = times[:2]
        start = datetime.strptime(start_str, "%Y-%m-%dT%H:%M:%SZ")
        end = datetime.strptime(end_str, "%Y-%m-%dT%H:%M:%SZ")
        time_records.append({
            "start_time": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_time": end.strftime("%Y-%m-%dT%H:%M:%SZ")
        })

with open("processed_data/time.csv", "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=["start_time", "end_time"])
    writer.writeheader()
    writer.writerows(time_records)

print(f"Saved {len(time_records)} time pairs to time.csv")
    
    


