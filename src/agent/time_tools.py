import re
from datetime import datetime, timedelta

def adjust_times(start_time: str, end_time: str):
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    start_dt = datetime.strptime(start_time, fmt)
    end_dt = datetime.strptime(end_time, fmt)

    if (end_dt - start_dt).total_seconds() == 1:
        end_dt = (end_dt + timedelta(minutes=2))

    return start_dt.strftime(fmt), end_dt.strftime(fmt)

def time_extraction(query):
    pattern = r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)"
    matches = re.findall(pattern, query)
    start_time = matches[0]
    end_time = matches[1] 
    start_time, end_time = adjust_times(start_time, end_time)
    
    idx = [m.start() for m in re.finditer(pattern, query)][1]
    query = query[:idx] + end_time + query[idx+len(matches[1]):]
    
    return query, start_time, end_time