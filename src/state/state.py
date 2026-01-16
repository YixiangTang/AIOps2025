from typing import TypedDict, List, Dict, Tuple

class RCAState(TypedDict):
    uuid: str
    query: str
    start_time: str
    end_time: str
    trace_data: str
    log_data: str
    metric_data: str
    componet: str
    reason: str
    
    