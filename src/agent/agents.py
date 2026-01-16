from state.state import RCAState
from agent.data_tools import metric_data, log_data, trace_data
from agent.time_tools import time_extraction
from config.system_prompt import *
from config.agent_prompt import *
from config.llm_config import get_openai_lm
from langchain.schema import HumanMessage, SystemMessage
import json
import time

llm = get_openai_lm(json=True)

def time_agent(state: RCAState) -> RCAState:
    state["query"], state["start_time"], state["end_time"], = time_extraction(state["query"]) 
    return state

def data_agent(state: RCAState) -> RCAState:
    start_time = state["start_time"]
    end_time = state["end_time"]
    start_metric = time.time()
    state["metric_data"] = metric_data(start_time, end_time)
    metric_time = time.time() - start_metric

    start_log = time.time()
    state["log_data"] = log_data(start_time, end_time)
    log_time = time.time() - start_log

    start_trace = time.time()
    state["trace_data"] = trace_data(start_time, end_time)
    trace_time = time.time() - start_trace

    print(state["metric_data"])
    print(state["log_data"])
    print(state["trace_data"])
    print("metric time:", metric_time)
    print("log time:", log_time)
    print("trace time:", trace_time)
    
    return state

def rca_agent(state: RCAState) -> RCAState:
    query = state["query"]
    metric_data = state["metric_data"]
    log_data = state["log_data"]
    trace_data = state["trace_data"]
    
    question = rca_agent_prompt.format(
        query = query,
        component_prompt = component_prompt,
        topological_prompt = topological_prompt,
        call_path_prompt = call_path_prompt,
        metric_data = metric_data,
        log_data = log_data,
        trace_data = trace_data
    )
    msg = [
        SystemMessage(content=rca_agent_system_prompt),
        HumanMessage(content=question)
    ]
    
    start = time.time()
    response = llm.invoke(msg).content
    end = time.time()
    print("rca time:", end - start)
    response = json.loads(response)
    state["componet"] = response.get("component", "")
    state["reason"] = response.get("reason", "")
    print(state["componet"])
    print(state["reason"])
    return state

def rank_agent(state: RCAState) -> RCAState:
    query = state["query"]
    metric_data = state["metric_data"]
    log_data = state["log_data"]
    trace_data = state["trace_data"]
    
    components = state["componet"]
    reasons = state["reason"]
    
    root_causes = ""
    for i in range(3):
        root_causes += f"{i+1}. {reasons} occured in {components[i]}.\n"
    
    question = rank_agent_prompt.format(
        query = query,
        root_causes = root_causes,
        component_prompt = component_prompt,
        topological_prompt = topological_prompt,
        call_path_prompt = call_path_prompt,
        metric_data = metric_data,
        log_data = log_data,
        trace_data = trace_data
    )
    msg = [
        SystemMessage(content=rank_agent_system_prompt),
        HumanMessage(content=question)
    ]
    
    response = llm.invoke(msg).content
    response = json.loads(response)
    
    return state