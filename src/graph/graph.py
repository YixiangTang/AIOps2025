from langgraph.graph import StateGraph, END
from state.state import RCAState
from agent.agents import time_agent, data_agent, rca_agent, rank_agent

    
def get_workflow():
    workflow = StateGraph(RCAState)

    workflow.add_node("time_agent", time_agent)
    workflow.add_node("data_agent", data_agent)
    workflow.add_node("rca_agent", rca_agent)
    workflow.add_node("rank_agent", rank_agent)


    workflow.set_entry_point("time_agent")
    workflow.add_edge("time_agent", "data_agent")
    workflow.add_edge("data_agent", "rca_agent")
    workflow.add_edge("rca_agent", "rank_agent")
    workflow.add_edge("rank_agent", END)
    
    
    return workflow.compile()