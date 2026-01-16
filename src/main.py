import asyncio
import json
from graph.graph import get_workflow

async def run_all_batches(query_list, batch_size=3):
    answer_list = []

    def batch_iter(lst, batch_size):
        for i in range(0, len(lst), batch_size):
            yield lst[i:i + batch_size]

    for batch_index, batch in enumerate(batch_iter(query_list, batch_size)):
        tasks = []
        for query in batch:
            query["query"] = query["Anomaly Description"]
            query["query"] = "The system experienced an anomaly from 2025-06-17T01:10:16Z to 2025-06-17T01:23:16Z. Please infer the possible cause."
            print(batch_index+1)
            print(query["query"])
            tasks.append(get_workflow().ainvoke(query))
        results = await asyncio.gather(*tasks)
        keep_keys = ["uuid", "componet", "reason"]
        results = [{k: row[k] for k in keep_keys if k in row} for row in results]
        answer_list.extend(results)  
    return answer_list

if __name__ == "__main__":
    with open("dataset/input.json", "r", encoding="utf-8") as f:
        query_list = json.load(f)

    answer_list = asyncio.run(run_all_batches(query_list, batch_size=1))
    
    with open("output.json", "w", encoding="utf-8") as f:
        json.dump(answer_list, f, ensure_ascii=False, indent=4)

