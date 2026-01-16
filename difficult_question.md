## Question 385
- 2025-06-27T21:10:07Z to 2025-06-27T21:35:07Z
- 错误回答：shippingservice-2 stress test   pod fault   network attack
- label: shippingservice  misconfiguration  target port misconfig 
- 依据
  - Metrics: shippingservice request response 缺失 没检测出来
  - Log
    - failed to get shipping quote: rpc error: code = Unavailable desc = connection error: desc = "transport: Error while dialing dial tcp <IP>:<PORT>: connect: connection refused
    - failed to get shipping quote: rpc error: code = Unavailable desc = transport is closing
    - failed to complete the order: rpc error: code = Internal desc = shipping quote failure: failed to get shipping quote: rpc error: code = Unavailable desc = connection error: desc = "transport: Error while dialing dial tcp <IP>:<PORT>: connect: connection refused
    - 都被算法发现了，但是由于容易与network attack混淆，在top-3 root cause中有时会出现，有时则被认为是network attack
  
## Question 387
- 2025-06-27T23:10:09Z to 2025-06-27T23:26:09Z
- 错误回答：给出一个关于frontend的故障类型'pod fault'，其余2个都不是frontend(shippingservice-0, aiops-k8s-02)
- label: frontend  erroneous change  code error
- 依据
  - trace：request_proportion_anomalies
  - log：302
    - 302没发现倒是在trace的日志中发现404,以及RPC状态码5，5和404都是notfond
    - jaeger initialization completed. initializing conf files. 等frontend重启日志，是被认为是pod fault的根本原因
  - metric："request", "response", pod_cpu_usage, pod_processes
    - 只发现了pod_processes， "request", "response"还是因为异常检测算法的问题。这也是被认为是其他组件故障的原因，因为frontend爆出的异常不够多。

## Question 397
- 2025-06-28T09:10:25Z to 2025-06-28T09:26:25Z
- 错误回答：'productcatalogservice' 'productcatalogservice-0' 'frontend' 'pod fault'
- label：tidb-tidb  pod fault
- 依据
  - 根因是：tidb-tidb相关故障导致了productcatalogservice故障
  - 异常检测算法大量发现productcatalogservice故障
  - 由于pod failure，tidb-tidb的指标大量缺失，异常检测算法直接忽略了这点。

## Question 317
- 2025-06-21T02:10:11Z to 2025-06-21T02:33:11Z
- 错误回答：tidb-tikv  io fault
- label: tidb-tikv  pod fault
- 依据
  - 定位到tidb-tikv故障但是和Question-397一样，pod fault导致指标缺失，异常检测算法没有发现，误认为是io fault。
  - tidb-tikv的故障传播回上游的frontend, productcatalogservice, checkoutservice, recommendationservice检测出异常。

## Question 18
- 2025-06-06T10:03:14Z to 2025-06-06T10:17:14Z
- 回答正确：recommendationservice, pod fault
- label: recommendationservice, pod failure
- 依据
  - 日志中出现failed to get product recommendations等
  - recommendationservice的3个pod的infra指标和recommendationservice的request response全都缺失，没有检测出来
  - 尽管回答正确还是有些侥幸，由于我在提示词中对于分类任务重点说明了日志重要性。

## Summary
总结来说1个大问题，就是异常检测算法对于指标缺失的处理不够好，导致pod fault等故障没有被识别出来。希望在未来的版本中能改进这一点。