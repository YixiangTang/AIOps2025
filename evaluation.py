import json
import jsonlines
from sklearn.metrics import accuracy_score, f1_score



# === 读取文件 ===
with open("output.json", "r") as f:
    outputs = json.load(f)

groundtruths = []
with jsonlines.open("dataset/groundtruth.jsonl", "r") as reader:
    for obj in reader:
        groundtruths.append(obj)

# === 取前 n 条 ===
outputs = outputs[211:]
groundtruths = groundtruths[211:]

# === 提取字段 ===
pred_components = [item["componet"] for item in outputs]
pred_reasons = [item["reason"] for item in outputs]

# ground truth 提取时兼容 instance 类型
true_components = []
true_reasons = []

for gt in groundtruths:
    inst = gt["instance"]
    if isinstance(inst, list):
        true_components.append(inst)
    else:
        true_components.append([inst])  # 统一为列表便于处理
    true_reasons.append(gt["fault_category"])

# === component 匹配逻辑：命中列表中的任意一个视为正确 ===
true_comp_flat = []  # 真实标签（扁平化）
pred_comp_flat = []  # 预测标签（修正后）

for pred, true_list in zip(pred_components, true_components):
    if pred in true_list:
        # 命中则使用正确类别标记（例如用 true_list[0] 代表）
        true_comp_flat.append(true_list[0])
        pred_comp_flat.append(true_list[0])
    else:
        # 未命中，则保留原预测（用于计算指标）
        true_comp_flat.append(true_list[0])
        pred_comp_flat.append(pred)

# === reason 直接比对 ===
true_reason_flat = true_reasons
pred_reason_flat = pred_reasons


def evaluate(true, pred, name):
    acc = accuracy_score(true, pred)
    macro_f1 = f1_score(true, pred, average="macro")
    micro_f1 = f1_score(true, pred, average="micro")
    weighted_f1 = f1_score(true, pred, average="weighted")
    print(f"\n=== {name} ===")
    print(f"Accuracy:    {acc:.4f}")
    print(f"Macro-F1:    {macro_f1:.4f}")
    print(f"Micro-F1:    {micro_f1:.4f}")
    print(f"Weighted-F1: {weighted_f1:.4f}")


evaluate(true_comp_flat, pred_comp_flat, "Component")
evaluate(true_reason_flat, pred_reason_flat, "Reason")
