import json

def json_to_jsonl(input_file, output_file):
    """
    将包含列表的 JSON 文件转换为 JSONL 格式
    :param input_file: 输入的 JSON 文件路径
    :param output_file: 输出的 JSONL 文件路径
    """
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)  # 读取 JSON 数据（应该是一个列表）
    
    with open(output_file, 'w', encoding='utf-8') as f:
        for item in data:
            # 将每个项目单独写入一行
            json.dump(item, f, ensure_ascii=False)
            f.write('\n')  # 写入换行符

# 使用示例
input_json = 'aiopschallenge2025-submission/result.json'  # 你的输入文件
output_jsonl = 'aiopschallenge2025-submission/result.jsonl'  # 输出文件
json_to_jsonl(input_json, output_jsonl)