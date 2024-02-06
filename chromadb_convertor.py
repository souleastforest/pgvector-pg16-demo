import json

# 读取collections.json文件
with open('./dbdata/knowledge/collections_202402050229.json', 'r', encoding='utf-8') as collections_file:
    collections_data = json.load(collections_file)
    name_array = [item['name'] for item in collections_data['collections']]

# 读取embedding.json文件
with open('./dbdata/knowledge/embedding_fulltext_search_content_202402050229.json', 'r', encoding='utf-8') as embedding_file:
    embedding_data = json.load(embedding_file)
    knowledge_and_question_array = [item['c0'] for item in embedding_data['embedding_fulltext_search_content']]

# 构造name_knowledge字典
name_knowledge = {}
name_knowledge["name_knowledge"] = [{"name": n, "knowledge": k} for n, k in zip(name_array, knowledge_and_question_array)]

# 输出name_knowledge字典为json文件
with open('./dbdata/name_knowledge.json', 'w', encoding='utf-8') as output_file:
    json.dump(name_knowledge, output_file)

# 读取name_knowledge.json文件
with open('./dbdata/name_knowledge.json', 'r', encoding='utf-8') as input_file:
    name_knowledge = json.load(input_file)
    name_list = [item['name'] for item in name_knowledge['name_knowledge']]
    knowledge_list = [item['knowledge'] for item in name_knowledge['name_knowledge']]

# Path: pgvector-pg16-demo/chromadb_convertor.py