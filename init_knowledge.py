import os
import sys
import json
import argparse

"""
    命令行，日志
"""

# 1. 定义命令行解析器对象
parser = argparse.ArgumentParser()
# 2. 添加命令行参数
parser.add_argument('--env', type=str, default='local')
parser.add_argument('--port', type=int, default=10201)
parser.add_argument('--db', type=str, default='test')

# 3. 从命令行中结构化解析参数
args = parser.parse_args()
env = args.env
port = args.port
db_name = args.db

token = 'seele_koko_pwd'
vectordb_pre_path = os.path.join('db/persistance_db')
logging_file_path = os.path.join('db/logging_db/init_knowledge.log')

# 创建 vectordb_pre_path 如果它不存在
if not os.path.exists(vectordb_pre_path):
    os.makedirs(vectordb_pre_path)

# 创建 logging_file_path 的父目录如果它不存在
logging_dir = os.path.dirname(logging_file_path)
if not os.path.exists(logging_dir):
    os.makedirs(logging_dir)

if env == 'test':
    vectordb_pre_path = '/media/data2/koko/vector/'
    logging_file_path = '/media/data2/koko/logs/vector.log'
elif env == 'prod':
    vectordb_pre_path = '/home/ubuntu/vectordb/'
    logging_file_path = '/home/ubuntu/logs/vector.log'

import logging
from logging import handlers

rota_handler = handlers.RotatingFileHandler(filename=logging_file_path,
                                            maxBytes=10 * 1024 * 1024,  # 10M
                                            backupCount=9,
                                            encoding='utf-8')  # 日志切割：按文件大小
ffmt = logging.Formatter("%(asctime)s-%(name)s-%(levelname)s-%(module)s[%(lineno)d]:%(message)s")
rota_handler.setFormatter(ffmt)
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s-%(name)s-%(levelname)s-%(module)s[%(lineno)d]:%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logging.getLogger().addHandler(rota_handler)


"""
    使用 psycopg 连接 pg
"""
from pgvector.psycopg import register_vector
import psycopg
from sentence_transformers import SentenceTransformer
# 连接到数据库
# Connect to an existing database
# conn = psycopg.connect("dbname=test user=postgresml host=localhost port=5433")
conn = psycopg.connect(f"dbname={db_name} user=root password=root port=5432")
# Open a cursor to perform database operations
cur = conn.cursor()

cur.execute('CREATE EXTENSION IF NOT EXISTS vector')
register_vector(conn)
# CREATE EXTENSION IF NOT EXISTS pgml;
# SELECT pgml.version();

# 造表
cur.execute('CREATE TABLE IF NOT EXISTS documents (id bigserial PRIMARY KEY, userRole text, knowledge text, embedding vector(384))')
# cur.execute("CREATE INDEX ON documents USING GIN (to_tsvector('english', knowledge))")

# 执行一个查询
# content = "testContent_1"
# embedding = "testEmbeddings"
# cur.execute('INSERT INTO documents (content, embedding) VALUES (%s, %s)', (content, embedding))


# 初始化模型
model = SentenceTransformer('multi-qa-MiniLM-L6-cos-v1')


# for row in rows:
#     print(row)


"""
    处理 知识库
"""

import character_config

collection_dict = {}
def init_collection_dict():
    if len(collection_dict) > 0:
        logging.info("collection_dict init done")
        return
    logging.info("collection_dict init start")
    role_dict = {
        "bronya": {"created": False, "hasKnowledge": True},
        "gwen": {"created": False, "hasKnowledge": True},
        "hutao": {"created": False, "hasKnowledge": True},
        "paimon": {"created": False, "hasKnowledge": True},
        "silverwolf": {"created": False, "hasKnowledge": True},

        "klee": {"created": False, "hasKnowledge": True},
        "kokomi": {"created": False, "hasKnowledge": True},
        "ganyu": {"created": False, "hasKnowledge": False},
        "kafka": {"created": False, "hasKnowledge": False},
        "kirara": {"created": False, "hasKnowledge": False},

        "walnut": {"created": False, "hasKnowledge": False},

        "yorforger": {"created": False, "hasKnowledge": False},
    }
    cur.execute("SELECT * FROM documents")
    # 获取查询结果
    collections = cur.fetchall()
    
    # 处理逻辑：
    # 先遍历k-v数据库，key 为 角色名，value 为 对应对话的embedding
    if collections is not None:
        for collection in collections:
            # 若在知识库中，则标记为已创建
            if role_dict.get(collection.name) is not None:
                role_dict.get(collection.name)['created'] = True
    for key, value in role_dict.items():
        created = value['created']
        # 分支预测，未创建：
        if created is False:
            logging.info(f'{key} not created hasKnowledge={value["hasKnowledge"]}')
            # 拥有知识库
            if value["hasKnowledge"] is True:
                
                count = 0
                for kno in character_config.knowledges.get(key):
                    # collection_tmp.add(documents=[kno],
                    #                    metadatas=[{"type": "knowledge"}],
                    #                    ids=str(count))
                    # 对句子进行 embedding 编码, 再加入数据库
                    embedding = model.encode(kno)
                    conn.execute('INSERT INTO documents (userRole, knowledge, embedding) VALUES (%s, %s, %s)', (key, kno, embedding))
                    count = count + 1
        else:
            logging.info(f'{key} created')
            # 获取角色的对话: 角色名-embedding
            collection_dict[key] = {key: model.encode(character_config.knowledges.get(key))}

    logging.info(f'collection_dict={len(collection_dict)}')
    

init_collection_dict()


conn.commit()

cur.close()
conn.close()