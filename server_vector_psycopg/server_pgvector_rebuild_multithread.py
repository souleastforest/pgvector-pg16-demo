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
# 3. 从命令行中结构化解析参数
args = parser.parse_args()
env = args.env
port = args.port

token = 'seele_koko_pwd'
vectordb_pre_path = os.path.join('db/persistance_db')
logging_file_path = os.path.join('db/logging_db/vector_multithread.log')

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
conn = psycopg.connect("dbname=test user=root password=root port=5432")
# Open a cursor to perform database operations


conn.execute('CREATE EXTENSION IF NOT EXISTS vector')
register_vector(conn)
# CREATE EXTENSION IF NOT EXISTS pgml;
# SELECT pgml.version();
cur = conn.cursor()
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
            if role_dict.get(collection[1]) is not None:
                role_dict.get(collection[1])['created'] = True
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
                    conn.commit()
                    count = count + 1
        else:
            logging.info(f'{key} created')
            # 获取角色的对话: 角色名-embedding
            collection_dict[key] = {key: model.encode(character_config.knowledges.get(key))}

    logging.info(f'collection_dict={len(collection_dict)}')
    
# 使用multithreadServer，在服务器启动前，初始化一次知识库
init_collection_dict()

"""
    hybrid_search_rrf
"""
sql_rrf = """
WITH semantic_search AS (
    SELECT id, userRole, embedding, RANK () OVER (ORDER BY embedding <=> %(embedding)s) AS rank
    FROM documents
    ORDER BY embedding <=> %(embedding)s
    LIMIT 20
),
keyword_search AS (
    SELECT id, userRole, embedding, RANK () OVER (ORDER BY ts_rank_cd(to_tsvector('english', knowledge), query) DESC)
    FROM documents, plainto_tsquery('english', %(query)s) query
    WHERE to_tsvector('english', knowledge) @@ query
    ORDER BY ts_rank_cd(to_tsvector('english', knowledge), query) DESC
    LIMIT 20
)
SELECT
    COALESCE(semantic_search.id, keyword_search.id) AS id,
    COALESCE(semantic_search.userRole, keyword_search.userRole) AS userRole,
    COALESCE(semantic_search.embedding, keyword_search.embedding) AS embedding,
    COALESCE(1.0 / (%(k)s + semantic_search.rank), 0.0) +
    COALESCE(1.0 / (%(k)s + keyword_search.rank), 0.0) AS score
FROM semantic_search
FULL OUTER JOIN keyword_search ON semantic_search.id = keyword_search.id
ORDER BY score DESC
LIMIT %(limit)s
"""

sql_simple = """
    SELECT id, userRole, embedding, knowledge, 0 AS rank
    FROM documents
    ORDER BY embedding <=> %(embedding)s
    LIMIT 5
"""



"""
    建立 http 服务器
    v1.0 完成功能逻辑: get请求返回文件，post请求返回json
    v1.1 重构了Post请求对应的处理函数
    v1.2 把HTTPServer改为ThreadingHTTPServer，增加吞吐量
    TODO: 
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from datetime import datetime
import urllib.parse as urlparse


class MyHttp(BaseHTTPRequestHandler):
    def do_GET(self):
        url = urlparse.urlparse(self.path)
        path = url.path
        filepath = path[path.find('/') + 1: len(path)]
        # folder/subfolder/file.txt -> subfolder/file.txt
        # filepath = path[path.find('D:\\code_space_python\\langchain\\file\\') + 1: len(path)]
        logging.info('url={} filepath={} client={}'.format(url, filepath, self.client_address))

        try:
            if os.path.isfile(filepath):
                self.send_response(200)
                self.send_header('Content-type', 'text/plain')
                if filepath.endswith('html'):
                    self.send_header('Content-type', 'text/html')
                    self.send_header('Cache-Control', 'public,max-age=1')

                self.end_headers()
                with open(filepath, 'rb') as file:
                    self.wfile.write(file.read())
            else:
                self.send_error(200, "File not found")
        except Exception as e:
            logging.error(e)
            self.send_error(500, "File not found")

    def do_POST(self):
        start_sec = datetime.timestamp(datetime.now())  # 秒，小数点后面是
        # 参数检查，以及预处理
        if not self.headers['content-type']:
            logging.info('no content_type=%s' % self.headers['content-type'])
            self.send_response(400)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            return

        if not self.headers['token'] or not self.headers['token'] == token:
            self.send_response(401)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            return

        content_type = self.headers['content-type'].lower()
        if content_type != 'application/json':
            logging.info('not json content_type=%s' % content_type)
            self.send_response(400)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            return

        req_datas = self.rfile.read(int(self.headers['content-length'])).decode('utf-8')
        logging.info('{} receive request client={} param={} '.format(datetime.now(), self.client_address, req_datas))
        if req_datas is None or len(req_datas) == 0:
            logging.info('receive empty json')
            self.send_response(400)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            return

        req_json = json.loads(req_datas)

        if not 'userId' in req_json or not 'type' in req_json or not 'roleName' in req_json:
            logging.info('empty param')
            self.send_response(400)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            return

        # 准备返回请求
        user_id = req_json['userId']
        role_name = req_json['roleName']
        type = req_json['type']
        resp_code = 200
        result_f = {"type": "empty"}

        # 处理不同的type
        if type == "insert":
            resp_code, result_f = self.handle_insert(req_json, user_id, role_name)
        elif type == "query":
            resp_code, result_f = self.handle_query(req_json, user_id, role_name)
        elif type == "clean":
            resp_code, result_f = self.handle_clean(req_json, user_id, role_name)
        else:
            resp_code = 400

        logging.info(f'type={type} cost_time={datetime.timestamp(datetime.now()) - start_sec}')
        self.send_response(resp_code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        data = {'code': "0", "data": result_f}
        self.wfile.write(json.dumps(data).encode())

    def handle_insert(self, req_json, user_id, role_name):
        try:
            question = req_json['question']
            id = req_json.get('id', None)

            if id is None:
                logging.info('empty param=[id]')
                return 400, {}

            embedding = model.encode(question)
            insert_sql = "INSERT INTO documents (userRole, knowledge, embedding) VALUES (%(userRole)s, %(knowledge)s, %(embedding)s)"
            conn.execute(insert_sql, {'userRole': f"{user_id}-{role_name}", 'knowledge': question, 'embedding':embedding})
            conn.commit()
            return 200, {}
        except Exception as e:
            logging.error(e, exc_info=True, stack_info=True)
            return 500, {}

    def handle_query(self, req_json, user_id, role_name):
        # 按需调整查询参数
        k = 60
        limit = 5
        try:
            init_collection_dict()
            question = req_json['question']
            embedding = model.encode(question)
            # 可以使用 rrf 混合查询
            # result_rrf = conn.execute(sql_rrf, {'query': question, 'embedding': embedding, 'k': k, 'limit': limit}).fetchall()
            # 使用 simple 查询
            results = conn.execute(sql_simple, {'query': question, 'embedding': embedding, 'k': k, 'limit': limit}).fetchall()
            result2 = None

            if collection_dict.get(role_name) is not None:
                result2 = conn.execute("SELECT knowledge FROM documents WHERE userRole = %(role_name)s", {'role_name': role_name}).fetchall()

            result_f = {}
            if results is not None and len(results[0]) > 0:
                result_f["question"] = results[0][3]
                result_f["question_id"] = results[0][0]

            if result2 is not None and result2[0] is not None and len(result2[0]) > 0:
                result_f["knowledge"] = result2[0][0]

            conn.commit()
            return 200, result_f
        except Exception as e:
            logging.error(e, exc_info=True, stack_info=True)
            return 500, {}

    def handle_clean(self, req_json, user_id, role_name):
        try:
            conn.execute("DELETE FROM documents WHERE userRole=%(role_name)s", {'role_name': f"{user_id}-{role_name}"})
            conn.commit()
            return 200, {}
        except Exception as e:
            logging.error(e, exc_info=True, stack_info=True)
            return 500, {}


host = ('0.0.0.0', port)
server = ThreadingHTTPServer(host, MyHttp)
logging.info("server listen at: %s:%s" % host)
server.serve_forever()



conn.commit()

cur.close()
conn.close()