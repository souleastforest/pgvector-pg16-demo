import os
import logging
from logging import handlers
import argparse
import psycopg
from sentence_transformers import SentenceTransformer
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
from datetime import datetime
import urllib.parse as urlparse
import character_config
from pgvector.psycopg import register_vector



# Constants
DEFAULT_ENV = 'local'
DEFAULT_PORT = 10201
TOKEN = 'seele_koko_pwd'
VECTORD_DB_PATHS = {
    'local': 'db/persistance_db',
    'test': '/media/data2/koko/vector/',
    'prod': '/home/ubuntu/vectordb/'
}
LOGGING_DB_PATHS = {
    'local': 'db/logging_db/vector.log',
    'test': '/media/data2/koko/logs/vector.log',
    'prod': '/home/ubuntu/logs/vector.log'
}

# Global Variable
collection_dict = {}
sql_simple = """
    SELECT id, userRole, embedding, knowledge, 0 AS rank
    FROM documents
    ORDER BY embedding <=> %(embedding)s
    LIMIT 5
"""

# Functions
def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--env', type=str, default=DEFAULT_ENV)
    parser.add_argument('--port', type=int, default=DEFAULT_PORT)
    return parser.parse_args()

def setup_logging(env):
    log_file_path = LOGGING_DB_PATHS[env]
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    rota_handler = handlers.RotatingFileHandler(
        filename=log_file_path, maxBytes=10 * 1024 * 1024, backupCount=9, encoding='utf-8')
    ffmt = logging.Formatter("%(asctime)s-%(name)s-%(levelname)s-%(module)s[%(lineno)d]:%(message)s")
    rota_handler.setFormatter(ffmt)
    logging.basicConfig(level=logging.INFO, handlers=[rota_handler])

def init_db_connection(env):
    vectordb_pre_path = VECTORD_DB_PATHS[env]
    os.makedirs(vectordb_pre_path, exist_ok=True)
    conn = psycopg.connect("dbname=test user=root password=root port=5432")
    conn.execute('CREATE EXTENSION IF NOT EXISTS vector')
    register_vector(conn)
    cur = conn.cursor()
    cur.execute('CREATE TABLE IF NOT EXISTS documents (id bigserial PRIMARY KEY, userRole text, knowledge text, embedding vector(384))')
    return conn, cur

def init_sentence_transformer():
    return SentenceTransformer('multi-qa-MiniLM-L6-cos-v1')

def init_collection_dict(cur):
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
                    count = count + 1
        else:
            logging.info(f'{key} created')
            # 获取角色的对话: 角色名-embedding
            collection_dict[key] = {key: model.encode(character_config.knowledges.get(key))}

    logging.info(f'collection_dict={len(collection_dict)}')

def run_http_server(port):
    server = HTTPServer(('0.0.0.0', port), MyHttp)
    logging.info("server listen at: %s:%s" % ('0.0.0.0', port))
    server.serve_forever()

# Classes
class MyHttp(BaseHTTPRequestHandler):
    # ... (existing logic) ...
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
        # 管理 collection 对象，获取gpt的记忆和修改
        
        if user_id is None or type is None or role_name is None:
            resp_code = 400
            # 当 type 为 insert 时，向 collection 中添加一个新的对话, 来源于请求的 question 字段
            # 元数据为 type : chat
        elif type == "insert":
            try:
                # 获取question字段的内容
                question = req_json['question']
                id = req_json.get('id', None)

                if id is None:
                    logging.info('empty param=[id]')
                    self.send_response(400)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    return
                # 在collection中记忆问题
                embedding = model.encode(question)
                insert_sql = "INSERT INTO documents (userRole, knowledge, embedding) VALUES (%(userRole)s, %(knowledge)s, %(embedding)s)"
                conn.execute(insert_sql, {'userRole': f"{user_id}-{role_name}", 'knowledge': question, 'embedding':embedding})
                # collection.add(documents=[question],
                #                metadatas=[{"type": "chat"}],
                #                ids=[str(id)])
                conn.commit()
            except Exception as e:
                logging.error(e, exc_info=True, stack_info=True)
                resp_code = 500
        elif type == "query":
            k = 60
            limit = 5
            # 类型为查询，则向 collection 中查询与 question 相似的 result
            try:
                init_collection_dict()
                question = req_json['question']
                # 直接查询
                embedding = model.encode(question)
                results = conn.execute(sql_simple, {'query': question, 'embedding': embedding, 'k': k, 'limit': limit}).fetchall()
                # result1 = collection.query(query_texts=[question], n_results=1)
                result2 = None
                # 若：角色知识库存在，查询角色对应的条目
                if collection_dict.get(role_name) is not None:
                    # result2 = collection_dict[role_name].query(query_texts=[question], n_results=1)
                    result2 = conn.execute("SELECT knowledge FROM documents WHERE userRole = %(role_name)s", {'role_name': role_name}).fetchall()
                # 若：查询返回条目大于1条，取最高得分获取知识库
                if results is not None and len(results[0]) > 0:
                    # 索引：id=0, userRole=1, embedding=2, knowledge=3, score=4
                    result_f["question"] = results[0][3]
                    result_f["question_id"] = results[0][0]

                if result2 is not None \
                        and result2[0] is not None \
                        and len(result2[0]) > 0:
                    result_f["knowledge"] = result2[0][0]

                conn.commit()
            except Exception as e:
                logging.error(e, exc_info=True, stack_info=True)
                resp_code = 500
        elif type == "clean":
            try:
                conn.execute("DELETE FROM documents WHERE userRole=%(role_name)s", {'role_name': f"{user_id}-{role_name}"})
                conn.commit()
            except Exception as e:
                logging.error(e, exc_info=True, stack_info=True)
                resp_code = 500
        else:
            resp_code = 400

        logging.info(f'type={type} cost_time={datetime.timestamp(datetime.now()) - start_sec}')
        self.send_response(resp_code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        data = {'code': "0", "data": result_f}
        self.wfile.write(json.dumps(data).encode())


# Main execution
if __name__ == "__main__":
    args = parse_arguments()
    setup_logging(args.env)
    conn, cur = init_db_connection(args.env)
    model = init_sentence_transformer()
    run_http_server(args.port)
    cur.close()
    conn.close()
