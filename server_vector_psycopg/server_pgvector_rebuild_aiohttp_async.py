import os
import sys
import json
import argparse
import logging
from logging import handlers


def setup_logging(env):
    global token
    token = 'seele_koko_pwd'
    vectordb_pre_path = os.path.join('db/persistance_db')
    logging_file_path = os.path.join('db/logging_db/vector_aiohttp.log')

    # 创建 vectordb_pre_path 如果它不存在
    if not os.path.exists(vectordb_pre_path):
        os.makedirs(vectordb_pre_path)

    # 创建 logging_file_path 的父目录如果它不存在
    logging_dir = os.path.dirname(logging_file_path)
    if not os.path.exists(logging_dir):
        os.makedirs(logging_dir)

    if env == 'test':
        vectordb_pre_path = '/media/data2/koko/vector_aiohttp/'
        logging_file_path = '/media/data2/koko/logs/vector_aiohttp.log'
    elif env == 'prod':
        vectordb_pre_path = '/home/ubuntu/vector_aiohttp/'
        logging_file_path = '/home/ubuntu/logs/vector_aiohttp.log'

    rota_handler = handlers.RotatingFileHandler(filename=logging_file_path,
                                                maxBytes=10 * 1024 * 1024,  # 10M
                                                backupCount=9,
                                                encoding='utf-8')  # 日志切割：按文件大小
    ffmt = logging.Formatter("%(asctime)s-%(name)s-%(levelname)s-%(module)s[%(lineno)d]:%(message)s")
    rota_handler.setFormatter(ffmt)
    # 让默认的日志器同时将日志输出到控制台和文件
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s-%(name)s-%(levelname)s-%(module)s[%(lineno)d]:%(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        handlers=[rota_handler, logging.StreamHandler()]
                        )
    logging.getLogger().addHandler(rota_handler)


# 若是Win32平台，需要设置event_loop_policy
import asyncio
import sys

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

"""
    使用 psycopg 连接 pg
"""
from pgvector.psycopg import register_vector, register_vector_async
import psycopg
from sentence_transformers import SentenceTransformer

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
    # 获取查询结果
    conn = psycopg.connect(dsn)
    collections = conn.execute("SELECT * FROM documents")

    # 处理逻辑：
    # 先遍历k-v数据库，key 为 角色名，value 为 对应对话的embedding
    if collections is not None:
        for collection in collections:
            # 若在知识库中，则标记为已创建
            if role_dict.get(collection[1]) is not None:
                role_dict.get(collection[1])['created'] = True

    # loop = asyncio.get_running_loop()
    for key, value in role_dict.items():
        created = value['created']
        # 分支预测，未创建：
        if created is False:
            logging.info(f'{key} not created hasKnowledge={value["hasKnowledge"]}')
            # 拥有知识库
            if value["hasKnowledge"] is True:
                count = 0
                for kno in character_config.knowledges.get(key):
                    embedding = model.encode(kno)
                    conn.execute('INSERT INTO documents (userRole, knowledge, embedding) VALUES ($1, $2, $3)',
                                       key, kno, embedding)

                    count = count + 1
                
        else:
            logging.info(f'{key} created')
            # 获取角色的对话: 角色名-embedding
            collection_dict[key] = {key: model.encode(character_config.knowledges.get(key))}
    conn.commit()
    conn.close()
    logging.info(f'collection_dict={len(collection_dict)}')


sql_simple = """
    SELECT id, userRole, embedding, knowledge, 0 AS rank
    FROM documents
    ORDER BY embedding <=> %(embedding)s
    LIMIT 5
"""

"""
    建立 epoll 服务器
    v1.0 完成功能逻辑: get请求返回文件，post请求返回json
    v1.1 重构了Post请求对应的处理函数
    v1.2 把HTTPServer修改为aiohttp server async 版本
"""

from aiohttp import web
import json
import asyncio
import asyncpg
import aiofiles
import os
from datetime import datetime
import urllib.parse as urlparse

# 连接池
from psycopg_pool import AsyncConnectionPool

async def handle_get(request):
    url = urlparse.urlparse(request.url)
    path = url.path
    filepath = path[path.find('/') + 1: len(path)]
    client_address = request.remote
    logging.info('url={} filepath={} client={}'.format(url, filepath, client_address))
    try:
        if os.path.isfile(filepath):
            content_type = 'text/plain'
            if filepath.endswith('.html'):
                content_type = 'text/html'
            async with aiofiles.open(filepath, 'rb') as file:
                content = await file.read()
                return web.Response(body=content, content_type=content_type)
        else:
            logging.info(f'File not found: {filepath}')
            return web.json_response({'error': 'File not found'}, status=404)
    except Exception as e:
        logging.error(e, exc_info=True)
        return web.json_response({'error': 'Server error: File not found'}, status=500)



async def handle_post(request):    
    try:
        # 验证token等
        if 'token' not in request.headers or request.headers['token'] != token:
            return web.json_response({'error': 'Unauthorized'}, status=401)

        content_type = request.headers.get('content-type', '').lower()
        if content_type != 'application/json':
            return web.json_response({'error': 'Invalid content type'}, status=400)

        data = await request.json()
        if not all(k in data for k in ('userId', 'type', 'roleName')):
            return web.json_response({'error': 'Invalid request'}, status=400)

        user_id = data['userId']
        role_name = data['roleName']
        type = request.match_info['type']

        start_sec = datetime.timestamp(datetime.now())
        logging.info(f'{type} request received from {request.remote} with data: {data}')

        # 处理不同的type

        if type == "insert":
            result_code, result_data = await handle_insert(data, user_id, role_name)
        elif type == "query":
            result_code, result_data = await handle_query(data, user_id, role_name)
        elif type == "clean":
            result_code, result_data = await handle_clean(data, user_id, role_name)
        else:
            return web.json_response({'error': 'Invalid type'}, status=400)

        logging.info(f'type={type} cost_time={datetime.timestamp(datetime.now()) - start_sec}')

        response_data = {'code': '0', 'data': result_data}
        return web.json_response(response_data, status=result_code)
    except Exception as e:
        logging.error(e, exc_info=True)
        return web.json_response({'error': 'Server error'}, status=500)

  
async def handle_insert(req_json, user_id, role_name):
    async with AsyncConnectionPool(conninfo=dsn, min_size=minconn, max_size=maxconn) as pool:
        async with pool.connection() as conn:
            await register_vector_async(conn)
            return await handle_insert_impl(req_json, user_id, role_name, conn)
async def handle_insert_impl(req_json, user_id, role_name, conn):
    try:
        question = req_json['question']
        id = req_json.get('id', None)

        if id is None:
            logging.info('empty param=[id]')
            return 400, {}

        loop = asyncio.get_running_loop()
        # 使用 run_in_executor 运行 CPU 密集型的 model.encode 方法
        embedding = await loop.run_in_executor(None, model.encode, question)

        insert_sql = "INSERT INTO documents (userRole, knowledge, embedding) VALUES (%s, %s, %s)"
        params = f"{user_id}-{role_name}", question, embedding
        # 阻塞逻辑
        async with conn.cursor() as cur:
            await cur.execute(insert_sql, params)
        return 200, {}
    except Exception as e:
        if not conn.closed:
            await conn.close()
        logging.error(e, exc_info=True, stack_info=True)
        return 500, {}


async def handle_query(req_json, user_id, role_name):
    async with AsyncConnectionPool(conninfo=dsn, min_size=minconn, max_size=maxconn) as pool:
        async with pool.connection() as conn:
            await register_vector_async(conn)
            return await handle_query_impl(req_json, user_id, role_name, conn)
async def handle_query_impl(req_json, user_id, role_name, conn):
    # 按需调整查询参数
    # 本函数有两个查询
    k = 60
    limit = 5
    try:
        init_collection_dict()
        question = req_json['question']

        loop = asyncio.get_running_loop()
        embedding = await loop.run_in_executor(None, model.encode, question)

        result_f = {}
        params = {'query': question, 'embedding': embedding, 'k': k, 'limit': limit}
        query = "SELECT id, userRole, embedding, knowledge, 0 AS rank FROM documents ORDER BY embedding <=> %(embedding)s LIMIT 5"
        # await 逻辑
        async with conn.cursor() as cur:
            
            await cur.execute(query, params)
            results = await cur.fetchall()
        # results = conn.execute(query, params).fetchall()
            if results and len(results[0]) > 0:
                result_f["question"] = results[0][3]
                result_f["question_id"] = results[0][0]

        # 检查是否需要执行第二个查询
        if collection_dict.get(role_name) is not None:
            async with conn.cursor() as cur:
                await cur.execute("SELECT knowledge FROM documents WHERE userRole = %(role_name)s",{'role_name': role_name})
                result2 = await cur.fetchall()
                result_f["knowledge"] = result2[0][0]
        # conn.close()
        return 200, result_f
    except Exception as e:
        logging.error(e, exc_info=True, stack_info=True)
        if not conn.closed:
            await conn.close()
        return 500, {}


async def handle_clean(data, user_id, role_name):
    conn = psycopg.connect(dsn)
    try:
        conn.execute("DELETE FROM documents WHERE userRole = %(role_name)s", {'role_name': f"{user_id}-{role_name}"})
        conn.commit()
        conn.close()
        return 200, {}
    except Exception as e:
        if not conn.closed:
            await conn.close()
        logging.error(e, exc_info=True, stack_info=True)
        return 500, {}


async def create_db_pool(loop):
    return await asyncpg.create_pool(dsn, loop=loop)

async def main():
    global model
    global sql_simple
    global dsn
    global minconn, maxconn

    minconn, maxconn = 1, 90
    dsn = 'postgresql://root:root@localhost:5432/test'
    model = SentenceTransformer('multi-qa-MiniLM-L6-cos-v1')

    
    conn = psycopg.connect(dsn)
    
    cur = conn.cursor()
    cur.execute('CREATE EXTENSION IF NOT EXISTS vector')
    register_vector(conn)
    # 造表
    cur.execute(
        'CREATE TABLE IF NOT EXISTS documents (id bigserial PRIMARY KEY, userRole text, knowledge text, embedding vector(384))')
    conn.commit()
    cur.close()
    conn.close()
    # pool = await asyncpg.create_pool(dsn)
    # 在服务器第一次启动前，初始化一次知识库
    init_collection_dict()
    app = web.Application()
    app.add_routes([web.get('/api/vector/{filepath:.*}', handle_get),
                    web.post('/api/vector/{type}', handle_post)])
    return app


if __name__ == '__main__':
    
    # 1. 定义命令行解析器对象
    parser = argparse.ArgumentParser()
    # 2. 添加命令行参数
    parser.add_argument('--env', type=str, default='local')
    parser.add_argument('--port', type=int, default=10203)
    # 3. 从命令行中结构化解析参数
    args = parser.parse_args()
    env = args.env
    port = args.port
    # 4. 启动服务
    setup_logging(env)
    try:
        app = asyncio.run(main())
        web.run_app(app, host='0.0.0.0', port=port)
    except KeyboardInterrupt:
        logging.info("Received exit signal. Shutting down...")
    except Exception as e:
        logging.error(e, exc_info=True)
        logging.info("Server stopped by Exception")


