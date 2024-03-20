import os
import sys
import argparse
import logging
from logging import handlers
import signal


def setup_logging(env):
    global token
    token = 'seele_koko_pwd'
    vectordb_pre_path = os.path.join('db/persistance_db')
    logging_file_path = os.path.join('db/logging_db/vector_asyncpg_aiohttp.log')

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
# import asyncio
# import sys

# if sys.platform == 'win32':
#     asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


from pgvector.asyncpg import register_vector

from sentence_transformers import SentenceTransformer

# 连接到数据库
# Connect to an existing database
# conn = psycopg.connect("dbname=test user=postgresml host=localhost port=5433")
# conn = psycopg.connect("dbname=test user=root password=root port=5432")
# Open a cursor to perform database operations


# conn.execute('CREATE EXTENSION IF NOT EXISTS vector')
# register_vector(conn)

# cur = conn.cursor()
# # 造表
# cur.execute('CREATE TABLE IF NOT EXISTS documents (id bigse1rial PRIMARY KEY, userRole text, knowledge text, embedding vector(384))')

# # 初始化模型
# model = SentenceTransformer('multi-qa-MiniLM-L6-cos-v1')


# for row in rows:
#     print(row)


"""
    处理 知识库
"""

import character_config

# collection_dict = {}
async def init_collection_dict(pool):
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
    async with pool.acquire() as conn:
        collections: list = await conn.fetch('SELECT * FROM documents')
        # 处理逻辑：
        # 先遍历k-v数据库，key 为 角色名，value 为 对应对话的embedding
        if collections is not None:
            for collection in collections:
                # 若在知识库中，则标记为已创建
                if role_dict.get(collection[1]) is not None:
                    role_dict.get(collection[1])['created'] = True
    loop = asyncio.get_running_loop()
    for key, value in role_dict.items():
        created = value['created']
        # 分支预测，未创建：
        if created is False:
            logging.info(f'{key} not created hasKnowledge={value["hasKnowledge"]}')
            # 拥有知识库
            if value["hasKnowledge"] is True:
                count = 0
                for kno in character_config.knowledges.get(key):
                    embedding = await loop.run_in_executor(None, model.encode, kno)
                    async with pool.acquire() as conn:
                        await register_vector(conn)
                        await conn.fetch('INSERT INTO documents (userRole, knowledge, embedding) VALUES ($1, $2, $3)', key, kno, embedding)
                    count = count + 1
        else:
            logging.info(f'{key} created')
            # 获取角色的对话: 存储角色名
            collection_dict[key] = {key: character_config.knowledges.get(key)}

    logging.info(f'collection_dict={len(collection_dict)}')



"""
    建立服务器
    v1.0 完成功能逻辑: get请求返回文件，post请求返回json
    v1.1 重构了Post请求对应的处理函数
    v1.2 把HTTPServer修改为aiohttp server
"""

from aiohttp import web
import json
import asyncio
import asyncpg
import aiofiles
import os
from datetime import datetime
import urllib.parse as urlparse


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
        # todo: 请求体内的 type 检查 ？
        start_sec = datetime.timestamp(datetime.now())
        # 设置日志的data，用json.dumps(data, indent=4)格式化输出，indent=4表示缩进4个空格
        logging.info(f'{type} request received from {request.remote} with data: {json.dumps(data, indent=4)}')

        # loop = asyncio.get_running_loop()
        # pool = await create_db_pool(loop)
        pool = request.app['pool']
        # 处理不同的type
        if type == "insert":
            result_code, result_data = await handle_insert(data, user_id, role_name, pool)
        elif type == "query":
            result_code, result_data = await handle_query(data, user_id, role_name, pool)
        elif type == "clean" or type == "cleanByUserId":
            result_code, result_data = await handle_clean(data, user_id, role_name, pool)
        else:
            return web.json_response({'error': 'Invalid type'}, status=400)

        logging.info(f'type={type} handle cost_time={datetime.timestamp(datetime.now()) - start_sec}')

        response_data = {'code': '0', 'data': result_data}
        return web.json_response(response_data, status=result_code)
    except Exception as e:
        logging.error(e, exc_info=True)
        return web.json_response({'error': 'Server error'}, status=500)

  
async def handle_insert(req_json, user_id, role_name, pool):
    try:
        question = req_json['question']
        id = req_json.get('id', None)

        if id is None:
            logging.info('empty param=[id]')
            return 400, {}

        # 使用 run_in_executor 运行 CPU 密集型的 model.encode 方法
        loop = asyncio.get_running_loop()
        embedding = await loop.run_in_executor(None, model.encode, question)

        insert_sql = "INSERT INTO documents (userRole, knowledge, embedding) VALUES ($1, $2, $3)"
        params = (f"{user_id}-{role_name}", question, embedding)
        async with pool.acquire() as conn:
            await register_vector(conn)
            await conn.execute(insert_sql, *params)

        return 200, {}
    except Exception as e:
        logging.error(e, exc_info=True, stack_info=True)
        return 500, {}


async def handle_query(req_json, user_id, role_name, pool):
    # 本函数有两个查询：
    # 按需调整查询:
    k = 60
    limit = 5
    try:
        await init_collection_dict(pool)
        question = req_json['question']

        loop = asyncio.get_running_loop()
        embedding = await loop.run_in_executor(None, model.encode, question)

        result_f = {}
        params_1 = (embedding, limit)
        query_1 = "SELECT * FROM documents ORDER BY embedding <=> $1 LIMIT $2"
        params_2 = (role_name,)
        query_2 = "SELECT knowledge FROM documents WHERE userRole = $1"

        async with pool.acquire() as conn:
            await register_vector(conn)
            async with conn.transaction():
                results = await conn.fetch(query_1, *params_1)
                if results and len(results) > 0:
                    result_f["question"] = results[0][2]
                    result_f["question_id"] = results[0][0]

                # 检查是否需要执行第二个查询
                if collection_dict.get(role_name) is not None:
                    result2 = await conn.fetch(query_2, *params_2)
                    if result2 and len(result2) > 0:
                        result_f["knowledge"] = result2[0][0]
                    else:
                        result_f["knowledge"] = ""
        return 200, result_f
    
    except Exception as e:
        logging.error(e, exc_info=True, stack_info=True)
        return 500, {}


async def handle_clean(data, user_id, role_name, pool):
    del_sql = "DELETE FROM documents WHERE userRole = $1"
    params = f"{user_id}-{role_name}"
    async with pool.acquire() as conn: 
        try:
            await conn.execute(del_sql, params)
            return 200, {}
        except Exception as e:
            logging.error(e, exc_info=True, stack_info=True)
            return 500, {}



async def init_db(app):
     """Initialize a connection pool."""
     app['pool'] = await asyncpg.create_pool(dsn=dsn, min_size=minconn, max_size=maxconn)
     pool = app['pool']
     async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute('CREATE EXTENSION IF NOT EXISTS vector')
            await conn.execute('CREATE TABLE IF NOT EXISTS documents (id bigserial PRIMARY KEY, userRole text, knowledge text, embedding vector(384))')
            # 在服务器第一次启动前，初始化一次知识库
            logging.info("Preheat the model, need 1~2s...")
            embedding = model.encode("preheat the model")
            await init_collection_dict(pool)

     yield
     logging.info('Closing connection pool')
     await app['pool'].close()
    #  await app['pool'].wait_closed()

def init_app():
    """Initialize the application server."""
    app = web.Application()
    # Create a database context
    app.cleanup_ctx.append(init_db)
    # Add routes to the web server
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
    global model
    global dsn
    global minconn, maxconn
    global collection_dict

    collection_dict = {}

    minconn, maxconn = 10, 90
    dsn = 'postgresql://root:root@localhost:5432/test'
    # model = SentenceTransformer('multi-qa-MiniLM-L6-cos-v1')
    model = SentenceTransformer('all-MiniLM-L6-v2')

    app = init_app()
    web.run_app(app, host='0.0.0.0', port=port)
