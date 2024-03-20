import asyncio
import asyncpg
from aiohttp import web
from settings import get_config
import os
from logging import handlers
import logging


async def handle(request):
    """Handle incoming requests."""
    pool = request.app['pool']
    power = int(request.match_info.get('power', 10))

    # Take a connection from the pool.
    async with pool.acquire() as connection:
        # Open a transaction.
        async with connection.transaction():
            # Run the query passing the request argument.
            result = await connection.fetchval('select 2 ^ $1', power)
            return web.Response(
                text="2 ^ {} is {}".format(power, result))



async def init_db(app):
    """Initialize a connection pool."""
    app['pool'] = await asyncpg.create_pool(dsn = 'postgresql://root:root@localhost:5432/test')
    yield
    await app['pool'].close()


def init_app():
    """Initialize the application server."""
    app = web.Application()
    app['config'] = get_config()
    # Create a database context
    app.cleanup_ctx.append(init_db)
    # Configure service routes
    app.router.add_route('GET', '/{power:\d+}', handle)
    app.router.add_route('GET', '/', handle)
    return app

logging_file_path = os.path.join('db/logging_db/aiohttp_demo.log')

logging_dir = os.path.dirname(logging_file_path)
if not os.path.exists(logging_dir):
    os.makedirs(logging_dir)

rota_handler = handlers.RotatingFileHandler(filename=logging_file_path,
                                            maxBytes=10 * 1024 * 1024,  # 10M
                                            backupCount=9,
                                            encoding='utf-8')  # 日志切割：按文件大小
ffmt = logging.Formatter("%(asctime)s-%(name)s-%(levelname)s-%(module)s[%(lineno)d]:%(message)s")
rota_handler.setFormatter(ffmt)
# 让默认的日志器同时将日志输出到控制台和文件
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s-%(name)s-%(levelname)s-%(module)s[%(lineno)d]:%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[rota_handler, logging.StreamHandler()]
                    )
logging.getLogger().addHandler(rota_handler)

app = init_app()
web.run_app(app, host='0.0.0.0', port=12345)