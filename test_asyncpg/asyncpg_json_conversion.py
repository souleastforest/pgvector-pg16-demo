import asyncio
import asyncpg
import json


async def main():
    conn = await asyncpg.connect()

    try:
        await conn.set_type_codec(
            'json',
            encoder=json.dumps,
            decoder=json.loads,
            schema='pg_catalog'
        )

        data = {'foo': 'bar', 'spam': 1}
        res = await conn.fetchval('SELECT $1::json', data)

    finally:
        await conn.close()

asyncio.run(main())