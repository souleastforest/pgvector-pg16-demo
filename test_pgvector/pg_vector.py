"""
    使用 psycopg 连接 pg
"""
from pgvector.psycopg import register_vector
import psycopg
from sentence_transformers import SentenceTransformer
# 连接到数据库
# Connect to an existing database
conn = psycopg.connect("dbname=test user=postgresml host=localhost port=5433")
# Open a cursor to perform database operations
cur = conn.cursor()

cur.execute('CREATE EXTENSION IF NOT EXISTS vector')
register_vector(conn)
# CREATE EXTENSION IF NOT EXISTS pgml;
# SELECT pgml.version();

# 造表
cur.execute('CREATE TABLE IF NOT EXISTS documents (id bigserial PRIMARY KEY, userRole text, knowledge text, embedding vector(384))')
cur.execute("CREATE INDEX ON documents USING GIN (to_tsvector('english', knowledge))")

# 执行一个查询
# content = "testContent_1"
# embedding = "testEmbeddings"
# cur.execute('INSERT INTO documents (content, embedding) VALUES (%s, %s)', (content, embedding))
cur.execute("SELECT * FROM documents")
# 获取查询结果
collections = cur.fetchall()
print(collections)

conn.commit()
cur.close()
conn.close()