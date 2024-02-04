from pgvector.psycopg import register_vector
import psycopg
from sentence_transformers import SentenceTransformer
from datetime import datetime

conn = psycopg.connect("dbname=test user=root password=root port=5432")
# conn = psycopg.connect("dbname=test user=postgresml host=localhost port=5433")
register_vector(conn)
# Open a cursor to perform database operations
cur = conn.cursor()
cur.execute('CREATE EXTENSION IF NOT EXISTS vector')


# 造表
cur.execute('CREATE TABLE IF NOT EXISTS documents (id bigserial PRIMARY KEY, userRole text, knowledge text, embedding vector(384))')

# 选择索引
# cur.execute("CREATE INDEX ON documents USING GIN (to_tsvector('english', knowledge))")
cur.execute("CREATE INDEX ON documents USING hnsw (embedding vector_cosine_ops)")

model = SentenceTransformer('multi-qa-MiniLM-L6-cos-v1')


sql_HNSW= """
WITH vector_search AS (
    SELECT id, userRole, embedding, knowledge, 0 AS rank
    FROM documents
    ORDER BY embedding <=> %(embedding)s
    LIMIT 10
),
text_search AS (
    SELECT id, userRole, embedding, knowledge, ts_rank_cd(to_tsvector('english', knowledge), query) AS rank
    FROM documents, plainto_tsquery('english', %(query)s) query
    WHERE to_tsvector('english', knowledge) @@ query
    ORDER BY rank DESC
    LIMIT 10
)
SELECT
    id, userRole, embedding, knowledge, rank
FROM (
    SELECT *, 1 AS source FROM vector_search
    UNION ALL
    SELECT *, 2 AS source FROM text_search
) AS combined_search
ORDER BY source, rank DESC
LIMIT %(limit)s;

"""


sql = """
WITH vector_search AS (
    SELECT id, userRole, embedding, knowledge, 0 AS rank
    FROM documents
    ORDER BY embedding <-> %(embedding)s
    LIMIT 10
),
text_search AS (
    SELECT id, userRole, embedding, knowledge, ts_rank_cd(to_tsvector('english', knowledge), query) AS rank
    FROM documents, plainto_tsquery('english', %(query)s) query
    WHERE to_tsvector('english', knowledge) @@ query
    ORDER BY rank DESC
    LIMIT 10
)
SELECT
    id, userRole, embedding, knowledge
FROM (
    SELECT *, 1 AS source FROM vector_search
    UNION ALL
    SELECT *, 2 AS source FROM text_search
) AS combined_search
ORDER BY source, rank DESC
LIMIT %(limit)s;

"""

sql_simple = """
    SELECT id, userRole, embedding, knowledge, 0 AS rank
    FROM documents
    ORDER BY embedding <=> %(embedding)s
    LIMIT 5
"""

query = 'is a character named by some food for fun?'
embedding = model.encode(query)
k = 60
limit = 5
# 区间测试
start_sec = datetime.timestamp(datetime.now())
# results = conn.execute(sql, {'query': query, 'embedding': embedding, 'k': k, 'limit': limit}).fetchall()
results = conn.execute(sql_simple, {'query': query, 'embedding': embedding, 'limit': limit}).fetchall()
end_sec = datetime.timestamp(datetime.now())

print((f'query_cost_time={end_sec - start_sec}'))
outputs = []
for row in results:
    # print('document:', row[0], 'RRF score:', row[1])
    id = row[0]
    userRole = row[1]
    embedding = row[2]
    knowledge = row[3]
    score = row[4]
    print('id: ', id, "userRole:", userRole, "score: ",score)
    # 将这些字段添加到你的结果字典中
    result = {
        'id': id,
        'userRole': userRole,
        'embedding': embedding,
        'knowledge': knowledge,
        'score': score
    }

    outputs.append(result)

conn.commit()
cur.close()
conn.close()