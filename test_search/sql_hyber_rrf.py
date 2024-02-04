from pgvector.psycopg import register_vector
import psycopg
from sentence_transformers import SentenceTransformer
from datetime import datetime

conn = psycopg.connect("dbname=test_rrf user=root password=root port=5432")
# conn = psycopg.connect("dbname=test user=postgresml host=localhost port=5433")

# Open a cursor to perform database operations
cur = conn.cursor()
cur.execute('CREATE EXTENSION IF NOT EXISTS vector')
# 注册vector插件
register_vector(conn)

# 造表
cur.execute('CREATE TABLE IF NOT EXISTS documents (id bigserial PRIMARY KEY, userRole text, knowledge text, embedding vector(384))')

# 选择索引
cur.execute("CREATE INDEX ON documents USING GIN (to_tsvector('english', knowledge))")
# cur.execute("CREATE INDEX ON documents USING hnsw (embedding vector_l2_ops);)")

model = SentenceTransformer('multi-qa-MiniLM-L6-cos-v1')


sql = """
WITH semantic_search AS (
    SELECT id, userRole, embedding, knowledge, RANK () OVER (ORDER BY embedding <=> %(embedding)s) AS rank
    FROM documents
    ORDER BY embedding <=> %(embedding)s
    LIMIT 20
),
keyword_search AS (
    SELECT id, userRole, embedding, knowledge, RANK () OVER (ORDER BY ts_rank_cd(to_tsvector('english', knowledge), query) DESC)
    FROM documents, plainto_tsquery('english', %(query)s) query
    WHERE to_tsvector('english', knowledge) @@ query
    ORDER BY ts_rank_cd(to_tsvector('english', knowledge), query) DESC
    LIMIT 20
)
SELECT
    COALESCE(semantic_search.id, keyword_search.id) AS id,
    COALESCE(semantic_search.userRole, keyword_search.userRole) AS userRole,
    COALESCE(semantic_search.embedding, keyword_search.embedding) AS embedding,
    COALESCE(semantic_search.knowledge, keyword_search.knowledge) AS knowledge,
    COALESCE(1.0 / (%(k)s + semantic_search.rank), 0.0) +
    COALESCE(1.0 / (%(k)s + keyword_search.rank), 0.0) AS score
FROM semantic_search
FULL OUTER JOIN keyword_search ON semantic_search.id = keyword_search.id
ORDER BY score DESC
LIMIT %(limit)s
"""
query = 'is a character named by some food for fun?'
embedding = model.encode(query)
k = 60
limit = 5
start_sec = datetime.timestamp(datetime.now())
results = conn.execute(sql, {'query': query, 'embedding': embedding, 'k': k, 'limit': limit}).fetchall()
end_sec = datetime.timestamp(datetime.now())
outputs = []

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