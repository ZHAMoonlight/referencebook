import random

from pymilvus import MilvusClient

# 1、初始化Milvus客户端
client = MilvusClient(
    uri='http://172.31.13.171:31115',
)

# 2. Insert randomly generated vectors
data = []

for i in range(1000):
    user_id = i % 2 + 1
    file_sha1 = ''.join(random.choices('abcdef0123456789', k=128))
    file_size = random.randint(1, 10000)
    file_name = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=20))
    text = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=100))
    vector = [random.random() for _ in range(1536)]
    data.append({
        "user_id": user_id,
        "file_sha1": file_sha1,
        "file_size": file_size,
        "file_name": file_name,
        "text": text,
        "vector": vector
    })

res = client.insert(
    collection_name="RAG_COLLECTION_P",
    data=data
)

# 3. Search with partition key
query_vectors = [[random.random() for _ in range(1536)]]

# # 4. Get entities by ID
# result = client.get(
#     collection_name="RAG_COLLECTION_P",
#     ids=[450392210085418717, 1, 2]
# )
#
result = client.search(
    collection_name="RAG_COLLECTION_P",
    data=query_vectors,
    filter="user_id == 1 && file_name like 'swx%'",
    search_params={"metric_type": "IP", "params": {"nprobe": 10}},
    output_fields=["user_id", "file_name", "text"],
    limit=3
)

print(result)
