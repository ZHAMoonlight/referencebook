import random

from pymilvus import MilvusClient, DataType

# 1、初始化Milvus客户端
client = MilvusClient(
    uri='http://172.31.13.171:31115',
)

# 2. Create schema
schema = MilvusClient.create_schema(
    auto_id=False,
    enable_dynamic_field=True,
    partition_key_field="user_id",
    num_partitions=16  # Number of partitions. Defaults to 16.
)

schema.add_field(field_name="pk", datatype=DataType.INT64, is_primary=True, auto_id=True)
schema.add_field(field_name="user_id", datatype=DataType.INT64, description='user unique id')
schema.add_field(field_name="file_sha1", datatype=DataType.VARCHAR, description='file sha1', max_length=128)
schema.add_field(field_name="file_size", datatype=DataType.INT64, description='file size')
schema.add_field(field_name="file_name", datatype=DataType.VARCHAR, description='file name', max_length=512)
schema.add_field(field_name="text", datatype=DataType.VARCHAR, description='the original content', max_length=65535)
schema.add_field(field_name="vector", datatype=DataType.FLOAT_VECTOR, description='Embedding vectors', dim=1536)

collection_name = "RAG_COLLECTION_DISKANN"
# 3、创建index
index_params = client.prepare_index_params()

index_params.add_index(
    field_name="vector",
    index_type="DISKANN",
    metric_type="IP"
)
# 4、创建collection，同时加载index
client.create_collection(
    collection_name=collection_name,
    schema=schema,
    index_params=index_params
)

# 5. Insert randomly generated vectors
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
    collection_name=collection_name,
    data=data
)

# 6. Search with partition key
query_vectors = [[random.random() for _ in range(1536)]]

result = client.search(
    collection_name=collection_name,
    data=query_vectors,
    filter="user_id == 1",
    search_params={"metric_type": "IP"},
    output_fields=["user_id", "file_name", "text"],
    limit=3
)

print(result)
