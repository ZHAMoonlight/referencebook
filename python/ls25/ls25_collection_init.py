from pymilvus import MilvusClient, DataType

# 1、初始化Milvus客户端
client = MilvusClient(
    uri='http://127.0.0.1:19530',
)

# 2. Create schema
schema = MilvusClient.create_schema(
    auto_id=True,
    enable_dynamic_field=True,
    partition_key_field="user_id",
    num_partitions=16 # Number of partitions. Defaults to 16.
)

schema.add_field(field_name="pk", datatype=DataType.INT64, is_primary=True)
schema.add_field(field_name="user_id", datatype=DataType.INT64, description='user unique id')
schema.add_field(field_name="file_sha1", datatype=DataType.VARCHAR, description='file sha1', max_length=128)
schema.add_field(field_name="file_size", datatype=DataType.INT64, description='file size')
schema.add_field(field_name="file_name", datatype=DataType.VARCHAR, description='file name', max_length=512)
schema.add_field(field_name="text", datatype=DataType.VARCHAR, description='the original content', max_length=65535)
schema.add_field(field_name="vector", datatype=DataType.FLOAT_VECTOR, description='Embedding vectors', dim=1536)

# 3、创建index
index_params = client.prepare_index_params()

index_params.add_index(
    field_name="vector",
    index_type="IVF_FLAT",
    metric_type="IP",
    params={"nlist": 128}
)
# 4、创建collection，同时加载index
client.create_collection(
    collection_name="RAG_COLLECTION",
    schema=schema,
    index_params=index_params
)


