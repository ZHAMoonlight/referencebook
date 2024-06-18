from pymilvus import MilvusClient, FieldSchema, DataType, CollectionSchema

# 1、初始化Milvus客户端
client = MilvusClient(
    uri='http://172.31.13.171:31115',
)

# 2. Create schema
schema = MilvusClient.create_schema(
    auto_id=False,
    enable_dynamic_field=True,
)

schema.add_field(field_name="pk", datatype=DataType.INT64, is_primary=True, auto_id=True)
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
# 4、创建分区
client.create_partition(
    collection_name="RAG_COLLECTION",
    partition_name="partition_technology"
)

client.create_partition(
    collection_name="RAG_COLLECTION",
    partition_name="partition_novel"
)


