from pymilvus import connections, db, MilvusClient

conn = connections.connect(host="172.31.13.171", port=31115)
database = db.create_database("RAG")

client = MilvusClient(
    uri='http://172.31.13.171:31115', # replace with your own Milvus server address
    token='root:Milvus' # replace with your own Milvus server token
)
client.create_role(
    role_name="roleA",
)

client.grant_privilege(
    role_name='roleA',
    object_type='User',
    object_name='SelectUser',
    privilege='SelectUser'
)

client.create_user(
    user_name='rag_user',
    password='P@ssw0rd'
)

client.grant_role(
    user_name='rag_user',
    role_name='roleA'
)