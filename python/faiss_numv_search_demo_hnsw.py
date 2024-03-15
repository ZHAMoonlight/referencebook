import faiss
import numpy as np
import time

d = 64  # 向量维度
nb = 2000000  # 数据库大小

# 随机生成一些向量作为示例数据
np.random.seed(1234)
database_vectors = np.random.random((nb, d)).astype('float32')

# 创建一个 FLAT 索引
#index = faiss.IndexFlatL2(d)

# 创建一个 HNSW 索引
index = faiss.IndexHNSWFlat(d, 16) # 这里的 16 是 HNSW 的 M 参数
add_index_start_time = time.time()
# 添加数据到索引
index.add(database_vectors)
add_index_end_time = time.time()
execution_time = (add_index_end_time - add_index_start_time)*1000
print(f"索引时间: {execution_time} 毫秒")
# 随机生成一些查询向量
nq = 10  # 查询数量
query_vectors = np.random.random((nq, d)).astype('float32')

# 搜索最近邻
k = 5  # 返回的最近邻数量

start_time = time.time()
# 搜索TOP 5最相似向量
distances, indices = index.search(query_vectors, k)
end_time = time.time()
execution_time = (end_time - start_time)*1000
print(f"执行时间: {execution_time} 毫秒")

# 打印结果
print(indices)
print(distances)
