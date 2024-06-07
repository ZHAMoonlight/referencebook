import os
import face_recognition
import numpy as np
import faiss
from flask import Flask, request, jsonify

supported_extensions = [".jpg", ".png", ".bmp"]
# 向量128维度
d = 128
# 创建faiss索引

nlist = 5  # how many cells
quantizer = faiss.IndexFlatL2(d)

index_need_train = faiss.IndexIVFFlat(quantizer, d, nlist)
index = faiss.IndexIDMap(index_need_train)


def load_pic_as_vector(face_dir: str):
    face_encodings_list = []
    face_id_list = []
    for filename in os.listdir(face_dir):
        file_name = os.path.splitext(filename)[0]
        file_ext = os.path.splitext(filename)[1]
        if file_ext in supported_extensions:
            image_path = os.path.join(face_dir, filename)
            image = face_recognition.load_image_file(image_path)
            face_locations = face_recognition.face_locations(image)  # (top, right, bottom, left)
            # 提取人脸特征
            face_encodings = face_recognition.face_encodings(image, face_locations)
            # 读取不到人脸或者超过1个人脸的都过滤掉
            if len(face_encodings) == 1:
                face_encodings_list.append(np.array(face_encodings)[0])
                face_id_list.append(np.array(file_name.split("_")[0]))
    return face_encodings_list, face_id_list


app = Flask("face_recognition")
face_encodings_list, face_id_list = load_pic_as_vector("/Users/xupeng/Downloads/face_lib")
index.train(np.array(face_encodings_list))

index.add_with_ids(np.array(face_encodings_list), face_id_list)

@app.route('/recognize', methods=['POST'])
def recognize_face_file():
    # 从请求中获取上传的图片文件
    image_file = request.files['image']
    image = face_recognition.load_image_file(image_file)
    face_locations = face_recognition.face_locations(image)  # (top, right, bottom, left)
    # 提取人脸特征
    face_encodings = face_recognition.face_encodings(image, face_locations)
    # 将人脸特征转换为 numpy 数组
    face_encodings_np = np.array(face_encodings)

    D, I = index.search(face_encodings_np, k=1)
    # 获取最相似人脸的 ID
    similar_face_id = I[0][0]

    # 返回结果
    return jsonify({'result': str(similar_face_id)})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
