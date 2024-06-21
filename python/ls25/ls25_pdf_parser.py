
from langchain_community.document_loaders import PyPDFLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter

loader = PyPDFLoader("/Users/xupeng/Downloads/启明创投x未尽研究 生成式AI报告.pdf")
docs = loader.load()

chunk_size = 500
chunk_overlap = 20

text_splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
    chunk_size=chunk_size, chunk_overlap=chunk_overlap, disallowed_special=())

docs_split = text_splitter.split_documents(docs)
print(docs)