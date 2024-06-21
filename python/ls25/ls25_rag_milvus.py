import bs4
from langchain_community.document_loaders import WebBaseLoader
from langchain_milvus import Milvus

from langchain_core.documents import Document
from langchain_core.runnables import RunnablePassthrough
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain.prompts import PromptTemplate

path = "https://lilianweng.github.io/posts/2023-06-23-agent/"
# Load, chunk and index the contents of the blog.
loader = WebBaseLoader(
    web_paths=(path,),
    bs_kwargs=dict(
        parse_only=bs4.SoupStrainer(
            class_=("post-content", "post-title", "post-header")
        )
    ),
)
docs = loader.load()

text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
splits = text_splitter.split_documents(docs)

MILVUS_HOST = "127.0.0.1"
MILVUS_PORT = "19530"

COLLECTION_NAME = "RAG_COLLECTION"

OPENAI_API_KEY = "sk-xxx"
OPENAI_API_BASE = "https://proxyhost:port/"

embeddings = OpenAIEmbeddings(
    model="text-embedding-3-small",
    openai_api_key=OPENAI_API_KEY,
    openai_api_type="openai",
    openai_api_base=OPENAI_API_BASE
)

llm = ChatOpenAI(model="gpt-4o", api_key=OPENAI_API_KEY,
                 base_url=OPENAI_API_BASE)

vector_db = Milvus(
    embeddings,
    COLLECTION_NAME,
    connection_args={"host": MILVUS_HOST, "port": MILVUS_PORT},
    auto_id=True
)

for doc in splits:
    metadata = {
        "user_id": 1001,
        "file_sha1": path,
        "file_size": len(splits),
        "file_name": path,
        "chunk_size": 500,
        "chunk_overlap": 200,
        "date": "2023-06-03"
    }
    doc_with_metadata = Document(
        page_content=doc.page_content, metadata=metadata)
    vector_db.add_documents([doc_with_metadata])

# Retrieve and generate using the relevant snippets of the blog.
retriever = vector_db.as_retriever()

template = """Use the following pieces of context to answer the question at the end.
If you don't know the answer, just say that you don't know. Use three sentences maximum and keep the answer as concise as possible.
Always say "thanks for asking!" at the end of the answer.
{context}
Question: {question}
Helpful Answer:"""

rag_prompt_custom = PromptTemplate.from_template(template)
rag_chain = ({"context": retriever, "question": RunnablePassthrough()} | rag_prompt_custom | llm)
result = rag_chain.invoke("What is Task Decomposition?")
print(result)
