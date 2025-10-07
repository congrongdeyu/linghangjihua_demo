
import os
import pickle
import shutil
from dotenv import load_dotenv

# 确保已安装所需库: pip install langchain-chroma langchain-community python-dotenv
from langchain_chroma import Chroma
from langchain_community.embeddings import ZhipuAIEmbeddings
from langchain_core.documents import Document

# 加载 .env 文件中的环境变量
load_dotenv()

def get_custom_metadata(pkl_file_path: str) -> dict:
    """
    根据文件路径生成自定义元数据。
    """
    metadata = {}
    path_parts = pkl_file_path.replace('\\', '/').split('/')

    try:
        if "原文" in path_parts:
            metadata["file_type"] = "original"
            metadata["source_info"] = os.path.basename(pkl_file_path).replace('.pkl', '.md')
        elif "解读" in path_parts:
            metadata["file_type"] = "construe"
            metadata["source_info"] = path_parts[-2]
        else:
            metadata["file_type"] = "unknown"
            metadata["source_info"] = os.path.basename(pkl_file_path)
    except IndexError:
        metadata["file_type"] = "error"
        metadata["source_info"] = "path_parsing_error"
        
    return metadata

def create_vector_db():
    """
    主函数，处理pkl文件，合并块，并存入ChromaDB。
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    knowledge_base_dir = os.path.join(script_dir, "knowledge_base")
    source_dir = os.path.join(knowledge_base_dir, "04_database", "01_langchain_split_documents_files")
    db_dir = os.path.join(knowledge_base_dir, "04_database", "02_vector_chroma_db")

    # 清理旧的数据库目录以确保全新创建
    if os.path.exists(db_dir):
        print(f"正在删除旧的数据库目录: {db_dir}")
        shutil.rmtree(db_dir)
    print(f"正在创建新的数据库目录: {db_dir}")
    os.makedirs(db_dir)

    if not os.path.isdir(source_dir):
        print(f"错误：源目录不存在 -> {source_dir}")
        return

    embeddings = ZhipuAIEmbeddings(model="embedding-3")
    vector_store = Chroma(
        collection_name="linghangjihua_collection",
        embedding_function=embeddings,
        persist_directory=db_dir
    )

    print(f"\n开始处理目录: {source_dir}")
    total_files_processed = 0
    total_vectors_added = 0

    for root, _, files in os.walk(source_dir):
        for file in files:
            if not file.endswith(".pkl"):
                continue

            total_files_processed += 1
            file_path = os.path.join(root, file)
            print(f"\n正在处理文件 ({total_files_processed}): {file_path}")

            try:
                with open(file_path, 'rb') as f:
                    original_chunks = pickle.load(f)
            except Exception as e:
                print(f"  -> 读取 .pkl 文件时出错: {e}")
                continue

            if not original_chunks:
                print("  -> 文件为空，跳过。")
                continue

            # 智能合并逻辑：将小块合并，直到达到最小尺寸
            merged_docs = []
            small_chunk_buffer = []
            buffer_char_count = 0
            min_chunk_size = 2000

            for chunk in original_chunks:
                chunk_len = len(chunk.page_content)

                if chunk_len >= min_chunk_size:
                    if small_chunk_buffer:
                        merged_content = "\n\n---\n\n".join([c.page_content for c in small_chunk_buffer])
                        merged_doc = Document(page_content=merged_content, metadata=small_chunk_buffer[0].metadata)
                        merged_docs.append(merged_doc)
                        small_chunk_buffer = []
                        buffer_char_count = 0
                    
                    merged_docs.append(chunk)
                else:
                    small_chunk_buffer.append(chunk)
                    buffer_char_count += chunk_len

                    if buffer_char_count >= min_chunk_size:
                        merged_content = "\n\n---\n\n".join([c.page_content for c in small_chunk_buffer])
                        merged_doc = Document(page_content=merged_content, metadata=small_chunk_buffer[0].metadata)
                        merged_docs.append(merged_doc)
                        small_chunk_buffer = []
                        buffer_char_count = 0
            
            if small_chunk_buffer:
                merged_content = "\n\n---\n\n".join([c.page_content for c in small_chunk_buffer])
                merged_doc = Document(page_content=merged_content, metadata=small_chunk_buffer[0].metadata)
                merged_docs.append(merged_doc)

            print(f"  -> 原始分块: {len(original_chunks)} -> 合并后分块: {len(merged_docs)}")

            # 为合并后的文档添加自定义元数据并存入数据库
            final_documents_to_add = []
            for doc in merged_docs:
                custom_meta = get_custom_metadata(file_path)
                doc.metadata.update(custom_meta)
                final_documents_to_add.append(doc)

            if final_documents_to_add:
                vector_store.add_documents(final_documents_to_add)
                total_vectors_added += len(final_documents_to_add)
                print(f"  -> {len(final_documents_to_add)} 个向量已添加至数据库。")

    print("\n数据库已成功创建并自动持久化！")
    print(f"总共处理了 {total_files_processed} 个文件，生成了 {total_vectors_added} 个向量。")

def verify_vector_db():
    """
    加载持久化的数据库并执行一次测试查询以验证其功能。
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_dir = os.path.join(script_dir, "knowledge_base", "04_database", "02_vector_chroma_db")

    if not os.path.isdir(db_dir):
        print(f"数据库目录不存在: {db_dir}")
        return

    print("正在加载持久化的向量数据库...")
    embeddings = ZhipuAIEmbeddings(model="embedding-3")
    vector_store = Chroma(
        collection_name="linghangjihua_collection",
        embedding_function=embeddings,
        persist_directory=db_dir
    )

    query = "人工智能是什么"
    print(f"\n正在执行测试查询: '{query}'")
    try:
        retrieved_docs = vector_store.similarity_search(query, k=1)
        if not retrieved_docs:
            print("测试查询未返回任何结果。数据库可能为空或查询内容不相关。")
            return

        print("--- 测试查询成功！检索到最相关的文档如下：---")
        doc = retrieved_docs[0]
        
        print("\n[元数据 (Metadata)]")
        print(doc.metadata)
        
        print("\n[内容预览 (Content Preview)]")
        content_preview = doc.page_content[:400] 
        print(content_preview + "..." if len(doc.page_content) > 400 else content_preview)
        print("-"*50)

    except Exception as e:
        print(f"执行测试查询时出错: {e}")


if __name__ == "__main__":
    create_vector_db()

    print("\n" + "="*60)
    print("--- 开始验证向量数据库 ---")
    print("="*60)
    verify_vector_db()
