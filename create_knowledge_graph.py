
import os
import pickle
import shutil
import time
from dotenv import load_dotenv

from langchain_experimental.graph_transformers import LLMGraphTransformer
from langchain_community.chat_models import ChatZhipuAI
from langchain_neo4j import Neo4jGraph

# 加载 .env 文件中的环境变量
load_dotenv()

def transform_and_save_as_pkl():
    """
    阶段一：遍历所有分块后的pkl文件，调用LLM将其转换为图谱文档，
    然后将这些中间结果保存为新的pkl文件，暂不写入数据库。
    """
    print("--- 阶段1：开始将文档块转换为中间图谱文件 ---")
    # --- 路径定义 ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    knowledge_base_dir = os.path.join(script_dir, "knowledge_base")
    source_dir = os.path.join(knowledge_base_dir, "04_database", "01_langchain_split_documents_files")
    intermediate_dir = os.path.join(knowledge_base_dir, "04_database", "03_kg_db")

    # --- 清理并创建中间目录 ---
    if os.path.exists(intermediate_dir):
        print(f"正在清理旧的中间文件目录: {intermediate_dir}")
        shutil.rmtree(intermediate_dir)
    os.makedirs(intermediate_dir)

    if not os.path.isdir(source_dir):
        print(f"错误：源目录不存在 -> {source_dir}")
        return

    # --- 初始化LLM和转换器 ---
    print("正在初始化LLM和Graph Transformer...")
    llm = ChatZhipuAI(model="glm-4.5-air", temperature=0.0)
    llm_transformer = LLMGraphTransformer(llm=llm)

    total_files_processed = 0
    for root, _, files in os.walk(source_dir):
        for file in files:
            if not file.endswith(".pkl"):
                continue

            total_files_processed += 1
            file_path = os.path.join(root, file)
            print(f"\n正在处理文件 ({total_files_processed}): {file_path}")

            try:
                with open(file_path, 'rb') as f:
                    documents = pickle.load(f)
            except Exception as e:
                print(f"  -> 读取 .pkl 文件时出错: {e}")
                continue

            if not documents:
                print("  -> 文件为空，跳过。")
                continue

            print(f"  -> 文件包含 {len(documents)} 个文档块，将一次性转换为图谱...")
            try:
                graph_documents = llm_transformer.convert_to_graph_documents(documents)
                
                relative_path = os.path.relpath(root, source_dir)
                intermediate_save_dir = os.path.join(intermediate_dir, relative_path)
                os.makedirs(intermediate_save_dir, exist_ok=True)
                intermediate_file_path = os.path.join(intermediate_save_dir, file)

                with open(intermediate_file_path, 'wb') as f:
                    pickle.dump(graph_documents, f)
                print(f"    -> 成功转换并暂存到: {intermediate_file_path}")

            except Exception as e:
                print(f"    -> 在处理文件并转换为图谱时发生严重错误: {e}")

    print(f"\n--- 阶段1完成：共处理并暂存了 {total_files_processed} 个文件的图谱数据。 ---")

def write_pkls_to_neo4j():
    """
    阶段二：读取所有暂存的图谱pkl文件，并分批写入Neo4j数据库。
    """
    print("\n--- 阶段2：开始将图谱数据写入Neo4j数据库 ---")
    # --- 路径和配置定义 ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    knowledge_base_dir = os.path.join(script_dir, "knowledge_base")
    intermediate_dir = os.path.join(knowledge_base_dir, "04_database", "03_kg_db")

    if not os.path.isdir(intermediate_dir):
        print(f"错误：中间文件目录不存在 -> {intermediate_dir}")
        return

    # --- 连接到Neo4j数据库 ---
    print("正在连接到Neo4j数据库...")
    try:
        # Neo4jGraph() 会自动从环境变量中读取NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD
        graph = Neo4jGraph()
        print("  -> Neo4j连接成功。")
        # 清空数据库以确保全新写入
        print("  -> 正在清空旧数据库...")
        graph.query("MATCH (n) DETACH DELETE n")
        print("  -> 旧数据库已清空。")
    except Exception as e:
        print(f"错误：无法连接到Neo4j数据库，请检查.env配置和数据库状态: {e}")
        return

    # --- 遍历所有图谱pkl文件并分批写入 ---
    all_graph_documents = []
    for root, _, files in os.walk(intermediate_dir):
        for file in files:
            if file.endswith(".pkl"):
                file_path = os.path.join(root, file)
                with open(file_path, 'rb') as f:
                    all_graph_documents.extend(pickle.load(f))

    if not all_graph_documents:
        print("未找到任何图谱数据可供写入。")
        return

    total_docs = len(all_graph_documents)
    batch_size = 5
    total_batches = (total_docs + batch_size - 1) // batch_size

    print(f"--- 开始写入 {total_docs} 个图谱文档，共分为 {total_batches} 个批次 --- ")

    for i in range(0, total_docs, batch_size):
        batch = all_graph_documents[i:i + batch_size]
        current_batch_num = i // batch_size + 1

        print(f"\n[批次 {current_batch_num}/{total_batches}] 正在写入 {len(batch)} 个图谱文档...")
        try:
            graph.add_graph_documents(batch, baseEntityLabel=True, include_source=True)
            print(f"  -> 批次 {current_batch_num} 数据写入完成。")
        except Exception as e:
            print(f"  -> [错误] 批次 {current_batch_num} 写入失败: {e}")
        
        time.sleep(1) # 避免API速率限制或数据库过载

    print("\n--- 阶段2完成：所有批次处理完成！---")

if __name__ == "__main__":
    transform_and_save_as_pkl()
    write_pkls_to_neo4j()
    print("\n知识图谱构建流程全部结束。")
