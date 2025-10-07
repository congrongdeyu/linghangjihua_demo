import os
import pickle
import time
from dotenv import load_dotenv

# LangChain and Neo4j imports
from langchain_experimental.graph_transformers import LLMGraphTransformer
from langchain_community.chat_models import ChatZhipuAI
from langchain_neo4j import Neo4jGraph

# 加载 .env 文件中的环境变量
load_dotenv()


def create_neo4j_graph_from_chunks():
    """
    主函数，采用“全局加载，分批处理”的策略，构建Neo4j知识图谱。
    """
    # --- 1. 路径定义 ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    knowledge_base_dir = os.path.join(script_dir, "knowledge_base")
    source_dir = os.path.join(
        knowledge_base_dir, "04_database", "01_langchain_split_documents_files"
    )

    if not os.path.isdir(source_dir):
        print(f"错误：源目录不存在 -> {source_dir}")
        return

    # --- 2. 一次性加载所有文档块 ---
    print(f"正在从 {source_dir} 加载所有文档块...")
    all_document_chunks = []
    for root, _, files in os.walk(source_dir):
        for file in files:
            if file.endswith(".pkl"):
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, "rb") as f:
                        all_document_chunks.extend(pickle.load(f))
                except Exception as e:
                    print(f"警告：读取文件 {file_path} 时出错: {e}")

    if not all_document_chunks:
        print("未能加载任何文档块，程序终止。")
        return

    # --- 3. 初始化组件和数据库 ---
    print("正在初始化LLM、Graph Transformer和Neo4j连接...")
    try:
        graph = Neo4jGraph()
    except Exception as e:
        print(f"错误：无法连接到Neo4j数据库，请检查.env配置和数据库状态: {e}")
        return

    zhipu_long_llm = ChatZhipuAI(model="glm-4-long", temperature=0.0)
    llm_transformer = LLMGraphTransformer(llm=zhipu_long_llm)

    # --- 4. 分批处理与写入 ---
    total_chunks = len(all_document_chunks)
    batch_size = 5
    total_batches = (total_chunks + batch_size - 1) // batch_size

    print(f"--- 开始处理 {total_chunks} 个文档块，共分为 {total_batches} 个批次 ---")

    for i in range(0, total_chunks, batch_size):
        batch = all_document_chunks[i : i + batch_size]
        current_batch_num = i // batch_size + 1

        print(
            f"\n[批次 {current_batch_num}/{total_batches}] 正在处理 {len(batch)} 个文档块..."
        )

        try:
            # 步骤 1: 将文本块转换为图文档
            print("  - 步骤 1: 调用 LLM 进行图谱转换...")
            graph_documents_batch = llm_transformer.convert_to_graph_documents(batch)
            total_nodes = sum(len(doc.nodes) for doc in graph_documents_batch)
            total_rels = sum(len(doc.relationships) for doc in graph_documents_batch)
            print(
                f"  - 步骤 1: 转换成功！生成了 {total_nodes} 个节点和 {total_rels} 个关系。"
            )

            # 步骤 2: 将生成的图文档添加到 Neo4j 数据库
            print("  - 步骤 2: 正在将图数据写入 Neo4j...")
            graph.add_graph_documents(
                graph_documents_batch, baseEntityLabel=True, include_source=True
            )
            print(f"  - 步骤 2: 批次 {current_batch_num} 数据写入完成。")

        except Exception as e:
            print(f"  - [错误] 批次 {current_batch_num} 处理失败: {e}")

        # 在批次之间加入短暂延迟
        time.sleep(1)

    print("\n--- 所有批次处理完成！知识图谱已在Neo4j中构建。 ---")


if __name__ == "__main__":
    create_neo4j_graph_from_chunks()
