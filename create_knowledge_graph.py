
import os
import pickle
import shutil
import kuzu
from dotenv import load_dotenv

# LangChain imports
from langchain_experimental.graph_transformers import LLMGraphTransformer
from langchain_community.chat_models import ChatZhipuAI
from langchain_kuzu import KuzuGraph
from langchain_core.documents import Document
from langchain_core.graphs import GraphDocument, Node, Relationship

# 加载 .env 文件中的环境变量
load_dotenv()

def transform_and_save_intermediate_graphs():
    """
    第一步：遍历所有分块后的pkl文件，调用LLM将其转换为图谱文档，
    然后将这些中间结果保存为新的pkl文件，暂不写入数据库。
    """
    print("--- 阶段1：开始将文档块转换为中间图谱文件 ---")
    # --- 路径定义 ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    knowledge_base_dir = os.path.join(script_dir, "knowledge_base")
    source_dir = os.path.join(knowledge_base_dir, "04_database", "01_langchain_split_documents_files")
    intermediate_dir = os.path.join(knowledge_base_dir, "04_database", "02_intermediate_graph_pkls")

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
    llm = ChatZhipuAI(model="glm-4.5-air", temperature=0, request_timeout=120)
    graph_transformer = LLMGraphTransformer(llm=llm)

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
                # 一次性处理单个文件的所有块，以保证内部纲要一致性
                graph_documents = graph_transformer.convert_to_graph_documents(documents)
                
                # 定义中间文件的保存路径
                relative_path = os.path.relpath(root, source_dir)
                intermediate_save_dir = os.path.join(intermediate_dir, relative_path)
                os.makedirs(intermediate_save_dir, exist_ok=True)
                intermediate_file_path = os.path.join(intermediate_save_dir, file)

                # 保存转换后的GraphDocument列表
                with open(intermediate_file_path, 'wb') as f:
                    pickle.dump(graph_documents, f)
                print(f"    -> 成功转换并暂存到: {intermediate_file_path}")

            except Exception as e:
                print(f"    -> 在处理文件并转换为图谱时发生严重错误: {e}")

    print(f"\n--- 阶段1完成：共处理并暂存了 {total_files_processed} 个文件的图谱数据。 ---")

def aggregate_and_write_to_kuzu():
    """
    第二步：读取所有中间图谱文件，聚合所有节点和关系，解决冲突，
    然后一次性将完整的、统一的图谱写入Kuzu数据库。
    """
    print("\n--- 阶段2：开始聚合图谱数据并写入Kuzu数据库 ---")
    # --- 路径定义 ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    knowledge_base_dir = os.path.join(script_dir, "knowledge_base")
    intermediate_dir = os.path.join(knowledge_base_dir, "04_database", "02_intermediate_graph_pkls")
    kuzu_parent_dir = os.path.join(knowledge_base_dir, "04_database", "03_kg_kuzu_db")
    db_dir = os.path.join(kuzu_parent_dir, "kuzu_graphrag")

    # --- 健壮的清理逻辑 ---
    if os.path.exists(db_dir):
        print(f"检测到旧的数据库路径存在，正在清理: {db_dir}")
        if os.path.isdir(db_dir):
            shutil.rmtree(db_dir)
        else:
            try:
                os.remove(db_dir)
            except OSError as e:
                print(f"  -> 清理文件时出错: {e}")
    os.makedirs(kuzu_parent_dir, exist_ok=True)

    if not os.path.isdir(intermediate_dir):
        print(f"错误：中间目录不存在 -> {intermediate_dir}")
        return

    # --- 全局聚合容器 ---
    all_nodes_dict = {}
    all_relationships_set = set()
    source_document_for_final_graph = None

    print("正在从中间文件聚合所有节点和关系...")
    for root, _, files in os.walk(intermediate_dir):
        for file in files:
            if not file.endswith(".pkl"):
                continue
            
            file_path = os.path.join(root, file)
            with open(file_path, 'rb') as f:
                graph_documents = pickle.load(f)
            
            if not source_document_for_final_graph and graph_documents:
                source_document_for_final_graph = graph_documents[0].source

            for gd in graph_documents:
                for node in gd.nodes:
                    # 核心冲突解决：如果节点ID已存在，则不更新，以第一次出现的类型为准
                    if node.id not in all_nodes_dict:
                        all_nodes_dict[node.id] = node
                for rel in gd.relationships:
                    all_relationships_set.add(rel)

    if not all_nodes_dict:
        print("未能从任何文件中提取有效的图谱数据，流程终止。")
        return

    # --- 一次性写入数据库 ---
    final_nodes = list(all_nodes_dict.values())
    final_relationships = list(all_relationships_set)
    # 注意：这里的source只是形式上的，因为图谱现在是全局的
    final_graph_document = [GraphDocument(nodes=final_nodes, relationships=final_relationships, source=source_document_for_final_graph)]

    print(f"聚合完成。总计 {len(final_nodes)} 个独立节点，{len(final_relationships)} 条独立关系。")
    print("正在初始化Kuzu数据库并一次性写入...")
    
    db = kuzu.Database(db_dir)
    graph = KuzuGraph(db, allow_dangerous_requests=True)
    graph.add_graph_documents(final_graph_document)
    
    print("--- 阶段2完成：知识图谱已成功创建并写入！ ---")
    print(f"Kuzu数据库文件已保存至: {db_dir}")

def verify_knowledge_graph():
    """
    第三步：加载持久化的知识图谱并打印其纲要（Schema）以进行验证。
    """
    print("\n--- 阶段3：开始验证知识图谱 ---")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    kuzu_parent_dir = os.path.join(script_dir, "knowledge_base", "04_database", "03_kg_kuzu_db")
    db_dir = os.path.join(kuzu_parent_dir, "kuzu_graphrag")

    if not os.path.exists(os.path.join(db_dir, "lock.file")):
        print(f"数据库文件不存在: {db_dir}")
        return

    print("正在加载持久化的知识图谱...")
    try:
        db = kuzu.Database(db_dir, read_only=True)
        graph = KuzuGraph(db, allow_dangerous_requests=True)
        schema = graph.get_schema
        print("--- 知识图谱纲要（Schema）获取成功！---")
        print(schema)
    except Exception as e:
        print(f"验证知识图谱时出错: {e}")


if __name__ == "__main__":
    transform_and_save_intermediate_graphs()
    aggregate_and_write_to_kuzu()
    verify_knowledge_graph()
