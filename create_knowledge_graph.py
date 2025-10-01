
import os
import pickle
import shutil
from dotenv import load_dotenv

# LangChain imports for graph creation
# 确保已安装所需库: pip install langchain_experimental langchain_kuzu langchain-community
from langchain_experimental.graph_transformers import LLMGraphTransformer
from langchain_community.chat_models import ChatZhipuAI
from langchain_kuzu.graphs import KuzuGraph

# 加载 .env 文件中的环境变量
load_dotenv()

def create_knowledge_graph():
    """
    主函数，处理文档块（chunks）并使用KuzuDB构建知识图谱。
    """
    # --- 1. 配置与设置 ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    knowledge_base_dir = os.path.join(script_dir, "knowledge_base")
    source_dir = os.path.join(knowledge_base_dir, "04_database", "01_langchain_split_documents_files")
    db_dir = os.path.join(knowledge_base_dir, "04_database", "03_kg_kuzu_db")

    # 为确保全新构建，清理旧的数据库目录
    if os.path.exists(db_dir):
        print(f"正在删除旧的知识图谱目录: {db_dir}")
        shutil.rmtree(db_dir)
    print(f"正在创建新的知识图谱目录: {db_dir}")
    os.makedirs(db_dir)

    if not os.path.isdir(source_dir):
        print(f"错误：源目录不存在 -> {source_dir}")
        return

    # --- 2. 初始化图谱和转换器组件 ---
    print("正在初始化KuzuGraph、LLM和Graph Transformer...")
    graph = KuzuGraph(directory=db_dir)
    
    llm = ChatZhipuAI(model="glm-4.5-air", temperature=0)
    graph_transformer = LLMGraphTransformer(llm=llm)

    print(f"\n开始处理目录中的 .pkl 文件: {source_dir}")
    total_files_processed = 0

    # --- 3. 迭代、转换并构建图谱 ---
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
                print("  -> 文件为空或不包含有效文档，跳过。")
                continue
            
            print(f"  -> 文件包含 {len(documents)} 个文档块，准备转换为知识图谱...")

            try:
                graph_documents = graph_transformer.convert_to_graph_documents(documents)
                print(f"  -> 成功生成 {len(graph_documents)} 个图谱文档。")

                graph.add_graph_documents(graph_documents)
                print(f"  -> 图谱文档已添加至Kuzu数据库。")

            except Exception as e:
                print(f"  -> 在处理文件并转换为图谱时发生严重错误: {e}")

    # --- 4. 最终报告 ---
    print("\n知识图谱创建完成！")
    print(f"总共处理了 {total_files_processed} 个文件。")
    print(f"Kuzu数据库文件已保存至: {db_dir}")

def verify_knowledge_graph():
    """
    加载持久化的知识图谱并打印其纲要（Schema）以进行验证。
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_dir = os.path.join(script_dir, "knowledge_base", "04_database", "03_kg_kuzu_db")

    if not os.path.exists(os.path.join(db_dir, "kuzu.db")):
        print(f"数据库文件不存在: {db_dir}")
        print("请先运行 create_knowledge_graph() 来创建数据库。")
        return

    print("正在加载持久化的知识图谱...")
    try:
        graph = KuzuGraph(directory=db_dir)
        schema = graph.get_schema
        print("--- 知识图谱纲要（Schema）获取成功！---")
        print(schema)
    except Exception as e:
        print(f"验证知识图谱时出错: {e}")


if __name__ == "__main__":
    # 第一步：创建知识图谱
    create_knowledge_graph()

    # 第二步：验证图谱是否创建成功并查看其纲要
    print("\n" + "="*60)
    print("--- 开始验证知识图谱 ---")
    print("="*60)
    verify_knowledge_graph()
