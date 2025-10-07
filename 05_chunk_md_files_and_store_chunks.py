
import os
import pickle

# 确保已安装所需库: pip install langchain-text-splitters
from langchain_text_splitters import MarkdownHeaderTextSplitter

def chunk_markdown_content(content: str, file_path: str) -> list:
    """
    使用 MarkdownHeaderTextSplitter 对文件内容进行分块。
    """
    print(f"  -> 正在使用 MarkdownHeaderTextSplitter 进行分块: {os.path.basename(file_path)}")
    
    headers_to_split_on = [
        ("#", "Header 1"),
        ("##", "Header 2"),
        ("###", "Header 3"),
    ]

    markdown_splitter = MarkdownHeaderTextSplitter(
        headers_to_split_on=headers_to_split_on, 
        strip_headers=False
    )
    
    try:
        chunks = markdown_splitter.split_text(content)
        print(f"  -> 文件被分成了 {len(chunks)} 块。")
        return chunks
    except Exception as e:
        print(f"  -> 分块时出错: {e}")
        return []

def chunk_and_save_files():
    """
    主函数，负责分块并将结果保存为 .pkl 文件。
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    knowledge_base_dir = os.path.join(script_dir, "knowledge_base")
    source_dir = os.path.join(knowledge_base_dir, "03_structure_md_files")
    output_dir = os.path.join(knowledge_base_dir, "04_database", "01_langchain_split_documents_files")

    if not os.path.isdir(output_dir):
        print(f"目录不存在，正在创建: {output_dir}")
        os.makedirs(output_dir)
    else:
        print(f"目录已存在: {output_dir}")

    if not os.path.isdir(source_dir):
        print(f"错误：源目录不存在 -> {source_dir}")
        return

    print(f"\n开始遍历和分块目录: {source_dir}")
    file_count = 0
    total_chunks = 0

    for root, _, files in os.walk(source_dir):
        for file in files:
            if file.endswith(".md"):
                file_count += 1
                file_path = os.path.join(root, file)
                print(f"正在处理文件 ({file_count}): {file_path}")

                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                except Exception as e:
                    print(f"  -> 读取文件时出错: {e}")
                    continue

                chunks = chunk_markdown_content(content, file_path)
                
                if not chunks:
                    print("  -> 未生成任何分块，跳过保存。")
                    continue
                
                total_chunks += len(chunks)

                relative_path = os.path.relpath(root, source_dir)
                pkl_filename = os.path.splitext(file)[0] + ".pkl"
                destination_path = os.path.join(output_dir, relative_path, pkl_filename)

                os.makedirs(os.path.dirname(destination_path), exist_ok=True)

                try:
                    with open(destination_path, 'wb') as f:
                        pickle.dump(chunks, f)
                    print(f"  -> 分块已保存到: {destination_path}")
                except Exception as e:
                    print(f"  -> 保存 .pkl 文件时出错: {e}")

    if file_count == 0:
        print("在源目录中没有找到任何 .md 文件。")
    else:
        print(f"\n处理完成！共处理了 {file_count} 个 Markdown 文件，总共生成了 {total_chunks} 个文本块。")

def view_a_sample_pkl_file():
    """
    查找第一个生成的 .pkl 文件，并打印其内容以供检查。
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, "knowledge_base", "04_database", "01_langchain_split_documents_files")

    sample_file_path = None
    for root, _, files in os.walk(output_dir):
        for file in files:
            if file.endswith(".pkl"):
                sample_file_path = os.path.join(root, file)
                break
        if sample_file_path:
            break

    if not sample_file_path:
        print("未找到任何 .pkl 文件可供查看。请先确保 chunk_and_save_files() 已成功运行。")
        return

    print(f"正在查看示例文件: {sample_file_path}")

    try:
        with open(sample_file_path, 'rb') as f:
            # 加载pkl文件，还原为Document对象列表
            chunks = pickle.load(f)

        if not chunks:
            print("文件为空，不包含任何分块。")
            return

        # 打印前两个块作为示例
        for i, chunk in enumerate(chunks[:2]):
            print(f"\n--- 文本块 (Chunk) {i + 1} ---")
            # LangChain的Document对象包含 page_content 和 metadata 属性
            print("元数据 (Metadata):")
            print(chunk.metadata)
            print("\n内容 (Content):")
            # 为防止内容过长刷屏，只显示前300个字符
            content_preview = chunk.page_content[:300]
            print(content_preview + "..." if len(chunk.page_content) > 300 else content_preview)

        if len(chunks) > 2:
            print(f"\n... (以及其他 {len(chunks) - 2} 个文本块)")

    except Exception as e:
        print(f"读取或解析 .pkl 文件时出错: {e}")


if __name__ == "__main__":
    # 第一步：执行分块和保存任务
    chunk_and_save_files()
    
    # 第二步：打印分隔符，并查看一个示例文件的内容
    print("\n" + "="*60)
    print("--- 查看一个分块文件的内容示例 ---")
    print("="*60)
    view_a_sample_pkl_file()
