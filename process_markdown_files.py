import json
import os
import shutil
import time

from dotenv import load_dotenv
from langchain_community.chat_models import ChatZhipuAI
from langchain_core.messages import HumanMessage, SystemMessage

# 加载 .env 文件中的环境变量
load_dotenv()


def update_metadata_log(knowledge_base_dir: str, failed_files_info: list):
    """
    创建或更新 metadata.json 文件，记录处理失败的文件信息。
    """
    metadata_path = os.path.join(knowledge_base_dir, "metadata.json")
    print(f"--- 正在更新失败记录到 {metadata_path} ---")

    try:
        with open(metadata_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        data = {}

    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    data[timestamp] = failed_files_info

    try:
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print("失败记录更新完成。")
    except Exception as e:
        print(f"写入 metadata.json 文件时出错: {e}")


def process_md_with_langchain(content: str) -> str:
    """
    使用智谱AI大模型处理单个Markdown文件的内容。
    """
    if not os.getenv("ZHIPUAI_API_KEY"):
        return "[AI处理失败：环境变量 ZHIPUAI_API_KEY 未设置]"

    system_prompt = """
    # Markdown文件清理与标题层级修复
    你是一个专业的文档处理助手，需要清理从PDF通过OCR识别转换得到的Markdown文件，并修复其中的标题层级结构。请严格按照以下步骤和要求操作。
    ## 处理步骤
    1. **清理符号**: 删除OCR乱码（如、□）、孤立的特殊字符，但保留正常标点。
    2. **识别标题**: 识别如“一、”、“（一）”、“1.”、“1.1”等序号格式。
    3. **重建层级**: 根据序号使用Markdown的`#`号重建标题层级。
    4. **处理特殊格式**: 对于“问：”和“答：”的段落，**不要**将其转为标题，应保留为普通段落，可用加粗突出。确保文档开头的引言和结尾的元数据（如日期、来源）被完整保留为普通文本。
    ## 输出要求
    - 输出结构清晰、内容完整、格式规范的Markdown文本。
    - **必须保留原文的所有重要内容**，只修正格式和删除无意义的符号。
    """
    messages = [SystemMessage(content=system_prompt), HumanMessage(content=content)]

    try:
        # 初始化AI模型，并增加超时时间
        llm = ChatZhipuAI(
            model="glm-4-long",
            temperature=0.5,
            request_timeout=120  # 将超时时间设置为120秒
        )
        response = llm.invoke(messages)
        return response.content
    except Exception as e:
        return f"[AI处理时发生错误：{e}]"


def setup_and_process_files():
    """
    主函数，负责整个流程，包含重试和失败回退逻辑。
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    knowledge_base_dir = os.path.join(script_dir, "knowledge_base")
    source_dir = os.path.join(knowledge_base_dir, "02_processed_files")
    target_dir = os.path.join(knowledge_base_dir, "03_langchain_processed")

    if not os.path.isdir(source_dir):
        print(f"错误：源目录不存在 -> {source_dir}")
        return

    print(f"正在从 {source_dir} 复制目录结构到 {target_dir}")
    shutil.copytree(source_dir, target_dir, ignore=shutil.ignore_patterns('*'), dirs_exist_ok=True)
    print("目录结构复制完成。")

    print("开始遍历和处理 Markdown 文件...")
    permanently_failed_files = []
    file_count = 0

    for root, _, files in os.walk(source_dir):
        for file in files:
            if not file.endswith(".md"):
                continue

            file_count += 1
            source_file_path = os.path.join(root, file)
            relative_path = os.path.relpath(source_file_path, source_dir)
            destination_file_path = os.path.join(target_dir, relative_path)

            print(f"\n正在处理文件 ({file_count}): {source_file_path}")

            try:
                with open(source_file_path, 'r', encoding='utf-8') as f:
                    original_content = f.read()
            except Exception as e:
                print(f"  -> 读取文件时出错: {e}")
                permanently_failed_files.append(
                    {"file_path": source_file_path, "error": f"读取文件失败: {e}", "action": "未处理"})
                continue

            processed_content = None
            last_error = ""
            for attempt in range(5):
                print(f"  -> 尝试处理 (第 {attempt + 1}/5 次)")
                result = process_md_with_langchain(original_content)

                if not result.startswith("[AI处理时发生错误"):
                    processed_content = result
                    print("  -> AI模型处理成功。")
                    break
                else:
                    last_error = result
                    print(f"  -> 处理失败: {last_error}")
                    if attempt < 4:
                        print("  -> 10秒后重试...")
                        time.sleep(10)

            os.makedirs(os.path.dirname(destination_file_path), exist_ok=True)
            if processed_content is not None:
                with open(destination_file_path, 'w', encoding='utf-8') as f:
                    f.write(processed_content)
                print(f"  -> 已保存到: {destination_file_path}")
            else:
                print(f"  -> 5次尝试后处理失败，将直接复制源文件。")
                shutil.copy2(source_file_path, destination_file_path)
                permanently_failed_files.append({
                    "file_path": source_file_path,
                    "error": last_error,
                    "action": "复制了源文件"
                })

    if not permanently_failed_files:
        print(f"\n处理完成！共成功处理了 {file_count} 个 Markdown 文件。")
    else:
        print(f"\n处理完成！在 {file_count} 个文件中，有 {len(permanently_failed_files)} 个文件处理失败。")
        print("以下文件在所有重试后仍然失败:")
        for item in permanently_failed_files:
            print(f"  - {item['file_path']}")
        update_metadata_log(knowledge_base_dir, permanently_failed_files)


if __name__ == "__main__":
    setup_and_process_files()
