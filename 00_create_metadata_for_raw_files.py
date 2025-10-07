import os
import json
import uuid


def create_metadata_file(raw_files_dir, kb_dir="knowledge_base"):
    """
    遍历指定目录下的文件，为每个文件生成一个UUID，并将元信息存储到JSON文件中。

    :param raw_files_dir: 存放原始文件的目录。
    :param kb_dir: 存放知识库和元数据文件的目录。
    """
    # --- 1. 定义路径 ---
    # 获取当前脚本所在目录的绝对路径作为项目根目录
    project_root = os.path.dirname(os.path.abspath(__file__))
    raw_files_path = os.path.join(project_root, raw_files_dir)
    kb_path = os.path.join(project_root, kb_dir)
    metadata_file_path = os.path.join(kb_path, "metadata.json")

    # --- 2. 确保目录存在 ---
    # 如果目录不存在，则创建它们
    os.makedirs(raw_files_path, exist_ok=True)
    os.makedirs(kb_path, exist_ok=True)

    print(f"正在扫描目录: {raw_files_path}")

    # --- 3. 检查原始文件目录是否为空 ---
    if not os.listdir(raw_files_path):
        print(f"警告: 目录 '{raw_files_dir}' 为空。")
        print(f"请先将文件放入该目录，然后再运行此脚本。")
        # 创建一个空的元数据文件
        with open(metadata_file_path, 'w', encoding='utf-8') as f:
            json.dump({}, f, indent=4)
        return

    # --- 4. 生成元数据 ---
    metadata = {}
    # os.walk可以遍历目录及其所有子目录
    for root, _, files in os.walk(raw_files_path):
        for filename in files:
            # 生成一个唯一的UUID作为文件的ID
            file_uuid = str(uuid.uuid4())
            
            # 获取文件的绝对路径和相对路径
            absolute_path = os.path.join(root, filename)
            relative_path = os.path.relpath(absolute_path, project_root)

            # 记录文件的元信息
            metadata[file_uuid] = {
                "file_name": filename,
                "absolute_path": absolute_path,
                "relative_path": relative_path.replace("\\", "/"),  # 统一路径分隔符为'/'
            }
            print(f"  - 已为文件 '{filename}' 分配UUID: {file_uuid}")

    # --- 5. 写入JSON文件 ---
    try:
        with open(metadata_file_path, 'w', encoding='utf-8') as f:
            # indent=4 使JSON文件格式优美，易于阅读
            # ensure_ascii=False 确保中文字符能正确显示
            json.dump(metadata, f, indent=4, ensure_ascii=False)
        print(f"\n元数据文件创建成功！已保存至: {metadata_file_path}")
    except IOError as e:
        print(f"\n错误：无法写入元数据文件。原因: {e}")

if __name__ == "__main__":
    # 指定原始文件存放在 "knowledge_base" 目录下的 "01_raw_files" 子目录中
    raw_directory = os.path.join("knowledge_base", "01_raw_files")
    create_metadata_file(raw_files_dir=raw_directory)