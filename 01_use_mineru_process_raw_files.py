import os
import json
import shutil
import requests


def process_knowledge_base(
    metadata_path,
    raw_dir,
    processed_dir,
    api_token,
    api_url="https://mineru.net/api/v4/file-urls/batch"
):
    """
    处理原始文件，将md文件复制，将非md文件上传并更新元数据。

    :param metadata_path: metadata.json 文件的路径。
    :param raw_dir: 原始文件目录 (e.g., 'knowledge_base/01_raw_files')。
    :param processed_dir: 处理后文件存放的目录 (e.g., 'knowledge_base/02_raw_md_files')。
    :param api_token: 用于API认证的token。
    :param api_url: MinerU API的端点。
    """
    # --- 1. 加载元数据 ---
    try:
        with open(metadata_path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        print(f"成功加载元数据: {metadata_path}")
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"错误: 无法加载或解析元数据文件 {metadata_path}。请先运行 'create_metadata.py'。原因: {e}")
        return
    
    # --- 2. 复制目录结构，包括空文件夹 ---
    print("\n--- 正在复制目录结构 ---")
    # 确保根处理目录存在
    os.makedirs(processed_dir, exist_ok=True)
    for root, _, _ in os.walk(raw_dir):
        # 计算相对路径
        relative_path = os.path.relpath(root, raw_dir)
        dest_dir_path = os.path.join(processed_dir, relative_path)
        os.makedirs(dest_dir_path, exist_ok=True)
    print("目录结构复制完成。")

    # --- 3. 准备待处理文件列表 ---
    md_files_to_copy = []
    files_to_upload = []

    for file_uuid, file_info in metadata.items():
        file_path = file_info.get("absolute_path")
        if not file_path or not os.path.exists(file_path):
            print(f"警告: 跳过文件 (UUID: {file_uuid})，路径不存在: {file_path}")
            continue

        if file_path.endswith('.md'):
            md_files_to_copy.append(file_info)
        else:
            # 为待上传文件添加UUID，以便后续更新元数据
            file_info['uuid'] = file_uuid
            files_to_upload.append(file_info)

    # --- 4. 复制Markdown文件 ---
    print("\n--- 开始处理Markdown文件 ---")
    if not md_files_to_copy:
        print("没有找到需要直接复制的Markdown文件。")
    else:
        for file_info in md_files_to_copy:
            src_path = file_info["absolute_path"]
            # 计算目标路径
            relative_to_raw = os.path.relpath(src_path, raw_dir)
            dest_path = os.path.join(processed_dir, relative_to_raw)

            # 直接复制文件，因为目录已提前创建
            shutil.copy2(src_path, dest_path)
            print(f"  - 已复制: {file_info['file_name']} -> {dest_path}")

    # --- 5. 上传非Markdown文件 ---
    print("\n--- 开始处理非Markdown文件 (上传) ---")
    if not files_to_upload:
        print("没有找到需要上传的非Markdown文件。")
    else:
        header = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_token}"
        }

        # 遍历每个文件，单独上传并记录batch_id
        for file_info in files_to_upload:
            file_uuid = file_info['uuid']
            print(f"\n- 开始处理文件: {file_info['file_name']}")

            # 1. 为单个文件构造API请求体
            data = {
                "enable_formula": True,
                "language": "ch",
                "enable_table": True,
                "files": [{"name": file_info["file_name"], "is_ocr": True, "data_id": file_uuid}]
            }

            try:
                # 2. 获取上传URL
                print(f"  - 正在请求上传链接...")
                response = requests.post(api_url, headers=header, json=data)
                response.raise_for_status()
                result = response.json()

                if result.get("code") == 0 and result["data"]["file_urls"]:
                    batch_id = result["data"]["batch_id"]
                    upload_url = result["data"]["file_urls"][0]
                    print(f"  - 获取链接成功。批处理ID: {batch_id}")

                    # 3. 上传文件
                    with open(file_info['absolute_path'], 'rb') as f:
                        res_upload = requests.put(upload_url, data=f)

                    if res_upload.status_code == 200:
                        print(f"  - 上传成功。")
                        # 4. 记录batch_id到元数据
                        metadata[file_uuid]['batch_id'] = batch_id
                    else:
                        print(f"  - 上传失败 (状态码: {res_upload.status_code})")
                else:
                    print(f"  - API请求失败: {result.get('msg', '未知错误')}")
            except requests.exceptions.RequestException as e:
                print(f"  - 网络请求错误: {e}")
            except (KeyError, IndexError) as e:
                print(f"  - 解析API响应失败: {e}")

    # --- 6. 保存更新后的元数据 ---
    with open(metadata_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=4, ensure_ascii=False)
    print(f"\n处理完成。元数据已更新并保存至: {metadata_path}")


if __name__ == "__main__":
    # --- 配置 ---
    PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
    RAW_FILES_DIR = os.path.join(PROJECT_ROOT, "knowledge_base", "01_raw_files")
    PROCESSED_FILES_DIR = os.path.join(PROJECT_ROOT, "knowledge_base", "02_raw_md_files")
    METADATA_PATH = os.path.join(PROJECT_ROOT, "knowledge_base", "metadata.json")

    # 请将此处的token替换为您自己的有效token
    API_TOKEN = "eyJ0eXBlIjoiSldUIiwiYWxnIjoiSFM1MTIifQ.eyJqdGkiOiI0MzIwOTA1MyIsInJvbCI6IlJPTEVfUkVHSVNURVIiLCJpc3MiOiJPcGVuWExhYiIsImlhdCI6MTc1OTEzNjg3OSwiY2xpZW50SWQiOiJsa3pkeDU3bnZ5MjJqa3BxOXgydyIsInBob25lIjoiMTg1Njg2ODEyMjQiLCJvcGVuSWQiOm51bGwsInV1aWQiOiJlYmRmOTZhYy0xMTBiLTRmYmUtOWI3Ni00M2E0Mzk0ODcyYTIiLCJlbWFpbCI6IiIsImV4cCI6MTc2MDM0NjQ3OX0.RljqtmU75fGF-KjqvXugEhG7BEqKL0AvYPSpow_Ub2zDcJ0hcfub35ACD3sJJjDEcNNGVSWFN3xq1e5Qq54pxA"
    process_knowledge_base(METADATA_PATH, RAW_FILES_DIR, PROCESSED_FILES_DIR, API_TOKEN)