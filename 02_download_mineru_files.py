import requests
import json
from tqdm import tqdm
import os
import zipfile
import time
import shutil

def download_and_move_file(url, file_info, raw_files_base_dir, processed_files_dir):
    """
    从URL下载文件，重命名为"原始文件名.zip"，并移动到processed_files_dir下的对应目录。
    :param url: 文件的下载链接
    :param file_info: 包含原始文件信息的元数据字典
    :param raw_files_base_dir: 原始文件根目录 (e.g., '.../01_raw_files')
    :param processed_files_dir: 处理后文件存放的根目录 (e.g., '.../02_raw_md_files')
    """
    try:
        # 从 URL 中提取文件名
        local_filename = url.split('/')[-1]
        # 临时下载路径
        temp_download_path = local_filename

        print(f"准备下载文件: {local_filename}")

        # 使用 stream=True 进行流式下载
        with requests.get(url, stream=True) as r:
            r.raise_for_status()  # 如果请求失败 (如 404), 会抛出异常
            total_size = int(r.headers.get('content-length', 0))

            # 使用 tqdm 创建进度条
            with open(temp_download_path, 'wb') as f, tqdm(
                desc=local_filename,
                total=total_size,
                unit='iB',
                unit_scale=True,
                unit_divisor=1024,
            ) as bar:
                for chunk in r.iter_content(chunk_size=8192):
                    size = f.write(chunk)
                    bar.update(size)
        print(f"文件下载成功，已临时保存至: {temp_download_path}")

        # --- 计算目标路径并移动文件 ---
        # 1. 新的文件名 = 原始文件名 + .zip
        new_filename = ''.join(file_info['file_name'].split('.')[:-1]) + '.zip'

        # 2. 计算相对于 '01_raw_files' 的路径
        relative_path_from_raw = os.path.relpath(file_info['absolute_path'], raw_files_base_dir)

        # 3. 构建在 '02_raw_md_files' 中的最终完整路径
        final_dir = os.path.join(processed_files_dir, os.path.dirname(relative_path_from_raw))
        final_path = os.path.join(final_dir, new_filename)

        # 4. 确保目标目录存在，然后移动并重命名文件
        os.makedirs(final_dir, exist_ok=True)
        shutil.move(temp_download_path, final_path)
        print(f"文件已重命名并移动到: {final_path}")

    except requests.exceptions.RequestException as e:
        print(f"下载失败: {e}")

if __name__ == "__main__":
    # --- 配置 ---
    PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
    METADATA_PATH = os.path.join(PROJECT_ROOT, "knowledge_base", "metadata.json")
    RAW_FILES_DIR = os.path.join(PROJECT_ROOT, "knowledge_base", "01_raw_files")
    PROCESSED_FILES_DIR = os.path.join(PROJECT_ROOT, "knowledge_base", "02_raw_md_files")
    
    # --- 1. 从元数据中收集所有需要处理的批处理任务 ---
    pending_tasks = {}
    with open(METADATA_PATH, 'r', encoding='utf-8') as f:
        metadata = json.load(f)
        for file_uuid, info in metadata.items():
            if "batch_id" in info:
                # 检查文件是否已经被处理过
                expected_zip_filename = info['file_name'] + '.zip'
                relative_path_from_raw = os.path.relpath(info['absolute_path'], RAW_FILES_DIR)
                final_zip_path = os.path.join(PROCESSED_FILES_DIR, os.path.dirname(relative_path_from_raw), expected_zip_filename)

                if not os.path.exists(final_zip_path):
                    pending_tasks[file_uuid] = info["batch_id"]
                else:
                    print(f"文件 '{info['file_name']}' 已处理，跳过。")
    
    if not pending_tasks:
        print("\n所有文件均已处理完毕，无需下载。")
        exit()

    token = "eyJ0eXBlIjoiSldUIiwiYWxnIjoiSFM1MTIifQ.eyJqdGkiOiI0MzIwOTA1MyIsInJvbCI6IlJPTEVfUkVHSVNURVIiLCJpc3MiOiJPcGVuWExhYiIsImlhdCI6MTc1OTEyOTE0OCwiY2xpZW50SWQiOiJsa3pkeDU3bnZ5MjJqa3BxOXgydyIsInBob25lIjoiMTg1Njg2ODEyMjQiLCJvcGVuSWQiOm51bGwsInV1aWQiOiI2YzEzNjgyNS0xZjBlLTRhOWMtODY4My03NzIyY2UyNTA4MDIiLCJlbWFpbCI6IiIsImV4cCI6MTc2MDMzODc0OH0.tzDW1w1vQ_eS_Fmv-UDRxmbap8tboR6IXXynw07PCTUt4xtinMT9hezUDd9MdVf_sWbQQ2dJRir7MToej343ig"
    token = "eyJ0eXBlIjoiSldUIiwiYWxnIjoiSFM1MTIifQ.eyJqdGkiOiI0MzIwOTA1MyIsInJvbCI6IlJPTEVfUkVHSVNURVIiLCJpc3MiOiJPcGVuWExhYiIsImlhdCI6MTc1OTEzNjg3OSwiY2xpZW50SWQiOiJsa3pkeDU3bnZ5MjJqa3BxOXgydyIsInBob25lIjoiMTg1Njg2ODEyMjQiLCJvcGVuSWQiOm51bGwsInV1aWQiOiJlYmRmOTZhYy0xMTBiLTRmYmUtOWI3Ni00M2E0Mzk0ODcyYTIiLCJlbWFpbCI6IiIsImV4cCI6MTc2MDM0NjQ3OX0.RljqtmU75fGF-KjqvXugEhG7BEqKL0AvYPSpow_Ub2zDcJ0hcfub35ACD3sJJjDEcNNGVSWFN3xq1e5Qq54pxA"

    header = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    # --- 2. 轮询处理每一个批处理任务 ---
    while pending_tasks:
        print(f"\n--- 开始新一轮查询，剩余 {len(pending_tasks)} 个任务 ---")
        # 使用 list(pending_tasks.items()) 来创建一个副本，以便在循环中安全地修改字典
        for file_uuid, batch_id in list(pending_tasks.items()):
            url = f"https://mineru.net/api/v4/extract-results/batch/{batch_id}"
            original_file_info = metadata.get(file_uuid)
            print(f"  - 正在查询 '{original_file_info['file_name']}' (Batch ID: {batch_id})...")
            
            try:
                res = requests.get(url, headers=header)
                if not res.ok:
                    print(f"    查询失败，状态码: {res.status_code}。将在下一轮重试。")
                    continue
                
                result_data = res.json()
                
                # 提取与当前文件UUID匹配的结果
                task_result = None
                if result_data.get("msg") == 'ok' and "data" in result_data and "extract_result" in result_data["data"]:
                    for item in result_data["data"]["extract_result"]:
                        if item.get("data_id") == file_uuid:
                            task_result = item
                            break
                
                if not task_result:
                    print(f"    在批处理 {batch_id} 的返回结果中未找到文件 {file_uuid} 的信息。")
                    continue

                state = task_result.get("state")
                if state == "done":
                    print(f"    状态: {state}。处理完成，准备下载。")
                    download_url = task_result["full_zip_url"]
                    download_and_move_file(download_url, original_file_info, RAW_FILES_DIR, PROCESSED_FILES_DIR)
                    del pending_tasks[file_uuid]  # 从待办事项中移除
                elif state in ["failed", "error"]:
                    print(f"    状态: {state}。处理失败，已从任务队列中移除。")
                    del pending_tasks[file_uuid]
                else:
                    print(f"    状态: {state}。仍在处理中...")

            except requests.exceptions.RequestException as e:
                print(f"    网络请求错误: {e}。将在下一轮重试。")
            except json.JSONDecodeError:
                print(f"    解析服务器响应失败 (非JSON格式)。将在下一轮重试。")

        # 一轮查询结束后，如果仍有待办任务，则等待
        if pending_tasks:
            print(f"\n本轮查询结束。仍有 {len(pending_tasks)} 个任务在处理中。")
            print("将在10秒后开始下一轮查询...")
            time.sleep(10)

    print("\n所有任务均已处理完毕。")