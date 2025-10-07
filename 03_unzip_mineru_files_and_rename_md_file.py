import os
import zipfile
import shutil


def unzip_and_process_files():
    """
    遍历 knowledge_base/02_raw_md_files 目录及其所有子目录中的zip文件，
    就地解压，处理 full.md 但不清理文件夹。
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # 从当前脚本目录开始向上查找 'knowledge_base' 目录
    knowledge_base_dir = None
    current_path = script_dir
    while True:
        kb_path = os.path.join(current_path, "knowledge_base")
        if os.path.isdir(kb_path):
            knowledge_base_dir = kb_path
            break
        parent_path = os.path.dirname(current_path)
        if parent_path == current_path:  # 已经到达根目录
            break
        current_path = parent_path

    if not knowledge_base_dir:
        print("错误：未找到 'knowledge_base' 目录。")
        return

    processed_files_path = os.path.join(knowledge_base_dir, "02_raw_md_files")

    if not os.path.isdir(processed_files_path):
        print(f"目录未找到：{processed_files_path}")
        return

    print(f"开始递归处理目录：{processed_files_path}")

    zip_files_found = False
    # 使用 os.walk 遍历所有子目录
    for root, _, files in os.walk(processed_files_path):
        for item in files:
            if item.endswith(".zip"):
                zip_files_found = True
                item_path = os.path.join(root, item)
                dir_name = os.path.splitext(item)[0]
                # 在 zip 文件所在的目录创建同名文件夹
                target_dir = os.path.join(root, dir_name)

                os.makedirs(target_dir, exist_ok=True)

                try:
                    with zipfile.ZipFile(item_path, "r") as zip_ref:
                        zip_ref.extractall(target_dir)
                    print(f"已解压 {item_path} 到 {target_dir}")

                    # 查找 full.md
                    full_md_path_in_subdir = None
                    for sub_root, _, sub_files in os.walk(target_dir):
                        if "full.md" in sub_files:
                            full_md_path_in_subdir = os.path.join(sub_root, "full.md")
                            break

                    if full_md_path_in_subdir:
                        # 移动并重命名 full.md 到 zip 文件所在的目录
                        new_md_name = dir_name + ".md"
                        destination_md_path = os.path.join(root, new_md_name)
                        shutil.move(full_md_path_in_subdir, destination_md_path)
                        print(f"已移动并重命名 'full.md' 到 {destination_md_path}")
                    else:
                        print(f"在 {target_dir} 的子目录中未找到 'full.md'")

                except zipfile.BadZipFile:
                    print(f"错误：{item} 不是一个有效的 zip 文件。")
                except Exception as e:
                    print(f"处理 {item} 时发生错误：{e}")

    if not zip_files_found:
        print("在目录及其子目录中未找到任何 zip 文件。")


if __name__ == "__main__":
    unzip_and_process_files()
