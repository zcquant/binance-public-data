import os
import zipfile

def unzip_to_directory(zip_path, extract_to):
    # 检查目标文件夹是否存在，如果不存在则创建
    if not os.path.exists(extract_to):
        os.makedirs(extract_to)
        
    # 打开ZIP文件
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        # 将所有内容解压到指定目录
        zip_ref.extractall(extract_to)

# 示例使用

path_root = "/opt/binance_public_data_zip/data/futures/um/daily/aggTrades"

for symbol in sorted(os.listdir(path_root)):
    extract_to = f'/opt/binance_public_data/data/futures/um/daily/aggTrades/{symbol}'  # 解压目标文件夹
    os.makedirs(extract_to, exist_ok=True)
    for f_date in sorted(os.listdir(f"{path_root}/{symbol}")):
        zip_path = f'{path_root}/{symbol}/{f_date}'  # ZIP文件路径
        print(f"Unzipping {zip_path} to {extract_to}")
        unzip_to_directory(zip_path, extract_to)