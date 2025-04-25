import os

import zipfile

import numpy as np
import pandas as pd

from tqdm import tqdm


# t = "spot"
t = "futures/um"

def check_zip_integrity(zip_path):
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            # 测试所有文件
            bad_file = zf.testzip()
            if bad_file is not None:
                print(f"Corrupted file found in {zip_path}: {bad_file}")
            else:
                # print(f"{zip_path} is OK")
                pass
    except zipfile.BadZipFile:
        print(f"{zip_path} is not a valid ZIP file")
    except Exception as e:
        print(f"Error checking {zip_path}: {e}")

# # 批量检查 ZIP 文件
# zip_dir = "/path/to/your/zipfiles"
# for file_name in os.listdir(zip_dir):
#     if file_name.endswith(".zip"):
#         check_zip_integrity(os.path.join(zip_dir, file_name))


# symbols = sorted(os.listdir(f"/opt/binance_public_data_zip/data/{t}/daily/aggTrades/"))
# symbols = pd.read_csv("/root/workspace/tardis-data/res/symbols-162.csv")["symbol"].values
symbols = sorted(os.listdir(f"/opt/binance_public_data_zip/data/{t}/daily/aggTrades"))
l = []
n = 0
for symbol in symbols:
    files = sorted(os.listdir(f"/opt/binance_public_data_zip/data/{t}/daily/aggTrades/{symbol}"))
    n += len(files)
    
    # if len(files) != 564 and len(files) != 0:
    #     print(symbol, len(files))
    #     l_wrong = [i[-14:-4] for i in files]
    #     print([i for i in l if i not in l_wrong])
    # else:
    #     l = [i[-14:-4] for i in files]
    
    # print(symbol)
    # for f in files:
    #     if os.path.getsize(f"/opt/binance_public_data_zip/data/futures/um/daily/aggTrades/{symbol}/{f}") == 0:
    #         print(symbol, f)
    #         # os.remove(f"/opt/binance_public_data_zip/data/futures/um/daily/aggTrades/{symbol}/{f}")
    #     check_zip_integrity(f"/opt/binance_public_data_zip/data/futures/um/daily/aggTrades/{symbol}/{f}")
    
print(n)