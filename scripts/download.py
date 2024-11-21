import os
import pandas as pd

import time


data = pd.read_csv("/home/shiyu/workspace/qwvct/res/ei.csv")

_t = time.time()

_need = 0
for i in data.itertuples():
    print(i)
    if i.symbol != "INJUSDT" and _need == 0:
        continue
    # _need = 1
    os.system(f"python python/download-aggTrade.py -t um -s {i.symbol} -startDate 2024-11-09 -endDate 2024-11-09 -folder /mnt/Data/tmp/binance_public_data_zip/ -skip-monthly 1")
    # break

print("\n\nime used", time.time() - _t)