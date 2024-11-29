import os
import numpy as np
import pandas as pd

import time


symbols = np.load("/root/workspace/tardis-data/res/symbols-165.npy", allow_pickle=True)

_t_total = time.time()

for _symbol in symbols:
    
    if _symbol in ["BTCUSDT", "ETHUSDT"]:
        continue

    _t = time.time()

    _start_date = "2023-06-01"
    _end_date = "2023-06-30"
    
    _cmd = f"python python/download-aggTrade.py -t um -s {_symbol} -startDate {_start_date} -endDate {_end_date} -folder /opt/binance_public_data_zip/ -skip-monthly 1"

    os.system(_cmd)
    # print("Command:", _cmd)

    print("\n\nime used", time.time() - _t)
    
print("\n\nTotal time used", time.time() - _t_total)
