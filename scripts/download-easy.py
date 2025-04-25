import os
import zipfile
import pandas as pd

# with open("res/bad_files.txt", "r") as f:
#     l = f.read().splitlines()
# print(l)

# for i in l:
#     data = i.split("/")
#     print(data)
#     _symbol = data[-2]
#     _start_date = data[-1][-14:-4]
#     _end_date = _start_date

#     # _symbol = "TURBOUSDT"

#     # _start_date = "2024-09-01"
#     # _end_date = "2024-09-01"

#     _cmd = f"python python/download-aggTrade.py -t um -s {_symbol} -startDate {_start_date} -endDate {_end_date} -folder /opt/tmp/binance_public_data_zip/ -skip-monthly 1"

#     os.system(_cmd)

data = pd.read_csv("res/need_matrix.csv", index_col=0)

for i in data.index:
    if i <= "2025-03-15":
        continue
    for j in data.columns:
        if data.loc[i, j] == 0:
            continue
        _symbol = j
        _start_date = i
        _end_date = _start_date
        
        # if i != "2025-03-10":
        #     continue
        
        # if _symbol != "TLMUSDT":
        #     continue
        
        print(_symbol, _start_date)

        # _symbol = "TURBOUSDT"

        # _start_date = "2024-09-01"
        # _end_date = "2024-09-01"

        _cmd = f"python python/download-aggTrade.py -t um -s {_symbol} -startDate {_start_date} -endDate {_end_date} -folder /opt/binance_public_data_zip/ -skip-monthly 1"

        os.system(_cmd)