import os

_symbol = "TLMUSDT"

_start_date = "2023-01-01"
_end_date = "2023-03-31"

_cmd = f"python python/download-aggTrade.py -t um -s {_symbol} -startDate {_start_date} -endDate {_end_date} -folder /opt/binance_public_data_zip/ -skip-monthly 1"

os.system(_cmd)