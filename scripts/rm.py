import os


path = "/opt/binance_public_data_zip/data/futures/um/daily/aggTrades"

for i in sorted(os.listdir(path)):
    os.system(f"rm {path}/{i}/{i}-aggTrades-2025-*")