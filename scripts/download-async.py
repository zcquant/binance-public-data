import os
import numpy as np
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import subprocess
from datetime import datetime, timedelta


def get_eligible_symbols(df, start_date, end_date):
    """
    获取符合条件的交易对列表
    
    参数:
    df: DataFrame，包含 symbol, availableSince, availableTo 列
    start_date: str，开始日期，格式 'YYYY-MM-DD'
    end_date: str，结束日期，格式 'YYYY-MM-DD'
    
    返回:
    list: 符合条件的交易对列表
    """
    # 转换日期字符串为 datetime 对象
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    
    # 计算 start_date 往前推6个月的日期（近似为180天）
    six_months_before_start = start_date - timedelta(days=180)
    
    # 转换 DataFrame 中的日期列为 datetime
    df['availableSince'] = pd.to_datetime(df['availableSince'])
    df['availableTo'] = pd.to_datetime(df['availableTo'])
    
    # 筛选符合条件的交易对
    eligible_symbols = df[
        (df['availableSince'] <= six_months_before_start) &  # 在 start_date 之前至少6个月上线
        (df['availableTo'] >= end_date)  # 在 end_date 时仍未下线
    ]['symbol'].tolist()
    
    return eligible_symbols


# 加载符号列表
# symbols = np.load("/root/workspace/tardis-data/res/symbols-165.npy", allow_pickle=True)
# symbols = pd.read_csv("/root/workspace/tardis-data/res/symbols-162.csv")["symbol"].values

# 定义下载函数
def download_data(symbol, start_date, end_date, folder, timeout=300):
    """
    下载数据的函数，支持超时机制。
    """
    # if symbol in ["BTCUSDT", "ETHUSDT"]:
    #     return f"Skipped {symbol}"

    _t = time.time()
    _cmd = [
        "python", "python/download-aggTrade.py",
        "-t", "um",
        # "-t", "spot",
        "-s", symbol,
        "-startDate", start_date,
        "-endDate", end_date,
        "-folder", folder,
        "-skip-monthly", "1"
    ]
    try:
        # 使用 subprocess.run 执行命令，并设置超时时间
        subprocess.run(_cmd, timeout=timeout, check=True)
        return f"Symbol: {symbol}, Time used: {time.time() - _t:.2f} seconds"
    except subprocess.TimeoutExpired:
        return f"Symbol: {symbol}, Timeout after {timeout} seconds"
    except subprocess.CalledProcessError as e:
        return f"Symbol: {symbol}, Error: {e}"

# 定义带重试机制的下载函数
def download_data_with_retry(symbol, start_date, end_date, folder, retries=3, timeout=300):
    """
    带重试机制的下载函数。
    """
    for attempt in range(retries):
        try:
            result = download_data(symbol, start_date, end_date, folder, timeout=timeout)
            return result
        except Exception as e:
            print(f"Retry {attempt + 1} for {symbol} failed: {e}")
            time.sleep(2)  # 等待 2 秒后重试
    return f"Symbol: {symbol}, Failed after {retries} retries"

# 生成按月的时间区间
def generate_monthly_intervals(start_date, end_date):
    """
    生成从 start_date 到 end_date 的每月时间区间。
    """
    intervals = []
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    while current_date <= end_date:
        # 计算每个月的起始和结束日期
        month_start = current_date
        next_month = month_start + timedelta(days=32)  # 跳到下个月的某一天
        month_end = datetime(next_month.year, next_month.month, 1) - timedelta(days=1)  # 当前月的最后一天

        # 如果 month_end 超过 end_date，则调整为 end_date
        if month_end > end_date:
            month_end = end_date

        # 添加时间区间
        intervals.append((month_start.strftime("%Y-%m-%d"), month_end.strftime("%Y-%m-%d")))

        # 跳到下个月的第一天
        current_date = month_end + timedelta(days=1)

    return intervals

# 主程序
if __name__ == "__main__":
    _start_date = "2021-12-01"
    _end_date = "2025-01-27"
    _folder = "/opt/binance_public_data_zip/"
    max_workers = 1  # 设置并行线程数
    timeout = 300  # 每个任务的超时时间（秒）
    retries = 3  # 每个任务的最大重试次数
    
    df = pd.read_csv("/root/workspace/tardis-data/res/symbol_list.csv")
    print(df)

    # 生成每月的时间区间
    monthly_intervals = generate_monthly_intervals(_start_date, _end_date)
    
    print("Monthly intervals:", monthly_intervals)

    _t_total = time.time()
    
    # 按月循环下载
    for start_date, end_date in monthly_intervals:
        print(f"\nDownloading data for period: {start_date} to {end_date}")
        
        # 获取符合条件的交易对
        # eligible_symbols = get_eligible_symbols(df, start_date, end_date)
        # print("Eligible symbols:", eligible_symbols)
        
        # eligible_symbols.remove("BTCUSDT")
        # eligible_symbols.remove("ETHUSDT")
        
        eligible_symbols = ["BTCUSDT", "ETHUSDT"]
        
        print(start_date, end_date, len(eligible_symbols))
        
        # 使用线程池并行执行任务
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            futures = {
                executor.submit(download_data_with_retry, symbol, start_date, end_date, _folder, retries, timeout): symbol
                for symbol in eligible_symbols
            }

            # 等待任务完成并获取结果
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    result = future.result()  # 获取任务结果
                    print(result)
                except Exception as e:
                    print(f"Symbol: {symbol}, Error: {e}")
        
        # break

    # 按月循环下载
    for _index, (start_date, end_date) in enumerate(monthly_intervals[1:]):
        print(f"\nDownloading data for period: {start_date} to {end_date}")
        
        # 获取符合条件的交易对
        # eligible_symbols = get_eligible_symbols(df, start_date, end_date)
        # print("Eligible symbols:", eligible_symbols)
        
        # eligible_symbols.remove("BTCUSDT")
        # eligible_symbols.remove("ETHUSDT")
        
        eligible_symbols = ["BTCUSDT", "ETHUSDT"]
        
        print(start_date, end_date, len(eligible_symbols))
        
        _start_date, _end_date = monthly_intervals[_index]
        
        # 使用线程池并行执行任务
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            futures = {
                executor.submit(download_data_with_retry, symbol, _start_date, _end_date, _folder, retries, timeout): symbol
                for symbol in eligible_symbols
            }

            # 等待任务完成并获取结果
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    result = future.result()  # 获取任务结果
                    print(result)
                except Exception as e:
                    print(f"Symbol: {symbol}, Error: {e}")
        
        # break

    print("\n\nTotal time used", time.time() - _t_total)
