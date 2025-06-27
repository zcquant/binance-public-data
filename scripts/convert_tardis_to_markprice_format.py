#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Tardis资金费率数据转换为币安标记价格格式脚本

该脚本用于将Tardis格式的资金费率数据转换为币安标记价格数据格式，
包括重采样到1小时频率、数据完整性检查和错误报告功能。
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import calendar
from rich.progress import Progress, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
import sys
import glob

def convert_tardis_to_markprice_format(input_base_path, output_base_path):
    """
    将Tardis格式的资金费率数据转换为币安标记价格数据格式
    
    :param input_base_path: 输入数据根目录，例如：/mnt/ssd1/tardis_public_data_unzip
    :type input_base_path: str
    :param output_base_path: 输出数据根目录，例如：/mnt/ssd1/binance_public_data/futures/um/mark_price_data/daily/markPrice
    :type output_base_path: str
    """
    console = Console()
    
    # 获取所有交易对目录
    try:
        symbols = []
        for item in os.listdir(input_base_path):
            symbol_path = os.path.join(input_base_path, item)
            if os.path.isdir(symbol_path):
                funding_rate_path = os.path.join(symbol_path, "funding_rate")
                if os.path.exists(funding_rate_path):
                    symbols.append(item)
        symbols = sorted(symbols)
    except FileNotFoundError:
        console.print(Panel(f"[bold red]错误：找不到目录 {input_base_path}[/bold red]"))
        return
    
    # 预期每天的行数（24小时）
    expected_rows = 24
    
    # 记录问题文件
    problem_files = []
    processed_files = 0
    total_files = 0
    
    # 计算总文件数
    for symbol in symbols:
        funding_rate_path = os.path.join(input_base_path, symbol, "funding_rate")
        if os.path.exists(funding_rate_path):
            csv_files = glob.glob(os.path.join(funding_rate_path, "*.csv"))
            total_files += len(csv_files)
    
    # 创建进度条
    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeRemainingColumn()
    ) as progress:
        task = progress.add_task("[cyan]转换Tardis数据到标记价格格式...", total=total_files)
        
        # 遍历所有交易对
        for symbol in symbols:
            funding_rate_path = os.path.join(input_base_path, symbol, "funding_rate")
            if not os.path.exists(funding_rate_path):
                continue
            
            # 创建输出目录
            output_dir = os.path.join(output_base_path, symbol)
            os.makedirs(output_dir, exist_ok=True)
            
            # 遍历所有CSV文件
            csv_files = sorted(glob.glob(os.path.join(funding_rate_path, "*.csv")))
            for input_file in csv_files:
                file_name = os.path.basename(input_file)
                progress.update(task, description=f"[cyan]处理 {symbol}/{file_name}")
                
                try:
                    # 读取输入文件
                    df = pd.read_csv(input_file)
                    
                    # 检查必要的列是否存在
                    required_columns = ['timestamp', 'funding_rate', 'mark_price', 'index_price', 'funding_timestamp']
                    missing_columns = [col for col in required_columns if col not in df.columns]
                    if missing_columns:
                        problem_files.append(f"{input_file} (缺少列: {missing_columns})")
                        progress.advance(task)
                        continue
                    
                    # 从文件名中提取日期 (格式: 2025-05-01.csv)
                    date_str = file_name.replace('.csv', '')
                    try:
                        input_date = datetime.strptime(date_str, '%Y-%m-%d')
                    except ValueError:
                        problem_files.append(f"{input_file} (无法解析日期格式)")
                        progress.advance(task)
                        continue
                    
                    # 将timestamp转换为datetime (假设是微秒时间戳)
                    df['datetime'] = pd.to_datetime(df['timestamp'], unit='us')
                    
                    # 设置datetime为索引
                    df.set_index('datetime', inplace=True)
                    
                    # 重采样到小时频率，取最后的值
                    resampled_df = df.resample('h').last()
                    
                    # 使用前值填充缺失值
                    resampled_df['funding_rate'] = resampled_df['funding_rate'].ffill()
                    resampled_df['mark_price'] = resampled_df['mark_price'].ffill()
                    resampled_df['index_price'] = resampled_df['index_price'].ffill()
                    resampled_df['funding_timestamp'] = resampled_df['funding_timestamp'].ffill()

                    # 如果最前面的值缺失，使用后值填充
                    resampled_df['funding_rate'] = resampled_df['funding_rate'].bfill()
                    resampled_df['mark_price'] = resampled_df['mark_price'].bfill()
                    resampled_df['index_price'] = resampled_df['index_price'].bfill()
                    resampled_df['funding_timestamp'] = resampled_df['funding_timestamp'].bfill()
                    
                    # 重置索引
                    resampled_df.reset_index(inplace=True)
                    
                    # 筛选当天的数据 (UTC时间)
                    day_start = input_date.replace(hour=0, minute=0, second=0, microsecond=0)
                    day_end = day_start + timedelta(days=1)
                    
                    day_data = resampled_df[
                        (resampled_df['datetime'] >= day_start) & 
                        (resampled_df['datetime'] < day_end)
                    ].copy()
                    
                    # 检查行数
                    if len(day_data) != expected_rows:
                        problem_files.append(f"{input_file} (行数: {len(day_data)}, 预期: {expected_rows})")
                        # 如果数据不足24行，尝试补全
                        if len(day_data) > 0:
                            # 创建完整的24小时时间序列
                            full_hours = pd.date_range(start=day_start, end=day_end, freq='h', inclusive='left')
                            full_df = pd.DataFrame({'datetime': full_hours})
                            
                            # 合并数据，使用前值填充
                            day_data = full_df.merge(day_data, on='datetime', how='left')
                            day_data['funding_rate'] = day_data['funding_rate'].ffill().bfill()
                            day_data['mark_price'] = day_data['mark_price'].ffill().bfill()
                            day_data['index_price'] = day_data['index_price'].ffill().bfill()
                            day_data['funding_timestamp'] = day_data['funding_timestamp'].ffill().bfill()
                        else:
                            progress.advance(task)
                            continue
                    
                    # 创建输出DataFrame
                    df_out = pd.DataFrame()
                    df_out['timestamp'] = (day_data['datetime'].astype(int) // 10**6).astype(int)  # 转换为毫秒时间戳
                    df_out['mark_price'] = day_data['mark_price'].round(8)
                    df_out['index_price'] = day_data['index_price'].round(8)
                    df_out['last_funding_rate'] = day_data['funding_rate'].round(8)

                    # 使用输入数据中的funding_timestamp作为next_funding_time
                    # 将微秒时间戳转换为毫秒时间戳
                    df_out['next_funding_time'] = (day_data['funding_timestamp'] // 1000).astype(int)
                    
                    # 计算输出文件名（第二天的0:00 UTC+0的时间戳）
                    # input_date是当天的日期，我们需要第二天的0:00 UTC+0
                    next_day_utc = input_date + timedelta(days=1)
                    # 确保是UTC时间的0:00:00
                    next_day_utc = next_day_utc.replace(hour=0, minute=0, second=0, microsecond=0)
                    # 使用UTC时间戳计算，避免本地时区影响
                    import calendar
                    output_timestamp = int(calendar.timegm(next_day_utc.timetuple()) * 1000)
                    output_file = os.path.join(output_dir, f"{output_timestamp}.csv")
                    
                    # 保存输出文件
                    df_out.to_csv(output_file, index=False)
                    
                    processed_files += 1
                    progress.advance(task)
                except Exception as e:
                    problem_files.append(f"{input_file} (错误: {str(e)})")
                    progress.advance(task)
    
    # 报告结果
    if problem_files:
        console.print(Panel(Text.from_markup(
            f"[bold yellow]转换完成，但有 {len(problem_files)} 个文件存在问题:[/bold yellow]\n" + 
            "\n".join(f"[red]- {f}[/red]" for f in problem_files[:10]) + 
            (f"\n[red]...以及其他 {len(problem_files) - 10} 个文件[/red]" if len(problem_files) > 10 else "")
        )))
    else:
        console.print(Panel(f"[bold green]全部 {processed_files} 个文件转换成功！[/bold green]"))

if __name__ == "__main__":
    if len(sys.argv) > 1:
        input_base_path = sys.argv[1]
    else:
        input_base_path = "/mnt/ssd1/tardis_public_data_unzip"
    
    if len(sys.argv) > 2:
        output_base_path = sys.argv[2]
    else:
        output_base_path = "/mnt/ssd1/binance_public_data/futures/um/mark_price_data/daily/markPrice"
    
    convert_tardis_to_markprice_format(input_base_path, output_base_path)
