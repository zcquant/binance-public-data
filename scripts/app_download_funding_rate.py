#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Funding Rate数据下载、处理和压缩工具

该脚本提供了一个完整的funding rate数据处理流程：
1. 从Tardis.dev下载数据
2. 解压数据文件
3. 将数据处理为5分钟频率
4. 压缩处理后的数据
5. 清理临时文件

主要功能：
- 交互式运行，使用rich美化界面
- 根据availability matrix信息决定每天要下载哪些数据
- 逐日处理，确保数据完整性
- 自动压缩和清理，节省存储空间
- 支持测试模式，可先处理一天的数据验证流程

使用方法：
1. 确保安装依赖：pip install tardis-dev rich pandas
2. 准备availability_matrix.csv文件（在res/目录下）
3. 运行脚本：python app_download_funding_rate.py
4. 按提示输入参数，建议首次使用启用测试模式

数据输出：
- 最终数据保存在指定目录下的final/子目录
- 每天的数据压缩为funding_rate_YYYY-MM-DD.tar.gz文件
- 解压后的数据结构：{symbol}/{timestamp}.csv
- CSV格式：timestamp,funding_rate（5分钟频率）

作者：自动生成
创建日期：2025-06-27
"""

import os
import sys
import gzip
import shutil
import tarfile
import pandas as pd
import multiprocessing as mp
from datetime import datetime, timedelta
from rich.progress import Progress, TextColumn, BarColumn, TimeRemainingColumn, SpinnerColumn, MofNCompleteColumn
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.align import Align
from rich.prompt import Prompt, Confirm

# 尝试导入tardis-dev
try:
    from tardis_dev import datasets
    TARDIS_AVAILABLE = True
except ImportError:
    TARDIS_AVAILABLE = False


def get_available_symbols_for_date(availability_matrix, date_str):
    """
    根据availability_matrix.csv获取指定日期可用的币种列表
    
    Args:
        availability_matrix: pandas DataFrame，可用性矩阵
        date_str: 日期字符串，格式为 'YYYY-MM-DD'
    
    Returns:
        list: 可用的币种列表
    """
    if date_str not in availability_matrix.index:
        return []
    
    # 获取该日期的行数据
    row = availability_matrix.loc[date_str]
    
    # 找出值为1的币种（即有数据的币种）
    available_symbols = []
    for symbol in row.index:
        if row[symbol] == 1:
            available_symbols.append(symbol)
    
    return available_symbols


def download_funding_rate_data(symbols, date_str, download_dir, console, api_key="", concurrency=5):
    """
    下载指定日期的funding rate数据

    Args:
        symbols: 交易对列表
        date_str: 日期字符串，格式为'YYYY-MM-DD'
        download_dir: 下载目录
        console: Rich控制台对象
        api_key: Tardis.dev API密钥
        concurrency: 并发数量

    Returns:
        bool: 是否成功
    """
    if not TARDIS_AVAILABLE:
        raise ImportError("tardis-dev库未安装，请运行: pip install tardis-dev")

    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        to_date = (date_obj + timedelta(days=1)).strftime("%Y-%m-%d")

        # 定义文件名生成函数
        def symbol_based_filename(exchange, data_type, date, symbol, file_format):
            return f"{symbol}/funding_rate/{date.strftime('%Y-%m-%d')}.{file_format}.gz"

        # 记录开始时已存在的文件
        initial_files = set()
        for symbol in symbols:
            target_file = os.path.join(download_dir, symbol, "funding_rate", f"{date_str}.csv.gz")
            if os.path.exists(target_file):
                initial_files.add(symbol)

        if initial_files:
            console.print(f"[yellow]⚠️  发现 {len(initial_files)} 个已存在的文件，将跳过下载[/yellow]")

        # 使用进度条显示下载状态
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeRemainingColumn(),
            console=console
        ) as progress:
            # 创建下载任务
            download_task = progress.add_task(
                f"[cyan]📥 下载 {date_str} 数据 ({len(symbols)} 个交易对)",
                total=len(symbols)
            )

            # 更新初始进度
            progress.update(download_task, completed=len(initial_files))

            # 使用tardis-dev下载（这是阻塞调用）
            datasets.download(
                exchange="binance-futures",
                data_types=["derivative_ticker"],
                symbols=symbols,
                from_date=date_str,
                to_date=to_date,
                format="csv",
                api_key=api_key,
                download_dir=download_dir,
                get_filename=symbol_based_filename,
                concurrency=concurrency
            )

            # 检查下载结果并更新进度
            final_downloaded = set()
            for symbol in symbols:
                target_file = os.path.join(download_dir, symbol, "funding_rate", f"{date_str}.csv.gz")
                if os.path.exists(target_file):
                    final_downloaded.add(symbol)

            # 更新最终进度
            progress.update(download_task, completed=len(final_downloaded))

        # 显示下载结果
        if len(final_downloaded) == len(symbols):
            console.print(f"[green]✅ 下载完成: {len(final_downloaded)}/{len(symbols)} 个文件[/green]")
        else:
            missing = set(symbols) - final_downloaded
            console.print(f"[yellow]⚠️  部分下载完成: {len(final_downloaded)}/{len(symbols)} 个文件[/yellow]")
            console.print(f"[yellow]缺失文件: {', '.join(missing)}[/yellow]")

        return True

    except Exception as e:
        raise Exception(f"下载失败: {e}")


def extract_gz_file(gz_path, extract_to):
    """
    解压.gz文件到指定目录
    
    Args:
        gz_path: .gz文件路径
        extract_to: 解压目标目录
    
    Returns:
        str: 解压后的文件路径
    """
    # 检查目标文件夹是否存在，如果不存在则创建
    if not os.path.exists(extract_to):
        os.makedirs(extract_to)
    
    # 获取解压后的文件名（去掉.gz扩展名）
    base_name = os.path.basename(gz_path)
    if base_name.endswith('.gz'):
        base_name = base_name[:-3]
    
    output_path = os.path.join(extract_to, base_name)
    
    # 解压文件
    with gzip.open(gz_path, 'rb') as f_in:
        with open(output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    
    return output_path


def extract_day_data(date_str, gz_base_path, extract_base_path, symbols, console):
    """
    解压指定日期的所有symbol数据
    
    Args:
        date_str: 日期字符串
        gz_base_path: .gz文件根目录
        extract_base_path: 解压目标根目录
        symbols: 交易对列表
        console: Rich控制台对象
    
    Returns:
        list: 解压后的文件列表
    """
    gz_filename = f"{date_str}.csv.gz"
    extracted_files = []
    
    # 创建进度条
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeRemainingColumn(),
        console=console
    ) as progress:
        task = progress.add_task(f"[cyan]📦 解压 {date_str} 数据", total=len(symbols))

        success_count = 0
        error_count = 0

        for symbol in symbols:
            progress.update(task, description=f"[cyan]📦 解压 {symbol} - {date_str}")

            # 构建.csv.gz文件路径
            gz_path = os.path.join(gz_base_path, symbol, "funding_rate", gz_filename)
            extract_to = os.path.join(extract_base_path, symbol, "funding_rate")

            # 检查.csv.gz文件是否存在
            if os.path.exists(gz_path):
                try:
                    extracted_file = extract_gz_file(gz_path, extract_to)
                    extracted_files.append(extracted_file)
                    success_count += 1
                except Exception as e:
                    error_count += 1
                    console.print(f"[red]❌ 解压失败:[/red] {symbol}/{gz_filename} - {str(e)}")
            else:
                error_count += 1
                console.print(f"[yellow]⚠️  文件不存在:[/yellow] {gz_path}")

            progress.advance(task)
    
    # 显示解压结果摘要
    result_table = Table.grid(padding=1)
    result_table.add_column(style="green", justify="center")
    result_table.add_column(style="red", justify="center")
    result_table.add_row(f"✅ 成功: {success_count}", f"❌ 失败: {error_count}")
    console.print(Panel(Align.center(result_table), title=f"解压结果 - {date_str}", border_style="green" if error_count == 0 else "yellow"))
    
    return extracted_files


# 导入现有的转换函数
from convert_tardis_to_markprice_format import convert_tardis_to_markprice_format


def convert_day_data(date_str, extract_base_path, output_base_path, symbols, console):
    """
    转换指定日期的数据，使用现有的转换函数

    Args:
        date_str: 日期字符串
        extract_base_path: 解压数据根目录
        output_base_path: 转换输出根目录
        symbols: 交易对列表
        console: Rich控制台对象

    Returns:
        dict: 转换结果统计
    """
    console.print(f"[cyan]🔄 使用现有转换函数处理 {date_str} 的数据...[/cyan]")

    try:
        # 直接调用现有的转换函数
        convert_tardis_to_markprice_format(extract_base_path, output_base_path)

        # 检查转换结果
        success_count = 0
        failed_count = 0

        for symbol in symbols:
            symbol_output_dir = os.path.join(output_base_path, symbol)
            if os.path.exists(symbol_output_dir):
                # 检查是否有对应日期的输出文件
                csv_files = [f for f in os.listdir(symbol_output_dir) if f.endswith('.csv')]
                if csv_files:
                    success_count += 1
                else:
                    failed_count += 1
            else:
                failed_count += 1

        # 显示转换结果摘要
        result_table = Table.grid(padding=1)
        result_table.add_column(style="green", justify="center")
        result_table.add_column(style="red", justify="center")
        result_table.add_row(f"✅ 成功: {success_count}", f"❌ 失败: {failed_count}")
        console.print(Panel(Align.center(result_table), title=f"转换结果 - {date_str}", border_style="green" if failed_count == 0 else "yellow"))

        return {'success': success_count, 'failed': failed_count, 'skipped': 0}

    except Exception as e:
        console.print(f"[red]❌ 转换失败: {e}[/red]")
        return {'success': 0, 'failed': len(symbols), 'skipped': 0}


def compress_day_data(date_str, output_base_path, final_dir, console):
    """
    压缩指定日期的处理后数据

    Args:
        date_str: 日期字符串
        output_base_path: 处理后数据根目录
        final_dir: 最终压缩文件存放目录
        console: Rich控制台对象

    Returns:
        bool: 是否成功
    """
    try:
        # 创建最终目录
        os.makedirs(final_dir, exist_ok=True)

        # 创建tar.gz文件
        tar_file = os.path.join(final_dir, f"funding_rate_{date_str}.tar.gz")

        # 计算该日期对应的时间戳
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        end_time = date_obj.replace(hour=8, minute=0, second=0, microsecond=0) + timedelta(days=1)
        target_timestamp = int(end_time.timestamp() * 1000)

        compressed_files = 0

        with tarfile.open(tar_file, 'w:gz') as tar:
            # 遍历所有symbol目录
            for symbol_dir in os.listdir(output_base_path):
                symbol_path = os.path.join(output_base_path, symbol_dir)
                if not os.path.isdir(symbol_path):
                    continue

                # 查找该日期对应的文件
                target_file = os.path.join(symbol_path, f"{target_timestamp}.csv")

                if os.path.exists(target_file):
                    # 添加到tar文件，保持目录结构
                    arcname = f"{symbol_dir}/{target_timestamp}.csv"
                    tar.add(target_file, arcname=arcname)
                    compressed_files += 1

        if compressed_files > 0:
            console.print(f"[green]✅ 压缩完成:[/green] {tar_file} (包含 {compressed_files} 个文件)")
            return True
        else:
            console.print(f"[yellow]⚠️  没有找到 {date_str} 的数据文件进行压缩[/yellow]")
            return False

    except Exception as e:
        console.print(f"[red]❌ 压缩失败:[/red] {date_str} - {str(e)}")
        return False


def cleanup_data(date_str, gz_base_path, extract_base_path, output_base_path, symbols, console):
    """
    清理指定日期的临时数据

    Args:
        date_str: 日期字符串
        gz_base_path: 原始.gz文件根目录
        extract_base_path: 解压临时目录
        output_base_path: 处理后数据目录
        symbols: 交易对列表
        console: Rich控制台对象

    Returns:
        dict: 清理结果统计
    """
    # 计算该日期对应的时间戳
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    end_time = date_obj.replace(hour=8, minute=0, second=0, microsecond=0) + timedelta(days=1)
    target_timestamp = int(end_time.timestamp() * 1000)

    # 计算总的清理任务数（每个symbol有3个文件要清理）
    total_tasks = len(symbols) * 3

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeRemainingColumn(),
        console=console
    ) as progress:
        task = progress.add_task(f"[yellow]🧹 清理 {date_str} 临时文件", total=total_tasks)

        success_count = 0
        error_count = 0

        # 清理原始.gz文件
        for symbol in symbols:
            progress.update(task, description=f"[yellow]🧹 清理 {symbol} 原始文件")
            gz_file = os.path.join(gz_base_path, symbol, "funding_rate", f"{date_str}.csv.gz")
            if os.path.exists(gz_file):
                try:
                    os.remove(gz_file)
                    success_count += 1
                except Exception as e:
                    error_count += 1
            progress.advance(task)

        # 清理解压后的文件
        for symbol in symbols:
            progress.update(task, description=f"[yellow]🧹 清理 {symbol} 解压文件")
            csv_file = os.path.join(extract_base_path, symbol, "funding_rate", f"{date_str}.csv")
            if os.path.exists(csv_file):
                try:
                    os.remove(csv_file)
                    success_count += 1
                except Exception as e:
                    error_count += 1
            progress.advance(task)

        # 清理处理后的文件
        for symbol in symbols:
            progress.update(task, description=f"[yellow]🧹 清理 {symbol} 处理文件")
            processed_file = os.path.join(output_base_path, symbol, f"{target_timestamp}.csv")
            if os.path.exists(processed_file):
                try:
                    os.remove(processed_file)
                    success_count += 1
                except Exception as e:
                    error_count += 1
            progress.advance(task)

        # 清理空目录（不计入进度）
        for base_path in [gz_base_path, extract_base_path, output_base_path]:
            for symbol in symbols:
                symbol_funding_dir = os.path.join(base_path, symbol, "funding_rate")
                symbol_dir = os.path.join(base_path, symbol)

                # 删除空的funding_rate目录
                if os.path.exists(symbol_funding_dir) and not os.listdir(symbol_funding_dir):
                    try:
                        os.rmdir(symbol_funding_dir)
                    except Exception:
                        pass

                # 删除空的symbol目录
                if os.path.exists(symbol_dir) and not os.listdir(symbol_dir):
                    try:
                        os.rmdir(symbol_dir)
                    except Exception:
                        pass

    # 显示清理结果
    result_table = Table.grid(padding=1)
    result_table.add_column(style="green", justify="center")
    result_table.add_column(style="red", justify="center")
    result_table.add_row(f"🗑️  清理: {success_count}", f"❌ 失败: {error_count}")
    console.print(Panel(Align.center(result_table), title=f"清理结果 - {date_str}", border_style="green" if error_count == 0 else "yellow"))

    return {'success': success_count, 'failed': error_count}


def process_single_date(date_str, availability_matrix, base_paths, console, api_key="", concurrency=5):
    """
    处理单个日期的完整流程

    Args:
        date_str: 日期字符串
        availability_matrix: 可用性矩阵
        base_paths: 包含各种路径的字典
        console: Rich控制台对象
        api_key: Tardis.dev API密钥
        concurrency: 下载并发数

    Returns:
        bool: 是否成功
    """
    # 获取该日期可用的交易对
    symbols = get_available_symbols_for_date(availability_matrix, date_str)
    if not symbols:
        console.print(f"[yellow]⚠️  跳过 {date_str}：没有可用的交易对[/yellow]")
        return False

    console.print(f"\n[bold blue]🗓️  处理日期: {date_str} (包含 {len(symbols)} 个交易对)[/bold blue]")

    try:
        # 步骤1：下载数据
        console.print(f"[cyan]📥 步骤1: 下载 {date_str} 的数据...[/cyan]")
        download_funding_rate_data(symbols, date_str, base_paths['download'], console, api_key, concurrency)

        # 步骤2：解压数据
        console.print(f"[cyan]📦 步骤2: 解压 {date_str} 的数据...[/cyan]")
        extracted_files = extract_day_data(date_str, base_paths['download'], base_paths['extract'], symbols, console)
        if not extracted_files:
            console.print(f"[red]❌ 解压失败，跳过后续步骤[/red]")
            return False

        # 步骤3：转换数据
        console.print(f"[cyan]🔄 步骤3: 转换 {date_str} 的数据...[/cyan]")
        convert_result = convert_day_data(date_str, base_paths['extract'], base_paths['output'], symbols, console)
        if convert_result['success'] == 0:
            console.print(f"[red]❌ 转换失败，跳过后续步骤[/red]")
            return False

        # 步骤4：压缩数据
        console.print(f"[cyan]📦 步骤4: 压缩 {date_str} 的数据...[/cyan]")
        if not compress_day_data(date_str, base_paths['output'], base_paths['final'], console):
            console.print(f"[yellow]⚠️  压缩失败，但继续清理[/yellow]")

        # 步骤5：清理临时数据
        console.print(f"[cyan]🧹 步骤5: 清理 {date_str} 的临时数据...[/cyan]")
        cleanup_data(date_str, base_paths['download'], base_paths['extract'], base_paths['output'], symbols, console)

        console.print(f"[green]✅ 成功完成 {date_str} 的处理[/green]")
        return True

    except Exception as e:
        console.print(f"[red]❌ 处理 {date_str} 时发生错误: {e}[/red]")
        return False


def get_user_input(console):
    """
    获取用户输入参数

    Args:
        console: Rich控制台对象

    Returns:
        dict: 用户输入的参数
    """
    console.print("\n[bold yellow]📝 请输入处理参数[/bold yellow]")

    start_date = Prompt.ask(
        "[cyan]请输入开始日期[/cyan]",
        default="2025-05-01"
    )

    end_date = Prompt.ask(
        "[cyan]请输入结束日期[/cyan]",
        default="2025-06-22"
    )

    base_path = Prompt.ask(
        "[cyan]请输入数据存储基础路径[/cyan]",
        default="/mnt/ssd1/funding_rate_data"
    )

    api_key = Prompt.ask(
        "[cyan]请输入 Tardis.dev API 密钥（可选）[/cyan]",
        default="",
        password=True
    )

    concurrency = int(Prompt.ask(
        "[cyan]请输入下载并发数量[/cyan]",
        default="5"
    ))

    max_workers = int(Prompt.ask(
        "[cyan]请输入转换并行进程数[/cyan]",
        default=str(min(mp.cpu_count(), 8))
    ))

    test_mode = Confirm.ask(
        "[cyan]是否启用测试模式（仅处理一天的数据）[/cyan]",
        default=False
    )

    return {
        'start_date': start_date,
        'end_date': end_date if not test_mode else start_date,
        'base_path': base_path,
        'api_key': api_key,
        'concurrency': concurrency,
        'max_workers': max_workers,
        'test_mode': test_mode
    }


def main():
    """
    主函数
    """
    # 创建控制台对象
    console = Console()

    # 显示启动信息
    startup_table = Table.grid(padding=1)
    startup_table.add_column(style="bold cyan", justify="center")
    startup_table.add_row("🚀 Funding Rate数据下载处理工具")
    startup_table.add_row("📥 下载 → 📦 解压 → 🔄 转换 → 📦 压缩 → 🧹 清理")
    console.print(Panel(Align.center(startup_table), title="启动", border_style="bold cyan"))

    # 检查依赖
    if not TARDIS_AVAILABLE:
        console.print(Panel("[bold red]❌ 错误：未安装 tardis-dev 库\n请运行: pip install tardis-dev[/bold red]", border_style="red"))
        sys.exit(1)

    try:
        # 获取用户输入
        params = get_user_input(console)

        # 解析日期
        start_date = datetime.strptime(params['start_date'], '%Y-%m-%d')
        end_date = datetime.strptime(params['end_date'], '%Y-%m-%d')

        # 验证日期范围
        if start_date > end_date:
            console.print(Panel("[bold red]❌ 错误：开始日期不能晚于结束日期[/bold red]", border_style="red"))
            sys.exit(1)

        # 设置路径
        base_paths = {
            'download': os.path.join(params['base_path'], 'download'),
            'extract': os.path.join(params['base_path'], 'extract'),
            'output': os.path.join(params['base_path'], 'output'),
            'final': os.path.join(params['base_path'], 'final')
        }

        # 创建目录
        for path in base_paths.values():
            os.makedirs(path, exist_ok=True)

        # 读取可用性矩阵
        availability_matrix_path = "res/availability_matrix.csv"
        if not os.path.exists(availability_matrix_path):
            console.print(Panel(f"[bold red]❌ 错误：找不到可用性矩阵文件 {availability_matrix_path}[/bold red]", border_style="red"))
            sys.exit(1)

        availability_matrix = pd.read_csv(availability_matrix_path, index_col=0)
        console.print(f"[green]✅ 成功加载可用性矩阵，包含 {len(availability_matrix)} 天的数据[/green]")

        # 显示配置信息
        config_table = Table(show_header=True, header_style="bold magenta")
        config_table.add_column("配置项", style="cyan", width=20)
        config_table.add_column("值", style="white")
        config_table.add_row("📅 开始日期", params['start_date'])
        config_table.add_row("📅 结束日期", params['end_date'])
        config_table.add_row("📁 数据根目录", params['base_path'])
        config_table.add_row("📊 处理天数", str((end_date - start_date).days + 1))
        config_table.add_row("⚡ 下载并发数", str(params['concurrency']))
        config_table.add_row("⚡ 转换进程数", str(params['max_workers']))
        config_table.add_row("🧪 测试模式", "是" if params['test_mode'] else "否")
        console.print(Panel(config_table, title="⚙️  配置信息", border_style="magenta"))

        # 确认开始处理
        if not Confirm.ask(f"\n[yellow]确认开始处理吗？[/yellow]"):
            console.print("[blue]👋 已取消处理[/blue]")
            sys.exit(0)

        # 开始处理
        current_date = start_date
        total_days = (end_date - start_date).days + 1
        successful_days = 0
        failed_days = []

        # 创建总体进度显示
        main_table = Table.grid(padding=1)
        main_table.add_column(style="bold cyan", justify="center")
        main_table.add_row(f"📅 处理日期范围: {start_date.strftime('%Y-%m-%d')} 到 {end_date.strftime('%Y-%m-%d')}")
        main_table.add_row(f"📊 总共需要处理 {total_days} 天的数据")
        console.print(Panel(Align.center(main_table), title="🚀 开始批量处理", border_style="bold cyan"))

        # 逐日处理
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')

            console.print(f"\n[bold cyan]📅 处理进度: {successful_days + len(failed_days) + 1}/{total_days} - {date_str}[/bold cyan]")

            if process_single_date(date_str, availability_matrix, base_paths, console,
                                 params['api_key'], params['concurrency']):
                successful_days += 1
                console.print(f"[green]✅ {date_str} 处理完成[/green]")
            else:
                failed_days.append(date_str)
                console.print(f"[red]❌ {date_str} 处理失败[/red]")

            current_date += timedelta(days=1)

        # 显示最终结果
        final_table = Table.grid(padding=1)
        final_table.add_column(style="bold green", justify="center")
        final_table.add_row("🎉 全部处理完成！")
        final_table.add_row(f"📊 成功处理: {successful_days}/{total_days} 天")
        if failed_days:
            final_table.add_row(f"❌ 失败日期: {', '.join(failed_days)}")
        console.print(Panel(Align.center(final_table), title="🏆 任务完成", border_style="bold green"))

        sys.exit(0 if len(failed_days) == 0 else 1)

    except KeyboardInterrupt:
        console.print(Panel("[bold yellow]⚠️  用户中断处理[/bold yellow]", border_style="yellow"))
        sys.exit(1)
    except Exception as e:
        console.print(Panel(f"[bold red]❌ 处理过程中发生错误: {str(e)}[/bold red]", border_style="red"))
        sys.exit(1)


if __name__ == "__main__":
    main()
