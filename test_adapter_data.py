#!/usr/bin/env python3
"""测试 xqshare_adapter.py 获取数据"""
import sys
import os
from datetime import datetime, timedelta

# 添加 quantnext 项目路径
sys.path.insert(0, '/Users/james/quant/quantnext')

from packages.data.adapters.xqshare_adapter import XqshareAdapter
from packages.data.adapters.base import BarQuery, Frequency

# 获取适配器
adapter = XqshareAdapter(
    host="192.168.64.2",
    port=18812,
    client_id="my-mac",
    client_secret=os.environ.get("XQSHARE_SECRET", "")
)

print("✓ Adapter 连接成功")
print(f"  Available: {adapter.is_available()}")
print()

# 1. 测试获取实时行情
print("--- 1. 获取实时行情 ---")
try:
    realtime = adapter.load_realtime("600519.SSE")
    if realtime:
        print(f"  股票名称: {realtime.name}")
        print(f"  最新价格: {realtime.last_price}")
        print(f"  涨跌: {realtime.change} ({realtime.chg_ratio}%)")
        print(f"  成交量: {realtime.volume}")
    else:
        print("  暂无实时行情数据")
except Exception as e:
    print(f"✗ 获取实时行情失败: {e}")

# 2. 测试获取历史K线
print("\n--- 2. 获取历史K线 ---")
try:
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    query = BarQuery(
        symbol="600519.SSE",
        frequency=Frequency.DAILY,
        start=start_date,
        end=end_date
    )
    bars = adapter.load_bars(query)
    print(f"  共获取 {len(bars)} 条K线数据")
    if bars:
        print(f"  最近一条: {bars[-1].datetime}, 收盘价={bars[-1].close}, 成交量={bars[-1].volume}")
except Exception as e:
    print(f"✗ 获取历史K线失败: {e}")

# 3. 测试获取交易日历
print("\n--- 3. 获取交易日历 ---")
try:
    dates = adapter.load_trading_dates("SH", "20260101", "20261231")
    print(f"  共获取 {len(dates)} 个交易日")
    if dates:
        print(f"  最近的交易日: {dates[-1]}")
except Exception as e:
    print(f"✗ 获取交易日历失败: {e}")

print("\n--- 测试完成 ---")
