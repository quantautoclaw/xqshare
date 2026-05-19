#!/usr/bin/env python3
"""测试 xqshare_adapter.py 连接到 192.168.64.2"""
import sys
import os

# 添加 quantnext 项目路径
sys.path.insert(0, '/Users/james/quant/quantnext')

from packages.data.adapters.xqshare_adapter import XqshareAdapter

# 尝试从环境变量获取 secret
xqshare_secret = os.environ.get("XQSHARE_SECRET", os.environ.get("XQSHARE_CLIENT_SECRET", ""))

print(f"当前配置:")
print(f"  Host: 192.168.64.2")
print(f"  Client ID: my-mac")
print(f"  Client Secret: {'已设置' if xqshare_secret else '未设置'}")
print()

try:
    adapter = XqshareAdapter(
        host="192.168.64.2",
        port=18812,
        client_id="my-mac",
        client_secret=xqshare_secret
    )
    print("✓ Adapter 创建成功")
    print(f"  Available: {adapter.is_available()}")
except Exception as e:
    print(f"✗ 连接失败: {e}")
    import traceback
    print(traceback.format_exc())
