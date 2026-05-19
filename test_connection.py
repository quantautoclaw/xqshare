#!/usr/bin/env python3
"""测试 xqshare 连接"""

import sys
import os

# 尝试导入 xqshare
try:
    from xqshare import XtQuantRemote
    print("✓ xqshare 导入成功")
except ImportError as e:
    print(f"✗ xqshare 导入失败: {e}")
    print("  请先安装: pip install -e .")
    sys.exit(1)

# 检查是否有环境变量配置
host = os.environ.get("XQSHARE_REMOTE_HOST", "localhost")
port = os.environ.get("XQSHARE_REMOTE_PORT", "18812")
print(f"\n当前配置:")
print(f"  服务端地址: {host}")
print(f"  服务端端口: {port}")

# 尝试连接
print("\n尝试连接...")
try:
    with XtQuantRemote(host, port, auto_reconnect=False, max_retries=1) as xt:
        print("✓ 连接成功!")
        print(f"  连接状态: {xt}")
        
        # 尝试获取服务状态
        status = xt.get_service_status()
        print(f"  服务状态: {status}")
        
except Exception as e:
    print(f"✗ 连接失败: {e}")
    print(f"\n  可能的原因:")
    print(f"  1. 服务端未启动")
    print(f"  2. 地址/端口配置错误")
    print(f"  3. 网络不通")
    print(f"  4. 防火墙阻止")
    sys.exit(1)
