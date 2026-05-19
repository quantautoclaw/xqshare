#!/usr/bin/env python3
"""测试昨天新增的 xtview 暴露 API 功能"""
import sys
import os

print("="*70)
print("  测试 xtview 暴露 API 功能")
print("="*70)

try:
    from xqshare import XtQuantRemote
    print("✓ 成功导入 xqshare")
except Exception as e:
    print(f"✗ 导入失败: {e}")
    sys.exit(1)

# 尝试连接（需要服务端在 192.168.64.2:18812 上运行）
host = os.environ.get("XQSHARE_REMOTE_HOST", "192.168.64.2")
port = int(os.environ.get("XQSHARE_REMOTE_PORT", "18812"))
client_id = os.environ.get("XQSHARE_CLIENT_ID", "my-mac")
client_secret = os.environ.get("XQSHARE_CLIENT_SECRET", "")

print(f"\n连接到: {host}:{port}")
print(f"客户端ID: {client_id}")
print(f"密钥已设置: {'是' if client_secret else '否'}")

try:
    with XtQuantRemote(host, port, client_id=client_id, client_secret=client_secret) as xt:
        print("✓ 连接成功！")
        
        # 测试 1: xtview 属性是否存在
        print("\n--- 测试 1: xtview 属性 ---")
        try:
            has_xtview = hasattr(xt, 'xtview')
            print(f"✓ xt.xtview 属性存在: {has_xtview}")
            if has_xtview:
                print(f"  类型: {type(xt.xtview)}")
        except Exception as e:
            print(f"✗ 访问 xt.xtview 失败: {e}")
        
        # 测试 2: 检查 xtview 模块的可用属性
        print("\n--- 测试 2: xtview 模块属性 ---")
        try:
            xtview_attrs = dir(xt.xtview)
            print(f"✓ xtview 模块属性数量: {len(xtview_attrs)}")
            print(f"  部分属性: {xtview_attrs[:20]}")
        except Exception as e:
            print(f"✗ 获取 xtview 属性失败: {e}")
        
        # 测试 3: 尝试调用 xtview 的方法（如果有的话）
        print("\n--- 测试 3: xtview 方法调用 ---")
        try:
            # 常见的 xtview 方法/属性
            if hasattr(xt.xtview, 'get_client'):
                print("✓ 发现 get_client() 方法")
                try:
                    client = xt.xtview.get_client()
                    print(f"  客户端对象: {client}")
                except Exception as e:
                    print(f"  ⚠ 调用失败: {e}")
            
            if hasattr(xt.xtview, 'query_schedule_task'):
                print("✓ 发现 query_schedule_task() 方法")
                try:
                    tasks = xt.xtview.query_schedule_task()
                    print(f"  调度任务: {tasks}")
                except Exception as e:
                    print(f"  ⚠ 调用失败: {e}")
        
        except Exception as e:
            print(f"✗ 方法调用测试失败: {e}")
        
        # 测试 4: 服务状态
        print("\n--- 测试 4: 服务状态 ---")
        try:
            status = xt.get_service_status()
            print(f"✓ 服务状态: {status}")
        except Exception as e:
            print(f"✗ 获取服务状态失败: {e}")

except Exception as e:
    print(f"\n✗ 连接失败: {e}")
    import traceback
    print("\n详细错误信息:")
    print(traceback.format_exc())
    sys.exit(1)

print("\n" + "="*70)
print("  测试完成！")
print("="*70)
