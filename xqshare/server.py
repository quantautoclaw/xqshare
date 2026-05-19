"""
XtQuant Share (xqshare) Server - Run on Windows to provide xtquant proxy service
"""

import rpyc
from rpyc.utils.server import ThreadedServer
import time
import os
import ssl
import logging
import functools
import json
import hmac
import hashlib
from datetime import datetime
from typing import Any, Dict, Optional

# 导入权限模块
from .auth import (
    PermissionChecker,
    PermissionError,
    AccountLevel,
    Permission,
    get_permission_checker,
)
from .scheduler import DataDownloadScheduler
from .tools.data_api import DataAPI, call_xtdata

# Import xtquant (only available on Windows)
try:
    import xtquant.xtdata as xtdata
    import xtquant.xttrader as xttrader
    import xtquant.xttype as xttype
    import xtquant.xtconstant as xtconstant
    from xtquant.xttrader import XtQuantTrader
    XTQUANT_AVAILABLE = True
except ImportError:
    XTQUANT_AVAILABLE = False
    xtdata = None
    xttrader = None
    xttype = None
    xtconstant = None
    XtQuantTrader = None

# xtview 模块单独导入（某些版本可能不存在）
try:
    import xtquant.xtview as xtview
    XTVIEW_AVAILABLE = True
except ImportError:
    xtview = None
    XTVIEW_AVAILABLE = False


# ==================== 日志配置 ====================

def setup_logging(log_dir: str = None, log_level: str = "INFO"):
    """配置日志系统"""
    if log_dir is None:
        log_dir = os.environ.get("XQSHARE_LOG_DIR", "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    formatter = logging.Formatter(
        fmt='%(asctime)s.%(msecs)03d | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    root_logger.addHandler(console_handler)
    
    file_handler = logging.FileHandler(
        os.path.join(log_dir, f"xtquant_service_{datetime.now().strftime('%Y%m%d')}.log"),
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)
    
    api_handler = logging.FileHandler(
        os.path.join(log_dir, f"api_calls_{datetime.now().strftime('%Y%m%d')}.log"),
        encoding='utf-8'
    )
    api_handler.setFormatter(formatter)
    api_logger = logging.getLogger('api')
    api_logger.addHandler(api_handler)
    api_logger.setLevel(logging.DEBUG)
    
    return logging.getLogger(__name__)


logger = logging.getLogger(__name__)
api_logger = logging.getLogger('api')


def _init_logging(log_level="INFO"):
    global logger, api_logger
    logger = setup_logging(log_level=log_level)
    api_logger = logging.getLogger('api')


# ==================== 日志装饰器 ====================

def _log_call(name: str, client_info: str, func, *args, **kwargs):
    """通用的 API 调用日志记录函数"""
    try:
        args_str = str(args)[:200] if args else ""
        kwargs_str = str(kwargs)[:200] if kwargs else ""
    except:
        args_str = "<unserializable>"
        kwargs_str = ""

    api_logger.info(f"[CALL] {name} | client={client_info} | args={args_str} | kwargs={kwargs_str}")

    start_time = time.perf_counter()
    try:
        result = func(*args, **kwargs)
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        result_summary = _summarize_result(result)
        api_logger.info(f"[OK] {name} | elapsed={elapsed_ms:.2f}ms | result={result_summary}")
        return result
    except Exception as e:
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        api_logger.error(f"[ERROR] {name} | elapsed={elapsed_ms:.2f}ms | error={type(e).__name__}: {str(e)[:200]}")
        raise


def log_api_call(func_name: str = None):
    """记录 API 调用的装饰器"""
    def decorator(func):
        def wrapper(self, *args, **kwargs):
            name = func_name or func.__name__
            client_info = getattr(self, '_client_info', 'unknown')
            return _log_call(name, client_info, func, self, *args, **kwargs)
        return wrapper
    return decorator


def _summarize_result(result: Any, max_len: int = 200) -> str:
    """生成返回值摘要"""
    try:
        if result is None:
            return "None"
        elif isinstance(result, (int, float, bool, str)):
            s = str(result)
            return s if len(s) <= max_len else s[:max_len] + "..."
        elif isinstance(result, (list, tuple)):
            return f"{type(result).__name__}[len={len(result)}]"
        elif isinstance(result, dict):
            keys = list(result.keys())[:5]
            return f"dict{{{', '.join(map(str, keys))}{'...' if len(result) > 5 else ''}}}"
        elif hasattr(result, '__class__'):
            return f"<{result.__class__.__module__}.{result.__class__.__name__}>"
        else:
            return str(type(result))
    except:
        return "<unserializable>"


# ==================== 异常定义 ====================

class AuthError(Exception):
    """认证错误"""
    pass


class CallbackManager:
    """Compatibility callback registry used by older clients and tests."""

    def __init__(self):
        self._callbacks = {}

    def register(self, callback_id: str, callback, client_info: str = ""):
        self._callbacks[callback_id] = {
            "callback": callback,
            "client_info": client_info,
            "created_at": time.time(),
        }

    def unregister(self, callback_id: str):
        self._callbacks.pop(callback_id, None)

    def invoke(self, callback_id: str, *args, **kwargs) -> bool:
        item = self._callbacks.get(callback_id)
        if item is None:
            return False
        try:
            item["callback"](*args, **kwargs)
            return True
        except Exception:
            self.unregister(callback_id)
            return False

    def list_callbacks(self):
        return list(self._callbacks.keys())

    def clear_client_callbacks(self, client_info: str):
        for callback_id, item in list(self._callbacks.items()):
            if item.get("client_info") == client_info:
                self.unregister(callback_id)


# ==================== 序列化传输优化 ====================

# 需要序列化传输的类型标记
SERIALIZED_MARKER = "__xqshare_serialized__"


def _serialize_for_transfer(result):
    """将结果序列化以优化 RPyC 传输性能

    对于大型列表/字典/DataFrame，序列化后传输比逐元素传输快很多。

    Args:
        result: API 调用返回值

    Returns:
        序列化后的数据结构，包含类型标记和序列化数据
    """
    import io

    if result is None:
        return {SERIALIZED_MARKER: "none", "data": None}

    # DataFrame: 转为 CSV 字符串
    try:
        import pandas as pd
        if isinstance(result, pd.DataFrame):
            csv_str = result.to_csv(index=True)
            return {SERIALIZED_MARKER: "dataframe_csv", "data": csv_str}
    except ImportError:
        pass

    # 字典: 检查是否包含 DataFrame（递归检查）
    if isinstance(result, dict):
        try:
            import pandas as pd

            def has_dataframe_recursive(obj):
                """递归检查对象中是否包含 DataFrame"""
                if isinstance(obj, pd.DataFrame):
                    return True
                if isinstance(obj, dict):
                    return any(has_dataframe_recursive(v) for v in obj.values())
                if isinstance(obj, (list, tuple)):
                    return any(has_dataframe_recursive(item) for item in obj)
                return False

            def serialize_dataframes(obj):
                """递归序列化 DataFrame"""
                if isinstance(obj, pd.DataFrame):
                    return {"__df__": True, "csv": obj.to_csv(index=True)}
                if isinstance(obj, dict):
                    return {k: serialize_dataframes(v) for k, v in obj.items()}
                if isinstance(obj, (list, tuple)):
                    return [serialize_dataframes(item) for item in obj]
                return obj

            if has_dataframe_recursive(result):
                serialized_dict = serialize_dataframes(result)
                json_str = json.dumps(serialized_dict, ensure_ascii=False, default=str)
                return {SERIALIZED_MARKER: "dict_with_dataframe", "data": json_str}
        except ImportError:
            pass

        # 普通字典: JSON 序列化
        try:
            json_str = json.dumps(result, ensure_ascii=False, default=str)
            return {SERIALIZED_MARKER: "json", "data": json_str}
        except (TypeError, ValueError):
            pass

    # 列表: JSON 序列化
    if isinstance(result, (list, tuple)):
        try:
            json_str = json.dumps(result, ensure_ascii=False, default=str)
            return {SERIALIZED_MARKER: "json", "data": json_str}
        except (TypeError, ValueError):
            # 无法 JSON 序列化，检查是否需要包装列表元素
            # 对于包含复杂对象的列表，不进行序列化，让 RPyC 原样传输
            pass

    # 其他类型原样返回
    return result


# ==================== 模块代理（带日志和权限检查） ====================

class LoggingProxy:
    """通用代理：拦截模块/对象的方法调用并记录日志，支持递归包装返回对象和权限检查"""

    def __init__(self, target, target_name: str, client_info_getter, permission_checker=None, account_level=None):
        object.__setattr__(self, '_target', target)
        object.__setattr__(self, '_target_name', target_name)
        object.__setattr__(self, '_get_client_info', client_info_getter)
        object.__setattr__(self, '_permission_checker', permission_checker)
        object.__setattr__(self, '_account_level', account_level)

    def __getattr__(self, name):
        target = object.__getattribute__(self, '_target')
        target_name = object.__getattribute__(self, '_target_name')
        get_client_info = object.__getattribute__(self, '_get_client_info')
        permission_checker = object.__getattribute__(self, '_permission_checker')
        account_level = object.__getattribute__(self, '_account_level')

        attr = getattr(target, name)

        # 如果是可调用对象，包装成带日志和权限检查的版本
        if callable(attr):
            def wrapper(*args, **kwargs):
                full_name = f"{target_name}.{name}"

                # 权限检查
                if permission_checker and account_level:
                    error = permission_checker.check_api_permission(
                        account_level, full_name, args, kwargs
                    )
                    if error:
                        api_logger.warning(f"[权限拒绝] {full_name} | client={get_client_info()} | {error}")
                        raise error

                def invoke(*call_args, **call_kwargs):
                    if target_name == "xtdata":
                        return call_xtdata(attr, *call_args, **call_kwargs)
                    return attr(*call_args, **call_kwargs)

                result = _log_call(full_name, get_client_info(), invoke, *args, **kwargs)

                # 如果返回的是复杂对象（非基本类型），递归包装
                if result is not None and hasattr(result, '__class__'):
                    if not isinstance(result, (int, float, str, bool, list, dict, tuple, type(None), bytes)):
                        if not result.__class__.__module__.startswith('builtins'):
                            return LoggingProxy(result, full_name, get_client_info, permission_checker, account_level)

                # 处理列表：检查是否包含复杂对象
                if isinstance(result, list):
                    wrapped_list = []
                    has_complex_obj = False
                    for item in result:
                        if item is not None and hasattr(item, '__class__'):
                            if not isinstance(item, (int, float, str, bool, dict, tuple, type(None), bytes)):
                                if not item.__class__.__module__.startswith('builtins'):
                                    wrapped_list.append(LoggingProxy(item, full_name, get_client_info, permission_checker, account_level))
                                    has_complex_obj = True
                                    continue
                        wrapped_list.append(item)
                    if has_complex_obj:
                        return wrapped_list

                # 序列化传输优化：将列表/字典/DataFrame 序列化以减少远程调用
                return _serialize_for_transfer(result)
            wrapper.__name__ = name
            return wrapper

        return attr

    def __setattr__(self, name, value):
        return setattr(object.__getattribute__(self, '_target'), name, value)

    def __dir__(self):
        return dir(object.__getattribute__(self, '_target'))

    def __repr__(self):
        return repr(object.__getattribute__(self, '_target'))

# 兼容别名
LoggingModuleProxy = LoggingProxy


# ==================== 服务类 ====================

class XtQuantService(rpyc.Service):
    """完全透明代理服务"""

    AUTH_KEY = os.environ.get("XQSHARE_AUTH_KEY", "xqshare-default-auth-key")
    _xtdata = xtdata
    _xttrader = xttrader
    _xttype = xttype
    _xtconstant = xtconstant
    _xtview = xtview
    _permission_checker = None  # 类级别的权限检查器
    _data_api = None
    _scheduler = None
    _tokens = {}
    _callback_manager = CallbackManager()

    def on_connect(self, conn):
        self._conn = conn
        self._authenticated = False
        self._client_id = None
        self._token = None
        self._account_level = AccountLevel.FREE  # 默认为免费等级
        # 权限检查器在服务启动时已加载
        # 兼容不同版本 rpyc：尝试获取客户端地址
        try:
            if hasattr(conn, 'peer'):
                self._client_info = f"{conn.peer}"
            elif hasattr(conn, '_channel') and hasattr(conn._channel, 'stream'):
                stream = conn._channel.stream
                if hasattr(stream, 'sock'):
                    peer = stream.sock.getpeername()
                    self._client_info = f"{peer[0]}:{peer[1]}"
                else:
                    self._client_info = "unknown"
            else:
                self._client_info = "unknown"
        except Exception:
            self._client_info = "unknown"
        logger.info(f"[连接] 客户端接入: {self._client_info}")

    def on_disconnect(self, conn):
        client_info = getattr(self, '_client_info', 'unknown')
        logger.info(f"[断开] 客户端离开: {client_info}")
        token = getattr(self, "_token", None)
        if token:
            tokens = getattr(self, "_tokens", XtQuantService._tokens)
            tokens.pop(token, None)
            if tokens is not XtQuantService._tokens:
                XtQuantService._tokens.pop(token, None)
        XtQuantService._callback_manager.clear_client_callbacks(client_info)

    def _delayed_disconnect(self, delay: float = 0.5):
        """延迟断开连接，确保异常能传输到客户端"""
        import threading
        def _close():
            try:
                self._conn.close()
            except:
                pass
        threading.Timer(delay, _close).start()

    def _require_auth(self):
        """检查认证状态，未认证则抛出异常并断开连接"""
        if not self._authenticated:
            logger.warning(f"[未授权] 未认证的访问尝试: {self._client_info}")
            self._delayed_disconnect()
            raise AuthError("未授权访问，请先认证")

    def _generate_token(self, client_id: str) -> str:
        timestamp = str(int(time.time()))
        message = f"{client_id}:{timestamp}".encode()
        signature = hmac.new(self.AUTH_KEY.encode(), message, hashlib.sha256).hexdigest()
        return f"{client_id}:{timestamp}:{signature}"

    def _verify_token(self, token: str) -> bool:
        try:
            client_id, timestamp, signature = token.split(":")
            if time.time() - int(timestamp) > 3600:
                return False
            message = f"{client_id}:{timestamp}".encode()
            expected = hmac.new(self.AUTH_KEY.encode(), message, hashlib.sha256).hexdigest()
            return hmac.compare_digest(signature, expected)
        except Exception:
            return False

    def _check_api_permission(self, method: str, args: tuple = (), kwargs: dict = None):
        checker = XtQuantService._permission_checker or get_permission_checker()
        XtQuantService._permission_checker = checker
        error = checker.check_api_permission(self._account_level, method, args, kwargs or {})
        if error:
            logger.warning(f"[权限拒绝] {method} | client={self._client_info} | {error}")
            raise error

    def _get_data_api(self):
        if XtQuantService._data_api is None:
            XtQuantService._data_api = DataAPI(self._xtdata)
        return XtQuantService._data_api

    def _call_data_api(self, method: str, *args, **kwargs):
        self._require_auth()
        self._check_api_permission(method, args, kwargs)
        api = self._get_data_api()
        start_time = time.perf_counter()
        result = getattr(api, method)(*args, **kwargs)
        api_elapsed_ms = (time.perf_counter() - start_time) * 1000

        serialized = _serialize_for_transfer(result)
        total_elapsed_ms = (time.perf_counter() - start_time) * 1000
        marker = serialized.get(SERIALIZED_MARKER) if isinstance(serialized, dict) else None
        api_logger.info(
            f"[DATA_API] {method} | compute={api_elapsed_ms:.2f}ms | total={total_elapsed_ms:.2f}ms | "
            f"marker={marker or 'raw'} | result={_summarize_result(result)}"
        )
        return serialized

    # ==================== 认证接口 ====================

    @log_api_call("authenticate")
    def exposed_authenticate(self, client_id, client_secret):
        checker = XtQuantService._permission_checker
        if checker is None:
            checker = get_permission_checker()
            XtQuantService._permission_checker = checker

        # 检查配置文件是否变更，如果变更则重新加载
        checker.check_and_reload_if_changed()

        # 验证密钥并获取账号等级
        valid, account_level = checker.verify_secret(client_id, client_secret)
        if not valid and getattr(checker, "_use_default_client", False):
            default_secret = os.environ.get("XQSHARE_CLIENT_SECRET")
            if default_secret and client_secret == default_secret:
                valid = True
                account_level = AccountLevel.FREE

        if not valid:
            logger.warning(f"[认证失败] client_id={client_id}")
            self._delayed_disconnect()
            raise AuthError("认证失败：无效的客户端凭证")

        self._authenticated = True
        self._client_id = client_id
        self._account_level = account_level
        self._token = self._generate_token(client_id)
        XtQuantService._tokens[self._token] = {
            "client_id": client_id,
            "level": account_level.value,
            "created_at": time.time(),
        }
        self._client_info = f"{client_id}@{getattr(self, '_client_info', 'unknown')}"
        logger.info(f"[认证成功] client_id={client_id} | level={account_level.value}")
        return {"success": True, "level": account_level.value, "token": self._token}

    @log_api_call("heartbeat")
    def exposed_heartbeat(self):
        return "pong"

    # ==================== 模块代理接口 ====================

    @log_api_call("get_xtdata")
    def exposed_get_xtdata(self):
        self._require_auth()
        return LoggingModuleProxy(
            self._xtdata, 'xtdata',
            lambda: self._client_info,
            XtQuantService._permission_checker,
            self._account_level
        )

    @log_api_call("get_xttype")
    def exposed_get_xttype(self):
        self._require_auth()
        return self._xttype

    def exposed_get_xtconstant(self):
        self._require_auth()
        return self._xtconstant

    @log_api_call("get_xtview")
    def exposed_get_xtview(self):
        self._require_auth()
        if self._xtview is None:
            raise RuntimeError("xtview 模块不可用，请检查 xtquant 版本是否支持")
        return LoggingModuleProxy(
            self._xtview, 'xtview',
            lambda: self._client_info,
            XtQuantService._permission_checker,
            self._account_level
        )

    @log_api_call("create_trader")
    def exposed_create_trader(self, userdata_path: str = None, session_id: int = None):
        """
        创建交易实例（不自动启动，由客户端控制生命周期）

        Args:
            userdata_path: QMT 客户端 userdata_mini 目录路径（可选，可通过环境变量配置）
            session_id: 会话ID（可选，默认自动生成时间戳）

        Returns:
            XtQuantTrader 实例（需客户端调用 start() 和 connect()）
        """
        self._require_auth()
        # 检查 trade 权限
        if self._account_level:
            error = XtQuantService._permission_checker.check_api_permission(
                self._account_level, "create_xttrader"
            )
            if error:
                logger.warning(f"[权限拒绝] create_xttrader | client={self._client_info} | {error}")
                raise error
        if not XTQUANT_AVAILABLE:
            raise RuntimeError("xtquant 库未安装")

        # 从环境变量获取默认值
        if userdata_path is None:
            userdata_path = os.environ.get("QMT_USERDATA_PATH")
        if userdata_path is None:
            raise ValueError("必须提供 userdata_path 参数或设置 QMT_USERDATA_PATH 环境变量")

        # 自动生成 session_id
        if session_id is None:
            session_id = int(time.time())

        # 创建 trader（不自动启动，由客户端控制生命周期）
        trader = XtQuantTrader(userdata_path, session_id)

        logger.info(f"[创建Trader] userdata_path={userdata_path} | session_id={session_id}")

        # 用 LoggingProxy 包装 trader，支持日志记录和序列化
        return LoggingProxy(
            trader, 'xttrader',
            lambda: self._client_info,
            XtQuantService._permission_checker,
            self._account_level
        )

    # ==================== 辅助接口 ====================

    @log_api_call("get_all_stocks")
    def exposed_get_all_stocks(self):
        self._require_auth()
        return call_xtdata(self._xtdata.get_stock_list_in_sector, "沪深A股")

    @log_api_call("get_index_list")
    def exposed_get_index_list(self):
        self._require_auth()
        return call_xtdata(self._xtdata.get_stock_list_in_sector, "沪深指数")

    # ==================== 显式数据接口 ====================

    @log_api_call("get_daily_bars")
    def exposed_get_daily_bars(self, stock_list, start_date, end_date):
        return self._call_data_api("get_daily_bars", stock_list, start_date, end_date)

    @log_api_call("get_minute_bars")
    def exposed_get_minute_bars(self, stock_list, period, start_date, end_date):
        return self._call_data_api("get_minute_bars", stock_list, period, start_date, end_date)

    @log_api_call("get_realtime_quote")
    def exposed_get_realtime_quote(self, stock_list):
        return self._call_data_api("get_realtime_quote", stock_list)

    @log_api_call("get_instruments")
    def exposed_get_instruments(self, stock_list):
        return self._call_data_api("get_instruments", stock_list)

    @log_api_call("get_trading_calendar")
    def exposed_get_trading_calendar(self, start_date, end_date, market: str = "SH"):
        return self._call_data_api("get_trading_calendar", start_date, end_date, market)

    @log_api_call("get_financial_data")
    def exposed_get_financial_data(self, stock_list, table_list, start_date: str = "", end_date: str = "",
                                   report_type: str = "report_time"):
        return self._call_data_api(
            "get_financial_data",
            stock_list,
            table_list,
            start_date,
            end_date,
            report_type,
        )

    @log_api_call("get_etf_info")
    def exposed_get_etf_info(self):
        return self._call_data_api("get_etf_info")

    @log_api_call("get_index_weight")
    def exposed_get_index_weight(self, index_code):
        return self._call_data_api("get_index_weight", index_code)

    @log_api_call("get_yield_curve")
    def exposed_get_yield_curve(self, date):
        return self._call_data_api("get_yield_curve", date)

    @log_api_call("get_suspended_days")
    def exposed_get_suspended_days(self, stock_list, start_date, end_date):
        return self._call_data_api("get_suspended_days", stock_list, start_date, end_date)

    # ==================== 服务端封装接口 ====================

    @log_api_call("download_history_data2")
    def exposed_download_history_data2(self, stock_list: list, period: str = "1d",
                                        start_time: str = "", end_time: str = "", incrementally: bool = None):
        """
        下载历史数据（服务端封装，避免回调传输问题）
        返回: {'finished': n, 'total': n, 'result': {...}}
        """
        self._require_auth()
        self._check_api_permission(
            "download_history_data2",
            (stock_list, period, start_time, end_time),
            {"incrementally": incrementally},
        )
        status = {'finished': 0, 'total': 0, 'done': False, 'result': {}, 'message': ''}

        def on_progress(data):
            status['finished'] = data.get('finished', 0)
            status['total'] = data.get('total', 0)
            status['done'] = status['finished'] >= status['total']
            status['message'] = data.get('message', '')
            if 'result' in data:
                import datetime as dt
                from xtquant import xtbson as bson
                regino_result = bson.BSON.decode(data.get('result'))
                for stock, info in regino_result.items():
                    info['start_time'] = str(dt.datetime.fromtimestamp(info.get('start_time') / 1000))
                    info['end_time'] = str(dt.datetime.fromtimestamp(info.get('end_time') / 1000))
                    status['result'][stock] = info

        # 调用原始方法（incrementally 参数需要转换为 None 或 bool）
        inc = incrementally
        call_xtdata(
            self._xtdata.download_history_data2,
            stock_list, period, start_time, end_time,
            callback=on_progress, incrementally=inc
        )

        return status

    # ==================== 服务状态 ====================

    @log_api_call("get_service_status")
    def exposed_get_service_status(self):
        self._require_auth()
        return {
            "uptime": time.time() - getattr(self, '_start_time', time.time()),
            "client_id": self._client_id,
            "active_tokens": len(XtQuantService._tokens),
            "active_callbacks": len(XtQuantService._callback_manager.list_callbacks()),
            "scheduler_running": bool(
                XtQuantService._scheduler and XtQuantService._scheduler.is_running
            ),
        }

    @log_api_call("ping")
    def exposed_ping(self):
        return "pong"

    @log_api_call("test_async_callback")
    def exposed_test_async_callback(self, callback_func, delay: float = 2.0, count: int = 5):
        """
        测试 RPyC netref 异步回调机制
        :param callback_func: 客户端传递的回调函数（netref）
        :param delay: 每次回调间隔秒数
        :param count: 回调次数
        :return: 立即返回 "已启动"
        """
        self._require_auth()
        # 检查 callback 权限
        if self._account_level:
            error = XtQuantService._permission_checker.check_api_permission(
                self._account_level, "test_async_callback"
            )
            if error:
                logger.warning(f"[权限拒绝] test_async_callback | client={self._client_info} | {error}")
                raise error

        import threading
        import time

        def async_call():
            for i in range(count):
                time.sleep(delay)
                try:
                    result = callback_func(f"异步回调 #{i+1}/{count}，时间: {time.strftime('%H:%M:%S')}")
                    api_logger.info(f"[异步回调] #{i+1} 执行成功，返回: {result}")
                except Exception as e:
                    api_logger.error(f"[异步回调] #{i+1} 执行失败: {e}")

        thread = threading.Thread(target=async_call, daemon=True)
        thread.start()
        return f"已启动异步回调，共 {count} 次，间隔 {delay} 秒"


# ==================== 服务启动 ====================

def create_ssl_context(certfile=None, keyfile=None):
    if not certfile or not keyfile:
        return None
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(certfile, keyfile)
    return ctx


def start_server(host="0.0.0.0", port=None, use_ssl=False, certfile=None, keyfile=None, log_level="INFO", env_file=None):
    """启动服务

    Args:
        host: 监听地址
        port: 监听端口
        use_ssl: 是否启用 SSL
        certfile: SSL 证书文件
        keyfile: SSL 密钥文件
        log_level: 日志级别
        env_file: 环境变量文件路径（None 时自动查找 .env）
    """
    # 加载环境变量文件（None 时自动查找 .env）
    try:
        from dotenv import load_dotenv
        load_dotenv(env_file)
    except ImportError:
        pass

    if port is None:
        port = int(os.environ.get("XQSHARE_PORT", "18812"))

    if not XTQUANT_AVAILABLE:
        print("错误: xtquant 库未安装，请先安装 xtquant")
        return

    _init_logging(log_level)
    XtQuantService._start_time = time.time()

    print("=" * 70)
    print("  XtQuant Share (xqshare) 服务")
    print("=" * 70)
    print(f"  监听地址: {host}:{port}")
    print(f"  SSL 加密: {'启用' if use_ssl else '禁用'}")
    print(f"  日志级别: {log_level}")
    print("=" * 70)
    
    # 预加载权限检查器（加载 clients.yaml 配置）
    if XtQuantService._permission_checker is None:
        XtQuantService._permission_checker = get_permission_checker()

    if XtQuantService._data_api is None:
        XtQuantService._data_api = DataAPI(xtdata)

    scheduler_enabled = os.environ.get("XQSHARE_SCHEDULER_ENABLED", "1").lower() not in ("0", "false", "no")
    scheduler_run_on_start = os.environ.get("XQSHARE_SCHEDULER_RUN_ON_START", "0").lower() in ("1", "true", "yes")
    if scheduler_enabled and XtQuantService._scheduler is None:
        XtQuantService._scheduler = DataDownloadScheduler(
            xtdata,
            data_api=XtQuantService._data_api,
            run_on_start=scheduler_run_on_start,
            logger=logger,
        )
        XtQuantService._scheduler.start()

    logger.info(f"服务启动 | host={host} | port={port} | ssl={use_ssl}")
    
    config = {
        'allow_public_attrs': True,
        'allow_pickle': True,
        'allow_getattr': True,
        'allow_setattr': True,
        'allow_delattr': True,
        'allow_all_attrs': True,
        'sync_request_timeout': 300,
    }
    
    ssl_context = None
    if use_ssl:
        ssl_context = create_ssl_context(certfile, keyfile)
        if ssl_context:
            logger.info("SSL 证书加载成功")
            print("  ✓ SSL 证书加载成功")
        else:
            logger.warning("SSL 证书加载失败")
            print("  ⚠ SSL 证书加载失败")
    
    # 构建 ThreadedServer 参数（兼容不同 rpyc 版本）
    server_kwargs = {
        'hostname': host,
        'port': port,
        'protocol_config': config,
    }
    
    # 尝试使用 ssl_context（新版本 rpyc）
    try:
        server = ThreadedServer(XtQuantService, ssl_context=ssl_context, **server_kwargs)
    except TypeError:
        # 旧版本 rpyc 不支持 ssl_context，使用其他方式
        if ssl_context:
            # 对于旧版本，通过 protocol_config 传递 SSL
            import socket
            import ssl as ssl_module
            
            # 创建 SSL 包装的 socket
            class SSLThreadedServer(ThreadedServer):
                def _accept_method(self, sock):
                    try:
                        return ssl_context.wrap_socket(sock, server_side=True)
                    except Exception as e:
                        logger.error(f"SSL 包装失败: {e}")
                        raise
            
            server = SSLThreadedServer(XtQuantService, **server_kwargs)
            logger.info("使用兼容模式启动 SSL")
        else:
            server = ThreadedServer(XtQuantService, **server_kwargs)
    
    print("\n  服务已启动，等待客户端连接...")
    print("  按 Ctrl+C 停止服务\n")
    
    try:
        server.start()
    except KeyboardInterrupt:
        logger.info("服务停止（用户中断）")
        print("\n  服务已停止")
        server.close()
    except Exception as e:
        logger.error(f"服务异常: {e}")
        raise
    finally:
        if XtQuantService._scheduler is not None:
            XtQuantService._scheduler.stop()
            XtQuantService._scheduler = None


def main():
    """命令行入口函数"""
    import argparse

    parser = argparse.ArgumentParser(
        description="XtQuant Share (xqshare) 服务",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  xqshare-server                    # 使用默认配置启动
  xqshare-server --port 18813       # 指定端口
  xqshare-server --ssl --cert cert.pem --key key.pem  # 启用 SSL

环境变量:
  XQSHARE_PORT      服务端口 (默认: 18812)
  QMT_USERDATA_PATH QMT userdata_mini 目录路径
        """
    )
    parser.add_argument("--host", default="0.0.0.0", help="监听地址 (默认: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=None, help="监听端口 (默认: 18812 或 XQSHARE_PORT)")
    parser.add_argument("--ssl", action="store_true", help="启用 SSL 加密")
    parser.add_argument("--cert", help="SSL 证书文件")
    parser.add_argument("--key", help="SSL 私钥文件")
    parser.add_argument("--log-level", default="INFO", help="日志级别 (默认: INFO)")
    parser.add_argument("--env-file", default=".env", help="环境变量文件 (默认: .env)")

    args = parser.parse_args()

    start_server(
        host=args.host,
        port=args.port,
        use_ssl=args.ssl,
        certfile=args.cert,
        keyfile=args.key,
        log_level=args.log_level,
        env_file=args.env_file
    )


if __name__ == "__main__":
    main()
