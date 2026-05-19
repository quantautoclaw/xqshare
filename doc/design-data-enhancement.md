# xqshare 数据增强方案

## 一、目标

1. **明确 API** — 暴露统一数据接口，而非透明代理
2. **智能下载** — 启动时自动预下载，增量更新
3. **补充元数据** — yield_curve、suspended_days 等 rqalpha bundle 中的数据

## 二、数据源优先级

```
xtquant > rqalpha bundle > tushare pro
```

| 数据项 | 来源 | 说明 |
|-------|------|------|
| 日线 K 线 | xtquant | `download_history_data2` |
| 实时行情 | xtquant | `get_market_data` |
| 板块/ETF/指数 | xtquant | `get_stock_list_in_sector` |
| instruments | xtquant | `get_instrument_detail` |
| trading_dates | xtquant | `get_trading_calendar` |
| yield_curve | rqalpha bundle | 数据稳定，更新频率低 |
| suspended_days | rqalpha bundle | 停牌信息 |
| split_factor | xtquant | 前复权因子已隐含 |

## 三、文件结构

```
xqshare/
├── __init__.py
├── client.py              # 现有：RPyC 客户端
├── server.py              # 现有：RPyC 服务端
├── auth.py                # 现有：权限系统
├── scheduler.py           # 新增：自动预下载调度器
├── metadata/              # 新增：补充元数据
│   ├── yield_curve.h5
│   └── suspended_days.h5
└── tools/
    ├── xtdata.py          # 现有：xtdata 工具函数
    ├── xttrader.py        # 现有：xttrader 工具函数
    └── data_api.py        # 新增：统一数据接口层
```

## 四、API 明确化（data_api.py）

### 4.1 新增接口

```python
# 行情数据
def exposed_get_daily_bars(stock_list, start_date, end_date) -> DataFrame
def exposed_get_minute_bars(stock_list, period, start_date, end_date) -> DataFrame
def exposed_get_realtime_quote(stock_list) -> DataFrame

# 合约信息
def exposed_get_instruments(stock_list) -> DataFrame

# 日历
def exposed_get_trading_calendar(start_date, end_date) -> List[datetime]

# 财务数据
def exposed_get_financial_data(stock_list, table_list) -> DataFrame

# ETF / 指数
def exposed_get_etf_info() -> DataFrame
def exposed_get_index_weight(index_code) -> DataFrame

# 元数据补充
def exposed_get_yield_curve(date) -> DataFrame
def exposed_get_suspended_days(stock_list, start_date, end_date) -> DataFrame
```

### 4.2 现有接口保留（向后兼容）

```python
def exposed_get_stock_list_in_sector(sector_name) -> List[str]
def exposed_download_history_data2(...) -> dict
def exposed_get_xtdata() -> RemoteModule
```

### 4.3 权限映射

| 接口 | 权限级别 |
|-----|---------|
| `get_daily_bars` | BASIC |
| `get_instruments` | BASIC |
| `get_trading_calendar` | BASIC |
| `get_yield_curve` | STANDARD |
| `get_suspended_days` | STANDARD |

## 五、Scheduler 调度器

### 5.1 触发时机

- 服务启动时执行一次（`_lifespan`）
- 每 24 小时定时循环

### 5.2 下载任务

```python
DAILY_TASKS = [
    ("板块成分", get_stock_list_in_sector + 本地缓存探测),
    ("ETF 申购赎回清单", download_etf_info),
    ("指数成分股权重", download_index_weight),
    ("交易日历", get_trading_calendar),
    ("财务数据增量", download_financial_incremental),
    ("K线增量", download_history_data_incremental),
]
```

### 5.3 并发保护

xtdata 调用统一通过 `asyncio.Lock` 串行化（参考 qmt-bridge 模式），避免 xtquant C 扩展线程安全问题。

## 六、补充元数据导入

### 6.1 来源

rqalpha bundle（已有，月度更新至 2026-05-01）

### 6.2 导入方式

首次启动时检测 `metadata/` 目录，不存在则自动从 rqalpha bundle 导入。

### 6.3 存储

| 数据 | 格式 | 存储位置 |
|-----|------|---------|
| yield_curve | HDF5 | `metadata/yield_curve.h5` |
| suspended_days | HDF5 | `metadata/suspended_days.h5` |

## 七、实施顺序

```
Phase 1：API 接口层
  ├── 定义 data_api.py 接口
  ├── server.py 接入新接口
  └── client.py 添加同步包装

Phase 2：Scheduler
  ├── 实现 scheduler.py
  ├── 接入 server.py 生命周期
  └── 配置每日任务

Phase 3：元数据补充
  ├── 创建 metadata/ 目录
  ├── 编写 yield_curve/suspended_days 导入脚本
  └── data_api.py 接入查询
```

## 八、注意事项

1. **HDF5 不适合增量写入** — 保持 xtquant 原生缓存格式，不做格式转换
2. **并发安全** — xtdata 调用必须串行化
3. **向后兼容** — 保留现有透明代理接口