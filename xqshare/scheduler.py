"""
Background data prefetch scheduler.

xtquant's C extensions are not assumed to be thread-safe, so all xtdata calls
go through the shared data_api lock.  The scheduler itself runs in a daemon
thread and uses an asyncio lock to keep its own tasks serialized as well.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import functools
import logging
import os
import threading
from typing import Optional

from .tools.data_api import DataAPI, call_xtdata


class DataDownloadScheduler:
    """Runs startup and daily xtdata prefetch jobs."""

    def __init__(
        self,
        xtdata_module,
        data_api: Optional[DataAPI] = None,
        interval_seconds: Optional[int] = None,
        run_on_start: bool = True,
        logger: Optional[logging.Logger] = None,
    ):
        self.xtdata = xtdata_module
        self.data_api = data_api or DataAPI(xtdata_module)
        self.interval_seconds = interval_seconds or int(os.environ.get("XQSHARE_SCHEDULER_INTERVAL", "86400"))
        self.run_on_start = run_on_start
        self.logger = logger or logging.getLogger(__name__)
        self._thread = None
        self._stop_event = threading.Event()
        self._started = False
        self._async_lock = None

    @property
    def is_running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def start(self):
        if self._started:
            return
        self._started = True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_thread, name="xqshare-data-scheduler", daemon=True)
        self._thread.start()
        self.logger.info("data scheduler started")

    def stop(self, timeout: float = 5):
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
        self._started = False

    def _run_thread(self):
        try:
            asyncio.run(self._run_loop())
        except Exception as exc:
            self.logger.exception("data scheduler stopped unexpectedly: %s", exc)

    async def _run_loop(self):
        self._async_lock = asyncio.Lock()
        if self.run_on_start:
            await self.run_once()

        while not self._stop_event.is_set():
            stopped = await _to_thread(self._stop_event.wait, self.interval_seconds)
            if stopped:
                break
            await self.run_once()

    async def run_once(self):
        self.logger.info("data scheduler cycle started")
        await self._run_task("metadata import", self.data_api.ensure_metadata_files)
        sector_stocks = await self._prefetch_sector_members()
        await self._run_task("download ETF info", self._download_if_available, "download_etf_info")
        await self._run_task("download index weight", self._download_if_available, "download_index_weight")
        await self._prefetch_trading_calendar()
        await self._prefetch_financial_data(sector_stocks)
        await self._prefetch_history_data(sector_stocks)
        self.logger.info("data scheduler cycle finished")

    async def _run_task(self, name, func, *args, **kwargs):
        try:
            result = await self._run_xtdata(func, *args, **kwargs)
            self.logger.info("scheduler task ok: %s", name)
            return result
        except Exception as exc:
            self.logger.warning("scheduler task failed: %s | %s: %s", name, type(exc).__name__, exc)
            return None

    async def _run_xtdata(self, func, *args, **kwargs):
        if self._async_lock is None:
            self._async_lock = asyncio.Lock()
        async with self._async_lock:
            return await _to_thread(call_xtdata, func, *args, **kwargs)

    async def _prefetch_sector_members(self):
        sectors = self._split_env("XQSHARE_PREFETCH_SECTORS", ["沪深A股", "沪深指数", "ETF基金"])
        all_stocks = []
        for sector in sectors:
            result = await self._run_task(
                "sector members: %s" % sector,
                self.xtdata.get_stock_list_in_sector,
                sector,
            )
            if result and sector == sectors[0]:
                all_stocks.extend(result)
        return self._resolve_prefetch_stocks(all_stocks)

    async def _prefetch_trading_calendar(self):
        today = dt.date.today()
        end = today + dt.timedelta(days=370)
        for market in self._split_env("XQSHARE_PREFETCH_MARKETS", ["SH", "SZ"]):
            # 优先使用 get_trading_dates（get_trading_calendar 某些账户不支持）
            await self._run_task(
                "trading calendar: %s" % market,
                self.xtdata.get_trading_dates,
                market,
                today.strftime("%Y%m%d"),
                end.strftime("%Y%m%d"),
            )

    def _call_trading_dates_fallback(self, market, start_date, end_date):
        """调用 get_trading_dates，fallback 到 get_trading_calendar"""
        try:
            result = self.xtdata.get_trading_dates(market, start_date, end_date)
            if result:
                return result
        except Exception:
            pass
        try:
            return self.xtdata.get_trading_calendar(market, start_date, end_date)
        except Exception:
            return []

    async def _prefetch_financial_data(self, stocks):
        if not stocks or not hasattr(self.xtdata, "download_financial_data"):
            return
        tables = self._split_env("XQSHARE_PREFETCH_FINANCIAL_TABLES", ["Balance", "Income", "CashFlow"])
        await self._run_task(
            "financial incremental",
            self.xtdata.download_financial_data,
            stocks,
            tables,
            "",
            "",
            True,
        )

    async def _prefetch_history_data(self, stocks):
        if not stocks or not hasattr(self.xtdata, "download_history_data2"):
            return
        period = os.environ.get("XQSHARE_PREFETCH_PERIOD", "1d")
        today = dt.date.today().strftime("%Y%m%d")
        start = os.environ.get("XQSHARE_PREFETCH_START_DATE", "")
        await self._run_task(
            "history data incremental",
            self.xtdata.download_history_data2,
            stocks,
            period,
            start,
            today,
            None,
            True,
        )

    def _download_if_available(self, method_name):
        method = getattr(self.xtdata, method_name, None)
        if method is None:
            return None
        return method()

    def _resolve_prefetch_stocks(self, sector_stocks):
        configured = self._split_env("XQSHARE_PREFETCH_STOCKS", [])
        stocks = configured or list(sector_stocks)
        limit = int(os.environ.get("XQSHARE_PREFETCH_LIMIT", "500"))
        if limit > 0:
            stocks = stocks[:limit]
        return stocks

    def _split_env(self, name, default):
        value = os.environ.get(name)
        if not value:
            return list(default)
        return [item.strip() for item in value.split(",") if item.strip()]


async def _to_thread(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    call = functools.partial(func, *args, **kwargs)
    return await loop.run_in_executor(None, call)
