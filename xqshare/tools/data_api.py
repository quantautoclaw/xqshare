"""
Explicit data API layer for xqshare.

The legacy xtdata proxy is intentionally kept available.  This module adds a
small, stable surface that normalizes common xtdata return shapes into plain
tabular results that are cheaper and clearer to consume remotely.
"""

from __future__ import annotations

import datetime as dt
import os
import shutil
import threading
from pathlib import Path
from typing import Any, Iterable, Optional, Union


_XTDATA_LOCK = threading.RLock()


def call_xtdata(func, *args, **kwargs):
    """Run an xtdata call under the process-wide serialization lock."""
    with _XTDATA_LOCK:
        return func(*args, **kwargs)


def _require_pandas():
    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError("pandas is required for the explicit data API") from exc
    return pd


def _as_list(value: Any) -> list:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, Iterable):
        return list(value)
    return [value]


def _format_date(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, dt.datetime):
        return value.strftime("%Y%m%d")
    if isinstance(value, dt.date):
        return value.strftime("%Y%m%d")
    if isinstance(value, (int, float)):
        value = str(int(value))
    text = str(value).strip()
    if not text:
        return ""
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 8:
        return digits[:8]
    return text


def _to_datetime(value: Any) -> dt.datetime:
    if isinstance(value, dt.datetime):
        return value
    if isinstance(value, dt.date):
        return dt.datetime.combine(value, dt.time.min)
    if isinstance(value, (int, float)):
        number = int(value)
        if number > 10_000_000_000:
            return dt.datetime.fromtimestamp(number / 1000)
        return dt.datetime.strptime(str(number), "%Y%m%d")
    text = str(value).strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 8:
        return dt.datetime.strptime(digits[:8], "%Y%m%d")
    return dt.datetime.fromisoformat(text)


def _normalize_index_column(frame, index_column: str):
    if frame.empty:
        return frame
    first_column = frame.columns[0]
    if first_column in ("index", "time", "date", "datetime", "trade_date", "trading_date"):
        return frame.rename(columns={first_column: index_column})
    return frame


def _dict_of_frames_to_frame(data: Any, stock_column: str = "stock_code", index_column: str = "datetime"):
    pd = _require_pandas()
    if data is None:
        return pd.DataFrame()
    if isinstance(data, pd.DataFrame):
        return _normalize_index_column(data.reset_index(), index_column)

    frames = []
    rows = []
    for stock_code, value in dict(data).items():
        if isinstance(value, pd.DataFrame):
            frame = _normalize_index_column(value.reset_index(), index_column)
            if stock_column not in frame.columns:
                frame.insert(0, stock_column, stock_code)
            frames.append(frame)
        elif isinstance(value, dict):
            rows.append({stock_column: stock_code, **value})
        else:
            rows.append({stock_column: stock_code, "value": value})

    if rows:
        frames.append(pd.DataFrame(rows))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True, sort=False)


def _mapping_to_frame(data: Any, key_column: str, value_column: str = "value", extra: Optional[dict] = None):
    pd = _require_pandas()
    if data is None:
        return pd.DataFrame()
    if isinstance(data, pd.DataFrame):
        frame = data.reset_index()
        for key, value in (extra or {}).items():
            if key not in frame.columns:
                frame.insert(0, key, value)
        return frame

    rows = []
    for key, value in dict(data).items():
        base = {key_column: key}
        if extra:
            base.update(extra)
        if isinstance(value, dict):
            rows.append({**base, **value})
        else:
            rows.append({**base, value_column: value})
    return pd.DataFrame(rows)


class DataAPI:
    """Stable data interface backed by xtdata and optional rqalpha metadata."""

    def __init__(
        self,
        xtdata_module,
        metadata_dir: Optional[Union[str, os.PathLike]] = None,
        rqalpha_bundle_path: Optional[Union[str, os.PathLike]] = None,
    ):
        self.xtdata = xtdata_module
        package_dir = Path(__file__).resolve().parents[1]
        self.metadata_dir = Path(metadata_dir) if metadata_dir else package_dir / "metadata"
        self.rqalpha_bundle_path = Path(rqalpha_bundle_path) if rqalpha_bundle_path else None

    def ensure_metadata_files(self) -> dict:
        """Best-effort import of supplemental rqalpha metadata files."""
        results = {}
        for name in ("yield_curve.h5", "suspended_days.h5"):
            try:
                results[name] = str(self._ensure_metadata_file(name))
            except FileNotFoundError as exc:
                results[name] = str(exc)
        return results

    def get_daily_bars(self, stock_list, start_date, end_date):
        return self._get_bars(stock_list, "1d", start_date, end_date)

    def get_minute_bars(self, stock_list, period, start_date, end_date):
        return self._get_bars(stock_list, period, start_date, end_date)

    def get_realtime_quote(self, stock_list):
        self._require_xtdata()
        stock_list = _as_list(stock_list)
        data = call_xtdata(self.xtdata.get_full_tick, stock_list)
        return _mapping_to_frame(data, "stock_code")

    def get_instruments(self, stock_list):
        self._require_xtdata()
        stock_list = _as_list(stock_list)
        if hasattr(self.xtdata, "get_instrument_detail_list"):
            data = call_xtdata(self.xtdata.get_instrument_detail_list, stock_list)
            return _mapping_to_frame(data, "stock_code")

        rows = {}
        for stock_code in stock_list:
            rows[stock_code] = call_xtdata(self.xtdata.get_instrument_detail, stock_code)
        return _mapping_to_frame(rows, "stock_code")

    def get_trading_calendar(self, start_date, end_date, market: str = "SH"):
        """获取交易日列表。

        优先使用 get_trading_dates（更通用），fallback 到 get_trading_calendar。
        """
        self._require_xtdata()
        start_str = _format_date(start_date) if start_date else ""
        end_str = _format_date(end_date) if end_date else ""

        # 优先使用 get_trading_dates（qmt-bridge 方案）
        try:
            raw = call_xtdata(self.xtdata.get_trading_dates, market, start_str, end_str)
            if raw:
                return [_to_datetime(item) for item in raw]
        except Exception:
            pass

        # fallback: get_trading_calendar
        try:
            raw = call_xtdata(self.xtdata.get_trading_calendar, market, start_str, end_str)
            return [_to_datetime(item) for item in raw] if raw else []
        except Exception:
            return []

    def get_financial_data(
        self,
        stock_list,
        table_list,
        start_date: Any = "",
        end_date: Any = "",
        report_type: str = "report_time",
    ):
        self._require_xtdata()
        data = call_xtdata(
            self.xtdata.get_financial_data,
            _as_list(stock_list),
            _as_list(table_list),
            _format_date(start_date),
            _format_date(end_date),
            report_type,
        )
        return self._financial_data_to_frame(data)

    def get_etf_info(self):
        self._require_xtdata()
        data = call_xtdata(self.xtdata.get_etf_info)
        return _mapping_to_frame(data, "etf_code")

    def get_index_weight(self, index_code):
        self._require_xtdata()
        data = call_xtdata(self.xtdata.get_index_weight, index_code)
        return _mapping_to_frame(data, "stock_code", "weight", {"index_code": index_code})

    def get_yield_curve(self, date):
        frame = self._read_hdf_frame("yield_curve.h5", date)
        return self._filter_frame_by_date(frame, date)

    def get_suspended_days(self, stock_list, start_date, end_date):
        pd = _require_pandas()
        frame = self._read_hdf_frame("suspended_days.h5")
        stocks = set(_as_list(stock_list))
        start = _format_date(start_date)
        end = _format_date(end_date)

        if frame.empty:
            return frame

        if stocks:
            for column in ("stock_code", "order_book_id", "symbol", "code"):
                if column in frame.columns:
                    frame = frame[frame[column].astype(str).isin(stocks)]
                    break
            else:
                matching_columns = [column for column in frame.columns if str(column) in stocks]
                if matching_columns:
                    frame = frame.loc[:, matching_columns]

        date_column = self._find_date_column(frame)
        if date_column:
            series = frame[date_column].map(_format_date)
            if start:
                frame = frame[series >= start]
                series = frame[date_column].map(_format_date)
            if end:
                frame = frame[series <= end]
        elif not isinstance(frame.index, pd.RangeIndex):
            index_dates = frame.index.map(_format_date)
            mask = [True] * len(frame)
            if start:
                mask = [ok and value >= start for ok, value in zip(mask, index_dates)]
            if end:
                mask = [ok and value <= end for ok, value in zip(mask, index_dates)]
            frame = frame.loc[mask]

        return frame.reset_index() if not isinstance(frame.index, pd.RangeIndex) else frame

    def _get_bars(self, stock_list, period, start_date, end_date):
        self._require_xtdata()
        data = call_xtdata(
            self.xtdata.get_market_data_ex,
            [],
            _as_list(stock_list),
            period,
            _format_date(start_date),
            _format_date(end_date),
        )
        return _dict_of_frames_to_frame(data)

    def _financial_data_to_frame(self, data: Any):
        pd = _require_pandas()
        if data is None:
            return pd.DataFrame()
        if isinstance(data, pd.DataFrame):
            return data.reset_index()

        frames = []
        rows = []
        for stock_code, tables in dict(data).items():
            if not isinstance(tables, dict):
                rows.append({"stock_code": stock_code, "value": tables})
                continue
            for table_name, value in tables.items():
                if isinstance(value, pd.DataFrame):
                    frame = value.reset_index()
                    frame.insert(0, "table_name", table_name)
                    frame.insert(0, "stock_code", stock_code)
                    frames.append(frame)
                elif isinstance(value, dict):
                    rows.append({"stock_code": stock_code, "table_name": table_name, **value})
                else:
                    rows.append({"stock_code": stock_code, "table_name": table_name, "value": value})

        if rows:
            frames.append(pd.DataFrame(rows))
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True, sort=False)

    def _read_hdf_frame(self, file_name: str, date: Any = None):
        pd = _require_pandas()
        path = self._ensure_metadata_file(file_name)
        target = _format_date(date)
        try:
            with pd.HDFStore(path, mode="r") as store:
                keys = store.keys()
                if not keys:
                    return pd.DataFrame()
                if target:
                    candidates = {
                        f"/{target}",
                        f"/{target[:4]}-{target[4:6]}-{target[6:8]}",
                        f"/{target[:4]}",
                    }
                    for key in keys:
                        if key in candidates or key.rstrip("/").endswith(target):
                            return store[key]
                return store[keys[0]]
        except ImportError as exc:
            raise RuntimeError("PyTables is required to read HDF5 metadata files") from exc
        except (KeyError, ValueError) as exc:
            raise RuntimeError(f"Failed to read metadata file {path}: {exc}") from exc

    def _filter_frame_by_date(self, frame, date):
        if frame.empty:
            return frame
        target = _format_date(date)
        if not target:
            return frame

        date_column = self._find_date_column(frame)
        if date_column:
            return frame[frame[date_column].map(_format_date) == target]

        if frame.index.nlevels > 1:
            for level in range(frame.index.nlevels):
                values = frame.index.get_level_values(level).map(_format_date)
                if target in set(values):
                    return frame[values == target].reset_index()
        else:
            values = frame.index.map(_format_date)
            if target in set(values):
                return frame[values == target].reset_index()

        return frame

    def _find_date_column(self, frame) -> Optional[str]:
        for column in ("date", "trading_date", "datetime", "time"):
            if column in frame.columns:
                return column
        return None

    def _ensure_metadata_file(self, file_name: str) -> Path:
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        target = self.metadata_dir / file_name
        if target.exists():
            return target

        source = self._find_bundle_file(file_name)
        if source is None:
            raise FileNotFoundError(
                f"{file_name} not found in {self.metadata_dir}; set XQSHARE_RQALPHA_BUNDLE_PATH "
                "or copy the rqalpha bundle metadata file into the metadata directory"
            )

        shutil.copy2(source, target)
        return target

    def _find_bundle_file(self, file_name: str) -> Optional[Path]:
        roots = []
        if self.rqalpha_bundle_path:
            roots.append(self.rqalpha_bundle_path)

        for env_name in ("XQSHARE_RQALPHA_BUNDLE_PATH", "RQALPHA_BUNDLE_PATH", "RQALPHA_BUNDLE_DIR"):
            env_value = os.environ.get(env_name)
            if env_value:
                roots.extend(Path(part).expanduser() for part in env_value.split(os.pathsep) if part)

        roots.extend([
            Path.home() / ".rqalpha" / "bundle",
            Path.home() / ".rqalpha" / "bundle" / "default",
        ])

        seen = set()
        for root in roots:
            root = root.expanduser()
            if root in seen or not root.exists():
                continue
            seen.add(root)
            direct = root / file_name
            if direct.exists():
                return direct
            for candidate in root.rglob(file_name):
                if candidate.is_file():
                    return candidate
        return None

    def _require_xtdata(self):
        if self.xtdata is None:
            raise RuntimeError("xtdata module is not available")
