import datetime as dt
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from xqshare.auth import AccountLevel, Permission, PermissionChecker, PermissionError
from xqshare.client import XtQuantRemote
from xqshare.server import XtQuantService, _serialize_for_transfer
from xqshare.client import _deserialize_from_transfer
from xqshare.tools.data_api import DataAPI


class TestDataAPI:
    def test_get_daily_bars_flattens_market_data_ex(self):
        xtdata = MagicMock()
        xtdata.get_market_data_ex.return_value = {
            "000001.SZ": pd.DataFrame(
                {"open": [10.0], "close": [10.5]},
                index=pd.Index(["20260518"], name="time"),
            )
        }

        result = DataAPI(xtdata).get_daily_bars(["000001.SZ"], "2026-05-18", "2026-05-18")

        assert result.to_dict(orient="records") == [
            {"stock_code": "000001.SZ", "datetime": "20260518", "open": 10.0, "close": 10.5}
        ]
        xtdata.get_market_data_ex.assert_called_once_with(
            [],
            ["000001.SZ"],
            "1d",
            "20260518",
            "20260518",
        )

    def test_get_realtime_quote_returns_frame(self):
        xtdata = MagicMock()
        xtdata.get_full_tick.return_value = {"000001.SZ": {"lastPrice": 10.5, "volume": 100}}

        result = DataAPI(xtdata).get_realtime_quote("000001.SZ")

        assert result.to_dict(orient="records") == [
            {"stock_code": "000001.SZ", "lastPrice": 10.5, "volume": 100}
        ]

    def test_get_instruments_uses_batch_api_when_available(self):
        xtdata = MagicMock()
        xtdata.get_instrument_detail_list.return_value = {
            "000001.SZ": {"InstrumentName": "Ping An Bank"}
        }

        result = DataAPI(xtdata).get_instruments(["000001.SZ"])

        assert result.to_dict(orient="records") == [
            {"stock_code": "000001.SZ", "InstrumentName": "Ping An Bank"}
        ]

    def test_get_trading_calendar_returns_datetimes(self):
        xtdata = MagicMock()
        xtdata.get_trading_calendar.return_value = ["20260518", "20260519"]

        result = DataAPI(xtdata).get_trading_calendar("20260518", "20260519")

        assert result == [dt.datetime(2026, 5, 18), dt.datetime(2026, 5, 19)]

    def test_get_financial_data_flattens_nested_frames(self):
        xtdata = MagicMock()
        xtdata.get_financial_data.return_value = {
            "000001.SZ": {
                "Balance": pd.DataFrame(
                    {"asset": [100]},
                    index=pd.Index(["20251231"], name="report_date"),
                )
            }
        }

        result = DataAPI(xtdata).get_financial_data(["000001.SZ"], ["Balance"])

        records = result.to_dict(orient="records")
        assert records[0]["stock_code"] == "000001.SZ"
        assert records[0]["table_name"] == "Balance"
        assert records[0]["asset"] == 100


class TestDataEnhancementPermissions:
    def test_standard_metadata_permission(self):
        checker = PermissionChecker("/path/does/not/exist.yaml")

        assert checker.check_api_permission(AccountLevel.PLUS, "get_yield_curve").permission == Permission.STANDARD
        assert checker.check_api_permission(AccountLevel.STANDARD, "get_yield_curve") is None
        assert checker.check_api_permission(AccountLevel.PREMIUM, "get_suspended_days") is None


class TestDataEnhancementServer:
    def test_service_get_daily_bars_serializes_dataframe(self, mock_service):
        mock_service.exposed_authenticate("free-user", "free-secret")
        api = MagicMock()
        api.get_daily_bars.return_value = pd.DataFrame({"stock_code": ["000001.SZ"], "close": [10.5]})
        XtQuantService._data_api = api

        result = mock_service.exposed_get_daily_bars(["000001.SZ"], "20260518", "20260518")
        frame = _deserialize_from_transfer(result)

        assert frame.iloc[0]["stock_code"] == "000001.SZ"
        assert frame.iloc[0]["close"] == 10.5

    def test_service_yield_curve_requires_standard(self, mock_service):
        mock_service.exposed_authenticate("plus-user", "plus-secret")
        api = MagicMock()
        api.get_yield_curve.return_value = pd.DataFrame({"date": ["20260518"]})
        XtQuantService._data_api = api

        with pytest.raises(PermissionError):
            mock_service.exposed_get_yield_curve("20260518")


class TestDataEnhancementClient:
    @patch("xqshare.client.rpyc.connect")
    @patch("xqshare.client.BgServingThread")
    def test_client_get_daily_bars_deserializes_dataframe(self, _bg_thread, mock_connect):
        frame = pd.DataFrame({"stock_code": ["000001.SZ"], "close": [10.5]})
        mock_conn = MagicMock()
        mock_conn.root.authenticate.return_value = {"success": True, "level": "standard"}
        mock_conn.root.get_daily_bars.return_value = _serialize_for_transfer(frame)
        mock_connect.return_value = mock_conn

        client = XtQuantRemote(host="localhost", heartbeat_interval=0)
        result = client.get_daily_bars(["000001.SZ"], "20260518", "20260518")

        assert result.iloc[0]["stock_code"] == "000001.SZ"
        assert result.iloc[0]["close"] == 10.5
