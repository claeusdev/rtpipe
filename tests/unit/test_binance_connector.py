import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.exchanges.manager import BinanceConnector


class DummyPipeline:
    async def process_single_message(self, message):
        return True


class DummyMetrics:
    async def increment_counter(self, *args, **kwargs):
        return None

    async def record_gauge(self, *args, **kwargs):
        return None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_binance_process_message_handles_raw_trade_payload():
    connector = BinanceConnector(SimpleNamespace(), DummyPipeline(), DummyMetrics())
    connector._process_trade = AsyncMock()
    connector._process_depth = AsyncMock()
    connector._process_ticker = AsyncMock()

    await connector._process_message(
        json.dumps(
            {
                "e": "trade",
                "s": "BTCUSDT",
                "t": 1,
                "p": "100.0",
                "q": "0.1",
                "m": False,
                "T": 123456,
            }
        )
    )

    connector._process_trade.assert_awaited_once()
    connector._process_depth.assert_not_called()
    connector._process_ticker.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_binance_process_message_handles_wrapped_depth_payload():
    connector = BinanceConnector(SimpleNamespace(), DummyPipeline(), DummyMetrics())
    connector._process_trade = AsyncMock()
    connector._process_depth = AsyncMock()
    connector._process_ticker = AsyncMock()

    await connector._process_message(
        json.dumps(
            {
                "stream": "btcusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "s": "BTCUSDT",
                    "E": 123456,
                    "b": [["100.0", "1.0"]],
                    "a": [["100.5", "1.2"]],
                },
            }
        )
    )

    connector._process_depth.assert_awaited_once()
    connector._process_trade.assert_not_called()
    connector._process_ticker.assert_not_called()
