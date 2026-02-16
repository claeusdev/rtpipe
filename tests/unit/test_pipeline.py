from types import SimpleNamespace

import pytest

from src.processors.pipeline import DataPipeline


class DummyMetrics:
    async def record_latency(self, *args, **kwargs):
        return None

    async def increment_counter(self, *args, **kwargs):
        return None

    async def record_gauge(self, *args, **kwargs):
        return None


class CapturingStorage:
    def __init__(self):
        self.trades = []

    async def store_trades(self, trades):
        self.trades.extend(trades)

    async def store_quotes(self, quotes):
        return None

    async def store_orderbooks(self, orderbooks):
        return None


class FailingStorage(CapturingStorage):
    async def store_trades(self, trades):
        raise RuntimeError("storage down")


def _build_config():
    return SimpleNamespace(
        processing=SimpleNamespace(batch_size=1, flush_interval=100),
        kafka=SimpleNamespace(
            topics=SimpleNamespace(
                trades="market.trades",
                quotes="market.quotes",
                orderbook="market.orderbook",
            )
        ),
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_process_single_message_handles_direct_trade_payload():
    storage = CapturingStorage()
    pipeline = DataPipeline(_build_config(), storage, DummyMetrics())

    ok = await pipeline.process_single_message(
        {
            "exchange": "coinbase",
            "symbol": "BTC-USD",
            "trade_id": "1",
            "price": "100.0",
            "quantity": "0.5",
            "side": "buy",
            "timestamp": "2025-01-01T00:00:00+00:00",
        }
    )

    assert ok is True
    assert len(storage.trades) == 1
    assert storage.trades[0].symbol == "BTC-USD"
    assert pipeline.message_buffer == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_flush_failure_does_not_drop_buffered_message():
    pipeline = DataPipeline(_build_config(), FailingStorage(), DummyMetrics())

    ok = await pipeline.process_single_message(
        {
            "exchange": "coinbase",
            "symbol": "BTC-USD",
            "trade_id": "1",
            "price": "100.0",
            "quantity": "0.5",
            "side": "buy",
            "timestamp": "2025-01-01T00:00:00+00:00",
        }
    )

    assert ok is False
    assert len(pipeline.message_buffer) == 1
