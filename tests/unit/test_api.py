from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from src.api.server import create_app
from src.utils.config import Config


class FakeStorage:
    async def get_recent_trades(self, exchange: str, symbol: str, limit: int = 100):
        return [
            {
                "exchange": exchange,
                "symbol": symbol,
                "price": 100.0,
                "quantity": 0.25,
                "side": "buy",
                "timestamp": "2025-01-01T00:00:00+00:00",
            }
        ][:limit]

    async def get_latest_orderbook(self, exchange: str, symbol: str):
        return {
            "exchange": exchange,
            "symbol": symbol,
            "bids": [[99.0, 1.0]],
            "asks": [[101.0, 1.5]],
            "timestamp": "2025-01-01T00:00:00+00:00",
        }


class FakeMetrics:
    def get_all_metrics(self):
        return {
            "messages_processed_total": 10,
            "processing_latency_ms": 0.5,
            "error_count_total": 1,
            "connection_status": {"pipeline": 1.0},
            "queue_depth": 3,
        }


@pytest.mark.unit
def test_trades_endpoint_accepts_iso_timestamp_and_returns_quantity():
    app = create_app(Config(), FakeStorage(), FakeMetrics())
    client = TestClient(app)

    response = client.get("/trades/BTC-USD?exchange=coinbase&limit=1")
    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["quantity"] == 0.25
    assert datetime.fromisoformat(payload[0]["timestamp"].replace("Z", "+00:00"))


@pytest.mark.unit
def test_health_endpoint_reports_uptime():
    app = create_app(Config(), FakeStorage(), FakeMetrics())
    client = TestClient(app)

    response = client.get("/health")
    assert response.status_code == 200
    uptime = response.json()["uptime"]
    assert isinstance(uptime, float)
    assert uptime >= 0.0
