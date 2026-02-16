import pytest

from src.monitoring.metrics import MetricsCollector
from src.utils.config import Config


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_all_metrics_returns_api_friendly_fields():
    collector = MetricsCollector(Config())

    await collector.increment_counter(
        "messages_processed",
        labels={"exchange": "coinbase", "type": "trade"},
    )
    await collector.increment_counter(
        "processing_errors",
        labels={"exchange": "coinbase", "error_type": "general"},
    )
    await collector.record_latency("message_processing", 1000.0)
    await collector.record_gauge("buffer_depth", 7)
    await collector.record_gauge("pipeline_status", 1)

    metrics = collector.get_all_metrics()

    assert metrics["messages_processed_total"] >= 1
    assert metrics["error_count_total"] >= 1
    assert "processing_latency_ms" in metrics
    assert metrics["queue_depth"] == 7
    assert metrics["connection_status"]["pipeline"] == 1
