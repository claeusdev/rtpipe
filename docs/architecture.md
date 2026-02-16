# Real-Time Pipeline: Architecture and Design Notes

## 1. Purpose and Scope

This document describes how the current repository works in practice, not just intended state. It covers:

- runtime architecture and module boundaries
- real-time data flow from exchanges to storage and API
- design tradeoffs and known constraints
- practical improvement roadmap

The system is designed to ingest market data from multiple exchanges, normalize it, persist it to low-latency and time-series stores, and expose it over HTTP/WebSocket.

## 2. High-Level Architecture

At runtime, the app is composed of five core services inside one Python process:

1. `ExchangeManager`: manages exchange WebSocket connectors (Binance, Coinbase, Kraken).
2. `DataPipeline`: parses/normalizes market payloads and manages buffering/flush.
3. `StorageManager`: writes to Redis (serving path) and InfluxDB (historical path).
4. `MetricsCollector`: internal metrics + Prometheus exporter.
5. FastAPI app: `/health`, `/metrics`, `/symbols`, `/trades/{symbol}`, `/orderbook/{symbol}`, `/ws`.

Supporting infrastructure (via Docker Compose):

- Kafka + Zookeeper
- Redis
- InfluxDB
- Prometheus
- Grafana
- Kafka UI

## 3. Runtime Modes and Startup

Entrypoint: `main.py`.

- `--mode pipeline`: runs ingestion + storage + metrics, no API server.
- `--mode api`: runs API server and initializes its own storage/metrics dependencies.
- `--mode both`: runs pipeline and API together, sharing the same initialized storage/metrics instances.

Shutdown is signal-driven (`SIGINT`, `SIGTERM`) with reverse-order stop.

## 4. Data Flow (Current State)

### 4.1 Exchange -> Pipeline -> Storage (primary live path)

1. Connector receives WebSocket event.
2. Event is mapped into typed model (`Trade`, `Quote`, `OrderBook`) and serialized.
3. `DataPipeline.process_single_message()` infers message type, appends to in-memory buffer.
4. Buffer flushes when:
   - message count reaches `processing.batch_size`, or
   - periodic flush interval elapses (`processing.flush_interval`).
5. Flush groups by type and writes to storage backends.

### 4.2 Kafka -> Pipeline -> Storage (secondary path)

`DataPipeline` also starts a Kafka consumer over configured topics and can process Kafka payloads into the same buffer/flush flow. This gives flexibility for future decoupling/replay.

### 4.3 Storage write model

Redis:
- recent trades list per key: `recent_trades:{exchange}:{symbol}`
- latest quote: `quote:{exchange}:{symbol}`
- latest orderbook: `orderbook:{exchange}:{symbol}`

InfluxDB:
- measurements: `trades`, `quotes`, `orderbooks`
- buffered writes with `influxdb.batch_size` / `influxdb.flush_interval`
- numeric fields are normalized to avoid field-type conflicts

## 5. API Semantics

- `/symbols`: derived from config (`config.yaml`), not live exchange discovery.
- `/trades/{symbol}`: reads Redis recent trades, supports `exchange` and `limit`.
- `/orderbook/{symbol}`: reads latest Redis orderbook for `exchange`.
- `/metrics`: returns app-level summarized metrics from `MetricsCollector`.

Important: symbol format must match exchange convention (`BTCUSDT` for Binance, `BTC-USD` for Coinbase).

## 6. Design Considerations and Tradeoffs

### 6.1 Why Redis + InfluxDB

- Redis gives low-latency read path for API queries.
- InfluxDB gives historical analytics/time-series retention.
- Tradeoff: dual-write complexity and partial-failure handling.

### 6.2 In-process pipeline

- Simple deployment and low overhead for MVP/early scale.
- Tradeoff: less isolation between ingestion, storage, and API failure domains.

### 6.3 Buffered flush strategy

- Reduces write amplification and connection churn.
- Tradeoff: small durability window while messages are in memory.

### 6.4 Hybrid ingestion (direct + Kafka)

- Direct path provides low-latency simplicity.
- Kafka path enables future replay/decoupling.
- Tradeoff: architectural ambiguity until one path becomes the canonical source of truth.

### 6.5 Orderbook representation

- Current orderbook persistence stores top levels from incoming updates.
- Tradeoff: without full snapshot+delta reconstruction, this may not represent a fully consistent L2 book at every moment.

## 7. Operational Concerns

### 7.1 Failure handling

- Exchange reconnect loop uses exponential backoff + jitter.
- Buffer flush failures keep data in memory (no silent drop on failure).
- Storage failures are surfaced (not swallowed) and counted.

### 7.2 Schema consistency

- Influx enforces per-field types by measurement.
- A single inconsistent write type can block subsequent writes (e.g., `spread` int vs float conflict).

### 7.3 Connection pressure

- Redis writes are batched with pipelines to avoid `Too many connections`.
- Key tuning knobs: `redis.max_connections`, `processing.batch_size`, `processing.flush_interval`.

## 8. What Can Be Improved

## 8.1 High priority

1. **Canonical ingestion architecture**  
Pick one primary path (Kafka-first or direct-first). Today both exist and increase mental/operational complexity.

2. **End-to-end integration tests with real infra**  
Add tests that run against ephemeral Redis/Influx/Kafka and verify:
- connector -> Redis key visibility
- Influx write acceptance
- API endpoint correctness for each exchange symbol format

3. **Backpressure and circuit breaking**  
When backend writes fail repeatedly, add controlled degradation modes (e.g., temporarily disable Influx writes while preserving Redis serving path).

4. **Orderbook correctness model**  
Implement snapshot + sequence-aware delta reconciliation per exchange.

## 8.2 Medium priority

1. **Type safety and static analysis cleanup**  
`mypy` strict mode reports many issues. Align annotations and remove stale type debt.

2. **Structured logging consistency**  
Unify logger usage to avoid mixed stdlib/structlog style and improve queryability.

3. **Metrics model hardening**  
Expose per-exchange/per-symbol lag, flush failure rate, and queue saturation indicators.

4. **Configuration hardening**  
Move secrets (`influxdb.token`, credentials) to env/secret store by default.

## 8.3 Long-term

1. **Horizontal scaling strategy**  
Split ingestion, processing, and API into separate deployable units with clear contracts.

2. **Replay and deterministic recovery**  
If Kafka becomes authoritative, build replay tooling for deterministic post-incident recovery.

3. **Schema governance**  
Introduce explicit schema/version controls for stored payloads and measurement fields.

## 9. Current Risks to Track

- Mixed architecture path can cause confusion about source of truth.
- Orderbook endpoint quality depends on exchange update semantics and current reconstruction strategy.
- Influx field-type conflicts can cascade into repeated flush failures if not guarded.
- Symbol mismatch (`BTCUSDT` vs `BTC-USD`) remains an easy user-footgun at API boundary.

## 10. Suggested Next Milestone

“Operationally hardened MVP”:

1. integration tests with dockerized dependencies
2. robust orderbook reconstruction
3. canonical ingestion path decision
4. strict type-checking baseline (`mypy` clean subset)
5. production-safe config profile (secrets/env, CORS/API auth tightening)
