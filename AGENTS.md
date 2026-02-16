# Repository Guidelines

## Project Structure & Module Organization
Core runtime code is in `src/`, with domain modules split by responsibility:
- `src/exchanges/` (exchange clients and ingestion),
- `src/processors/` (pipeline processing),
- `src/storage/` (Redis/InfluxDB access),
- `src/api/` (FastAPI endpoints),
- `src/monitoring/` (metrics),
- `src/models/` (data models), and `src/utils/` (config/logging).

Top-level entrypoints and ops files:
- `main.py` starts the pipeline.
- `config.yaml` holds runtime configuration.
- `scripts/setup_db.py` initializes Redis/InfluxDB.
- `scripts/generate_test_data.py` creates local datasets in `test_data/`.
- `monitoring/` contains Prometheus and Grafana provisioning.

## Build, Test, and Development Commands
- `uv sync --dev`: install app + dev dependencies.
- `docker-compose up -d`: start Kafka, Redis, InfluxDB, and observability stack.
- `uv run python scripts/setup_db.py`: initialize storage backends.
- `uv run python main.py` (or `uv run pipeline`): run the data pipeline.
- `uv run pytest`: run all tests.
- `uv run black . && uv run isort . && uv run flake8 src/`: format + lint.
- `uv run mypy src/`: strict type checks.

## Coding Style & Naming Conventions
Use Python 3.11 and PEP 8 defaults with project tooling:
- 4-space indentation, max line length 88 (`black`).
- Import ordering via `isort` (`profile = "black"`).
- Prefer explicit typing; `mypy` is configured in strict mode.
- Naming: modules/functions `snake_case`, classes `PascalCase`, constants `UPPER_SNAKE_CASE`.

## Testing Guidelines
`pytest` is configured to discover tests under `tests/` with patterns `test_*.py` and `*_test.py`.
- Place fast unit tests in `tests/unit/`.
- Place integration tests in `tests/integration/` (mark with `@pytest.mark.integration`).
- Use markers `unit`, `integration`, and `slow` consistently.
- Before opening a PR, run `uv run pytest` and include results.

## Commit & Pull Request Guidelines
Recent history uses uppercase prefixes (example: `FEAT: update to latest dev version`).
- Follow `<TYPE>: concise imperative summary` (e.g., `FEAT: add Kraken reconnect backoff`).
- Keep commits scoped to one change.
- PRs should include: purpose, affected paths, config changes, and test evidence.
- Link related issues and call out operational impacts (latency, throughput, storage schema).
