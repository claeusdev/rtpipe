"""
Configuration management for the pipeline
"""

import yaml
from pathlib import Path
from typing import Any, Dict, Optional
import warnings
from pydantic import BaseModel, Field, PrivateAttr
from pydantic_settings import BaseSettings


class AppConfig(BaseModel):
    """Application configuration"""
    name: str = "market-data-pipeline"
    version: str = "1.0.0"
    debug: bool = False
    log_level: str = "INFO"


class APIConfig(BaseModel):
    """API server configuration"""
    host: str = "0.0.0.0"
    port: int = 8000
    workers: int = 4
    reload: bool = False


class KafkaTopics(BaseModel):
    """Kafka topic configuration"""
    trades: str = "market.trades"
    quotes: str = "market.quotes"
    orderbook: str = "market.orderbook"
    aggregated: str = "market.aggregated"


class KafkaConfig(BaseModel):
    """Kafka configuration"""
    bootstrap_servers: str = "localhost:9092"
    topics: KafkaTopics = Field(default_factory=KafkaTopics)
    consumer_group: str = "market-data-processor"
    auto_offset_reset: str = "latest"
    enable_auto_commit: bool = True
    auto_commit_interval_ms: int = 1000
    max_poll_records: int = 1000
    fetch_min_bytes: int = 1
    fetch_max_wait_ms: int = 500


class RedisTTL(BaseModel):
    """Redis TTL configuration"""
    trades: int = 3600
    quotes: int = 1800
    orderbook: int = 600


class RedisConfig(BaseModel):
    """Redis configuration"""
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    max_connections: int = 100
    socket_timeout: float = 1.0
    socket_connect_timeout: float = 1.0
    ttl: RedisTTL = Field(default_factory=RedisTTL)


class InfluxDBConfig(BaseModel):
    """InfluxDB configuration"""
    url: str = "http://localhost:8086"
    token: str = "my-super-secret-auth-token"
    org: str = "jumptrading"
    bucket: str = "marketdata"
    timeout: int = 10000
    batch_size: int = 5000
    flush_interval: int = 1000


class ExchangeConfig(BaseModel):
    """Individual exchange configuration"""
    enabled: bool = True
    websocket_url: str
    rest_url: str
    symbols: list[str] = []
    reconnect_interval: int = 5
    heartbeat_interval: int = 30


class BinanceConfig(ExchangeConfig):
    """Binance specific configuration"""
    websocket_url: str = "wss://stream.binance.com:9443/ws"
    rest_url: str = "https://api.binance.com"
    streams: list[str] = ["trade", "depth@100ms", "ticker"]


class CoinbaseConfig(ExchangeConfig):
    """Coinbase specific configuration"""
    websocket_url: str = "wss://ws-feed.exchange.coinbase.com"
    rest_url: str = "https://api.exchange.coinbase.com"
    channels: list[str] = ["matches", "level2", "ticker"]


class KrakenConfig(ExchangeConfig):
    """Kraken specific configuration"""
    websocket_url: str = "wss://ws.kraken.com"
    rest_url: str = "https://api.kraken.com"
    subscriptions: list[str] = ["trade", "book", "ticker"]


class ExchangesConfig(BaseModel):
    """All exchanges configuration"""
    binance: BinanceConfig = Field(default_factory=BinanceConfig)
    coinbase: CoinbaseConfig = Field(default_factory=CoinbaseConfig)
    kraken: KrakenConfig = Field(default_factory=KrakenConfig)


class OrderBookConfig(BaseModel):
    """Order book processing configuration"""
    depth_levels: int = 20
    update_frequency: int = 100
    snapshot_interval: int = 5000


class AggregationConfig(BaseModel):
    """Trade aggregation configuration"""
    intervals: list[int] = [1, 5, 15, 30, 60]
    enable_vwap: bool = True
    enable_volume_profile: bool = True


class ProcessingConfig(BaseModel):
    """Data processing configuration"""
    batch_size: int = 1000
    flush_interval: int = 100
    max_latency: int = 1000
    enable_deduplication: bool = True
    enable_validation: bool = True
    enable_enrichment: bool = True
    orderbook: OrderBookConfig = Field(default_factory=OrderBookConfig)
    aggregation: AggregationConfig = Field(default_factory=AggregationConfig)


class PrometheusConfig(BaseModel):
    """Prometheus configuration"""
    port: int = 8001
    path: str = "/metrics"


class MonitoringAlertsConfig(BaseModel):
    """Monitoring alerts configuration"""
    high_latency_threshold: float = 0.001
    error_rate_threshold: float = 0.01
    queue_depth_threshold: int = 10000
    memory_threshold: float = 0.8
    cpu_threshold: float = 0.8


class MonitoringConfig(BaseModel):
    """Monitoring configuration"""
    enabled: bool = True
    prometheus: PrometheusConfig = Field(default_factory=PrometheusConfig)
    metrics: list[str] = [
        "messages_processed_total",
        "processing_latency_seconds",
        "error_count_total",
        "connection_status",
        "queue_depth",
        "memory_usage_bytes",
        "cpu_usage_percent"
    ]
    alerts: MonitoringAlertsConfig = Field(default_factory=MonitoringAlertsConfig)


class LoggingConfig(BaseModel):
    """Logging configuration"""
    level: str = "INFO"
    format: str = "json"
    file: str = "logs/pipeline.log"
    max_size: str = "100MB"
    max_files: int = 10


class AsyncConfig(BaseModel):
    """Async configuration"""
    max_workers: int = 100
    connection_pool_size: int = 50
    connection_timeout: int = 30


class MemoryConfig(BaseModel):
    """Memory configuration"""
    gc_threshold: int = 100000
    max_cache_size: int = 1000000


class NetworkConfig(BaseModel):
    """Network configuration"""
    tcp_nodelay: bool = True
    tcp_keepalive: bool = True
    buffer_size: int = 65536


class PerformanceConfig(BaseModel):
    """Performance configuration"""
    async_: AsyncConfig = Field(default_factory=AsyncConfig, alias="async")
    memory: MemoryConfig = Field(default_factory=MemoryConfig)
    network: NetworkConfig = Field(default_factory=NetworkConfig)


class RateLimitConfig(BaseModel):
    """Rate limiting configuration"""
    enabled: bool = True
    requests_per_minute: int = 1000


class CORSConfig(BaseModel):
    """CORS configuration"""
    enabled: bool = True
    origins: list[str] = ["*"]
    methods: list[str] = ["GET", "POST"]
    headers: list[str] = ["*"]


class SecurityConfig(BaseModel):
    """Security configuration"""
    api_key_header: str = "X-API-Key"
    rate_limit: RateLimitConfig = Field(default_factory=RateLimitConfig)
    cors: CORSConfig = Field(default_factory=CORSConfig)


class DevelopmentConfig(BaseModel):
    """Development configuration"""
    mock_data: bool = False
    data_replay: bool = False
    replay_speed: float = 1.0
    test_symbols: list[str] = ["BTCUSDT"]


class Config(BaseSettings):
    """Main configuration class"""
    
    app: AppConfig = Field(default_factory=AppConfig)
    api: APIConfig = Field(default_factory=APIConfig)
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    influxdb: InfluxDBConfig = Field(default_factory=InfluxDBConfig)
    exchanges: ExchangesConfig = Field(default_factory=ExchangesConfig)
    processing: ProcessingConfig = Field(default_factory=ProcessingConfig)
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    performance: PerformanceConfig = Field(default_factory=PerformanceConfig)
    security: SecurityConfig = Field(default_factory=SecurityConfig)
    development: DevelopmentConfig = Field(default_factory=DevelopmentConfig)
    _loaded_config_path: Optional[str] = PrivateAttr(default=None)
    
    def __init__(self, config_path: str = "config.yaml", **kwargs):
        """Initialize configuration from file and environment variables"""

        # Load YAML config from a robust set of candidate paths.
        config_data: Dict[str, Any] = {}
        requested_path = Path(config_path)
        repo_root = Path(__file__).resolve().parents[2]
        candidates = [requested_path]
        if not requested_path.is_absolute():
            candidates.append(repo_root / requested_path)

        selected_path = next((path for path in candidates if path.exists()), None)
        loaded_config_path = str(selected_path) if selected_path else None

        if selected_path:
            with open(selected_path, "r", encoding="utf-8") as f:
                config_data = yaml.safe_load(f) or {}
        else:
            warnings.warn(
                f"Config file '{config_path}' not found; using model defaults.",
                RuntimeWarning,
                stacklevel=2,
            )
        
        # Merge with any passed kwargs
        config_data.update(kwargs)
        
        super().__init__(**config_data)
        self._loaded_config_path = loaded_config_path

    @property
    def loaded_config_path(self) -> Optional[str]:
        """Filesystem path of the loaded config file, if any."""
        return self._loaded_config_path
    
    def get_enabled_exchanges(self) -> list[str]:
        """Get list of enabled exchanges"""
        enabled = []
        for name, config in self.exchanges.model_dump().items():
            if config.get('enabled', False):
                enabled.append(name)
        return enabled
    
    def get_exchange_config(self, exchange_name: str) -> ExchangeConfig:
        """Get configuration for specific exchange"""
        return getattr(self.exchanges, exchange_name)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary"""
        return self.model_dump()
    
    class Config:
        env_file = ".env"
        env_prefix = "PIPELINE_"
        case_sensitive = False
