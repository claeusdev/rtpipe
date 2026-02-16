"""
Metrics collection and monitoring
"""

import asyncio
import logging
from typing import Dict, Any, Optional
from collections import defaultdict, deque

from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry, start_http_server


class MetricsCollector:
    """Collects and manages application metrics"""
    
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Use a custom registry to avoid conflicts
        self.registry = CollectorRegistry()
        
        # Prometheus metrics
        self.counters = {}
        self.histograms = {}
        self.gauges = {}
        
        # Internal metrics storage
        self.latency_data = defaultdict(lambda: deque(maxlen=10000))
        self.counter_data = defaultdict(int)
        self.gauge_data = defaultdict(float)
        
        self.is_running = False
        self._tasks: list[asyncio.Task] = []
        
        # Initialize Prometheus metrics
        self._initialize_metrics()
    
    def _initialize_metrics(self):
        """Initialize Prometheus metrics"""
        
        # Clear existing metrics to avoid duplicates
        self.counters.clear()
        self.histograms.clear()
        self.gauges.clear()
        
        # Counters
        self.counters['messages_processed'] = Counter(
            'messages_processed_total',
            'Total number of messages processed',
            ['exchange', 'type'],
            registry=self.registry
        )
        
        self.counters['processing_errors'] = Counter(
            'processing_errors_total',
            'Total number of processing errors',
            ['exchange', 'error_type'],
            registry=self.registry
        )
        
        self.counters['reconnects'] = Counter(
            'exchange_reconnects_total',
            'Total number of exchange reconnects',
            ['exchange'],
            registry=self.registry
        )
        
        # Histograms for latency
        self.histograms['processing_latency'] = Histogram(
            'processing_latency_seconds',
            'Message processing latency',
            ['operation'],
            buckets=[0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
            registry=self.registry
        )
        
        self.histograms['storage_latency'] = Histogram(
            'storage_latency_seconds',
            'Storage operation latency',
            ['storage_type'],
            buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
            registry=self.registry
        )
        
        # Gauges
        self.gauges['buffer_depth'] = Gauge(
            'buffer_depth',
            'Current buffer depth',
            ['buffer_type'],
            registry=self.registry
        )
        
        self.gauges['connection_status'] = Gauge(
            'connection_status',
            'Exchange connection status (1=connected, 0=disconnected)',
            ['exchange'],
            registry=self.registry
        )
        
        self.gauges['queue_depth'] = Gauge(
            'queue_depth',
            'Queue depth for different components',
            ['component'],
            registry=self.registry
        )
        
        self.gauges['memory_usage'] = Gauge(
            'memory_usage_bytes',
            'Memory usage in bytes',
            ['component'],
            registry=self.registry
        )
        
        self.gauges['cpu_usage'] = Gauge(
            'cpu_usage_percent',
            'CPU usage percentage',
            ['component'],
            registry=self.registry
        )
    
    async def initialize(self):
        """Initialize metrics collector"""
        self.logger.info("Initializing metrics collector...")
        
        if self.config.monitoring.enabled:
            # Start Prometheus HTTP server
            start_http_server(self.config.monitoring.prometheus.port, registry=self.registry)
            self.logger.info(f"Prometheus metrics server started on port {self.config.monitoring.prometheus.port}")
    
    async def start(self):
        """Start metrics collection"""
        self.logger.info("Starting metrics collection...")
        self.is_running = True
        
        # Start metrics collection tasks
        self._tasks = [
            asyncio.create_task(self._collect_system_metrics()),
            asyncio.create_task(self._log_metrics_summary()),
        ]
    
    async def stop(self):
        """Stop metrics collection"""
        self.logger.info("Stopping metrics collection...")
        self.is_running = False

        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
    
    async def increment_counter(self, metric_name: str, labels: Optional[Dict[str, str]] = None, value: float = 1):
        """Increment a counter metric"""
        # Update internal counter
        self.counter_data[metric_name] += value
        
        # Update Prometheus counter if it exists
        if metric_name in self.counters:
            counter = self.counters[metric_name]
            resolved_labels = labels or {}
            if metric_name == 'messages_processed':
                resolved_labels.setdefault('exchange', 'unknown')
                resolved_labels.setdefault('type', 'unknown')
            elif metric_name == 'processing_errors':
                resolved_labels.setdefault('exchange', 'unknown')
                resolved_labels.setdefault('error_type', 'general')
            elif metric_name == 'reconnects':
                resolved_labels.setdefault('exchange', 'unknown')
            counter.labels(**resolved_labels).inc(value)
        
        # Handle specific counter names without labels for convenience
        elif any(key in metric_name for key in ['binance', 'coinbase', 'kraken']):
            exchange = None
            for exch in ['binance', 'coinbase', 'kraken']:
                if exch in metric_name:
                    exchange = exch
                    break
            
            if 'trades_processed' in metric_name:
                self.counters['messages_processed'].labels(exchange=exchange, type='trade').inc(value)
            elif 'depth_processed' in metric_name or 'l2_processed' in metric_name:
                self.counters['messages_processed'].labels(exchange=exchange, type='orderbook').inc(value)
            elif 'tickers_processed' in metric_name:
                self.counters['messages_processed'].labels(exchange=exchange, type='quote').inc(value)
            elif 'reconnects' in metric_name:
                self.counters['reconnects'].labels(exchange=exchange).inc(value)
            elif 'errors' in metric_name:
                self.counters['processing_errors'].labels(exchange=exchange, error_type='general').inc(value)
    
    async def record_latency(self, operation: str, latency_microseconds: float):
        """Record latency measurement"""
        latency_seconds = latency_microseconds / 1_000_000
        
        # Store in internal data
        self.latency_data[operation].append(latency_microseconds)
        
        # Update Prometheus histogram
        if operation in ['message_processing', 'buffer_flush']:
            self.histograms['processing_latency'].labels(operation=operation).observe(latency_seconds)
        elif operation in ['redis_write', 'influx_write']:
            storage_type = operation.split('_')[0]
            self.histograms['storage_latency'].labels(storage_type=storage_type).observe(latency_seconds)
    
    async def record_gauge(self, metric_name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Record gauge metric"""
        # Update internal gauge
        self.gauge_data[metric_name] = value
        
        # Update Prometheus gauge
        if metric_name == 'buffer_depth':
            buffer_type = labels.get('buffer_type', 'default') if labels else 'default'
            self.gauges['buffer_depth'].labels(buffer_type=buffer_type).set(value)
        elif metric_name == 'pipeline_status':
            self.gauges['connection_status'].labels(exchange='pipeline').set(value)
        elif metric_name.startswith("connection_status_"):
            exchange = metric_name.replace("connection_status_", "")
            self.gauges['connection_status'].labels(exchange=exchange).set(value)
        elif 'queue_depth' in metric_name:
            component = labels.get('component', 'default') if labels else 'default'
            self.gauges['queue_depth'].labels(component=component).set(value)
    
    async def get_latency_stats(self, operation: str) -> Dict[str, float]:
        """Get latency statistics for an operation"""
        if operation not in self.latency_data or not self.latency_data[operation]:
            return {}
        
        latencies = list(self.latency_data[operation])
        latencies.sort()
        
        n = len(latencies)
        
        return {
            'count': n,
            'min': latencies[0],
            'max': latencies[-1],
            'mean': sum(latencies) / n,
            'p50': latencies[int(n * 0.5)],
            'p95': latencies[int(n * 0.95)],
            'p99': latencies[int(n * 0.99)],
            'p999': latencies[int(n * 0.999)] if n >= 1000 else latencies[-1]
        }
    
    async def get_throughput_stats(self, time_window_seconds: int = 60) -> Dict[str, float]:
        """Get throughput statistics"""
        # Calculate messages per second based on counters
        stats = {}
        
        for counter_name, count in self.counter_data.items():
            if 'processed' in counter_name:
                # Simple approximation - in reality you'd want to track timestamps
                stats[counter_name + '_per_second'] = count / time_window_seconds
        
        return stats
    
    async def _collect_system_metrics(self):
        """Collect system-level metrics"""
        while self.is_running:
            try:
                # Collect memory usage
                import psutil
                process = psutil.Process()
                
                memory_info = process.memory_info()
                self.gauges['memory_usage'].labels(component='pipeline').set(memory_info.rss)
                
                # Collect CPU usage
                cpu_percent = process.cpu_percent()
                self.gauges['cpu_usage'].labels(component='pipeline').set(cpu_percent)
                
                await asyncio.sleep(5)  # Collect every 5 seconds
                
            except Exception as e:
                self.logger.error(f"Error collecting system metrics: {e}")
                await asyncio.sleep(5)
    
    async def _log_metrics_summary(self):
        """Log metrics summary periodically"""
        while self.is_running:
            try:
                await asyncio.sleep(30)  # Log every 30 seconds
                
                # Log counter stats
                total_messages = sum(
                    count for name, count in self.counter_data.items() 
                    if 'processed' in name
                )
                
                total_errors = sum(
                    count for name, count in self.counter_data.items() 
                    if 'error' in name
                )
                
                # Log latency stats
                processing_stats = await self.get_latency_stats('message_processing')
                
                self.logger.info(
                    "Metrics Summary total_messages=%s total_errors=%s processing_latency_p95_us=%.2f buffer_depth=%s",
                    total_messages,
                    total_errors,
                    float(processing_stats.get('p95', 0)),
                    self.gauge_data.get('buffer_depth', 0),
                )
                
            except Exception as e:
                self.logger.error(f"Error logging metrics summary: {e}")
    
    def get_all_metrics(self) -> Dict[str, Any]:
        """Get all current metrics"""
        total_messages = sum(
            count for name, count in self.counter_data.items()
            if "processed" in name
        )
        total_errors = sum(
            count for name, count in self.counter_data.items()
            if "error" in name
        )

        processing_stats = {}
        if self.latency_data.get("message_processing"):
            latencies = sorted(self.latency_data["message_processing"])
            n = len(latencies)
            if n > 0:
                processing_stats = {
                    "count": n,
                    "min": latencies[0],
                    "max": latencies[-1],
                    "mean": sum(latencies) / n,
                    "p50": latencies[int(n * 0.5)],
                    "p95": latencies[int(n * 0.95)],
                    "p99": latencies[int(n * 0.99)],
                }

        connection_status = {
            key.replace("connection_status_", ""): value
            for key, value in self.gauge_data.items()
            if key.startswith("connection_status_")
        }
        if "pipeline_status" in self.gauge_data:
            connection_status["pipeline"] = self.gauge_data["pipeline_status"]

        return {
            "messages_processed_total": int(total_messages),
            "processing_latency_ms": float(processing_stats.get("p95", 0.0)) / 1000.0,
            "error_count_total": int(total_errors),
            "connection_status": connection_status,
            "queue_depth": int(self.gauge_data.get("buffer_depth", 0)),
            'counters': dict(self.counter_data),
            'gauges': dict(self.gauge_data),
            "latency_stats": processing_stats,
        }
