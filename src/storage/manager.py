"""
Storage manager for market data persistence
"""

import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import json

import redis.asyncio as redis
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from influxdb_client import Point, WritePrecision
from influxdb_client.client.write_api_async import WriteApiAsync

from ..models.market_data import Trade, Quote, OrderBook, MarketDataMessage


class StorageManager:
    """Manages data storage to Redis and InfluxDB"""
    
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Storage clients
        self.redis_client = None
        self.influx_client = None
        self.influx_write_api = None
        
        # Write buffers
        self.influx_buffer = []
        self.buffer_size = config.influxdb.batch_size
        self.last_flush = datetime.utcnow()
        
    async def initialize(self):
        """Initialize storage connections"""
        self.logger.info("Initializing storage manager...")
        
        # Initialize Redis
        self.redis_client = redis.Redis(
            host=self.config.redis.host,
            port=self.config.redis.port,
            db=self.config.redis.db,
            password=self.config.redis.password,
            max_connections=self.config.redis.max_connections,
            socket_timeout=self.config.redis.socket_timeout,
            socket_connect_timeout=self.config.redis.socket_connect_timeout
        )
        
        # Test Redis connection
        await self.redis_client.ping()
        self.logger.info("Redis connection established")
        
        # Initialize InfluxDB
        self.influx_client = InfluxDBClientAsync(
            url=self.config.influxdb.url,
            token=self.config.influxdb.token,
            org=self.config.influxdb.org,
            timeout=self.config.influxdb.timeout
        )
        
        self.influx_write_api = self.influx_client.write_api()
        
        # Test InfluxDB connection
        health = await self.influx_client.health()
        if health.status == "pass":
            self.logger.info("InfluxDB connection established")
        else:
            raise Exception(f"InfluxDB health check failed: {health.message}")
    
    async def close(self):
        """Close storage connections"""
        self.logger.info("Closing storage connections...")
        
        # Flush remaining data
        if self.influx_buffer:
            await self._flush_influx_buffer()
        
        # Close connections
        if self.redis_client:
            await self.redis_client.close()
        
        if self.influx_client:
            await self.influx_client.close()
        
        self.logger.info("Storage connections closed")
    
    async def store_trades(self, trades: List[Trade]):
        """Store trade data to both Redis and InfluxDB"""
        try:
            # Store to Redis for fast access
            redis_tasks = []
            for trade in trades:
                redis_tasks.append(self._store_trade_to_redis(trade))
            
            # Store to InfluxDB for historical data
            influx_tasks = []
            for trade in trades:
                influx_tasks.append(self._add_trade_to_influx_buffer(trade))
            
            # Execute storage operations
            await asyncio.gather(*redis_tasks, return_exceptions=True)
            await asyncio.gather(*influx_tasks, return_exceptions=True)
            
            # Flush InfluxDB buffer if needed
            await self._check_and_flush_influx_buffer()
            
            self.logger.debug(f"Stored {len(trades)} trades")
            
        except Exception as e:
            self.logger.error(f"Error storing trades: {e}")
            raise
    
    async def store_quotes(self, quotes: List[Quote]):
        """Store quote data to both Redis and InfluxDB"""
        try:
            # Store to Redis for fast access
            redis_tasks = []
            for quote in quotes:
                redis_tasks.append(self._store_quote_to_redis(quote))
            
            # Store to InfluxDB for historical data
            influx_tasks = []
            for quote in quotes:
                influx_tasks.append(self._add_quote_to_influx_buffer(quote))
            
            # Execute storage operations
            await asyncio.gather(*redis_tasks, return_exceptions=True)
            await asyncio.gather(*influx_tasks, return_exceptions=True)
            
            # Flush InfluxDB buffer if needed
            await self._check_and_flush_influx_buffer()
            
            self.logger.debug(f"Stored {len(quotes)} quotes")
            
        except Exception as e:
            self.logger.error(f"Error storing quotes: {e}")
            raise
    
    async def store_orderbooks(self, orderbooks: List[OrderBook]):
        """Store order book data to both Redis and InfluxDB"""
        try:
            # Store to Redis for fast access
            redis_tasks = []
            for orderbook in orderbooks:
                redis_tasks.append(self._store_orderbook_to_redis(orderbook))
            
            # Store to InfluxDB for historical data
            influx_tasks = []
            for orderbook in orderbooks:
                influx_tasks.append(self._add_orderbook_to_influx_buffer(orderbook))
            
            # Execute storage operations
            await asyncio.gather(*redis_tasks, return_exceptions=True)
            await asyncio.gather(*influx_tasks, return_exceptions=True)
            
            # Flush InfluxDB buffer if needed
            await self._check_and_flush_influx_buffer()
            
            self.logger.debug(f"Stored {len(orderbooks)} order books")
            
        except Exception as e:
            self.logger.error(f"Error storing order books: {e}")
            raise
    
    async def _store_trade_to_redis(self, trade: Trade):
        """Store trade to Redis with TTL"""
        key = f"trade:{trade.exchange}:{trade.symbol}:{trade.trade_id}"
        data = {
            'exchange': trade.exchange,
            'symbol': trade.symbol,
            'trade_id': trade.trade_id,
            'price': float(trade.price),
            'quantity': float(trade.quantity),
            'side': trade.side,
            'timestamp': trade.timestamp.isoformat(),
            'buyer_maker': trade.buyer_maker
        }
        
        await self.redis_client.setex(
            key, 
            self.config.redis.ttl.trades, 
            json.dumps(data)
        )
        
        # Also add to recent trades list
        recent_key = f"recent_trades:{trade.exchange}:{trade.symbol}"
        await self.redis_client.lpush(recent_key, json.dumps(data))
        await self.redis_client.ltrim(recent_key, 0, 999)  # Keep last 1000 trades
        await self.redis_client.expire(recent_key, self.config.redis.ttl.trades)
    
    async def _store_quote_to_redis(self, quote: Quote):
        """Store quote to Redis with TTL"""
        key = f"quote:{quote.exchange}:{quote.symbol}"
        data = {
            'exchange': quote.exchange,
            'symbol': quote.symbol,
            'bid_price': float(quote.bid_price),
            'bid_quantity': float(quote.bid_quantity),
            'ask_price': float(quote.ask_price),
            'ask_quantity': float(quote.ask_quantity),
            'timestamp': quote.timestamp.isoformat(),
            'spread': float(quote.spread),
            'mid_price': float(quote.mid_price)
        }
        
        await self.redis_client.setex(
            key, 
            self.config.redis.ttl.quotes, 
            json.dumps(data)
        )
    
    async def _store_orderbook_to_redis(self, orderbook: OrderBook):
        """Store order book to Redis with TTL"""
        key = f"orderbook:{orderbook.exchange}:{orderbook.symbol}"
        data = {
            'exchange': orderbook.exchange,
            'symbol': orderbook.symbol,
            'bids': [[float(level.price), float(level.quantity)] for level in orderbook.bids[:20]],
            'asks': [[float(level.price), float(level.quantity)] for level in orderbook.asks[:20]],
            'timestamp': orderbook.timestamp.isoformat(),
            'is_snapshot': orderbook.is_snapshot
        }
        
        if orderbook.best_bid and orderbook.best_ask:
            data['best_bid'] = float(orderbook.best_bid.price)
            data['best_ask'] = float(orderbook.best_ask.price)
            data['spread'] = float(orderbook.spread) if orderbook.spread else None
            data['mid_price'] = float(orderbook.mid_price) if orderbook.mid_price else None
        
        await self.redis_client.setex(
            key, 
            self.config.redis.ttl.orderbook, 
            json.dumps(data)
        )
    
    async def _add_trade_to_influx_buffer(self, trade: Trade):
        """Add trade to InfluxDB buffer"""
        point = Point("trades") \
            .tag("exchange", trade.exchange) \
            .tag("symbol", trade.symbol) \
            .tag("side", trade.side) \
            .field("price", float(trade.price)) \
            .field("quantity", float(trade.quantity)) \
            .field("notional", float(trade.notional)) \
            .time(trade.timestamp, WritePrecision.NS)
        
        if trade.buyer_maker is not None:
            point = point.field("buyer_maker", trade.buyer_maker)
        
        self.influx_buffer.append(point)
    
    async def _add_quote_to_influx_buffer(self, quote: Quote):
        """Add quote to InfluxDB buffer"""
        point = Point("quotes") \
            .tag("exchange", quote.exchange) \
            .tag("symbol", quote.symbol) \
            .field("bid_price", float(quote.bid_price)) \
            .field("bid_quantity", float(quote.bid_quantity)) \
            .field("ask_price", float(quote.ask_price)) \
            .field("ask_quantity", float(quote.ask_quantity)) \
            .field("spread", float(quote.spread)) \
            .field("mid_price", float(quote.mid_price)) \
            .field("spread_bps", float(quote.spread_bps)) \
            .time(quote.timestamp, WritePrecision.NS)
        
        self.influx_buffer.append(point)
    
    async def _add_orderbook_to_influx_buffer(self, orderbook: OrderBook):
        """Add order book to InfluxDB buffer"""
        if orderbook.best_bid and orderbook.best_ask:
            point = Point("orderbooks") \
                .tag("exchange", orderbook.exchange) \
                .tag("symbol", orderbook.symbol) \
                .field("best_bid", float(orderbook.best_bid.price)) \
                .field("best_ask", float(orderbook.best_ask.price)) \
                .field("bid_quantity", float(orderbook.best_bid.quantity)) \
                .field("ask_quantity", float(orderbook.best_ask.quantity)) \
                .field("spread", float(orderbook.spread) if orderbook.spread else 0) \
                .field("mid_price", float(orderbook.mid_price) if orderbook.mid_price else 0) \
                .field("bid_volume_5", float(orderbook.total_bid_volume(5))) \
                .field("ask_volume_5", float(orderbook.total_ask_volume(5))) \
                .time(orderbook.timestamp, WritePrecision.NS)
            
            self.influx_buffer.append(point)
    
    async def _check_and_flush_influx_buffer(self):
        """Check if InfluxDB buffer should be flushed"""
        if (len(self.influx_buffer) >= self.buffer_size or 
            (datetime.utcnow() - self.last_flush).total_seconds() * 1000 >= self.config.influxdb.flush_interval):
            await self._flush_influx_buffer()
    
    async def _flush_influx_buffer(self):
        """Flush InfluxDB buffer"""
        if not self.influx_buffer:
            return
        
        try:
            await self.influx_write_api.write(
                bucket=self.config.influxdb.bucket,
                record=self.influx_buffer
            )
            
            count = len(self.influx_buffer)
            self.influx_buffer.clear()
            self.last_flush = datetime.utcnow()
            
            self.logger.debug(f"Flushed {count} points to InfluxDB")
            
        except Exception as e:
            self.logger.error(f"Error flushing to InfluxDB: {e}")
            # Keep buffer for retry
            raise
    
    async def get_latest_trade(self, exchange: str, symbol: str) -> Optional[Dict]:
        """Get latest trade from Redis"""
        try:
            recent_key = f"recent_trades:{exchange}:{symbol}"
            data = await self.redis_client.lindex(recent_key, 0)
            
            if data:
                return json.loads(data)
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting latest trade: {e}")
            return None
    
    async def get_latest_quote(self, exchange: str, symbol: str) -> Optional[Dict]:
        """Get latest quote from Redis"""
        try:
            key = f"quote:{exchange}:{symbol}"
            data = await self.redis_client.get(key)
            
            if data:
                return json.loads(data)
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting latest quote: {e}")
            return None
    
    async def get_latest_orderbook(self, exchange: str, symbol: str) -> Optional[Dict]:
        """Get latest order book from Redis"""
        try:
            key = f"orderbook:{exchange}:{symbol}"
            data = await self.redis_client.get(key)
            
            if data:
                return json.loads(data)
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting latest order book: {e}")
            return None