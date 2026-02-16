"""
Storage manager for market data persistence
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import json

import redis.asyncio as redis
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from influxdb_client import Point, WritePrecision

from ..models.market_data import Trade, Quote, OrderBook


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

    @staticmethod
    def _enum_value(value: Any) -> Any:
        """Return `.value` for Enum-like objects, otherwise raw value."""
        return getattr(value, "value", value)

    @staticmethod
    def _loads_json(raw: Any) -> Dict[str, Any]:
        """Decode JSON payloads from Redis bytes/str into dicts."""
        if isinstance(raw, (bytes, bytearray)):
            return json.loads(raw.decode("utf-8"))
        return json.loads(raw)
        
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
        
        # Test InfluxDB connection by trying to create write API
        try:
            self.influx_write_api = self.influx_client.write_api()
            self.logger.info("InfluxDB connection established")
        except Exception as e:
            self.logger.error(f"InfluxDB connection failed: {e}")
            raise Exception(f"InfluxDB connection failed: {e}")
    
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
            # Store to Redis using a single batched pipeline connection.
            await self._store_trades_to_redis_batch(trades)

            # Store to InfluxDB for historical data.
            for trade in trades:
                await self._add_trade_to_influx_buffer(trade)
            
            # Flush InfluxDB buffer if needed
            await self._check_and_flush_influx_buffer()
            
            self.logger.debug(f"Stored {len(trades)} trades")
            
        except Exception as e:
            self.logger.error(f"Error storing trades: {e}")
            raise
    
    async def store_quotes(self, quotes: List[Quote]):
        """Store quote data to both Redis and InfluxDB"""
        try:
            # Store to Redis using a single batched pipeline connection.
            await self._store_quotes_to_redis_batch(quotes)

            # Store to InfluxDB for historical data.
            for quote in quotes:
                await self._add_quote_to_influx_buffer(quote)
            
            # Flush InfluxDB buffer if needed
            await self._check_and_flush_influx_buffer()
            
            self.logger.debug(f"Stored {len(quotes)} quotes")
            
        except Exception as e:
            self.logger.error(f"Error storing quotes: {e}")
            raise
    
    async def store_orderbooks(self, orderbooks: List[OrderBook]):
        """Store order book data to both Redis and InfluxDB"""
        try:
            # Store to Redis using a single batched pipeline connection.
            await self._store_orderbooks_to_redis_batch(orderbooks)

            # Store to InfluxDB for historical data.
            for orderbook in orderbooks:
                await self._add_orderbook_to_influx_buffer(orderbook)
            
            # Flush InfluxDB buffer if needed
            await self._check_and_flush_influx_buffer()
            
            self.logger.debug(f"Stored {len(orderbooks)} order books")
            
        except Exception as e:
            self.logger.error(f"Error storing order books: {e}")
            raise
    
    async def _store_trade_to_redis(self, trade: Trade):
        """Store trade to Redis with TTL"""
        exchange = self._enum_value(trade.exchange)
        side = self._enum_value(trade.side)
        key = f"trade:{exchange}:{trade.symbol}:{trade.trade_id}"
        data = {
            'exchange': exchange,
            'symbol': trade.symbol,
            'trade_id': trade.trade_id,
            'price': float(trade.price),
            'quantity': float(trade.quantity),
            'side': side,
            'timestamp': trade.timestamp.isoformat(),
            'buyer_maker': trade.buyer_maker
        }
        
        await self.redis_client.setex(
            key, 
            self.config.redis.ttl.trades, 
            json.dumps(data)
        )
        
        # Also add to recent trades list
        recent_key = f"recent_trades:{exchange}:{trade.symbol}"
        await self.redis_client.lpush(recent_key, json.dumps(data))
        await self.redis_client.ltrim(recent_key, 0, 999)  # Keep last 1000 trades
        await self.redis_client.expire(recent_key, self.config.redis.ttl.trades)

    async def _store_trades_to_redis_batch(self, trades: List[Trade]):
        """Store many trades with one Redis pipeline execution."""
        if not trades:
            return

        pipe = self.redis_client.pipeline(transaction=False)
        for trade in trades:
            exchange = self._enum_value(trade.exchange)
            side = self._enum_value(trade.side)
            key = f"trade:{exchange}:{trade.symbol}:{trade.trade_id}"
            data = {
                'exchange': exchange,
                'symbol': trade.symbol,
                'trade_id': trade.trade_id,
                'price': float(trade.price),
                'quantity': float(trade.quantity),
                'side': side,
                'timestamp': trade.timestamp.isoformat(),
                'buyer_maker': trade.buyer_maker
            }
            serialized = json.dumps(data)
            pipe.setex(key, self.config.redis.ttl.trades, serialized)
            recent_key = f"recent_trades:{exchange}:{trade.symbol}"
            pipe.lpush(recent_key, serialized)
            pipe.ltrim(recent_key, 0, 999)
            pipe.expire(recent_key, self.config.redis.ttl.trades)
        await pipe.execute()
    
    async def _store_quote_to_redis(self, quote: Quote):
        """Store quote to Redis with TTL"""
        exchange = self._enum_value(quote.exchange)
        key = f"quote:{exchange}:{quote.symbol}"
        data = {
            'exchange': exchange,
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

    async def _store_quotes_to_redis_batch(self, quotes: List[Quote]):
        """Store many quotes with one Redis pipeline execution."""
        if not quotes:
            return

        pipe = self.redis_client.pipeline(transaction=False)
        for quote in quotes:
            exchange = self._enum_value(quote.exchange)
            key = f"quote:{exchange}:{quote.symbol}"
            data = {
                'exchange': exchange,
                'symbol': quote.symbol,
                'bid_price': float(quote.bid_price),
                'bid_quantity': float(quote.bid_quantity),
                'ask_price': float(quote.ask_price),
                'ask_quantity': float(quote.ask_quantity),
                'timestamp': quote.timestamp.isoformat(),
                'spread': float(quote.spread),
                'mid_price': float(quote.mid_price)
            }
            pipe.setex(key, self.config.redis.ttl.quotes, json.dumps(data))
        await pipe.execute()
    
    async def _store_orderbook_to_redis(self, orderbook: OrderBook):
        """Store order book to Redis with TTL"""
        exchange = self._enum_value(orderbook.exchange)
        key = f"orderbook:{exchange}:{orderbook.symbol}"
        data = {
            'exchange': exchange,
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

    async def _store_orderbooks_to_redis_batch(self, orderbooks: List[OrderBook]):
        """Store many order books with one Redis pipeline execution."""
        if not orderbooks:
            return

        pipe = self.redis_client.pipeline(transaction=False)
        for orderbook in orderbooks:
            exchange = self._enum_value(orderbook.exchange)
            key = f"orderbook:{exchange}:{orderbook.symbol}"
            data = {
                'exchange': exchange,
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
            pipe.setex(key, self.config.redis.ttl.orderbook, json.dumps(data))
        await pipe.execute()
    
    async def _add_trade_to_influx_buffer(self, trade: Trade):
        """Add trade to InfluxDB buffer"""
        exchange = self._enum_value(trade.exchange)
        side = self._enum_value(trade.side)
        point = Point("trades") \
            .tag("exchange", exchange) \
            .tag("symbol", trade.symbol) \
            .tag("side", side) \
            .field("price", float(trade.price)) \
            .field("quantity", float(trade.quantity)) \
            .field("notional", float(trade.notional)) \
            .time(trade.timestamp, WritePrecision.NS)
        
        if trade.buyer_maker is not None:
            point = point.field("buyer_maker", trade.buyer_maker)
        
        self.influx_buffer.append(point)
    
    async def _add_quote_to_influx_buffer(self, quote: Quote):
        """Add quote to InfluxDB buffer"""
        exchange = self._enum_value(quote.exchange)
        point = Point("quotes") \
            .tag("exchange", exchange) \
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
            spread_value = float(orderbook.spread) if orderbook.spread is not None else 0.0
            mid_price_value = float(orderbook.mid_price) if orderbook.mid_price is not None else 0.0
            exchange = self._enum_value(orderbook.exchange)
            point = Point("orderbooks") \
                .tag("exchange", exchange) \
                .tag("symbol", orderbook.symbol) \
                .field("best_bid", float(orderbook.best_bid.price)) \
                .field("best_ask", float(orderbook.best_ask.price)) \
                .field("bid_quantity", float(orderbook.best_bid.quantity)) \
                .field("ask_quantity", float(orderbook.best_ask.quantity)) \
                .field("spread", spread_value) \
                .field("mid_price", mid_price_value) \
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
        recent_trades = await self.get_recent_trades(exchange=exchange, symbol=symbol, limit=1)
        return recent_trades[0] if recent_trades else None

    async def get_recent_trades(self, exchange: str, symbol: str, limit: int = 100) -> List[Dict]:
        """Get recent trades for an exchange/symbol pair from Redis."""
        try:
            if not self.redis_client:
                self.logger.warning("Redis client not initialized")
                return []
                
            recent_key = f"recent_trades:{exchange}:{symbol}"
            data = await self.redis_client.lrange(recent_key, 0, max(limit - 1, 0))
            
            return [self._loads_json(item) for item in data] if data else []
            
        except Exception as e:
            self.logger.error(f"Error getting recent trades: {e}")
            return []
    
    async def get_latest_quote(self, exchange: str, symbol: str) -> Optional[Dict]:
        """Get latest quote from Redis"""
        try:
            if not self.redis_client:
                self.logger.warning("Redis client not initialized")
                return None
                
            key = f"quote:{exchange}:{symbol}"
            data = await self.redis_client.get(key)
            
            if data:
                return self._loads_json(data)
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting latest quote: {e}")
            return None
    
    async def get_latest_orderbook(self, exchange: str, symbol: str) -> Optional[Dict]:
        """Get latest order book from Redis"""
        try:
            if not self.redis_client:
                self.logger.warning("Redis client not initialized")
                return None
                
            key = f"orderbook:{exchange}:{symbol}"
            data = await self.redis_client.get(key)
            
            if data:
                return self._loads_json(data)
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting latest order book: {e}")
            return None
