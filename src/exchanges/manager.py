"""
Exchange manager for handling multiple exchange connections
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any
import random
import websockets
import json
from datetime import datetime

from ..models.market_data import Trade, Quote, OrderBook, Exchange


class ExchangeManager:
    """Manages connections to multiple exchanges"""
    
    def __init__(self, config, data_pipeline, metrics_collector):
        self.config = config
        self.data_pipeline = data_pipeline
        self.metrics_collector = metrics_collector
        self.logger = logging.getLogger(__name__)
        
        # Exchange connectors
        self.connectors = {}
        self.is_running = False
        
    async def initialize(self):
        """Initialize exchange connectors"""
        self.logger.info("Initializing exchange manager...")
        
        enabled_exchanges = self.config.get_enabled_exchanges()
        
        for exchange_name in enabled_exchanges:
            exchange_config = self.config.get_exchange_config(exchange_name)
            
            if exchange_name == "binance":
                connector = BinanceConnector(exchange_config, self.data_pipeline, self.metrics_collector)
            elif exchange_name == "coinbase":
                connector = CoinbaseConnector(exchange_config, self.data_pipeline, self.metrics_collector)
            elif exchange_name == "kraken":
                connector = KrakenConnector(exchange_config, self.data_pipeline, self.metrics_collector)
            else:
                self.logger.warning(f"Unknown exchange: {exchange_name}")
                continue
            
            self.connectors[exchange_name] = connector
            await connector.initialize()
        
        self.logger.info(f"Initialized {len(self.connectors)} exchange connectors")
    
    async def start(self):
        """Start all exchange connections"""
        self.logger.info("Starting exchange connections...")
        self.is_running = True
        
        start_tasks = []
        for connector in self.connectors.values():
            start_tasks.append(connector.start())
        
        await asyncio.gather(*start_tasks, return_exceptions=True)
        self.logger.info("All exchange connections started")
    
    async def stop(self):
        """Stop all exchange connections"""
        self.logger.info("Stopping exchange connections...")
        self.is_running = False
        
        stop_tasks = []
        for connector in self.connectors.values():
            stop_tasks.append(connector.stop())
        
        await asyncio.gather(*stop_tasks, return_exceptions=True)
        self.logger.info("All exchange connections stopped")


class BaseExchangeConnector:
    """Base class for exchange connectors"""
    
    def __init__(self, config, data_pipeline, metrics_collector):
        self.config = config
        self.data_pipeline = data_pipeline
        self.metrics_collector = metrics_collector
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
        self.websocket = None
        self.is_running = False
        self.reconnect_count = 0
        self.connection_task = None
        
    async def initialize(self):
        """Initialize connector"""
        pass
    
    async def start(self):
        """Start connection"""
        self.is_running = True
        self.connection_task = asyncio.create_task(self._connection_loop())
    
    async def stop(self):
        """Stop connection"""
        self.is_running = False
        if self.websocket:
            await self.websocket.close()
        if self.connection_task:
            self.connection_task.cancel()
            await asyncio.gather(self.connection_task, return_exceptions=True)
            self.connection_task = None
    
    async def _connection_loop(self):
        """Main connection loop with reconnection logic"""
        while self.is_running:
            try:
                await self.metrics_collector.record_gauge(
                    "connection_status_" + self.exchange_name,
                    0,
                )
                await self._connect_and_process()
            except Exception as e:
                self.logger.error(f"Connection error: {e}")
                self.reconnect_count += 1
                await self.metrics_collector.increment_counter(f"{self.exchange_name}_reconnects")
                
                if self.is_running:
                    base_delay = max(int(self.config.reconnect_interval), 1)
                    backoff_delay = min(base_delay * (2 ** min(self.reconnect_count, 5)), 60)
                    jitter = random.uniform(0, 0.5)
                    await asyncio.sleep(backoff_delay + jitter)
    
    async def _connect_and_process(self):
        """Connect to exchange and process messages"""
        self.logger.info(f"Connecting to {self.exchange_name}...")
        
        async with websockets.connect(
            self.config.websocket_url,
            ping_interval=self.config.heartbeat_interval
        ) as websocket:
            self.websocket = websocket
            
            # Send subscription message
            await self._send_subscription()
            self.logger.info(f"Connected to {self.exchange_name}")
            
            # Reset reconnect count on successful connection
            self.reconnect_count = 0
            await self.metrics_collector.record_gauge(
                "connection_status_" + self.exchange_name,
                1,
            )
            
            # Process messages
            async for message in websocket:
                if not self.is_running:
                    break
                
                try:
                    if isinstance(message, bytes):
                        message = message.decode("utf-8")
                    await self._process_message(message)
                except Exception as e:
                    self.logger.error(f"Error processing message: {e}")
                    await self.metrics_collector.increment_counter(f"{self.exchange_name}_message_errors")
    
    async def _send_subscription(self):
        """Send subscription message (to be implemented by subclasses)"""
        raise NotImplementedError
    
    async def _process_message(self, message: str):
        """Process incoming message (to be implemented by subclasses)"""
        raise NotImplementedError
    
    @property
    def exchange_name(self) -> str:
        """Exchange name"""
        raise NotImplementedError


class BinanceConnector(BaseExchangeConnector):
    """Binance WebSocket connector"""
    
    @property
    def exchange_name(self) -> str:
        return "binance"
    
    async def _send_subscription(self):
        """Send Binance subscription message"""
        streams = []
        
        for symbol in self.config.symbols:
            symbol_lower = symbol.lower()
            for stream in self.config.streams:
                if stream == "trade":
                    streams.append(f"{symbol_lower}@trade")
                elif stream == "depth@100ms":
                    streams.append(f"{symbol_lower}@depth@100ms")
                elif stream == "ticker":
                    streams.append(f"{symbol_lower}@ticker")
        
        subscription = {
            "method": "SUBSCRIBE",
            "params": streams,
            "id": 1
        }
        
        await self.websocket.send(json.dumps(subscription))
        self.logger.info(f"Subscribed to {len(streams)} Binance streams")
    
    async def _process_message(self, message: str):
        """Process Binance message"""
        try:
            data = json.loads(message)

            if not isinstance(data, dict):
                return

            # Skip subscription confirmations.
            if "result" in data and "id" in data:
                return

            # Binance can send either wrapped combined messages (`stream` + `data`)
            # or raw event payloads directly when using /ws + SUBSCRIBE.
            stream_data = data.get("data", data)
            stream_name = data.get("stream", "")
            event_type = stream_data.get("e", "")

            if event_type == "trade" or "@trade" in stream_name:
                await self._process_trade(stream_data)
            elif event_type == "depthUpdate" or "@depth" in stream_name:
                await self._process_depth(stream_data)
            elif event_type in ("24hrTicker", "bookTicker") or "@ticker" in stream_name:
                await self._process_ticker(stream_data)
                
        except Exception as e:
            self.logger.error(f"Error parsing Binance message: {e}")
    
    async def _process_trade(self, data: Dict[str, Any]):
        """Process Binance trade message"""
        trade = Trade(
            exchange=Exchange.BINANCE,
            symbol=data["s"],
            trade_id=str(data["t"]),
            price=float(data["p"]),
            quantity=float(data["q"]),
            side="buy" if not data["m"] else "sell",  # m=true means buyer is market maker
            timestamp=datetime.fromtimestamp(data["T"] / 1000),
            buyer_maker=data["m"]
        )
        
        ok = await self.data_pipeline.process_single_message(trade.to_kafka_value())
        if ok:
            await self.metrics_collector.increment_counter("binance_trades_processed")
        else:
            await self.metrics_collector.increment_counter("binance_message_errors")
    
    async def _process_depth(self, data: Dict[str, Any]):
        """Process Binance depth message"""
        orderbook = OrderBook(
            exchange=Exchange.BINANCE,
            symbol=data["s"],
            bids=data["b"],
            asks=data["a"],
            timestamp=datetime.fromtimestamp(data["E"] / 1000),
            is_snapshot=False
        )
        
        ok = await self.data_pipeline.process_single_message(orderbook.to_kafka_value())
        if ok:
            await self.metrics_collector.increment_counter("binance_depth_processed")
        else:
            await self.metrics_collector.increment_counter("binance_message_errors")
    
    async def _process_ticker(self, data: Dict[str, Any]):
        """Process Binance ticker message"""
        timestamp_ms = data.get("E") or data.get("T")
        if timestamp_ms is None:
            timestamp = datetime.utcnow()
        else:
            timestamp = datetime.fromtimestamp(float(timestamp_ms) / 1000)

        quote = Quote(
            exchange=Exchange.BINANCE,
            symbol=data["s"],
            bid_price=float(data["b"]),
            bid_quantity=float(data["B"]),
            ask_price=float(data["a"]),
            ask_quantity=float(data["A"]),
            timestamp=timestamp
        )
        
        ok = await self.data_pipeline.process_single_message(quote.to_kafka_value())
        if ok:
            await self.metrics_collector.increment_counter("binance_tickers_processed")
        else:
            await self.metrics_collector.increment_counter("binance_message_errors")


class CoinbaseConnector(BaseExchangeConnector):
    """Coinbase Pro WebSocket connector"""

    async def initialize(self):
        """Normalize legacy Coinbase endpoints."""
        if "ws-feed.pro.coinbase.com" in self.config.websocket_url:
            self.logger.warning(
                "Legacy Coinbase WebSocket URL detected; switching to ws-feed.exchange.coinbase.com"
            )
            self.config.websocket_url = "wss://ws-feed.exchange.coinbase.com"

        if "api.pro.coinbase.com" in self.config.rest_url:
            self.config.rest_url = "https://api.exchange.coinbase.com"
    
    @property
    def exchange_name(self) -> str:
        return "coinbase"
    
    async def _send_subscription(self):
        """Send Coinbase subscription message"""
        subscription = {
            "type": "subscribe",
            "product_ids": self.config.symbols,
            "channels": self.config.channels
        }
        
        await self.websocket.send(json.dumps(subscription))
        self.logger.info(f"Subscribed to Coinbase channels: {self.config.channels}")
    
    async def _process_message(self, message: str):
        """Process Coinbase message"""
        try:
            data = json.loads(message)
            
            message_type = data.get("type")
            
            if message_type == "match":
                await self._process_match(data)
            elif message_type == "l2update":
                await self._process_l2_update(data)
            elif message_type == "ticker":
                await self._process_ticker(data)
                
        except Exception as e:
            self.logger.error(f"Error parsing Coinbase message: {e}")
    
    async def _process_match(self, data: Dict[str, Any]):
        """Process Coinbase match (trade) message"""
        trade = Trade(
            exchange=Exchange.COINBASE,
            symbol=data["product_id"],
            trade_id=str(data["trade_id"]),
            price=float(data["price"]),
            quantity=float(data["size"]),
            side=data["side"],
            timestamp=datetime.fromisoformat(data["time"].replace("Z", "+00:00")),
            buyer_maker=data["side"] == "sell"  # Coinbase logic
        )
        
        ok = await self.data_pipeline.process_single_message(trade.to_kafka_value())
        if ok:
            await self.metrics_collector.increment_counter("coinbase_trades_processed")
        else:
            await self.metrics_collector.increment_counter("coinbase_message_errors")
    
    async def _process_l2_update(self, data: Dict[str, Any]):
        """Process Coinbase L2 update message"""
        # Convert changes to bid/ask format
        bids = []
        asks = []
        
        for change in data["changes"]:
            side, price, size = change
            if side == "buy":
                if float(size) > 0:
                    bids.append([float(price), float(size)])
            else:  # sell
                if float(size) > 0:
                    asks.append([float(price), float(size)])
        
        if bids or asks:
            orderbook = OrderBook(
                exchange=Exchange.COINBASE,
                symbol=data["product_id"],
                bids=bids,
                asks=asks,
                timestamp=datetime.fromisoformat(data["time"].replace("Z", "+00:00")),
                is_snapshot=False
            )
            
            ok = await self.data_pipeline.process_single_message(orderbook.to_kafka_value())
            if ok:
                await self.metrics_collector.increment_counter("coinbase_l2_processed")
            else:
                await self.metrics_collector.increment_counter("coinbase_message_errors")
    
    async def _process_ticker(self, data: Dict[str, Any]):
        """Process Coinbase ticker message"""
        quote = Quote(
            exchange=Exchange.COINBASE,
            symbol=data["product_id"],
            bid_price=float(data["best_bid"]),
            bid_quantity=0,  # Coinbase doesn't provide quantities in ticker
            ask_price=float(data["best_ask"]),
            ask_quantity=0,
            timestamp=datetime.fromisoformat(data["time"].replace("Z", "+00:00"))
        )
        
        ok = await self.data_pipeline.process_single_message(quote.to_kafka_value())
        if ok:
            await self.metrics_collector.increment_counter("coinbase_tickers_processed")
        else:
            await self.metrics_collector.increment_counter("coinbase_message_errors")


class KrakenConnector(BaseExchangeConnector):
    """Kraken WebSocket connector"""
    
    @property
    def exchange_name(self) -> str:
        return "kraken"
    
    async def _send_subscription(self):
        """Send Kraken subscription message"""
        subscription = {
            "event": "subscribe",
            "pair": self.config.symbols,
            "subscription": {
                "name": "trade"
            }
        }
        
        await self.websocket.send(json.dumps(subscription))
        self.logger.info(f"Subscribed to Kraken trade feed")
    
    async def _process_message(self, message: str):
        """Process Kraken message"""
        try:
            data = json.loads(message)
            
            # Skip system messages
            if isinstance(data, dict):
                return
            
            if len(data) >= 4 and data[2] == "trade":
                await self._process_trade(data)
                
        except Exception as e:
            self.logger.error(f"Error parsing Kraken message: {e}")
    
    async def _process_trade(self, data: List[Any]):
        """Process Kraken trade message"""
        trades_data = data[1]
        symbol = data[3]
        
        for trade_data in trades_data:
            price, volume, time, side, order_type, misc = trade_data
            
            trade = Trade(
                exchange=Exchange.KRAKEN,
                symbol=symbol,
                trade_id=f"{time}_{price}_{volume}",  # Kraken doesn't provide trade ID
                price=float(price),
                quantity=float(volume),
                side="buy" if side == "b" else "sell",
                timestamp=datetime.fromtimestamp(float(time)),
                buyer_maker=None
            )
            
            ok = await self.data_pipeline.process_single_message(trade.to_kafka_value())
            if ok:
                await self.metrics_collector.increment_counter("kraken_trades_processed")
            else:
                await self.metrics_collector.increment_counter("kraken_message_errors")
