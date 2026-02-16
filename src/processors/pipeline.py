"""
Main data processing pipeline for market data
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any
import time
from datetime import datetime

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import orjson

from ..models.market_data import MarketDataMessage, Trade, Quote, OrderBook, MessageType


class DataPipeline:
    """Core data processing pipeline"""
    
    def __init__(self, config, storage_manager, metrics_collector):
        self.config = config
        self.storage_manager = storage_manager
        self.metrics_collector = metrics_collector
        self.logger = logging.getLogger(__name__)
        
        # Kafka components
        self.consumer = None
        self.producer = None
        
        # Processing state
        self.is_running = False
        self.processing_tasks = []
        
        # Message buffers
        self.message_buffer = []
        self.buffer_size = config.processing.batch_size
        self.last_flush = time.time()
        self._flush_lock = asyncio.Lock()
        
    async def initialize(self):
        """Initialize the data pipeline"""
        self.logger.info("Initializing data pipeline...")
        
        # Initialize Kafka consumer
        self.consumer = AIOKafkaConsumer(
            self.config.kafka.topics.trades,
            self.config.kafka.topics.quotes,
            self.config.kafka.topics.orderbook,
            bootstrap_servers=self.config.kafka.bootstrap_servers,
            group_id=self.config.kafka.consumer_group,
            auto_offset_reset=self.config.kafka.auto_offset_reset,
            value_deserializer=lambda x: orjson.loads(x) if x else None
        )
        
        # Initialize Kafka producer
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.config.kafka.bootstrap_servers,
            value_serializer=lambda x: orjson.dumps(x)
        )
        
        await self.consumer.start()
        await self.producer.start()
        
        self.logger.info("Data pipeline initialized successfully")
    
    async def start(self):
        """Start the data pipeline"""
        self.logger.info("Starting data pipeline...")
        self.is_running = True
        
        # Start processing tasks
        self.processing_tasks = [
            asyncio.create_task(self._consume_messages()),
            asyncio.create_task(self._flush_buffer_periodically()),
            asyncio.create_task(self._update_metrics())
        ]
        
        self.logger.info("Data pipeline started")
    
    async def stop(self):
        """Stop the data pipeline"""
        self.logger.info("Stopping data pipeline...")
        self.is_running = False
        
        # Cancel processing tasks
        for task in self.processing_tasks:
            task.cancel()
        
        # Wait for tasks to complete
        await asyncio.gather(*self.processing_tasks, return_exceptions=True)
        
        # Flush remaining messages
        if self.message_buffer:
            await self._flush_buffer()
        
        # Stop Kafka components
        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()
        
        self.logger.info("Data pipeline stopped")
    
    async def _consume_messages(self):
        """Consume messages from Kafka topics"""
        try:
            async for message in self.consumer:
                if not self.is_running:
                    break
                
                start_time = time.time()
                
                try:
                    # Deserialize and validate message
                    processed_message = await self._process_message(
                        message.topic, message.value
                    )
                    
                    if processed_message:
                        # Add to buffer
                        self.message_buffer.append(processed_message)
                        
                        # Flush buffer if full
                        if len(self.message_buffer) >= self.buffer_size:
                            await self._flush_buffer()
                    
                    # Record processing latency
                    latency = (time.time() - start_time) * 1000000  # microseconds
                    await self.metrics_collector.record_latency('message_processing', latency)
                    
                except Exception as e:
                    self.logger.error(f"Error processing message: {e}")
                    await self.metrics_collector.increment_counter('processing_errors')
                    
        except Exception as e:
            self.logger.error(f"Consumer error: {e}")
    
    async def _process_message(self, topic: str, data: Dict[str, Any]) -> Optional[MarketDataMessage]:
        """Process individual message"""
        if not data:
            return None
        
        try:
            # Determine message type based on topic
            if topic == self.config.kafka.topics.trades:
                return self._parse_trade_message(data)
            elif topic == self.config.kafka.topics.quotes:
                return self._parse_quote_message(data)
            elif topic == self.config.kafka.topics.orderbook:
                return self._parse_orderbook_message(data)
            else:
                self.logger.warning(f"Unknown topic: {topic}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error parsing message: {e}")
            return None
    
    def _parse_trade_message(self, data: Dict[str, Any]) -> Trade:
        """Parse trade message"""
        timestamp = str(data.get('timestamp', datetime.utcnow().isoformat()))
        return Trade(
            exchange=data.get('exchange'),
            symbol=data.get('symbol'),
            trade_id=data.get('trade_id'),
            price=data.get('price'),
            quantity=data.get('quantity'),
            side=data.get('side'),
            timestamp=datetime.fromisoformat(timestamp.replace('Z', '+00:00')),
            buyer_maker=data.get('buyer_maker')
        )
    
    def _parse_quote_message(self, data: Dict[str, Any]) -> Quote:
        """Parse quote message"""
        timestamp = str(data.get('timestamp', datetime.utcnow().isoformat()))
        return Quote(
            exchange=data.get('exchange'),
            symbol=data.get('symbol'),
            bid_price=data.get('bid_price'),
            bid_quantity=data.get('bid_quantity'),
            ask_price=data.get('ask_price'),
            ask_quantity=data.get('ask_quantity'),
            timestamp=datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        )
    
    def _parse_orderbook_message(self, data: Dict[str, Any]) -> OrderBook:
        """Parse order book message"""
        timestamp = str(data.get('timestamp', datetime.utcnow().isoformat()))
        return OrderBook(
            exchange=data.get('exchange'),
            symbol=data.get('symbol'),
            bids=data.get('bids', []),
            asks=data.get('asks', []),
            is_snapshot=data.get('is_snapshot', False),
            timestamp=datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        )

    def _infer_message_type(self, data: Dict[str, Any]) -> Optional[MessageType]:
        """Infer message type for direct (non-Kafka) payloads."""
        message_type = data.get("message_type")
        if message_type:
            try:
                return MessageType(str(message_type))
            except ValueError:
                self.logger.warning("Unknown message_type field: %s", message_type)

        if "trade_id" in data:
            return MessageType.TRADE
        if "bid_price" in data and "ask_price" in data:
            return MessageType.QUOTE
        if "bids" in data and "asks" in data:
            return MessageType.ORDER_BOOK
        return None

    async def _process_direct_message(self, data: Dict[str, Any]) -> Optional[MarketDataMessage]:
        """Process direct payloads produced by exchange connectors."""
        message_type = self._infer_message_type(data)
        if message_type == MessageType.TRADE:
            return self._parse_trade_message(data)
        if message_type == MessageType.QUOTE:
            return self._parse_quote_message(data)
        if message_type == MessageType.ORDER_BOOK:
            return self._parse_orderbook_message(data)

        self.logger.warning("Unable to infer direct message type from payload keys")
        return None
    
    async def _flush_buffer(self) -> bool:
        """Flush message buffer to storage"""
        async with self._flush_lock:
            if not self.message_buffer:
                return True

            start_time = time.time()
            buffer_to_flush = list(self.message_buffer)

            try:
                # Group messages by type
                trades = []
                quotes = []
                orderbooks = []

                for message in buffer_to_flush:
                    if message.message_type == MessageType.TRADE:
                        trades.append(message)
                    elif message.message_type == MessageType.QUOTE:
                        quotes.append(message)
                    elif message.message_type == MessageType.ORDER_BOOK:
                        orderbooks.append(message)

                # Store in parallel and fail fast if any backend errors.
                storage_tasks = []
                if trades:
                    storage_tasks.append(self.storage_manager.store_trades(trades))
                if quotes:
                    storage_tasks.append(self.storage_manager.store_quotes(quotes))
                if orderbooks:
                    storage_tasks.append(self.storage_manager.store_orderbooks(orderbooks))

                if storage_tasks:
                    await asyncio.gather(*storage_tasks)

                # Remove only the messages we successfully flushed.
                del self.message_buffer[:len(buffer_to_flush)]
                message_count = len(buffer_to_flush)
                self.last_flush = time.time()

                # Record metrics
                flush_latency = (time.time() - start_time) * 1000000  # microseconds
                await self.metrics_collector.record_latency('buffer_flush', flush_latency)
                await self.metrics_collector.record_gauge('messages_flushed', message_count)

                self.logger.debug(f"Flushed {message_count} messages in {flush_latency:.2f}μs")
                return True

            except Exception as e:
                self.logger.error("Error flushing buffer: %s", e)
                await self.metrics_collector.increment_counter('processing_errors')
                return False
    
    async def _flush_buffer_periodically(self):
        """Periodically flush buffer based on time interval"""
        while self.is_running:
            try:
                await asyncio.sleep(self.config.processing.flush_interval / 1000)  # Convert to seconds
                
                time_since_flush = time.time() - self.last_flush
                if time_since_flush * 1000 >= self.config.processing.flush_interval:
                    await self._flush_buffer()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in periodic flush: {e}")
    
    async def _update_metrics(self):
        """Update pipeline metrics"""
        while self.is_running:
            try:
                await asyncio.sleep(1)  # Update every second
                
                # Record buffer depth
                await self.metrics_collector.record_gauge(
                    'buffer_depth', len(self.message_buffer)
                )
                
                # Record processing status
                await self.metrics_collector.record_gauge(
                    'pipeline_status', 1 if self.is_running else 0
                )
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error updating metrics: {e}")
    
    async def process_single_message(self, message_data: Dict[str, Any]) -> bool:
        """Process a single direct message from exchange connectors."""
        try:
            processed = await self._process_direct_message(message_data)
            if processed:
                self.message_buffer.append(processed)
                if len(self.message_buffer) >= self.buffer_size:
                    return await self._flush_buffer()
                return True
            return False
        except Exception as e:
            self.logger.error(f"Error processing single message: {e}")
            return False
