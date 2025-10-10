"""
Market data models for the pipeline
"""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional, Union
from pydantic import BaseModel, Field, validator
import time


class Side(str, Enum):
    """Order/Trade side"""
    BUY = "buy"
    SELL = "sell"
    BID = "bid"
    ASK = "ask"


class MessageType(str, Enum):
    """Message types"""
    TRADE = "trade"
    QUOTE = "quote"
    ORDER_BOOK = "orderbook"
    TICKER = "ticker"
    HEARTBEAT = "heartbeat"
    SNAPSHOT = "snapshot"
    UPDATE = "update"


class Exchange(str, Enum):
    """Supported exchanges"""
    BINANCE = "binance"
    COINBASE = "coinbase"
    KRAKEN = "kraken"


class BaseMessage(BaseModel):
    """Base message structure"""
    message_type: MessageType
    exchange: Exchange
    symbol: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    received_at: datetime = Field(default_factory=datetime.utcnow)
    sequence: Optional[int] = None
    
    @validator('timestamp', 'received_at', pre=True, always=True)
    def parse_timestamp(cls, v):
        if isinstance(v, (int, float)):
            return datetime.fromtimestamp(v / 1000 if v > 1e10 else v)
        elif isinstance(v, str):
            try:
                return datetime.fromisoformat(v.replace('Z', '+00:00'))
            except ValueError:
                return datetime.utcnow()
        return v or datetime.utcnow()
    
    def to_kafka_value(self) -> Dict:
        """Convert to Kafka-serializable format"""
        return self.model_dump(mode='json')


class Trade(BaseMessage):
    """Trade message"""
    message_type: MessageType = MessageType.TRADE
    trade_id: str
    price: Decimal
    quantity: Decimal
    side: Side
    buyer_maker: Optional[bool] = None
    
    @validator('price', 'quantity', pre=True)
    def parse_decimal(cls, v):
        if isinstance(v, str):
            return Decimal(v)
        return Decimal(str(v))
    
    @property
    def notional(self) -> Decimal:
        """Calculate notional value"""
        return self.price * self.quantity
    
    @property
    def is_buy(self) -> bool:
        """Check if trade is a buy"""
        return self.side in [Side.BUY, Side.BID]
    
    @property
    def is_sell(self) -> bool:
        """Check if trade is a sell"""
        return self.side in [Side.SELL, Side.ASK]


class Quote(BaseMessage):
    """Quote/Level 1 message"""
    message_type: MessageType = MessageType.QUOTE
    bid_price: Decimal
    bid_quantity: Decimal
    ask_price: Decimal
    ask_quantity: Decimal
    
    @validator('bid_price', 'bid_quantity', 'ask_price', 'ask_quantity', pre=True)
    def parse_decimal(cls, v):
        if isinstance(v, str):
            return Decimal(v)
        return Decimal(str(v))
    
    @property
    def spread(self) -> Decimal:
        """Calculate bid-ask spread"""
        return self.ask_price - self.bid_price
    
    @property
    def mid_price(self) -> Decimal:
        """Calculate mid price"""
        return (self.bid_price + self.ask_price) / 2
    
    @property
    def spread_bps(self) -> Decimal:
        """Calculate spread in basis points"""
        if self.mid_price > 0:
            return (self.spread / self.mid_price) * 10000
        return Decimal('0')


class OrderBookLevel(BaseModel):
    """Single order book level"""
    price: Decimal
    quantity: Decimal
    
    @validator('price', 'quantity', pre=True)
    def parse_decimal(cls, v):
        if isinstance(v, str):
            return Decimal(v)
        return Decimal(str(v))
    
    def __str__(self) -> str:
        return f"{self.price}@{self.quantity}"


class OrderBook(BaseMessage):
    """Order book message"""
    message_type: MessageType = MessageType.ORDER_BOOK
    bids: List[OrderBookLevel] = Field(default_factory=list)
    asks: List[OrderBookLevel] = Field(default_factory=list)
    is_snapshot: bool = False
    
    @validator('bids', 'asks', pre=True)
    def parse_levels(cls, v):
        if not v:
            return []
        
        levels = []
        for level in v:
            if isinstance(level, (list, tuple)) and len(level) >= 2:
                levels.append(OrderBookLevel(price=level[0], quantity=level[1]))
            elif isinstance(level, dict):
                levels.append(OrderBookLevel(**level))
            elif isinstance(level, OrderBookLevel):
                levels.append(level)
        return levels
    
    def sort_levels(self):
        """Sort bids (desc) and asks (asc)"""
        self.bids.sort(key=lambda x: x.price, reverse=True)
        self.asks.sort(key=lambda x: x.price)
    
    @property
    def best_bid(self) -> Optional[OrderBookLevel]:
        """Get best bid"""
        return self.bids[0] if self.bids else None
    
    @property
    def best_ask(self) -> Optional[OrderBookLevel]:
        """Get best ask"""
        return self.asks[0] if self.asks else None
    
    @property
    def spread(self) -> Optional[Decimal]:
        """Calculate spread"""
        if self.best_bid and self.best_ask:
            return self.best_ask.price - self.best_bid.price
        return None
    
    @property
    def mid_price(self) -> Optional[Decimal]:
        """Calculate mid price"""
        if self.best_bid and self.best_ask:
            return (self.best_bid.price + self.best_ask.price) / 2
        return None
    
    def get_bid_depth(self, levels: int = 5) -> List[OrderBookLevel]:
        """Get top N bid levels"""
        return self.bids[:levels]
    
    def get_ask_depth(self, levels: int = 5) -> List[OrderBookLevel]:
        """Get top N ask levels"""
        return self.asks[:levels]
    
    def total_bid_volume(self, levels: int = None) -> Decimal:
        """Calculate total bid volume"""
        bids_to_use = self.bids[:levels] if levels else self.bids
        return sum(level.quantity for level in bids_to_use)
    
    def total_ask_volume(self, levels: int = None) -> Decimal:
        """Calculate total ask volume"""
        asks_to_use = self.asks[:levels] if levels else self.asks
        return sum(level.quantity for level in asks_to_use)


class Ticker(BaseMessage):
    """Ticker/24hr stats message"""
    message_type: MessageType = MessageType.TICKER
    open_price: Optional[Decimal] = None
    high_price: Optional[Decimal] = None
    low_price: Optional[Decimal] = None
    close_price: Optional[Decimal] = None
    volume: Optional[Decimal] = None
    quote_volume: Optional[Decimal] = None
    price_change: Optional[Decimal] = None
    price_change_percent: Optional[Decimal] = None
    weighted_avg_price: Optional[Decimal] = None
    
    @validator('open_price', 'high_price', 'low_price', 'close_price', 
              'volume', 'quote_volume', 'price_change', 'price_change_percent',
              'weighted_avg_price', pre=True, allow_reuse=True)
    def parse_optional_decimal(cls, v):
        if v is None or v == '':
            return None
        if isinstance(v, str):
            return Decimal(v)
        return Decimal(str(v))


class TradeAggregation(BaseModel):
    """Aggregated trade data"""
    symbol: str
    exchange: Exchange
    interval_start: datetime
    interval_end: datetime
    interval_seconds: int
    
    # OHLCV data
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: Decimal
    quote_volume: Decimal
    
    # Trade statistics
    trade_count: int
    buy_volume: Decimal
    sell_volume: Decimal
    
    # Advanced metrics
    vwap: Optional[Decimal] = None
    typical_price: Optional[Decimal] = None
    
    @validator('open_price', 'high_price', 'low_price', 'close_price',
              'volume', 'quote_volume', 'buy_volume', 'sell_volume',
              'vwap', 'typical_price', pre=True, allow_reuse=True)
    def parse_decimal_fields(cls, v):
        if v is None:
            return None
        if isinstance(v, str):
            return Decimal(v)
        return Decimal(str(v))
    
    @property
    def price_change(self) -> Decimal:
        """Calculate price change"""
        return self.close_price - self.open_price
    
    @property
    def price_change_percent(self) -> Decimal:
        """Calculate price change percentage"""
        if self.open_price > 0:
            return (self.price_change / self.open_price) * 100
        return Decimal('0')


class HeartBeat(BaseMessage):
    """Heartbeat message to track connection health"""
    message_type: MessageType = MessageType.HEARTBEAT
    
    
class ProcessingMetrics(BaseModel):
    """Processing metrics for monitoring"""
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    exchange: Exchange
    symbol: Optional[str] = None
    
    # Latency metrics (microseconds)
    ingestion_latency: Optional[int] = None
    processing_latency: Optional[int] = None
    total_latency: Optional[int] = None
    
    # Throughput metrics
    messages_per_second: Optional[float] = None
    bytes_per_second: Optional[float] = None
    
    # Error metrics
    error_count: int = 0
    error_rate: float = 0.0
    
    # Queue metrics
    queue_depth: int = 0
    
    def calculate_latency(self, start_time: float) -> int:
        """Calculate latency from start time in microseconds"""
        end_time = time.time()
        return int((end_time - start_time) * 1_000_000)


# Type aliases for convenience
MarketDataMessage = Union[Trade, Quote, OrderBook, Ticker, HeartBeat]