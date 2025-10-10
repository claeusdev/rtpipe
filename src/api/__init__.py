"""
FastAPI server for the Real-Time Market Data Pipeline
Provides REST API endpoints and WebSocket connections for market data
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn

from ..utils.config import Config
from ..storage.manager import StorageManager
from ..monitoring.metrics import MetricsCollector


# Pydantic models for API responses
class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    timestamp: datetime
    version: str
    uptime: float


class SymbolInfo(BaseModel):
    """Symbol information"""
    symbol: str
    exchange: str
    base_currency: str
    quote_currency: str
    status: str


class TradeData(BaseModel):
    """Trade data response"""
    symbol: str
    price: float
    size: float
    side: str
    timestamp: int
    exchange: str


class OrderBookLevel(BaseModel):
    """Order book level"""
    price: float
    size: float


class OrderBookData(BaseModel):
    """Order book response"""
    symbol: str
    bids: List[OrderBookLevel]
    asks: List[OrderBookLevel]
    timestamp: int
    sequence: int


class MetricsResponse(BaseModel):
    """Metrics response"""
    messages_processed: int
    processing_latency_ms: float
    error_count: int
    connection_status: Dict[str, str]
    queue_depth: int


class WebSocketManager:
    """Manages WebSocket connections"""
    
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.logger = logging.getLogger(__name__)
    
    async def connect(self, websocket: WebSocket):
        """Accept a new WebSocket connection"""
        await websocket.accept()
        self.active_connections.append(websocket)
        self.logger.info(f"WebSocket connected. Total connections: {len(self.active_connections)}")
    
    def disconnect(self, websocket: WebSocket):
        """Remove a WebSocket connection"""
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        self.logger.info(f"WebSocket disconnected. Total connections: {len(self.active_connections)}")
    
    async def send_personal_message(self, message: str, websocket: WebSocket):
        """Send message to specific WebSocket"""
        try:
            await websocket.send_text(message)
        except Exception as e:
            self.logger.error(f"Error sending message: {e}")
            self.disconnect(websocket)
    
    async def broadcast(self, message: str):
        """Broadcast message to all connected WebSockets"""
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception as e:
                self.logger.error(f"Error broadcasting to connection: {e}")
                disconnected.append(connection)
        
        # Remove disconnected connections
        for connection in disconnected:
            self.disconnect(connection)


def create_app(
    config: Config,
    storage_manager: StorageManager,
    metrics_collector: MetricsCollector
) -> FastAPI:
    """Create and configure the FastAPI application"""
    
    app = FastAPI(
        title="Real-Time Market Data Pipeline API",
        description="High-performance real-time market data processing system",
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc"
    )
    
    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=config.security.cors.origins,
        allow_credentials=True,
        allow_methods=config.security.cors.methods,
        allow_headers=config.security.cors.headers,
    )
    
    # WebSocket manager
    websocket_manager = WebSocketManager()
    
    # Store dependencies
    app.state.config = config
    app.state.storage_manager = storage_manager
    app.state.metrics_collector = metrics_collector
    app.state.websocket_manager = websocket_manager
    
    @app.get("/", response_model=Dict[str, str])
    async def root():
        """Root endpoint"""
        return {
            "message": "Real-Time Market Data Pipeline API",
            "version": "1.0.0",
            "docs": "/docs"
        }
    
    @app.get("/health", response_model=HealthResponse)
    async def health_check():
        """Health check endpoint"""
        return HealthResponse(
            status="healthy",
            timestamp=datetime.now(timezone.utc),
            version="1.0.0",
            uptime=0.0  # TODO: Implement actual uptime tracking
        )
    
    @app.get("/metrics", response_model=MetricsResponse)
    async def get_metrics():
        """Get system metrics"""
        try:
            # Get metrics from metrics collector
            metrics = await metrics_collector.get_metrics()
            
            return MetricsResponse(
                messages_processed=metrics.get("messages_processed_total", 0),
                processing_latency_ms=metrics.get("processing_latency_ms", 0.0),
                error_count=metrics.get("error_count_total", 0),
                connection_status=metrics.get("connection_status", {}),
                queue_depth=metrics.get("queue_depth", 0)
            )
        except Exception as e:
            logging.error(f"Error getting metrics: {e}")
            raise HTTPException(status_code=500, detail="Failed to retrieve metrics")
    
    @app.get("/symbols", response_model=List[SymbolInfo])
    async def get_symbols():
        """Get available trading symbols"""
        try:
            # Get symbols from Redis
            symbols = await storage_manager.get_active_symbols()
            
            symbol_info = []
            for symbol in symbols:
                metadata = await storage_manager.get_symbol_metadata(symbol)
                symbol_info.append(SymbolInfo(
                    symbol=symbol,
                    exchange=metadata.get("exchange", "unknown"),
                    base_currency=metadata.get("base_currency", ""),
                    quote_currency=metadata.get("quote_currency", ""),
                    status=metadata.get("status", "unknown")
                ))
            
            return symbol_info
        except Exception as e:
            logging.error(f"Error getting symbols: {e}")
            raise HTTPException(status_code=500, detail="Failed to retrieve symbols")
    
    @app.get("/trades/{symbol}", response_model=List[TradeData])
    async def get_recent_trades(symbol: str, limit: int = 100):
        """Get recent trades for a symbol"""
        try:
            trades = await storage_manager.get_recent_trades(symbol, limit)
            
            trade_data = []
            for trade in trades:
                trade_data.append(TradeData(
                    symbol=trade.get("symbol", symbol),
                    price=trade.get("price", 0.0),
                    size=trade.get("size", 0.0),
                    side=trade.get("side", "unknown"),
                    timestamp=trade.get("timestamp", 0),
                    exchange=trade.get("exchange", "unknown")
                ))
            
            return trade_data
        except Exception as e:
            logging.error(f"Error getting trades for {symbol}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to retrieve trades for {symbol}")
    
    @app.get("/orderbook/{symbol}", response_model=OrderBookData)
    async def get_orderbook(symbol: str):
        """Get current order book for a symbol"""
        try:
            orderbook = await storage_manager.get_orderbook(symbol)
            
            if not orderbook:
                raise HTTPException(status_code=404, detail=f"Order book not found for {symbol}")
            
            return OrderBookData(
                symbol=symbol,
                bids=[OrderBookLevel(price=level[0], size=level[1]) for level in orderbook.get("bids", [])],
                asks=[OrderBookLevel(price=level[0], size=level[1]) for level in orderbook.get("asks", [])],
                timestamp=orderbook.get("timestamp", 0),
                sequence=orderbook.get("sequence", 0)
            )
        except HTTPException:
            raise
        except Exception as e:
            logging.error(f"Error getting orderbook for {symbol}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to retrieve order book for {symbol}")
    
    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket):
        """WebSocket endpoint for real-time data"""
        await websocket_manager.connect(websocket)
        
        try:
            while True:
                # Wait for client message (could be subscription request)
                data = await websocket.receive_text()
                
                # Parse subscription request
                try:
                    import json
                    request = json.loads(data)
                    
                    if request.get("type") == "subscribe":
                        symbols = request.get("symbols", [])
                        # TODO: Implement subscription logic
                        await websocket_manager.send_personal_message(
                            json.dumps({"type": "subscribed", "symbols": symbols}),
                            websocket
                        )
                    elif request.get("type") == "ping":
                        await websocket_manager.send_personal_message(
                            json.dumps({"type": "pong"}),
                            websocket
                        )
                except json.JSONDecodeError:
                    await websocket_manager.send_personal_message(
                        json.dumps({"type": "error", "message": "Invalid JSON"}),
                        websocket
                    )
                
        except WebSocketDisconnect:
            websocket_manager.disconnect(websocket)
        except Exception as e:
            logging.error(f"WebSocket error: {e}")
            websocket_manager.disconnect(websocket)
    
    return app


async def run_server(config: Config, storage_manager: StorageManager, metrics_collector: MetricsCollector):
    """Run the API server"""
    app = create_app(config, storage_manager, metrics_collector)
    
    server_config = uvicorn.Config(
        app,
        host=config.api.host,
        port=config.api.port,
        workers=1,  # Use 1 worker for async app
        log_level=config.logging.level.lower(),
        reload=config.api.reload
    )
    
    server = uvicorn.Server(server_config)
    await server.serve()


if __name__ == "__main__":
    # For testing the server directly
    import asyncio
    from ..utils.config import Config
    from ..storage.manager import StorageManager
    from ..monitoring.metrics import MetricsCollector
    
    async def main():
        config = Config()
        storage_manager = StorageManager(config)
        metrics_collector = MetricsCollector(config)
        
        await run_server(config, storage_manager, metrics_collector)
    
    asyncio.run(main())
