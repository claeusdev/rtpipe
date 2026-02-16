#!/usr/bin/env python3
"""
Real-Time Market Data Pipeline
Main entry point for the application
"""

import asyncio
import signal
import sys
import logging
from typing import Optional
import uvicorn

from src.api.server import create_app
from src.processors.pipeline import DataPipeline
from src.exchanges.manager import ExchangeManager
from src.storage.manager import StorageManager
from src.monitoring.metrics import MetricsCollector
from src.utils.logger import setup_logging
from src.utils.config import Config


class MarketDataPipeline:
    """Main application orchestrator"""
    
    def __init__(self, config_path: str = "config.yaml"):
        self.config = Config(config_path)
        self.logger = setup_logging(self.config)
        
        # Core components
        self.storage_manager = None
        self.exchange_manager = None
        self.data_pipeline = None
        self.metrics_collector = None
        self.api_server = None
        
        # Shutdown event
        self.shutdown_event = asyncio.Event()
        
    async def initialize(self):
        """Initialize all components"""
        self.logger.info("Initializing Market Data Pipeline...")
        
        try:
            # Initialize storage manager
            self.storage_manager = StorageManager(self.config)
            await self.storage_manager.initialize()
            
            # Initialize metrics collector
            self.metrics_collector = MetricsCollector(self.config)
            await self.metrics_collector.initialize()
            
            # Initialize data pipeline
            self.data_pipeline = DataPipeline(
                config=self.config,
                storage_manager=self.storage_manager,
                metrics_collector=self.metrics_collector
            )
            await self.data_pipeline.initialize()
            
            # Initialize exchange manager
            self.exchange_manager = ExchangeManager(
                config=self.config,
                data_pipeline=self.data_pipeline,
                metrics_collector=self.metrics_collector
            )
            await self.exchange_manager.initialize()
            
            # Create API server
            self.api_server = create_app(
                config=self.config,
                storage_manager=self.storage_manager,
                metrics_collector=self.metrics_collector
            )
            
            self.logger.info("All components initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize components: {e}")
            raise
    
    async def start(self):
        """Start all services"""
        self.logger.info("Starting Market Data Pipeline...")
        
        try:
            # Start data pipeline
            await self.data_pipeline.start()
            
            # Start exchange connections
            await self.exchange_manager.start()
            
            # Start metrics collection
            await self.metrics_collector.start()
            
            self.logger.info("Pipeline started successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to start pipeline: {e}")
            raise
    
    async def stop(self):
        """Stop all services gracefully"""
        self.logger.info("Stopping Market Data Pipeline...")
        
        try:
            # Stop in reverse order
            if self.exchange_manager:
                await self.exchange_manager.stop()
                
            if self.data_pipeline:
                await self.data_pipeline.stop()
                
            if self.metrics_collector:
                await self.metrics_collector.stop()
                
            if self.storage_manager:
                await self.storage_manager.close()
                
            self.logger.info("Pipeline stopped successfully")
            
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")
    
    async def run(self):
        """Main application loop"""
        try:
            await self.initialize()
            await self.start()
            
            # Wait for shutdown signal
            await self.shutdown_event.wait()
            
        except Exception as e:
            self.logger.error(f"Pipeline error: {e}")
            raise
        finally:
            await self.stop()
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info(f"Received signal {signum}, initiating shutdown...")
        self.shutdown_event.set()


async def run_api_server(
    config: Config,
    storage_manager: Optional[StorageManager] = None,
    metrics_collector: Optional[MetricsCollector] = None,
    shutdown_event: Optional[asyncio.Event] = None,
):
    """Run the API server with managed or shared dependencies."""
    managed_dependencies = storage_manager is None or metrics_collector is None

    if managed_dependencies:
        storage_manager = StorageManager(config)
        metrics_collector = MetricsCollector(config)
        await storage_manager.initialize()
        await metrics_collector.initialize()
        await metrics_collector.start()

    app = create_app(
        config=config,
        storage_manager=storage_manager,
        metrics_collector=metrics_collector,
    )

    server_config = uvicorn.Config(
        app,
        host=config.api.host,
        port=config.api.port,
        workers=1,  # Use 1 worker for async app
        log_level=config.logging.level.lower(),
        reload=config.api.reload,
    )
    server = uvicorn.Server(server_config)

    shutdown_task = None
    if shutdown_event:
        async def _watch_shutdown():
            await shutdown_event.wait()
            server.should_exit = True
        shutdown_task = asyncio.create_task(_watch_shutdown())

    try:
        await server.serve()
    finally:
        if shutdown_task:
            shutdown_task.cancel()
            await asyncio.gather(shutdown_task, return_exceptions=True)

        if managed_dependencies:
            await metrics_collector.stop()
            await storage_manager.close()


async def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Real-Time Market Data Pipeline")
    parser.add_argument(
        "--config", 
        default="config.yaml", 
        help="Configuration file path"
    )
    parser.add_argument(
        "--mode",
        choices=["pipeline", "api", "both"],
        default="both",
        help="Run mode: pipeline only, API only, or both"
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level"
    )
    
    args = parser.parse_args()
    
    # Setup basic logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    try:
        if args.mode == "api":
            # Run API server only
            config = Config(args.config)
            await run_api_server(config)
            
        elif args.mode == "pipeline":
            # Run pipeline only
            pipeline = MarketDataPipeline(args.config)
            
            # Setup signal handlers
            signal.signal(signal.SIGINT, pipeline.signal_handler)
            signal.signal(signal.SIGTERM, pipeline.signal_handler)
            
            await pipeline.run()
            
        else:  # both
            # Run both pipeline and API server
            pipeline = MarketDataPipeline(args.config)
            
            # Setup signal handlers
            signal.signal(signal.SIGINT, pipeline.signal_handler)
            signal.signal(signal.SIGTERM, pipeline.signal_handler)

            await pipeline.initialize()
            await pipeline.start()
            try:
                await run_api_server(
                    config=pipeline.config,
                    storage_manager=pipeline.storage_manager,
                    metrics_collector=pipeline.metrics_collector,
                    shutdown_event=pipeline.shutdown_event,
                )
            finally:
                await pipeline.stop()
                
    except KeyboardInterrupt:
        logging.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logging.error(f"Application error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Set event loop policy for better performance on Linux
    if sys.platform == "linux":
        asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
    
    asyncio.run(main())
