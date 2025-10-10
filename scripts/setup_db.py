#!/usr/bin/env python3
"""
Database initialization script for the Real-Time Market Data Pipeline
Initializes InfluxDB buckets and Redis data structures
"""

import asyncio
import logging
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import httpx
import redis.asyncio as redis
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from utils.config import Config
from utils.logger import setup_logging


class DatabaseInitializer:
    """Initialize databases for the market data pipeline"""
    
    def __init__(self, config_path: str = "config.yaml"):
        self.config = Config(config_path)
        self.logger = setup_logging(self.config)
        
    async def initialize_influxdb(self) -> bool:
        """Initialize InfluxDB buckets and retention policies"""
        try:
            self.logger.info("Initializing InfluxDB...")
            
            # Create InfluxDB client
            client = InfluxDBClient(
                url=self.config.influxdb.url,
                token=self.config.influxdb.token,
                org=self.config.influxdb.org
            )
            
            # Check if bucket exists, create if not
            buckets_api = client.buckets_api()
            bucket_name = self.config.influxdb.bucket
            
            try:
                bucket = buckets_api.find_bucket_by_name(bucket_name)
                self.logger.info(f"InfluxDB bucket '{bucket_name}' already exists")
            except Exception:
                # Create bucket
                retention_rules = [
                    {
                        "type": "expire",
                        "everySeconds": 7 * 24 * 3600  # 7 days default
                    }
                ]
                
                bucket = buckets_api.create_bucket(
                    bucket_name=bucket_name,
                    org=self.config.influxdb.org,
                    retention_rules=retention_rules
                )
                self.logger.info(f"Created InfluxDB bucket '{bucket_name}' with retention 7 days")
            
            # Test write with current timestamp
            write_api = client.write_api(write_options=SYNCHRONOUS)
            current_time = datetime.now(timezone.utc)
            test_point = {
                "measurement": "test",
                "tags": {"test": "init"},
                "fields": {"value": 1},
                "time": current_time.isoformat()
            }
            write_api.write(bucket=bucket_name, record=test_point)
            
            # Clean up test data
            delete_api = client.delete_api()
            delete_api.delete(
                start=(current_time - timedelta(minutes=1)).isoformat(),
                stop=(current_time + timedelta(minutes=1)).isoformat(),
                predicate='_measurement="test"',
                bucket=bucket_name
            )
            
            client.close()
            self.logger.info("InfluxDB initialization completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize InfluxDB: {e}")
            return False
    
    async def initialize_redis(self) -> bool:
        """Initialize Redis data structures"""
        try:
            self.logger.info("Initializing Redis...")
            
            # Create Redis client
            redis_client = redis.Redis(
                host=self.config.redis.host,
                port=self.config.redis.port,
                password=self.config.redis.password if self.config.redis.password else None,
                decode_responses=True
            )
            
            # Test connection
            await redis_client.ping()
            
            # Initialize Redis data structures
            pipeline = redis_client.pipeline()
            
            # Create sets for active symbols
            pipeline.sadd("active_symbols", "BTC-USD", "ETH-USD", "ADA-USD")
            
            # Create hash for symbol metadata
            pipeline.hset("symbol_metadata:BTC-USD", mapping={
                "exchange": "coinbase",
                "base_currency": "BTC",
                "quote_currency": "USD",
                "min_trade_size": "0.001",
                "status": "active"
            })
            
            pipeline.hset("symbol_metadata:ETH-USD", mapping={
                "exchange": "coinbase",
                "base_currency": "ETH", 
                "quote_currency": "USD",
                "min_trade_size": "0.01",
                "status": "active"
            })
            
            pipeline.hset("symbol_metadata:ADA-USD", mapping={
                "exchange": "coinbase",
                "base_currency": "ADA",
                "quote_currency": "USD", 
                "min_trade_size": "1.0",
                "status": "active"
            })
            
            # Create hash for exchange status
            pipeline.hset("exchange_status", mapping={
                "coinbase": "connected",
                "binance": "connected", 
                "kraken": "connected"
            })
            
            # Create sorted sets for recent trades (with TTL)
            pipeline.zadd("recent_trades:BTC-USD", {"test_trade": 1640995200})
            pipeline.expire("recent_trades:BTC-USD", 3600)  # 1 hour TTL
            
            # Execute pipeline
            await pipeline.execute()
            
            await redis_client.aclose()
            self.logger.info("Redis initialization completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Redis: {e}")
            return False
    
    async def initialize_databases(self) -> bool:
        """Initialize all databases"""
        self.logger.info("Starting database initialization...")
        
        influxdb_success = await self.initialize_influxdb()
        redis_success = await self.initialize_redis()
        
        if influxdb_success and redis_success:
            self.logger.info("All databases initialized successfully!")
            return True
        else:
            failed_dbs = []
            if not influxdb_success:
                failed_dbs.append("InfluxDB")
            if not redis_success:
                failed_dbs.append("Redis")
            
            self.logger.error(f"Database initialization failed for: {', '.join(failed_dbs)}")
            return False


async def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Initialize databases for Market Data Pipeline")
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Configuration file path"
    )
    parser.add_argument(
        "--influxdb-only",
        action="store_true",
        help="Initialize only InfluxDB"
    )
    parser.add_argument(
        "--redis-only", 
        action="store_true",
        help="Initialize only Redis"
    )
    
    args = parser.parse_args()
    
    # Setup basic logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    initializer = DatabaseInitializer(args.config)
    
    try:
        if args.influxdb_only:
            success = await initializer.initialize_influxdb()
        elif args.redis_only:
            success = await initializer.initialize_redis()
        else:
            success = await initializer.initialize_databases()
        
        if success:
            print("✅ Database initialization completed successfully!")
            sys.exit(0)
        else:
            print("❌ Database initialization failed!")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n⚠️  Database initialization interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error during initialization: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
