#!/usr/bin/env python3
"""
Test data generation script for the Real-Time Market Data Pipeline
Generates sample market data for testing and development
"""

import asyncio
import json
import random
import sys
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from utils.config import Config
from utils.logger import setup_logging


class TestDataGenerator:
    """Generate test market data for development and testing"""
    
    def __init__(self, config_path: str = "config.yaml"):
        self.config = Config(config_path)
        self.logger = setup_logging(self.config)
        
        # Sample symbols and their characteristics
        self.symbols = {
            "BTC-USD": {"base_price": 45000, "volatility": 0.02},
            "ETH-USD": {"base_price": 3000, "volatility": 0.03},
            "ADA-USD": {"base_price": 0.5, "volatility": 0.04},
            "SOL-USD": {"base_price": 100, "volatility": 0.05},
            "DOT-USD": {"base_price": 8, "volatility": 0.04},
        }
        
        self.exchanges = ["coinbase", "binance", "kraken"]
        
    def generate_trade_data(self, symbol: str, count: int = 1000) -> List[Dict[str, Any]]:
        """Generate sample trade data for a symbol"""
        trades = []
        symbol_info = self.symbols[symbol]
        base_price = symbol_info["base_price"]
        volatility = symbol_info["volatility"]
        
        current_price = base_price
        
        for i in range(count):
            # Generate price movement
            price_change = random.gauss(0, volatility * current_price)
            current_price += price_change
            
            # Ensure price stays positive
            current_price = max(current_price, base_price * 0.1)
            
            # Generate trade size
            size = random.uniform(0.001, 10.0)
            
            # Generate trade
            trade = {
                "symbol": symbol,
                "price": round(current_price, 2),
                "size": round(size, 6),
                "side": random.choice(["buy", "sell"]),
                "exchange": random.choice(self.exchanges),
                "timestamp": int(time.time() * 1000) - (count - i) * 1000,  # Spread over time
                "trade_id": f"{symbol}_{i}_{int(time.time())}",
                "maker_order_id": f"maker_{random.randint(1000, 9999)}",
                "taker_order_id": f"taker_{random.randint(1000, 9999)}"
            }
            
            trades.append(trade)
            
        return trades
    
    def generate_orderbook_data(self, symbol: str) -> Dict[str, Any]:
        """Generate sample order book data"""
        symbol_info = self.symbols[symbol]
        base_price = symbol_info["base_price"]
        
        # Generate bids (buy orders)
        bids = []
        current_price = base_price * 0.99
        
        for i in range(20):
            price = round(current_price - (i * base_price * 0.001), 2)
            size = round(random.uniform(0.1, 5.0), 4)
            bids.append([price, size])
        
        # Generate asks (sell orders)
        asks = []
        current_price = base_price * 1.01
        
        for i in range(20):
            price = round(current_price + (i * base_price * 0.001), 2)
            size = round(random.uniform(0.1, 5.0), 4)
            asks.append([price, size])
        
        return {
            "symbol": symbol,
            "bids": bids,
            "asks": asks,
            "timestamp": int(time.time() * 1000),
            "sequence": random.randint(1000, 9999)
        }
    
    def generate_ticker_data(self, symbol: str) -> Dict[str, Any]:
        """Generate sample ticker data"""
        symbol_info = self.symbols[symbol]
        base_price = symbol_info["base_price"]
        
        # Generate price with some randomness
        price = base_price * random.uniform(0.95, 1.05)
        
        return {
            "symbol": symbol,
            "price": round(price, 2),
            "bid": round(price * 0.999, 2),
            "ask": round(price * 1.001, 2),
            "volume_24h": round(random.uniform(1000000, 10000000), 2),
            "volume_30d": round(random.uniform(30000000, 300000000), 2),
            "high_24h": round(price * 1.05, 2),
            "low_24h": round(price * 0.95, 2),
            "timestamp": int(time.time() * 1000)
        }
    
    def save_to_csv(self, data: List[Dict[str, Any]], filename: str):
        """Save data to CSV file"""
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False)
        self.logger.info(f"Saved {len(data)} records to {filename}")
    
    def save_to_json(self, data: Any, filename: str):
        """Save data to JSON file"""
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        self.logger.info(f"Saved data to {filename}")
    
    def generate_all_data(self, output_dir: str = "test_data"):
        """Generate all types of test data"""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        self.logger.info("Generating test data...")
        
        # Generate trade data for each symbol
        for symbol in self.symbols.keys():
            trades = self.generate_trade_data(symbol, count=5000)
            self.save_to_csv(trades, output_path / f"trades_{symbol.replace('-', '_')}.csv")
        
        # Generate order book data
        orderbooks = {}
        for symbol in self.symbols.keys():
            orderbooks[symbol] = self.generate_orderbook_data(symbol)
        self.save_to_json(orderbooks, output_path / "orderbooks.json")
        
        # Generate ticker data
        tickers = {}
        for symbol in self.symbols.keys():
            tickers[symbol] = self.generate_ticker_data(symbol)
        self.save_to_json(tickers, output_path / "tickers.json")
        
        # Generate sample configuration
        sample_config = {
            "symbols": list(self.symbols.keys()),
            "exchanges": self.exchanges,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_trades": sum(len(self.generate_trade_data(symbol, 1)) for symbol in self.symbols.keys())
        }
        self.save_to_json(sample_config, output_path / "sample_config.json")
        
        self.logger.info(f"Test data generation completed. Files saved to {output_path}")


async def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate test data for Market Data Pipeline")
    parser.add_argument(
        "--config",
        default="config.yaml", 
        help="Configuration file path"
    )
    parser.add_argument(
        "--output-dir",
        default="test_data",
        help="Output directory for generated data"
    )
    parser.add_argument(
        "--symbol",
        help="Generate data for specific symbol only"
    )
    parser.add_argument(
        "--count",
        type=int,
        default=5000,
        help="Number of trades to generate per symbol"
    )
    
    args = parser.parse_args()
    
    # Setup basic logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    generator = TestDataGenerator(args.config)
    
    try:
        if args.symbol:
            # Generate data for specific symbol
            if args.symbol in generator.symbols:
                trades = generator.generate_trade_data(args.symbol, args.count)
                generator.save_to_csv(trades, f"test_data/trades_{args.symbol.replace('-', '_')}.csv")
                print(f"✅ Generated {len(trades)} trades for {args.symbol}")
            else:
                print(f"❌ Unknown symbol: {args.symbol}")
                print(f"Available symbols: {', '.join(generator.symbols.keys())}")
                sys.exit(1)
        else:
            # Generate all data
            generator.generate_all_data(args.output_dir)
            print("✅ Test data generation completed successfully!")
        
    except KeyboardInterrupt:
        print("\n⚠️  Test data generation interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error during generation: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
