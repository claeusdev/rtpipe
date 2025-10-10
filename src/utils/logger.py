"""
Logging utilities for the real-time pipeline
"""

import logging
import sys
from typing import Optional
import structlog
from pathlib import Path


def setup_logging(config) -> logging.Logger:
    """Setup structured logging for the application"""
    
    # Create logs directory
    log_file = Path(config.logging.file)
    log_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Setup standard logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, config.logging.level.upper())
    )
    
    # Create file handler
    file_handler = logging.FileHandler(config.logging.file)
    file_handler.setLevel(getattr(logging, config.logging.level.upper()))
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    
    # Get root logger and add handler
    logger = logging.getLogger("market_data_pipeline")
    logger.addHandler(file_handler)
    logger.setLevel(getattr(logging, config.logging.level.upper()))
    
    return logger