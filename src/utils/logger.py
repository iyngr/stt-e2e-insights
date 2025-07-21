"""Logging utility for STT E2E Insights."""

import logging
import structlog
import sys
from pathlib import Path
from typing import Optional

from .config_loader import get_config_section


def setup_logging(config_override: Optional[dict] = None) -> structlog.stdlib.BoundLogger:
    """Setup structured logging for the application.
    
    Args:
        config_override: Optional logging configuration override.
        
    Returns:
        Configured logger instance.
    """
    try:
        log_config = config_override or get_config_section('logging')
    except KeyError:
        # Fallback configuration if logging section is missing
        log_config = {
            'level': 'INFO',
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'file_path': 'logs/stt_insights.log'
        }
    
    # Create logs directory if it doesn't exist
    log_file_path = Path(log_config.get('file_path', 'logs/stt_insights.log'))
    log_file_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Configure logging level
    log_level = getattr(logging, log_config.get('level', 'INFO').upper())
    
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="ISO"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    # Configure standard library logging
    logging.basicConfig(
        format=log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
        level=log_level,
        handlers=[
            logging.FileHandler(log_file_path),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Create and return logger
    logger = structlog.get_logger("stt_insights")
    logger.info("Logging configured", level=log_config.get('level', 'INFO'), file=str(log_file_path))
    
    return logger


def get_logger(name: str = "stt_insights") -> structlog.stdlib.BoundLogger:
    """Get a logger instance with the specified name.
    
    Args:
        name: Logger name.
        
    Returns:
        Logger instance.
    """
    return structlog.get_logger(name)


class LoggerMixin:
    """Mixin class to add logging capabilities to other classes."""
    
    @property
    def logger(self) -> structlog.stdlib.BoundLogger:
        """Get logger instance for this class."""
        if not hasattr(self, '_logger'):
            self._logger = get_logger(self.__class__.__name__)
        return self._logger