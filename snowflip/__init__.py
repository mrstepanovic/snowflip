"""
SnowFlip: A Python package for accessing Flipside Crypto datasets via Snowflake

This package provides a simple interface to connect to and query Flipside Crypto's
curated blockchain datasets available through Snowflake.
"""

__version__ = "0.1.0"
__author__ = "Your Name"
__email__ = "your.email@example.com"

from .client import SnowflipClient
from .connection import SnowflakeConnection
from .exceptions import SnowflipError, ConnectionError, QueryError

__all__ = [
    "SnowflipClient",
    "SnowflakeConnection", 
    "SnowflipError",
    "ConnectionError",
    "QueryError",
] 