"""
Custom exceptions for the SnowFlip package.
"""


class SnowflipError(Exception):
    """Base exception class for SnowFlip package."""
    pass


class ConnectionError(SnowflipError):
    """Raised when there's an error connecting to Snowflake."""
    pass


class QueryError(SnowflipError):
    """Raised when there's an error executing a SQL query."""
    pass


class AuthenticationError(SnowflipError):
    """Raised when authentication fails."""
    pass


class ConfigurationError(SnowflipError):
    """Raised when there's a configuration error."""
    pass 