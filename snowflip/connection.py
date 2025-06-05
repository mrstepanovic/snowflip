"""
Snowflake connection management for SnowFlip package.
"""

import os
import logging
from typing import Optional, Dict, Any
from cryptography.fernet import Fernet
import snowflake.connector
from snowflake.connector import DictCursor

from .exceptions import ConnectionError, AuthenticationError, ConfigurationError

logger = logging.getLogger(__name__)


class SnowflakeConnection:
    """Manages Snowflake database connections with encrypted credentials."""
    
    def __init__(
        self,
        account: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        encrypted_credentials: Optional[str] = None,
        encryption_key: Optional[str] = None,
    ):
        """
        Initialize Snowflake connection.
        
        Args:
            account: Snowflake account identifier
            user: Username
            password: Password (will be encrypted if encryption_key provided)
            warehouse: Snowflake warehouse
            database: Database name
            schema: Schema name
            encrypted_credentials: Pre-encrypted credentials string
            encryption_key: Key for encrypting/decrypting credentials
        """
        self.connection = None
        self.cursor = None
        
        # Load from environment variables if not provided
        self.account = account or os.getenv('SNOWFLAKE_ACCOUNT')
        self.user = user or os.getenv('SNOWFLAKE_USER')
        self.warehouse = warehouse or os.getenv('SNOWFLAKE_WAREHOUSE')
        self.database = database or os.getenv('SNOWFLAKE_DATABASE', 'FLIPSIDE_PROD_DB')
        self.schema = schema or os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
        
        # Handle encrypted credentials
        if encrypted_credentials and encryption_key:
            self._decrypt_credentials(encrypted_credentials, encryption_key)
        else:
            self.password = password or os.getenv('SNOWFLAKE_PASSWORD')
            
        # Validate required parameters
        self._validate_connection_params()
    
    def _validate_connection_params(self):
        """Validate that all required connection parameters are provided."""
        required_params = ['account', 'user', 'password']
        missing_params = [param for param in required_params if not getattr(self, param)]
        
        if missing_params:
            raise ConfigurationError(
                f"Missing required connection parameters: {', '.join(missing_params)}. "
                "Please provide them as arguments or set the corresponding environment variables: "
                "SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD"
            )
    
    def _decrypt_credentials(self, encrypted_credentials: str, encryption_key: str):
        """Decrypt stored credentials."""
        try:
            f = Fernet(encryption_key.encode())
            decrypted = f.decrypt(encrypted_credentials.encode()).decode()
            
            # Parse decrypted credentials (expected format: user:password)
            if ':' in decrypted:
                self.user, self.password = decrypted.split(':', 1)
            else:
                raise ConfigurationError("Invalid encrypted credentials format")
                
        except Exception as e:
            raise AuthenticationError(f"Failed to decrypt credentials: {str(e)}")
    
    @staticmethod
    def encrypt_credentials(user: str, password: str, encryption_key: Optional[str] = None) -> tuple:
        """
        Encrypt user credentials for secure storage.
        
        Args:
            user: Username
            password: Password
            encryption_key: Optional encryption key. If not provided, generates a new one.
            
        Returns:
            Tuple of (encrypted_credentials, encryption_key)
        """
        if not encryption_key:
            encryption_key = Fernet.generate_key().decode()
        
        f = Fernet(encryption_key.encode())
        credentials = f"{user}:{password}"
        encrypted = f.encrypt(credentials.encode()).decode()
        
        return encrypted, encryption_key
    
    def connect(self) -> None:
        """Establish connection to Snowflake."""
        try:
            logger.info(f"Connecting to Snowflake account: {self.account}")
            
            connection_params = {
                'account': self.account,
                'user': self.user,
                'password': self.password,
                'warehouse': self.warehouse,
                'database': self.database,
                'schema': self.schema,
            }
            
            # Remove None values
            connection_params = {k: v for k, v in connection_params.items() if v is not None}
            
            self.connection = snowflake.connector.connect(**connection_params)
            self.cursor = self.connection.cursor(DictCursor)
            
            logger.info("Successfully connected to Snowflake")
            
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Snowflake: {str(e)}")
    
    def disconnect(self) -> None:
        """Close the Snowflake connection."""
        if self.cursor:
            self.cursor.close()
            self.cursor = None
            
        if self.connection:
            self.connection.close()
            self.connection = None
            
        logger.info("Disconnected from Snowflake")
    
    def is_connected(self) -> bool:
        """Check if connection is active."""
        return self.connection is not None and not self.connection.is_closed()
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect() 