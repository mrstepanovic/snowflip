"""
Main client class for SnowFlip package.
"""

import logging
from typing import Optional, Dict, Any, List, Union
import pandas as pd
import sqlparse
from tabulate import tabulate

from .connection import SnowflakeConnection
from .exceptions import QueryError, ConnectionError
from .datasets import FlipsideDatasets

logger = logging.getLogger(__name__)


class SnowflipClient:
    """
    Main client for accessing Flipside Crypto datasets via Snowflake.
    
    This class provides a user-friendly interface for connecting to Snowflake
    and querying Flipside's curated blockchain datasets.
    """
    
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
        auto_connect: bool = True,
    ):
        """
        Initialize SnowFlip client.
        
        Args:
            account: Snowflake account identifier
            user: Username
            password: Password
            warehouse: Snowflake warehouse
            database: Database name (defaults to FLIPSIDE_PROD_DB)
            schema: Schema name (defaults to PUBLIC)
            encrypted_credentials: Pre-encrypted credentials
            encryption_key: Encryption key for credentials
            auto_connect: Whether to automatically connect on initialization
        """
        self.connection = SnowflakeConnection(
            account=account,
            user=user,
            password=password,
            warehouse=warehouse,
            database=database,
            schema=schema,
            encrypted_credentials=encrypted_credentials,
            encryption_key=encryption_key,
        )
        
        self.datasets = FlipsideDatasets()
        
        if auto_connect:
            self.connect()
    
    def connect(self) -> None:
        """Establish connection to Snowflake."""
        self.connection.connect()
    
    def disconnect(self) -> None:
        """Close connection to Snowflake."""
        self.connection.disconnect()
    
    def is_connected(self) -> bool:
        """Check if connected to Snowflake."""
        return self.connection.is_connected()
    
    def query(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None,
        as_dataframe: bool = True,
        limit: Optional[int] = None,
        show_sql: bool = False,
    ) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
        """
        Execute a SQL query against Flipside datasets.
        
        Args:
            sql: SQL query string
            params: Optional parameters for parameterized queries
            as_dataframe: Return results as pandas DataFrame (default: True)
            limit: Optional row limit
            show_sql: Whether to print the formatted SQL query
            
        Returns:
            Query results as DataFrame or list of dictionaries
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to Snowflake. Call connect() first.")
        
        try:
            # Add limit if specified
            if limit:
                sql = f"{sql.rstrip(';')} LIMIT {limit}"
            
            # Show formatted SQL if requested
            if show_sql:
                formatted_sql = sqlparse.format(sql, reindent=True, keyword_case='upper')
                print("Executing SQL:")
                print("=" * 50)
                print(formatted_sql)
                print("=" * 50)
            
            logger.info(f"Executing query with {len(sql)} characters")
            
            # Execute query
            if params:
                self.connection.cursor.execute(sql, params)
            else:
                self.connection.cursor.execute(sql)
            
            # Fetch results
            results = self.connection.cursor.fetchall()
            
            logger.info(f"Query returned {len(results)} rows")
            
            if as_dataframe:
                return pd.DataFrame(results)
            else:
                return results
                
        except Exception as e:
            raise QueryError(f"Query execution failed: {str(e)}")
    
    def quick_query(self, table: str, limit: int = 10, columns: str = "*") -> pd.DataFrame:
        """
        Quick query to explore a table.
        
        Args:
            table: Table name (can include schema prefix)
            limit: Number of rows to return (default: 10)
            columns: Columns to select (default: "*")
            
        Returns:
            DataFrame with query results
        """
        sql = f"SELECT {columns} FROM {table} LIMIT {limit}"
        return self.query(sql)
    
    def describe_table(self, table: str) -> pd.DataFrame:
        """
        Get table schema information.
        
        Args:
            table: Table name
            
        Returns:
            DataFrame with column information
        """
        sql = f"DESCRIBE TABLE {table}"
        return self.query(sql)
    
    def list_tables(self, schema: Optional[str] = None) -> pd.DataFrame:
        """
        List available tables in a schema.
        
        Args:
            schema: Schema name (defaults to current schema)
            
        Returns:
            DataFrame with table information
        """
        if schema:
            sql = f"SHOW TABLES IN SCHEMA {schema}"
        else:
            sql = "SHOW TABLES"
        
        return self.query(sql)
    
    def list_schemas(self, database: Optional[str] = None) -> pd.DataFrame:
        """
        List available schemas in a database.
        
        Args:
            database: Database name (defaults to current database)
            
        Returns:
            DataFrame with schema information
        """
        if database:
            sql = f"SHOW SCHEMAS IN DATABASE {database}"
        else:
            sql = "SHOW SCHEMAS"
        
        return self.query(sql)
    
    def get_datasets(self) -> Dict[str, List[str]]:
        """
        Get information about available Flipside datasets.
        
        Returns:
            Dictionary mapping blockchain names to available tables
        """
        return self.datasets.get_available_datasets()
    
    def search_datasets(self, keyword: str) -> Dict[str, List[str]]:
        """
        Search for datasets containing a keyword.
        
        Args:
            keyword: Search term
            
        Returns:
            Dictionary of matching datasets
        """
        return self.datasets.search_datasets(keyword)
    
    def get_table_info(self, table: str) -> Dict[str, Any]:
        """
        Get comprehensive information about a table.
        
        Args:
            table: Table name
            
        Returns:
            Dictionary with table metadata
        """
        try:
            # Get basic info
            description = self.describe_table(table)
            
            # Get row count
            count_result = self.query(f"SELECT COUNT(*) as row_count FROM {table}")
            row_count = count_result.iloc[0]['row_count']
            
            # Get sample data
            sample_data = self.quick_query(table, limit=5)
            
            return {
                'table_name': table,
                'row_count': row_count,
                'columns': description.to_dict('records'),
                'sample_data': sample_data.to_dict('records'),
            }
            
        except Exception as e:
            raise QueryError(f"Failed to get table info for {table}: {str(e)}")
    
    def format_results(self, df: pd.DataFrame, format_type: str = "table") -> str:
        """
        Format DataFrame results for display.
        
        Args:
            df: DataFrame to format
            format_type: Format type ("table", "grid", "pipe", "html")
            
        Returns:
            Formatted string
        """
        if format_type == "html":
            return df.to_html(index=False)
        else:
            return tabulate(df, headers='keys', tablefmt=format_type, showindex=False)
    
    def __enter__(self):
        """Context manager entry."""
        if not self.is_connected():
            self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
    
    def __repr__(self):
        """String representation."""
        status = "connected" if self.is_connected() else "disconnected"
        return f"SnowflipClient(database={self.connection.database}, status={status})" 