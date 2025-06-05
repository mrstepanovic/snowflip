"""
Dataset information and utilities for Flipside Crypto data.
"""

from typing import Dict, List, Set
import re


class FlipsideDatasets:
    """
    Helper class for discovering and working with Flipside Crypto datasets.
    
    This class provides information about available blockchain datasets,
    common table patterns, and search functionality.
    """
    
    def __init__(self):
        """Initialize with known dataset information."""
        self.blockchains = {
            'ethereum': {
                'schema': 'ethereum',
                'common_tables': [
                    'fact_transactions',
                    'fact_blocks',
                    'fact_token_transfers',
                    'fact_decoded_event_logs',
                    'fact_traces',
                    'dim_contracts',
                    'dim_labels',
                    'fact_hourly_token_prices',
                    'fact_daily_token_prices',
                ]
            },
            'bitcoin': {
                'schema': 'bitcoin',
                'common_tables': [
                    'fact_transactions',
                    'fact_blocks',
                    'fact_inputs',
                    'fact_outputs',
                ]
            },
            'polygon': {
                'schema': 'polygon',
                'common_tables': [
                    'fact_transactions',
                    'fact_blocks',
                    'fact_token_transfers',
                    'fact_decoded_event_logs',
                    'fact_traces',
                    'dim_contracts',
                    'dim_labels',
                ]
            },
            'avalanche': {
                'schema': 'avalanche',
                'common_tables': [
                    'fact_transactions',
                    'fact_blocks',
                    'fact_token_transfers',
                    'fact_decoded_event_logs',
                    'fact_traces',
                ]
            },
            'bsc': {
                'schema': 'bsc',
                'common_tables': [
                    'fact_transactions',
                    'fact_blocks',
                    'fact_token_transfers',
                    'fact_decoded_event_logs',
                    'fact_traces',
                ]
            },
            'arbitrum': {
                'schema': 'arbitrum',
                'common_tables': [
                    'fact_transactions',
                    'fact_blocks',
                    'fact_token_transfers',
                    'fact_decoded_event_logs',
                    'fact_traces',
                ]
            },
            'optimism': {
                'schema': 'optimism',
                'common_tables': [
                    'fact_transactions',
                    'fact_blocks',
                    'fact_token_transfers',
                    'fact_decoded_event_logs',
                    'fact_traces',
                ]
            },
            'solana': {
                'schema': 'solana',
                'common_tables': [
                    'fact_transactions',
                    'fact_blocks',
                    'fact_transfers',
                    'fact_events',
                ]
            },
            'crosschain': {
                'schema': 'crosschain',
                'common_tables': [
                    'dim_address_labels',
                    'dim_contracts',
                    'fact_hourly_token_prices',
                    'fact_daily_token_prices',
                ]
            }
        }
        
        # Common table patterns and their descriptions
        self.table_patterns = {
            'fact_transactions': 'Core transaction data with basic transaction details',
            'fact_blocks': 'Block-level data including timestamps and metadata',
            'fact_token_transfers': 'ERC-20/token transfer events',
            'fact_decoded_event_logs': 'Decoded smart contract event logs',
            'fact_traces': 'Internal transaction traces',
            'dim_contracts': 'Contract metadata and information',
            'dim_labels': 'Address labels and classifications',
            'fact_hourly_token_prices': 'Hourly token price data',
            'fact_daily_token_prices': 'Daily token price data',
            'fact_inputs': 'Bitcoin transaction inputs',
            'fact_outputs': 'Bitcoin transaction outputs',
            'fact_transfers': 'Solana transfer data',
            'fact_events': 'Solana event data',
        }
    
    def get_available_datasets(self) -> Dict[str, List[str]]:
        """
        Get all available blockchain datasets.
        
        Returns:
            Dictionary mapping blockchain names to their available tables
        """
        result = {}
        for blockchain, info in self.blockchains.items():
            result[blockchain] = info['common_tables']
        return result
    
    def get_blockchain_info(self, blockchain: str) -> Dict[str, any]:
        """
        Get information about a specific blockchain.
        
        Args:
            blockchain: Name of the blockchain
            
        Returns:
            Dictionary with blockchain information
        """
        blockchain = blockchain.lower()
        if blockchain in self.blockchains:
            return self.blockchains[blockchain]
        else:
            raise ValueError(f"Unknown blockchain: {blockchain}. Available: {list(self.blockchains.keys())}")
    
    def search_datasets(self, keyword: str) -> Dict[str, List[str]]:
        """
        Search for datasets containing a specific keyword.
        
        Args:
            keyword: Search term
            
        Returns:
            Dictionary of matching datasets
        """
        keyword = keyword.lower()
        results = {}
        
        for blockchain, info in self.blockchains.items():
            matching_tables = []
            
            # Search in blockchain name
            if keyword in blockchain:
                matching_tables.extend(info['common_tables'])
            else:
                # Search in table names
                for table in info['common_tables']:
                    if keyword in table.lower():
                        matching_tables.append(table)
            
            if matching_tables:
                results[blockchain] = list(set(matching_tables))  # Remove duplicates
        
        return results
    
    def get_table_description(self, table_name: str) -> str:
        """
        Get description for a table pattern.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Description of the table
        """
        return self.table_patterns.get(table_name, "No description available")
    
    def suggest_queries(self, blockchain: str, use_case: str = None) -> List[str]:
        """
        Suggest common queries for a blockchain.
        
        Args:
            blockchain: Blockchain name
            use_case: Optional use case ("defi", "nft", "basic", "price")
            
        Returns:
            List of suggested SQL queries
        """
        blockchain = blockchain.lower()
        
        if blockchain not in self.blockchains:
            raise ValueError(f"Unknown blockchain: {blockchain}")
        
        schema = self.blockchains[blockchain]['schema']
        
        basic_queries = [
            f"SELECT * FROM {schema}.fact_transactions LIMIT 10",
            f"SELECT COUNT(*) as total_transactions FROM {schema}.fact_transactions",
            f"SELECT block_timestamp, COUNT(*) as tx_count FROM {schema}.fact_transactions GROUP BY block_timestamp ORDER BY block_timestamp DESC LIMIT 10",
        ]
        
        if use_case == "defi":
            defi_queries = [
                f"SELECT * FROM {schema}.fact_decoded_event_logs WHERE contract_address = '0x...' LIMIT 10",
                f"SELECT event_name, COUNT(*) FROM {schema}.fact_decoded_event_logs GROUP BY event_name ORDER BY COUNT(*) DESC LIMIT 10",
            ]
            return basic_queries + defi_queries
        
        elif use_case == "nft":
            nft_queries = [
                f"SELECT * FROM {schema}.fact_token_transfers WHERE token_address = '0x...' LIMIT 10",
                f"SELECT token_address, COUNT(*) as transfer_count FROM {schema}.fact_token_transfers GROUP BY token_address ORDER BY transfer_count DESC LIMIT 10",
            ]
            return basic_queries + nft_queries
        
        elif use_case == "price":
            price_queries = [
                f"SELECT * FROM crosschain.fact_hourly_token_prices WHERE token_address = '0x...' ORDER BY hour DESC LIMIT 10",
                f"SELECT symbol, AVG(price) as avg_price FROM crosschain.fact_hourly_token_prices WHERE hour >= CURRENT_DATE - 7 GROUP BY symbol ORDER BY avg_price DESC LIMIT 10",
            ]
            return basic_queries + price_queries
        
        else:
            return basic_queries
    
    def get_connection_examples(self) -> Dict[str, str]:
        """
        Get example connection strings and configurations.
        
        Returns:
            Dictionary with connection examples
        """
        return {
            "environment_variables": """
# Set these environment variables
export SNOWFLAKE_ACCOUNT="your-account.snowflakecomputing.com"
export SNOWFLAKE_USER="your-username"
export SNOWFLAKE_PASSWORD="your-password"
export SNOWFLAKE_WAREHOUSE="your-warehouse"
export SNOWFLAKE_DATABASE="FLIPSIDE_PROD_DB"
""",
            "basic_connection": """
from snowflip import SnowflipClient

# Basic connection
client = SnowflipClient(
    account="your-account.snowflakecomputing.com",
    user="your-username",
    password="your-password",
    warehouse="your-warehouse"
)
""",
            "encrypted_connection": """
from snowflip import SnowflipClient
from snowflip.connection import SnowflakeConnection

# First, encrypt your credentials
encrypted_creds, key = SnowflakeConnection.encrypt_credentials("username", "password")

# Then use encrypted connection
client = SnowflipClient(
    account="your-account.snowflakecomputing.com",
    encrypted_credentials=encrypted_creds,
    encryption_key=key,
    warehouse="your-warehouse"
)
""",
            "context_manager": """
from snowflip import SnowflipClient

# Use context manager for automatic connection handling
with SnowflipClient(account="...", user="...", password="...") as client:
    df = client.query("SELECT * FROM ethereum.fact_transactions LIMIT 10")
    print(df.head())
"""
        }
    
    def get_sample_notebooks(self) -> Dict[str, str]:
        """
        Get sample Jupyter notebook code snippets.
        
        Returns:
            Dictionary with notebook examples
        """
        return {
            "basic_analysis": '''
# Cell 1: Setup
import pandas as pd
from snowflip import SnowflipClient
import matplotlib.pyplot as plt

# Cell 2: Connect
client = SnowflipClient()  # Uses environment variables

# Cell 3: Explore datasets
datasets = client.get_datasets()
print("Available datasets:", list(datasets.keys()))

# Cell 4: Quick data exploration
eth_transactions = client.quick_query("ethereum.fact_transactions", limit=100)
print(f"Shape: {eth_transactions.shape}")
print(eth_transactions.head())

# Cell 5: Custom analysis
daily_tx_count = client.query("""
    SELECT 
        DATE(block_timestamp) as date,
        COUNT(*) as transaction_count
    FROM ethereum.fact_transactions 
    WHERE block_timestamp >= CURRENT_DATE - 30
    GROUP BY DATE(block_timestamp)
    ORDER BY date
""")

# Cell 6: Visualization
plt.figure(figsize=(12, 6))
plt.plot(daily_tx_count['date'], daily_tx_count['transaction_count'])
plt.title('Ethereum Daily Transaction Count (Last 30 Days)')
plt.xlabel('Date')
plt.ylabel('Transaction Count')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
''',
            "defi_analysis": '''
# DeFi Protocol Analysis
uniswap_swaps = client.query("""
    SELECT 
        block_timestamp,
        tx_hash,
        event_inputs:amount0In::FLOAT as amount0_in,
        event_inputs:amount1Out::FLOAT as amount1_out
    FROM ethereum.fact_decoded_event_logs 
    WHERE contract_address = '0x...'  -- Uniswap pair address
    AND event_name = 'Swap'
    AND block_timestamp >= CURRENT_DATE - 7
    ORDER BY block_timestamp DESC
""", limit=1000)

print(f"Found {len(uniswap_swaps)} swaps in the last 7 days")
'''
        } 