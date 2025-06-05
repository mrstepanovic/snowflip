#!/usr/bin/env python3
"""
Basic usage example for SnowFlip package.

This example demonstrates the fundamental features of SnowFlip:
- Connecting to Snowflake
- Querying Flipside datasets
- Basic data exploration
"""

import os
from snowflip import SnowflipClient


def main():
    """Main example function."""
    print("SnowFlip Basic Usage Example")
    print("=" * 40)
    
    # Check if environment variables are set
    required_vars = ['SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"Missing environment variables: {', '.join(missing_vars)}")
        print("Please set them and try again.")
        return
    
    try:
        # Create client with auto-connect
        print("1. Connecting to Snowflake...")
        client = SnowflipClient(auto_connect=False)
        client.connect()
        print("✓ Connected successfully!")
        
        # Explore available datasets
        print("\n2. Exploring available datasets...")
        datasets = client.get_datasets()
        print(f"Available blockchains: {', '.join(datasets.keys())}")
        
        # Show Ethereum datasets
        eth_tables = datasets.get('ethereum', [])
        print(f"Ethereum has {len(eth_tables)} tables")
        
        # Quick query example
        print("\n3. Running a quick query...")
        try:
            # Get latest 5 transactions from Ethereum
            latest_txs = client.quick_query("ethereum.fact_transactions", limit=5)
            print(f"Latest transactions shape: {latest_txs.shape}")
            print("Columns:", list(latest_txs.columns))
            
            if not latest_txs.empty:
                print("Sample data:")
                print(latest_txs[['block_number', 'tx_hash', 'from_address', 'to_address']].head())
        
        except Exception as e:
            print(f"Query failed (this might be expected if you don't have access): {e}")
        
        # Search datasets
        print("\n4. Searching datasets...")
        token_datasets = client.search_datasets("token")
        print(f"Found {len(token_datasets)} blockchains with 'token' in their dataset names")
        
        # Show table info
        print("\n5. Getting table information...")
        try:
            table_info = client.describe_table("ethereum.fact_transactions")
            print(f"ethereum.fact_transactions has {len(table_info)} columns")
        except Exception as e:
            print(f"Describe table failed: {e}")
        
        # Custom query example
        print("\n6. Running a custom analytics query...")
        try:
            # Daily transaction count for the last 7 days
            daily_stats = client.query("""
                SELECT 
                    DATE(block_timestamp) as date,
                    COUNT(*) as transaction_count
                FROM ethereum.fact_transactions 
                WHERE block_timestamp >= CURRENT_DATE - 7
                GROUP BY DATE(block_timestamp)
                ORDER BY date DESC
                LIMIT 7
            """)
            
            print("Daily transaction counts (last 7 days):")
            print(daily_stats)
            
        except Exception as e:
            print(f"Custom query failed: {e}")
        
        # Disconnect
        print("\n7. Disconnecting...")
        client.disconnect()
        print("✓ Disconnected successfully!")
        
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main() 