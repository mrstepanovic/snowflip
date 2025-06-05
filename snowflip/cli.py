"""
Command-line interface for SnowFlip package.
"""

import argparse
import sys
from typing import Optional
from .client import SnowflipClient
from .datasets import FlipsideDatasets
from .connection import SnowflakeConnection


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="SnowFlip - Access Flipside Crypto datasets via Snowflake",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  snowflip datasets                    # List available datasets
  snowflip search nft                  # Search for NFT-related datasets
  snowflip encrypt-creds               # Generate encrypted credentials
  snowflip examples                    # Show connection examples
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Datasets command
    datasets_parser = subparsers.add_parser('datasets', help='List available datasets')
    datasets_parser.add_argument('--blockchain', '-b', help='Filter by blockchain')
    
    # Search command
    search_parser = subparsers.add_parser('search', help='Search datasets')
    search_parser.add_argument('keyword', help='Search keyword')
    
    # Encrypt credentials command
    encrypt_parser = subparsers.add_parser('encrypt-creds', help='Encrypt credentials')
    encrypt_parser.add_argument('--user', '-u', required=True, help='Username')
    encrypt_parser.add_argument('--password', '-p', required=True, help='Password')
    
    # Examples command
    examples_parser = subparsers.add_parser('examples', help='Show connection examples')
    examples_parser.add_argument('--type', '-t', choices=['env', 'basic', 'encrypted', 'context'], 
                                default='basic', help='Example type')
    
    # Test connection command
    test_parser = subparsers.add_parser('test', help='Test connection')
    test_parser.add_argument('--account', help='Snowflake account')
    test_parser.add_argument('--user', help='Username')
    test_parser.add_argument('--password', help='Password')
    test_parser.add_argument('--warehouse', help='Warehouse')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        if args.command == 'datasets':
            handle_datasets_command(args)
        elif args.command == 'search':
            handle_search_command(args)
        elif args.command == 'encrypt-creds':
            handle_encrypt_command(args)
        elif args.command == 'examples':
            handle_examples_command(args)
        elif args.command == 'test':
            handle_test_command(args)
            
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def handle_datasets_command(args):
    """Handle datasets command."""
    datasets = FlipsideDatasets()
    available = datasets.get_available_datasets()
    
    if args.blockchain:
        blockchain = args.blockchain.lower()
        if blockchain in available:
            print(f"\n{blockchain.upper()} datasets:")
            for table in available[blockchain]:
                description = datasets.get_table_description(table)
                print(f"  {table:<30} - {description}")
        else:
            print(f"Blockchain '{blockchain}' not found. Available: {', '.join(available.keys())}")
    else:
        print("Available Flipside datasets:\n")
        for blockchain, tables in available.items():
            print(f"{blockchain.upper()}:")
            for table in tables:
                print(f"  {table}")
            print()


def handle_search_command(args):
    """Handle search command."""
    datasets = FlipsideDatasets()
    results = datasets.search_datasets(args.keyword)
    
    if not results:
        print(f"No datasets found matching '{args.keyword}'")
        return
    
    print(f"Datasets matching '{args.keyword}':\n")
    for blockchain, tables in results.items():
        print(f"{blockchain.upper()}:")
        for table in tables:
            description = datasets.get_table_description(table)
            print(f"  {table:<30} - {description}")
        print()


def handle_encrypt_command(args):
    """Handle encrypt credentials command."""
    encrypted_creds, key = SnowflakeConnection.encrypt_credentials(args.user, args.password)
    
    print("Encrypted credentials generated successfully!\n")
    print("Encrypted credentials:")
    print(encrypted_creds)
    print("\nEncryption key:")
    print(key)
    print("\nStore these securely and use them with SnowflipClient:")
    print("client = SnowflipClient(")
    print("    account='your-account',")
    print(f"    encrypted_credentials='{encrypted_creds}',")
    print(f"    encryption_key='{key}',")
    print("    warehouse='your-warehouse'")
    print(")")


def handle_examples_command(args):
    """Handle examples command."""
    datasets = FlipsideDatasets()
    examples = datasets.get_connection_examples()
    
    example_type = args.type
    type_map = {
        'env': 'environment_variables',
        'basic': 'basic_connection',
        'encrypted': 'encrypted_connection',
        'context': 'context_manager'
    }
    
    key = type_map.get(example_type, 'basic_connection')
    
    if key in examples:
        print(f"{example_type.title()} connection example:\n")
        print(examples[key])
    else:
        print("Available example types: env, basic, encrypted, context")


def handle_test_command(args):
    """Handle test connection command."""
    print("Testing Snowflake connection...")
    
    try:
        client = SnowflipClient(
            account=args.account,
            user=args.user,
            password=args.password,
            warehouse=args.warehouse,
            auto_connect=False
        )
        
        client.connect()
        print("✓ Connection successful!")
        
        # Try a simple query
        result = client.query("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()", limit=1)
        print(f"✓ Database: {result.iloc[0, 0]}")
        print(f"✓ Schema: {result.iloc[0, 1]}")
        
        client.disconnect()
        print("✓ Connection closed successfully")
        
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main() 