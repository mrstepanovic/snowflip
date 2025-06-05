# SnowFlip 🏂

**Quick and easy access to Flipside Crypto's blockchain datasets via Snowflake**

[![PyPI version](https://badge.fury.io/py/snowflip.svg)](https://badge.fury.io/py/snowflip)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)

## 🚀 Quick Start

### 1. Install
```bash
pip install snowflip
```

### 2. Set Up Snowflake Access to Flipside Data

You need your own Snowflake account and access to Flipside datasets through the Snowflake Marketplace:

**Step 1: Get a Snowflake Account**
- Sign up for a free Snowflake account at [snowflake.com](https://snowflake.com)
- Note your account URL (e.g., `abc12345.us-east-1.snowflakecomputing.com`)

**Step 2: Add Flipside Datasets from Snowflake Marketplace**
1. Log into your Snowflake account
2. Go to **Marketplace** → Search for "Flipside Crypto"
3. Add the free Flipside datasets you need:
   - [Flipside Crypto: Ethereum Core](https://app.snowflake.com/marketplace/listing/GZT0Z438N7EA6/flipside-crypto-ethereum-core-onchain-data)
   - [Flipside Crypto: Bitcoin Core](https://app.snowflake.com/marketplace/listing/GZT0Z438N7EA6/flipside-crypto-bitcoin-core-onchain-data)
   - [Flipside Crypto: Polygon Core](https://app.snowflake.com/marketplace/listing/GZT0Z438N7EA6/flipside-crypto-polygon-core-onchain-data)
   - And others as needed
4. Click **Get** on each dataset (they're free)

**Step 3: Find Your Database Names**
After adding datasets, check what they're called in your account:
- In Snowflake, run: `SHOW DATABASES;`
- Look for databases like `FLIPSIDE_PROD_DB`, `ETHEREUM`, or similar

### 3. Set Environment Variables

Use your **own Snowflake credentials**:

```bash
export SNOWFLAKE_ACCOUNT="abc12345.us-east-1.snowflakecomputing.com"  # Your account URL
export SNOWFLAKE_USER="your-snowflake-username"
export SNOWFLAKE_PASSWORD="your-snowflake-password"
export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"  # Or your warehouse name
export SNOWFLAKE_DATABASE="FLIPSIDE_PROD_DB"  # Check what your database is called
```

Or copy and edit the config template:
```bash
cp config_template.env .env
# Edit .env with your credentials
source .env
```

### 4. Start Analyzing
```python
from snowflip import SnowflipClient

# Connect using your Snowflake account
client = SnowflipClient()

# First, see what databases you have access to
databases = client.query("SHOW DATABASES")
print("Available databases:")
print(databases[['name']].head(10))

# Check available schemas in your Flipside database
schemas = client.list_schemas("FLIPSIDE_PROD_DB")  # Or whatever your DB is called
print("Available schemas:")
print(schemas[['name']].head())

# Query Ethereum transactions
eth_txs = client.query("""
    SELECT 
        block_timestamp,
        from_address, 
        to_address,
        eth_value
    FROM ethereum.fact_transactions 
    WHERE block_timestamp >= CURRENT_DATE - 1
    LIMIT 100
""")

print(f"Found {len(eth_txs)} transactions")
print(eth_txs.head())
```

## 📊 Available Datasets

Once you've added Flipside datasets from the Snowflake Marketplace:

| Blockchain | Example Tables |
|------------|---------------|
| **Ethereum** | `ethereum.fact_transactions`, `ethereum.fact_token_transfers`, `ethereum.fact_decoded_event_logs` |
| **Bitcoin** | `bitcoin.fact_transactions`, `bitcoin.fact_blocks` |
| **Polygon** | `polygon.fact_transactions`, `polygon.fact_token_transfers` |
| **Solana** | `solana.fact_transactions`, `solana.fact_transfers` |
| **Arbitrum** | `arbitrum.fact_transactions`, `arbitrum.fact_token_transfers` |
| **Optimism** | `optimism.fact_transactions`, `optimism.fact_token_transfers` |
| **Avalanche** | `avalanche.fact_transactions`, `avalanche.fact_token_transfers` |
| **BSC** | `bsc.fact_transactions`, `bsc.fact_token_transfers` |
| **Cross-chain** | `crosschain.fact_hourly_token_prices`, `crosschain.dim_address_labels` |

**Note**: Table names may vary depending on how Flipside structures their marketplace offerings. Use `client.list_tables()` to see what's actually available in your account.

## 🔧 Common Use Cases

### Quick Data Exploration
```python
# See what's available in your account
databases = client.query("SHOW DATABASES")
print(databases[['name']])

# Explore tables in a specific schema
tables = client.list_tables("ethereum")
print(tables[['name']].head())

# Get latest transactions
latest = client.quick_query("ethereum.fact_transactions", limit=10)

# Describe table structure  
schema = client.describe_table("ethereum.fact_transactions")
```

### DeFi Analysis
```python
# Uniswap swaps in the last 24 hours
swaps = client.query("""
    SELECT 
        block_timestamp,
        tx_hash,
        event_inputs
    FROM ethereum.fact_decoded_event_logs 
    WHERE contract_address = '0x...'  -- Uniswap pair
    AND event_name = 'Swap'
    AND block_timestamp >= CURRENT_DATE - 1
""")
```

### Price Analysis
```python
# Token prices over time
prices = client.query("""
    SELECT 
        hour,
        symbol,
        price
    FROM crosschain.fact_hourly_token_prices 
    WHERE symbol IN ('ETH', 'BTC', 'MATIC')
    AND hour >= CURRENT_DATE - 7
    ORDER BY hour
""")
```

### Cross-Chain Comparison
```python
# Compare transaction volumes
volumes = client.query("""
    SELECT 'Ethereum' as chain, COUNT(*) as daily_txs
    FROM ethereum.fact_transactions 
    WHERE block_timestamp >= CURRENT_DATE - 1
    
    UNION ALL
    
    SELECT 'Polygon' as chain, COUNT(*) as daily_txs  
    FROM polygon.fact_transactions
    WHERE block_timestamp >= CURRENT_DATE - 1
""")
```

## 🔐 Secure Credentials

For production use, encrypt your credentials:

```python
from snowflip.connection import SnowflakeConnection

# Encrypt once
encrypted_creds, key = SnowflakeConnection.encrypt_credentials(
    "your-snowflake-username", "your-snowflake-password"
)

# Use encrypted credentials
client = SnowflipClient(
    account="abc12345.us-east-1.snowflakecomputing.com",
    encrypted_credentials=encrypted_creds,
    encryption_key=key,
    warehouse="COMPUTE_WH"
)
```

## 🛠️ CLI Tools

```bash
# List available datasets in your account
snowflip datasets

# Search for specific data
snowflip search defi

# Test your connection
snowflip test --account abc12345.us-east-1.snowflakecomputing.com --user myuser --password mypass --warehouse COMPUTE_WH

# Get connection examples
snowflip examples
```

## 📝 Jupyter Notebooks

Perfect for data analysis workflows:

```python
import pandas as pd
import matplotlib.pyplot as plt
from snowflip import SnowflipClient

client = SnowflipClient()

# Daily transaction trends
daily_stats = client.query("""
    SELECT 
        DATE(block_timestamp) as date,
        COUNT(*) as tx_count
    FROM ethereum.fact_transactions 
    WHERE block_timestamp >= CURRENT_DATE - 30
    GROUP BY date
    ORDER BY date
""")

# Plot the results
plt.plot(daily_stats['date'], daily_stats['tx_count'])
plt.title('Ethereum Daily Transactions')
plt.show()
```

## 🆘 Troubleshooting

**Getting Started Issues:**
1. **No Flipside data?** → Make sure you've added Flipside datasets from the Snowflake Marketplace
2. **Table not found?** → Check your database/schema names with `SHOW DATABASES` and `SHOW SCHEMAS`
3. **Wrong table names?** → Use `client.list_tables()` to see what's actually available

**Connection Issues:**
- Verify your Snowflake account credentials are correct
- Check your account URL format: `account-name.region.snowflakecomputing.com`
- Ensure your warehouse is running: `ALTER WAREHOUSE COMPUTE_WH RESUME;`

**Query Issues:**
- Start with small LIMIT clauses for testing
- Use `client.describe_table()` to see available columns
- Check table names with `client.list_tables()`

**Need Help?**
- Check examples in the `examples/` folder
- Open an issue on GitHub
- Use `snowflip --help` for CLI options

## 📄 License

MIT License - see [LICENSE](LICENSE) file.

---

**Happy analyzing! 🚀📊**
