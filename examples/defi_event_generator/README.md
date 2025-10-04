# DeltaStream SDK DeFi Event Generator

This example demonstrates how to generate realistic DeFi (Decentralized Finance) events for testing stream processing and fraud detection systems. It simulates flash loan attacks, normal blockchain transactions, DEX price updates, and mempool activity.

## What It Does

The generator creates three types of streaming data:

### 1. **Normal Background Traffic** (every 5 seconds by default)
- Regular on-chain transactions (USDC transfers)
- DEX price updates (WETH/DAI with normal fluctuations around $2300)
- Mempool gas price data (normal range: 30-40 Gwei)

### 2. **Flash Loan Attack Simulation** (every 2 minutes by default)
A coordinated sequence of 5 events that simulate a DeFi flash loan attack:

1. **Flash Loan Initiation**: Attacker borrows 50M DAI from Aave
2. **DEX Manipulation**: Attacker swaps all DAI for WETH on Uniswap
3. **Price Oracle Anomaly**: WETH/DAI price drops to $1850 (from ~$2300)
4. **Gas Price Spike**: Mempool shows gas spike to 250 Gwei
5. **Loan Repayment**: Attacker repays the flash loan in the same block

### 3. **Generated Entities (Kafka Topics)**
- `onchain_transactions`: Blockchain transaction records
- `dex_prices`: DEX price oracle updates
- `mempool_data`: Mempool and gas price information

## Use Cases

- **Fraud Detection**: Test stream processing pipelines that detect flash loan attacks
- **Real-time Analytics**: Develop dashboards for DeFi monitoring
- **Pattern Recognition**: Train ML models on DeFi attack patterns
- **Stream Processing**: Test DeltaStream queries and aggregations
- **Load Testing**: Generate continuous event streams for performance testing

## Prerequisites

- Python 3.11+
- `uv` package manager
- Access to a DeltaStream instance with valid credentials
- A configured Kafka store in DeltaStream

## Setup

1. **Navigate to the example directory**:

   ```bash
   cd examples/defi_event_generator
   ```

2. **Install dependencies**:

   ```bash
   uv sync
   ```

3. **Configure environment**:

   ```bash
   # Copy the environment template
   cp env.example .env
   
   # Edit .env with your DeltaStream credentials
   vim .env  # or your preferred editor
   ```

## Configuration

The example uses the following environment variables:

### Required Variables

- `DELTASTREAM_TOKEN`: Your DeltaStream API token
- `DELTASTREAM_ORG_ID`: Your DeltaStream organization ID

### Optional Variables

- `DELTASTREAM_SERVER_URL`: Custom server URL (defaults to production)
- `DELTASTREAM_DATABASE_NAME`: Default database to use
- `STORE_NAME`: Name of the Kafka store (defaults to "kafka_store")
- `NORMAL_TRAFFIC_INTERVAL`: Seconds between normal traffic (default: 5)
- `ATTACK_INTERVAL`: Seconds between flash loan attacks (default: 120)
- `DURATION`: Total runtime in seconds (0 = run indefinitely)

## Running the Example

```bash
# Run indefinitely (Ctrl+C to stop)
uv run defi-event-generator

# Run for a specific duration (e.g., 5 minutes)
DURATION=300 uv run defi-event-generator

# Customize intervals
NORMAL_TRAFFIC_INTERVAL=10 ATTACK_INTERVAL=60 uv run defi-event-generator
```

## Expected Output

```
🚀 Started generating real-time on-chain data...
   Normal traffic every 5s
   Flash loan attacks every 120s
   Running indefinitely (Ctrl+C to stop)

=== ENTITY SETUP ===
✅ Entity 'onchain_transactions' already exists
✅ Entity 'dex_prices' already exists
✅ Entity 'mempool_data' already exists

.....

🚨 --- SIMULATING FLASH LOAN ATTACK by 0xabc123... at block 18000001 --- 🚨

  → onchain_transactions: {"tx_hash": "0x...", "from_address": "0xA9754f1D...", ...}
1. Attacker takes 50M DAI flash loan.
  → onchain_transactions: {"tx_hash": "0x...", "from_address": "0xabc123...", ...}
2. Attacker swaps DAI for WETH, causing price slippage.
  → dex_prices: {"event_timestamp": 1234567890, "token_pair": "WETH/DAI", "price": 1850.75}
3. DEX price oracle for WETH/DAI reports anomalous drop.
  → mempool_data: {"event_timestamp": 1234567890, "block_number": 18000001, "avg_gas_price_gwei": 250}
4. Mempool shows massive gas spike.
  → onchain_transactions: {"tx_hash": "0x...", "to_address": "0xA9754f1D...", ...}
5. Attacker repays the 50M DAI flash loan.

.....
```

## Data Schemas

### Transaction Record
```json
{
  "tx_hash": "0x...",
  "block_number": 18000001,
  "event_timestamp": 1234567890123,
  "from_address": "0x...",
  "to_address": "0x...",
  "tx_token": "DAI",
  "tx_amount": 50000000.0
}
```

### Price Update
```json
{
  "event_timestamp": 1234567890123,
  "token_pair": "WETH/DAI",
  "price": 2295.42,
  "join_helper": "A"
}
```

### Mempool Data
```json
{
  "event_timestamp": 1234567890123,
  "block_number": 18000001,
  "avg_gas_price_gwei": 35
}
```

## Example DeltaStream Queries

Once the data is flowing, you can analyze it with DeltaStream SQL:

### Detect Flash Loan Attacks
```sql
-- Find transactions with same from/to addresses within 1 minute
SELECT 
  t1.tx_hash as loan_hash,
  t2.tx_hash as repay_hash,
  t1.from_address as loan_provider,
  t1.to_address as attacker,
  t1.tx_amount as loan_amount,
  t2.tx_amount as repay_amount
FROM onchain_transactions t1
JOIN onchain_transactions t2 
  ON t1.to_address = t2.from_address
  AND t1.from_address = t2.to_address
WHERE t2.event_timestamp BETWEEN t1.event_timestamp AND t1.event_timestamp + 60000
  AND t1.tx_amount > 1000000;
```

### Monitor Price Anomalies
```sql
-- Detect significant price drops
SELECT 
  token_pair,
  price,
  LAG(price) OVER (PARTITION BY token_pair ORDER BY event_timestamp) as prev_price,
  (price - LAG(price) OVER (PARTITION BY token_pair ORDER BY event_timestamp)) / 
    LAG(price) OVER (PARTITION BY token_pair ORDER BY event_timestamp) * 100 as pct_change
FROM dex_prices
WHERE ABS((price - LAG(price) OVER (PARTITION BY token_pair ORDER BY event_timestamp)) / 
    LAG(price) OVER (PARTITION BY token_pair ORDER BY event_timestamp) * 100) > 10;
```

### Track Gas Spikes
```sql
-- Find unusual gas price spikes
SELECT 
  block_number,
  avg_gas_price_gwei,
  AVG(avg_gas_price_gwei) OVER (ORDER BY event_timestamp ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) as moving_avg
FROM mempool_data
WHERE avg_gas_price_gwei > 100;
```

## Architecture

The generator follows these patterns:

1. **Entity Management**: Automatically creates Kafka topics as DeltaStream entities
2. **Event Correlation**: Flash loan attacks use coordinated events with temporal relationships
3. **Realistic Simulation**: Uses actual DeFi contract addresses and realistic amounts
4. **Async Operations**: Leverages Python asyncio for non-blocking event generation
5. **Configurable Timing**: Adjustable intervals for different testing scenarios

## Comparison with Java Version

This Python implementation provides the same functionality as the original Java version with these differences:

| Feature | Java | Python |
|---------|------|--------|
| Kafka Client | Direct KafkaProducer | DeltaStream SDK (entities.insert_values) |
| Concurrency | ScheduledExecutorService | asyncio |
| Configuration | Hard-coded constants | Environment variables |
| JSON Handling | Jackson ObjectMapper | Python dict + json module |
| Entity Management | Manual topic creation | SDK EntityCreateParams |

## Troubleshooting

### Entities Not Created
If entities fail to create, check:
- Store name is correct and exists in your DeltaStream instance
- Your credentials have permission to create entities
- The store is a Kafka store (not other types)

### Insert Failures
If inserts fail:
- Verify the store is accessible
- Check network connectivity to DeltaStream
- Ensure your token hasn't expired

### Performance Issues
If the generator is slow:
- Reduce `NORMAL_TRAFFIC_INTERVAL` for less frequent events
- Check your network latency to DeltaStream
- Monitor your Kafka cluster capacity

## Clean Up

To stop the generator:
```bash
# Press Ctrl+C when running interactively
```

To delete the created entities:
```sql
-- In DeltaStream SQL console
DROP ENTITY onchain_transactions;
DROP ENTITY dex_prices;
DROP ENTITY mempool_data;
```

