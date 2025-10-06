# DeltaStream SDK CSA Data Generator

This example demonstrates how to generate realistic Customer Service Analytics (CSA) events for testing stream processing and customer behavior analysis systems. It simulates user pageviews, cart updates, and chat messages.

## What It Does

The generator creates continuous streaming data simulating e-commerce customer activity:

### Generated Events

1. **Page Views** (100% of cycles)
   - User navigation through product pages, checkout, and support
   - Tracks which pages users visit in real-time

2. **Cart Updates** (30% probability)
   - ADD or REMOVE actions on shopping cart items
   - Includes item SKU tracking

3. **Chat Messages** (15% probability)
   - Customer support chat interactions
   - Common questions about stock, returns, and general help

### Generated Entities (Kafka Topics)

- `csa_pageviews`: User page navigation events
- `csa_cart_updates`: Shopping cart modification events
- `csa_chat_messages`: Customer support chat interactions

## Use Cases

- **Customer Journey Analysis**: Track user behavior across the e-commerce platform
- **Proactive Customer Service**: Identify users who need help before they ask
- **Conversion Optimization**: Analyze patterns leading to checkout
- **Real-time Analytics**: Monitor customer activity dashboards
- **Stream Processing Testing**: Test DeltaStream queries and aggregations
- **Pattern Recognition**: Train ML models on customer behavior

## Prerequisites

- Python 3.11+
- `uv` package manager
- Access to a DeltaStream instance with valid credentials
- A configured Kafka store in DeltaStream

## Setup

1. **Navigate to the example directory**:

   ```bash
   cd examples/csa_data_generator
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
- `DURATION`: Total runtime in seconds (0 = run indefinitely)

## Running the Example

```bash
# Run indefinitely (Ctrl+C to stop)
uv run csa-data-generator

# Run for a specific duration (e.g., 5 minutes)
DURATION=300 uv run csa-data-generator
```

## Expected Output

```
🚀 Started generating customer service analytics data...
   Running indefinitely (Ctrl+C to stop)

=== ENTITY SETUP ===
✅ Entity 'csa_pageviews' already exists
✅ Entity 'csa_cart_updates' already exists
✅ Entity 'csa_chat_messages' already exists

Sent page_view: {'event_timestamp': '2024-01-15 10:30:45.123456Z', 'user_id': 'user_123', 'page': '/products/A1'}
Sent cart_update: {'event_timestamp': '2024-01-15 10:30:45.123456Z', 'user_id': 'user_123', 'cart_action': 'ADD', 'item_id': 'SKU42'}
Sent page_view: {'event_timestamp': '2024-01-15 10:30:46.234567Z', 'user_id': 'user_456', 'page': '/checkout'}
Sent chat_message: {'event_timestamp': '2024-01-15 10:30:46.234567Z', 'user_id': 'user_456', 'message': 'is this in stock?'}
Sent page_view: {'event_timestamp': '2024-01-15 10:30:47.345678Z', 'user_id': 'user_223', 'page': '/support'}
...
```

## Data Schemas

### Page View Event
```json
{
  "event_timestamp": "2024-01-15 10:30:45.123456Z",
  "user_id": "user_123",
  "page": "/products/A1"
}
```

### Cart Update Event
```json
{
  "event_timestamp": "2024-01-15 10:30:45.123456Z",
  "user_id": "user_123",
  "cart_action": "ADD",
  "item_id": "SKU42"
}
```

### Chat Message Event
```json
{
  "event_timestamp": "2024-01-15 10:30:45.123456Z",
  "user_id": "user_123",
  "message": "is this in stock?"
}
```

## Example DeltaStream Queries

Once the data is flowing, you can analyze it with DeltaStream SQL:

### Identify Users Needing Proactive Support

```sql
-- Find users who visited support page but haven't chatted yet
SELECT 
  pv.user_id,
  pv.page,
  pv.event_timestamp
FROM csa_pageviews pv
LEFT JOIN csa_chat_messages cm 
  ON pv.user_id = cm.user_id
  AND cm.event_timestamp BETWEEN pv.event_timestamp AND pv.event_timestamp + 300000
WHERE pv.page = '/support'
  AND cm.user_id IS NULL;
```

### Track Shopping Cart Abandonment

```sql
-- Find users who added items but didn't checkout
SELECT 
  cu.user_id,
  COUNT(*) as items_added,
  MAX(cu.event_timestamp) as last_cart_activity
FROM csa_cart_updates cu
LEFT JOIN csa_pageviews pv
  ON cu.user_id = pv.user_id
  AND pv.page = '/checkout'
  AND pv.event_timestamp > cu.event_timestamp
WHERE cu.cart_action = 'ADD'
  AND pv.user_id IS NULL
GROUP BY cu.user_id
HAVING COUNT(*) > 1;
```

### Monitor Customer Journey

```sql
-- Analyze user behavior leading to support contact
SELECT 
  cm.user_id,
  cm.message,
  ARRAY_AGG(pv.page ORDER BY pv.event_timestamp) as page_journey,
  COUNT(DISTINCT pv.page) as pages_visited
FROM csa_chat_messages cm
JOIN csa_pageviews pv
  ON cm.user_id = pv.user_id
  AND pv.event_timestamp BETWEEN cm.event_timestamp - 600000 AND cm.event_timestamp
GROUP BY cm.user_id, cm.message;
```

### Real-time Active Users

```sql
-- Count active users in the last 5 minutes
SELECT 
  COUNT(DISTINCT user_id) as active_users,
  TUMBLE_START(event_timestamp, INTERVAL '1' MINUTE) as window_start
FROM csa_pageviews
GROUP BY TUMBLE(event_timestamp, INTERVAL '1' MINUTE);
```

### Cart Conversion Rate

```sql
-- Calculate add-to-cart to checkout conversion
SELECT 
  COUNT(DISTINCT CASE WHEN cart_action = 'ADD' THEN user_id END) as users_added_items,
  COUNT(DISTINCT CASE WHEN page = '/checkout' THEN user_id END) as users_checked_out,
  CAST(COUNT(DISTINCT CASE WHEN page = '/checkout' THEN user_id END) AS DOUBLE) / 
    COUNT(DISTINCT CASE WHEN cart_action = 'ADD' THEN user_id END) * 100 as conversion_rate
FROM (
  SELECT user_id, NULL as cart_action, page, event_timestamp FROM csa_pageviews
  UNION ALL
  SELECT user_id, cart_action, NULL as page, event_timestamp FROM csa_cart_updates
);
```

## Architecture

The generator follows these patterns:

1. **Entity Management**: Automatically creates Kafka topics as DeltaStream entities
2. **Probabilistic Events**: Cart updates (30%) and chat messages (15%) occur randomly
3. **Realistic Timing**: Random delays (0-1000ms) between events mimic real user behavior
4. **Multi-User Simulation**: Tracks 8 different users with concurrent activities
5. **Async Operations**: Leverages Python asyncio for non-blocking event generation

## Comparison with Java Version

This Python implementation provides the same functionality as the original Java version:

| Feature | Java | Python |
|---------|------|--------|
| Kafka Client | Direct KafkaProducer | DeltaStream SDK (entities.insert_values) |
| Concurrency | Thread.sleep in loop | asyncio.sleep |
| Configuration | Hard-coded constants | Environment variables |
| JSON Handling | Jackson ObjectMapper | Python dict |
| Entity Management | Manual topic creation | SDK EntityCreateParams |
| Timestamp Format | DateTimeFormatter | datetime.strftime |

## Simulated Users

The generator simulates activity for 8 users:
- `user_123`, `user_456`, `user_223`, `user_356`
- `user_163`, `user_466`, `user_823`, `user_856`

## Simulated Data

### Pages
- `/products/A1` - Product page for item A1
- `/products/B2` - Product page for item B2
- `/checkout` - Checkout page
- `/support` - Customer support page

### Cart Actions
- `ADD` - Add item to cart
- `REMOVE` - Remove item from cart

### Chat Messages
- "is this in stock?"
- "how do I return an item?"
- "thanks for the help!"

### SKU Range
- `SKU0` through `SKU99` (100 different products)

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
- Check your network latency to DeltaStream
- Monitor your Kafka cluster capacity
- Verify your DeltaStream quotas

## Clean Up

To stop the generator:
```bash
# Press Ctrl+C when running interactively
```

To delete the created entities:
```sql
-- In DeltaStream SQL console
DROP ENTITY csa_pageviews;
DROP ENTITY csa_cart_updates;
DROP ENTITY csa_chat_messages;
```