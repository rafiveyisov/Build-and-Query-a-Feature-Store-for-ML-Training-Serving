
# Feast Feature Store - End-to-End Project

## Project Overview

Build a production-ready feature store for fraud detection using Feast, Redis, and Kafka.

**Dataset**: PaySim (mobile money transactions, 6.3M rows)

---

## Requirements

| Requirement | Tool |
|-------------|------|
| Feature Store | Feast 0.37.1 |
| Online Store | Redis |
| Offline Store | Parquet |
| Streaming | Kafka |

---

## Repository Structure
```
.
├── feature_repo/
│   ├── feature_store.yaml    # Feast config (Redis connection)
│   └── definitions.py        # Entities + Feature Views
├── data/
│   └── transactions.parquet  # Processed dataset
├── .gitignore
└── README.md
```

---

## Setup

```bash
# Create environment
python -m venv feast_env
source feast_env/bin/activate  # or feast_env\Scripts\activate (Windows)

# Install packages
pip install feast pandas redis pyarrow kagglehub

# Start Redis (Docker)
docker run --name feast-redis -p 6379:6379 -d redis
```

---

## Dataset Preparation

**Input columns used**:
- `step` (time, 1 step = 1 hour)
- `type` (PAYMENT, TRANSFER, CASH_OUT, CASH_IN, DEBIT)
- `amount` (transaction amount)
- `nameOrig` (customer ID who sent money)
- `nameDest` (recipient ID)
- `isFraud` (1 = fraudulent, 0 = legitimate)
- `isFlaggedFraud` (1 = flagged, 0 = not flagged)

**Feature engineering**:
- `timestamp` = convert step to datetime
- `transaction_count_24h` = rolling transaction count per customer (24-hour window)
- `avg_amount_24h` = rolling average amount per customer
- `amount_zscore` = anomaly score (std deviation from rolling average)
- `rapid_succession` = 1 if transaction occurred within 1 hour of previous

**Output**: `feast_df` saved as `data/transactions.parquet`

---

## Feature Store Configuration

### feature_store.yaml

```yaml
project: paysim_feature_store
registry: data/registry.db
provider: local

online_store:
    type: redis
    connection_string: 127.0.0.1:6379

offline_store:
    type: file

entity_key_serialization_version: 2
```

### definitions.py

**Entity**:
```python
customer_entity = Entity(
    name="customer_id",
    join_keys=["customer_id"],
    value_type=ValueType.STRING
)
```

**Feature View**:
```python
transaction_features = FeatureView(
    name="transaction_features",
    entities=[customer_entity],
    ttl=timedelta(days=30),
    source=FileSource(
        path="../data/transactions.parquet",
        timestamp_field="event_timestamp",
    ),
    schema=[
        Field(name="step", dtype=Int32),
        Field(name="type", dtype=String),
        Field(name="amount", dtype=Float32),
        Field(name="transaction_count_24h", dtype=Int64),
        Field(name="avg_amount_24h", dtype=Float32),
        Field(name="amount_zscore", dtype=Float32),
        Field(name="rapid_succession", dtype=Int64),
    ],
    online=True,
)
```

---

## Execution Steps

### Step 1: Apply Definitions

```bash
cd feature_repo
feast apply
```

### Step 2: Materialize to Redis

```python
store = FeatureStore(repo_path="feature_repo")
store.materialize(
    feature_views=["transaction_features"],
    start_date=feast_df['event_timestamp'].min(),
    end_date=feast_df['event_timestamp'].max()
)
```

### Step 3: Online Retrieval (Real-time inference)

```python
features = store.get_online_features(
    features=[
        "transaction_features:amount",
        "transaction_features:transaction_count_24h",
        "transaction_features:avg_amount_24h",
    ],
    entity_rows=[{"customer_id": "C1000036340"}]
).to_dict()
```

### Step 4: Offline Retrieval (Batch training)

```python
entity_df = pd.DataFrame({
    "customer_id": ["C1000036340", "C1000074914"],
    "event_timestamp": [timestamp1, timestamp2]
})

training_df = store.get_historical_features(
    features=[
        "transaction_features:amount",
        "transaction_features:avg_amount_24h",
    ],
    entity_df=entity_df,
).to_df()
```

### Step 5: Streaming Source (Kafka - Bonus)

```python
kafka_source = KafkaSource(
    name="streaming_transactions",
    timestamp_field="event_timestamp",
    message_format=JsonFormat(...),
    kafka_bootstrap_servers="localhost:9092",
    topic="transactions_topic",
)

stream_feature_view = StreamFeatureView(
    name="streaming_transaction_features",
    entities=[customer_entity],
    source=kafka_source,
    online=True,
)
```

### Step 6: Feature Monitoring (Bonus)

```python
monitor = FeatureMonitor(store, "transaction_features", "amount", threshold=0.3)
monitor.check_feature_stats([{"customer_id": "C1000036340"}])
```

---

## Results

| Component | Status | Latency |
|-----------|--------|---------|
| Dataset load + feature engineering | ✅ | ~2-5 seconds |
| Feast apply | ✅ | ~1 second |
| Materialization (Redis) | ✅ | ~3-10 seconds |
| Online store query | ✅ | <10ms |
| Offline store query | ✅ | ~1 second |
| Streaming source | ✅ | Configured |
| Monitoring (null ratio) | ✅ | 0% |

**Sample online output**:
```
   customer_id         amount  transaction_count_24h  avg_amount_24h
0  C1000036340   253648.687500                      1   253648.687500
1  C1000074914   315857.562500                      1   315857.562500
2  C1000086512    33676.589844                      1    33676.589844
```

---

## Common Issues & Fixes

| Problem | Solution |
|---------|----------|
| Redis connection refused | Use `127.0.0.1:6379` instead of `localhost` |
| Materialization hangs | Use smaller batches (e.g., 6-hour windows) |
| `event_timestamp` not found | Use `feast_df` (not `df`) for timestamp column |
| Null values in online query | Re-run materialization; check Redis is running |

---

## .gitignore

```gitignore
# Python
__pycache__/
*.pyc
feast_env/

# Feast
feature_repo/data/
*.db
*.db-shm
*.db-wal

# Data
data/*.parquet
dataset/

# IDE
.vscode/
.ipynb_checkpoints/
```

---

## Requirements Met

- ✅ Define entities and feature sets
- ✅ Feast feature store setup
- ✅ Materialize features from batch source
- ✅ Query feature vectors (online - Redis)
- ✅ Query feature vectors (offline - Parquet)
- ✅ Streaming source (bonus)
- ✅ Feature monitoring alerts (bonus)

---# Build-and-Query-a-Feature-Store-for-ML-Training-Serving
