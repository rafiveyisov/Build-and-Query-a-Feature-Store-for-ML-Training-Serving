from datetime import timedelta
from feast import Entity, FeatureView, FileSource, Field, ValueType
from feast.types import Float32, Int32, Int64, String

# Entities
customer_entity = Entity(
    name="customer_id",
    join_keys=["customer_id"],
    value_type=ValueType.STRING
)

# Source
source = FileSource(
    path="../data/transactions.parquet",
    timestamp_field="event_timestamp",
)

# Feature View
transaction_features = FeatureView(
    name="transaction_features",
    entities=[customer_entity],
    ttl=timedelta(days=30),
    source=source,
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
