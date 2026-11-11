# Data Dictionary

## customers.csv
| Column | Type | Nullable | Rule |
|---|---|---|---|
| customer_id | string | no | Unique stable key |
| customer_name | string | no | Customer name |
| city | string | no | Output partition attribute |

## transactions.csv
| Column | Source type | Nullable | Rule |
|---|---|---|---|
| transaction_id | string | no | Stable event key |
| customer_id | string | no | Foreign key |
| amount | string -> decimal(18,2) | no | Must parse and be > 0 |
| currency | string | no | INR, USD, EUR |
| event_ts | string -> timestamp | no | ISO-8601 UTC |
| status | string | yes | Source status |

Derived fields: `transaction_count`, `total_amount`, `latest_event_ts`, `reject_reason`, `source_file`, and `run_id`.
