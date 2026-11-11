# Design and Trade-offs
1. Explicit schemas keep raw values as strings until validation so malformed amounts can be quarantined.
2. Baseline uses a regular join. Optimized repartitions facts by customer_id and broadcasts the smaller customer dimension.
3. Broadcast is suitable only when the dimension fits safely in executor memory.
4. Repartitioning and caching add overhead; benchmark rather than assuming speedup.
5. Output is partitioned by city, a low-cardinality attribute suitable for downstream filtering.
6. `exceptAll` in both directions verifies output equivalence including duplicate rows.
7. Overwrite mode makes local reruns safe. Production should use transactional tables and a durable run ledger.
