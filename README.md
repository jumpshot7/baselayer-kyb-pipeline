# baselayer-kyb-pipeline

Execution.py 
GCS (two CSV files)
        ↓
Apache Beam reads both files
        ↓
Pydantic validates every row
        ↓
Valid records written to Postgres
(nyc_dca_businesses + nys_corp_entities)
        ↓
Fuzzy matching runs across both tables
        ↓
Anomaly flags computed
        ↓
Results written to kyb_anomalies

The Key Complexity Warning
The fuzzy matching step (Step 4) is an O(n × m) operation — every NYC business compared against every NYS entity. With ~100k NYC records and ~4M NYS records that's potentially 400 billion comparisons if done naively.
We'll handle this with two optimizations:

Zip code pre-filtering — only compare businesses in the same zip code
Threshold cutoff — stop comparing once we find a good enough match

This brings it down to something manageable.

# Section 1: DB connection helpers
# Section 2: Table creation from database.sql
# Section 3: Check if already populated
# Section 4: Beam DoFn classes (the transform units)
# Section 5: Run Beam pipeline for NYC dataset
# Section 6: Run Beam pipeline for NYS dataset
# Section 7: Fuzzy matching + anomaly detection
# Section 8: Write anomalies to Postgres
# Section 9: Orchestrator (ties everything together)
# Section 10: Entry point