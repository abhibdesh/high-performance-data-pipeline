## Stage 1 Pandas Based Pipeline

Initial implementation using pandas for data ingestion and processing.

Features:
- CSV based ingestion
- Data cleaning and deduplication
- Batch insertion into PostgreSQL

Limitations:
- Single threaded processing
- Slow performance on large datasets
- Not suitable for millions of records

This stage represents the baseline implementation before optimization.