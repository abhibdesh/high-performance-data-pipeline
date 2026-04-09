# High Performance Data Ingestion Pipeline

## Overview

This project demonstrates the evolution of a data ingestion system designed to process large scale transactional datasets efficiently.

The system was iteratively improved from a basic pandas based approach to a distributed pipeline using Dask, enabling high performance processing of multi million record datasets.

---

## Problem

The initial system processed large transactional files using a single threaded approach.

* Dataset size ranged from 1.2 crore to 3 crore records per file
* Processing time was approximately 3 hours per file
* System struggled with scalability and memory constraints

---

## Solution

The pipeline was redesigned in multiple stages to improve performance and scalability.

### Stage 1 Pandas Based Processing

* CSV ingestion using pandas
* Data cleaning and transformation
* Batch insertion into PostgreSQL
* Limitation: slow and not scalable

### Stage 2 Dask Introduction

* Introduced Dask DataFrame for handling larger datasets
* Attempted parallel execution
* Limitation: inefficient parallelization and memory usage

### Stage 3 Distributed Pipeline

* Implemented partition based processing using Dask
* Separated ETL and ingestion logic
* Enabled parallel execution across workers
* Optimized batch insertion into PostgreSQL

---

## System Flow

CSV Input
→ Data Cleaning and Transformation
→ Partition Based Processing
→ Batch Insert into PostgreSQL

---

## Performance Impact

* Dataset size handled: up to 3 crore records
* Processing time reduced from 3 hours to 17 seconds
* Improvement of more than 99 percent

---

## Project Structure

pipeline

* stage1_pandas

  * Basic ingestion and transformation using pandas

* stage2_dask_attempt

  * Initial attempt at scaling using Dask

* stage3_distributed_pipeline

  * Final optimized distributed pipeline

original_scripts

* Contains legacy scripts representing the original implementation
* Provided for reference and evolution tracking

data

* Contains sample dataset for demonstration

---

## Tech Stack

* Python
* Pandas
* Dask
* PostgreSQL
* psycopg2

---

## How to Run

1. Add database credentials in environment variables
2. Place input file in data folder
3. Run the pipeline

```bash
python main.py
```

---

## Notes

* Sample dataset is included for demonstration purposes
* Original system was tested on significantly larger datasets
* All sensitive information has been removed

---

## Key Takeaways

* Transitioned from single machine processing to distributed execution
* Improved system performance by optimizing data processing strategy
* Demonstrates practical experience in handling large scale data pipelines

---
