## Stage 2 Dask Based Processing

Transition from pandas to Dask for handling larger datasets.

Improvements:
- Introduced Dask DataFrame
- Attempted parallel processing
- Batch insertion retained

Limitations:
- Parallel execution not fully optimized
- Entire dataset still processed in a single task
- Memory inefficiencies

This stage represents the experimentation phase with distributed processing.