# Task implementation analysis
* * *

This document aims to provide a thorough analysis of what needs to be improved or fixed in the current implementation of the task.

## Implementation overview

Briefly speaking, this test program implements the following SQL statement to filter and aggregate data in the `nyc-tlc (yellow_tripdata_2020)` dataset:

```SQL
SELECT mean(trip_distance), passenger_count
FROM yellow_tripdata_2020
WHERE tpep_pickup_datetime >= ? AND tpep_dropoff_datetime <= ?
GROUP BY passenger_count
```

The specific execution steps are performed using [Apache Arrow](https://github.com/apache/arrow/) framework, which is designed to be a building block for various solutions that manage big volumes of heterogeneous data.

The dataset itself is stored in [Apache Parquet](https://parquet.apache.org/) data format on a public S3 bucket at `s3://nyc-tlc.s3.amazonaws.com?region=us-east-1`.
It is loaded into `arrow::dataset::Dataset` class instance by `LoadNycTlcDataset()` function. The `Dataset` class is a convenient and generic abstraction for the tabular data, which can be read into the internal Arrow format from a wide variety of sources (CSV, Parquet, etc.) using a unified interface.

The core algorithm is implemented by `AverageDistances::GetAverageDistances()` function and consists of the following steps:

1. Create a `Scanner` object to read data in batches from the dataset
2. Set projection and filter for the scanner (which is equivalent to adding `project` and `filter` nodes to the execution plan directly)
3. Construct the execution plan (`arrow::compute::ExecPlan`) given a sequence of declarative exec node descriptions (`arrow::compute::Declaration`):
    * `source` node, which feeds the date (projected and filtered) into the execution pipeline
    * `aggregate` node with `hash_mean` grouped aggregation function, which calculates mean value from `trip_distance` given aggregation key `passenger_count`
    * `sink` node to gather and output the processed data
4. Transform the result into `map<int, double>` and return to the consumer

Also, there is a small test suite that operates on an in-memory dataset, which mimicks the schema of the original dataset (except that only the necessary fields are used). The data is read from the test dataset and processed by `AverageDistances` utility afterwards. The data is validated after that.

The tests are provided via a separate test executable `taxi_ride_tests` and are also executable via `ctest` utility.

Test cases include the following:
* Full scan, i.e. no filter conditions
* Left-side filter condition, trimming data from the left side
* Right-side filter condition, trimming data from the right side
* Both filters are set and cut off some records from both sides
* Testing a filter expression that yields the empty result

## Drawbacks of the current implementation

* There is no local caching of the downloaded dataset, so that every subsequent invocation of the program involves network operations.
* Source nodes' `MakeReaderGenerator()` operates on the default CPU thread pool, which can cause performance to suffer. The idea was to re-use `GetIOThreadPool()` from Apache Arrow, but it's not exposed to the clients of the library. This operation should instead use a separate thread pool, and thread count should be selected wisely to avoid performance hits when the operation is run alongside other arrow reads computations.
* More attention should be paid to Scan options, e.g.: batch size/readahead values, read options for individual fragments. For now, defaults are used.
* The program does not check for invalid filter conditions (e.g. `start_date` is ahead of `end_date`) or filters that are known to yield empty results (e.g. `start_date == end_date`), in such cases the implementation should return an error or empty result as early as possible and avoid arrow computations at all.
* There are some result transformations (`optional<ExecBatch> -> ExecBatch -> RecordBatch`), that are convenient and improve code readability a little bit, but can be avoided to improve performance.
* Plan execution in Apache Arrow is async, and it is used in the implementation only partially, the `AverageDistances` interface is synchronous. To improve parallelism it would be beneficial to make `AverageDistances::GetAverageDistances` asynchronous (i.e. return a `Future`).
* The code uses low-level `assert()`:s and `.ValueOrDie()` all over the place. Need to introduce proper exception handling.
* `GetAverageDistances` method should use `std::chrono::time_point` arguments instead of Arrow-specific types (i.e. `arrow::utils::optional<arrow::TimestampScalar>`).
