# NYC-TLC Yellow trip data taxi rides test
* * *

## How to build

NOTE: the build was tested using Fedora 36 distribution and build instructions are written assuming one uses fc36 as the build/run system.

At first, you will need to build [Apache Arrow](https://github.com/apache/arrow/) libraries and install it (either locally or globally):

```sh
# Clone apache/arrow repository
git clone git@github.com:apache/arrow.git
# Create a build directory for an out-of-source build of C++ libs
mkdir -p arrow/build
cd arrow/build
# Configure the project, enable additional modules support
cmake -GNinja ../cpp/ -DARROW_PARQUET=ON -DARROW_CSV=ON -DARROW_COMPUTE=ON -DARROW_DATASET=ON -DARROW_S3=ON -DARROW_JSON=ON
# Build the arrow libraries
ninja
# Install libraries, headers and CMake modules (installation root defaults to /usr/local)
sudo cmake --install .
```

Now you can clone and build the test project:

```sh
git clone git@github.com:ManManson/taxi_ride.git
mkdir build
cd build
cmake .. -GNinja
ninja
```

## How to run unit-tests

This project has integration with CTest framework, so that you can execute
all unit-tests by simply calling `ctest` utility without arguments.

Sample test output:
```sh
$ ctest
Test project /home/.../taxi_ride/build
    Start 1: taxi_ride_tests
1/1 Test #1: taxi_ride_tests ..................   Passed    0.14 sec

100% tests passed, 0 tests failed out of 1

Total Test time (real) =   0.14 sec
```

## How to run the `taxi_ride` executable

```sh
$ ./taxi_ride --help
Examine the public NYC-TLC dataset hosted on S3 and calculate
mean value of trip distances grouped by passenger count.

Allowed command-line options:
  -h [ --help ]         print usage message
  --start-date arg      start date passed to the filter for the taxi rides data
  --end-date arg        end date passed to the filter for the taxi rides data
```

You can also pass optional arguments `--start-date` or `--end-date` (or both, simultaneously) to enable filtering the dataset by pickup and dropoff dates.

Example program call:

```sh
./taxi_ride --start-date "2020-08-01 00:28:54" --end-date "2020-08-01 00:40:00"
```

Example output:

```
 ./taxi_ride --start-date "2020-08-01 00:28:54" --end-date "2020-08-01 00:40:00"
[2022-05-27 12:26:07.313584] [0x00007f97a9ac3100] [info]    Loading dataset from s3://nyc-tlc.s3.amazonaws.com?region=us-east-1
[2022-05-27 12:26:15.328710] [0x00007f97a9ac3100] [debug]   Looking for parquet files matching the following pattern: yellow_tripdata_2020*
[2022-05-27 12:26:17.608548] [0x00007f97a9ac3100] [debug]   Filtered 12 files
[2022-05-27 12:26:17.609062] [0x00007f97a9ac3100] [debug]   Materializing dataset from selected parquet files
[2022-05-27 12:26:17.820980] [0x00007f97a9ac3100] [info]    Dataset successfully loaded
[2022-05-27 12:26:17.821007] [0x00007f97a9ac3100] [info]    Aggregating mean distances grouped by passenger count
[2022-05-27 12:26:17.881935] [0x00007f97a9ac3100] [debug]   Validating execution plan
[2022-05-27 12:26:17.881983] [0x00007f97a9ac3100] [debug]   Executing the plan
[2022-05-27 12:27:11.917834] [0x00007f97a9ac3100] [debug]   Plan executed successfully, processing the results
[2022-05-27 12:27:11.919736] [0x00007f97a9ac3100] [info]    Mean distances distribution among various passenger counts:
passenger_count: 0 => mean trip distance: 0.01
passenger_count: 1 => mean trip distance: 1.548
passenger_count: 2 => mean trip distance: 1.11
passenger_count: 3 => mean trip distance: 1.8
passenger_count: 4 => mean trip distance: 0.55
passenger_count: 6 => mean trip distance: 1.08
```
