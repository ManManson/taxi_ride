cmake_minimum_required(VERSION 3.20)

project(taxi_ride CXX)

# For simplicity, assume that Apache Arrow is installed to /usr/local
list(APPEND CMAKE_MODULE_PATH /usr/local/lib64/cmake/arrow)

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
find_package(ArrowDataset REQUIRED)

include(ArrowTargets)
include(ParquetTargets)
include(ArrowDatasetTargets)

find_package(Boost REQUIRED COMPONENTS program_options unit_test_framework log)

add_library(taxi_ride_base OBJECT load_dataset.cc average_distances.cc)
target_link_libraries(taxi_ride_base PUBLIC
    parquet_shared
    arrow_shared
    arrow_dataset_shared
    Boost::log)
target_compile_features(taxi_ride_base PUBLIC cxx_std_20)
target_compile_definitions(taxi_ride_base PUBLIC BOOST_LOG_DYN_LINK)

add_executable(taxi_ride main.cc)

target_link_libraries(taxi_ride PRIVATE
    Boost::program_options taxi_ride_base)

include(CTest)

if(BUILD_TESTING)
    add_executable(taxi_ride_tests test.cc)
    target_link_libraries(taxi_ride_tests PRIVATE
        Boost::unit_test_framework taxi_ride_base)
    target_compile_definitions(taxi_ride_tests PRIVATE BOOST_TEST_DYN_LINK)
    add_test(NAME taxi_ride_tests COMMAND taxi_ride_tests)
endif()
