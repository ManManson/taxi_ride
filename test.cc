#define BOOST_TEST_MODULE taxi_ride_tests
#include <boost/test/unit_test.hpp>

#include "average_distances.hh"
#include "load_dataset.hh"

#include <arrow/dataset/dataset.h>
#include <arrow/io/interfaces.h>
#include <arrow/io/memory.h>
#include <arrow/json/options.h>
#include <arrow/json/reader.h>
#include <arrow/type_fwd.h>
#include <arrow/util/string_view.h>
#include <arrow/util/value_parsing.h>

namespace {

// Extracted from `arrow/json/test_common.h` to avoid including GTest dependency
arrow::Status
MakeStream(arrow::util::string_view src_str, std::shared_ptr<arrow::io::InputStream>* out)
{
    auto src = std::make_shared<arrow::Buffer>(src_str);
    *out = std::make_shared<arrow::io::BufferReader>(src);
    return arrow::Status::OK();
}

std::shared_ptr<arrow::TimestampScalar>
MakeTimestampScalar(std::string str)
{
    arrow::TimestampType::c_type raw;
    arrow::internal::ParseValue<arrow::TimestampType>(
      arrow::TimestampType(arrow::TimeUnit::SECOND), str.data(), str.size(), &raw);
    return std::make_shared<arrow::TimestampScalar>(raw, arrow::TimeUnit::SECOND);
}

} // anonymous namespace

std::shared_ptr<arrow::dataset::Dataset>
TestDataset()
{
    constexpr arrow::util::string_view content = R"(
    {
        "tpep_pickup_datetime" : "2020-08-01 00:20:00",
        "tpep_dropoff_datetime" : "2020-08-01 00:45:00",
        "passenger_count" : 1,
        "trip_distance" : 2.5
    }
    {
        "tpep_pickup_datetime" : "2020-08-01 00:21:00",
        "tpep_dropoff_datetime" : "2020-08-01 00:44:00",
        "passenger_count" : 1,
        "trip_distance" : 5
    }
    {
        "tpep_pickup_datetime" : "2020-08-01 00:25:00",
        "tpep_dropoff_datetime" : "2020-08-01 00:46:00",
        "passenger_count" : 2,
        "trip_distance" : 5
    }
    {
        "tpep_pickup_datetime" : "2020-08-01 00:26:00",
        "tpep_dropoff_datetime" : "2020-08-01 00:43:00",
        "passenger_count" : 2,
        "trip_distance" : 10
    }
)";

    std::shared_ptr<arrow::io::InputStream> input;
    std::shared_ptr<arrow::json::TableReader> reader;
    arrow::json::ParseOptions parse_options = arrow::json::ParseOptions::Defaults();
    parse_options.explicit_schema = std::make_shared<arrow::Schema>(arrow::FieldVector{
      arrow::field("tpep_pickup_datetime", arrow::timestamp(arrow::TimeUnit::MICRO)),
      arrow::field("tpep_dropoff_datetime", arrow::timestamp(arrow::TimeUnit::MICRO)),
      arrow::field("passenger_count", arrow::float64()),
      arrow::field("trip_distance", arrow::float64()) });

    assert(MakeStream(content, &input).ok());
    reader = arrow::json::TableReader::Make(arrow::default_memory_pool(),
                                            input,
                                            arrow::json::ReadOptions::Defaults(),
                                            parse_options)
               .ValueOrDie();
    auto table = reader->Read().ValueOrDie();
    return std::make_shared<arrow::dataset::InMemoryDataset>(std::move(table));
}

namespace btt = boost::test_tools;
namespace {
const auto cmp_float_tolerance = btt::tolerance(0.00001);
} // anonymous namesapce

BOOST_AUTO_TEST_SUITE(master_test_suite)

BOOST_AUTO_TEST_CASE(full_scan)
{
    auto ds = TestDataset();
    AverageDistances avg_calc(*ds);
    auto avg_distances = avg_calc.GetAverageDistances(nullptr, nullptr);
    BOOST_TEST(avg_distances[1] == 3.75, cmp_float_tolerance);
    BOOST_TEST(avg_distances[2] == 7.5, cmp_float_tolerance);
}

BOOST_AUTO_TEST_CASE(scan_with_start_date)
{
    auto start = MakeTimestampScalar("2020-08-01 00:23:00");
    auto ds = TestDataset();
    AverageDistances avg_calc(*ds);
    auto avg_distances = avg_calc.GetAverageDistances(std::move(start), nullptr);
    BOOST_TEST(!avg_distances.contains(1));
    BOOST_TEST(avg_distances[2] == 7.5, cmp_float_tolerance);
}

BOOST_AUTO_TEST_CASE(scan_with_end_date)
{
    auto end = MakeTimestampScalar("2020-08-01 00:45:00");
    auto ds = TestDataset();
    AverageDistances avg_calc(*ds);
    auto avg_distances = avg_calc.GetAverageDistances(nullptr, std::move(end));
    BOOST_TEST(avg_distances[1] == 3.75, cmp_float_tolerance);
    BOOST_TEST(avg_distances[2] == 10, cmp_float_tolerance);
}

BOOST_AUTO_TEST_CASE(empty_scan)
{
    auto start = MakeTimestampScalar("2020-08-01 00:30:00");
    auto end = MakeTimestampScalar("2020-08-01 00:40:00");
    auto ds = TestDataset();
    AverageDistances avg_calc(*ds);
    auto avg_distances = avg_calc.GetAverageDistances(std::move(start), std::move(end));
    BOOST_TEST(avg_distances.empty());
}

BOOST_AUTO_TEST_CASE(scan_with_start_end_date)
{
    auto start = MakeTimestampScalar("2020-08-01 00:20:05");
    auto end = MakeTimestampScalar("2020-08-01 00:44:05");
    auto ds = TestDataset();
    AverageDistances avg_calc(*ds);
    auto avg_distances = avg_calc.GetAverageDistances(std::move(start), std::move(end));
    BOOST_TEST(avg_distances[1] == 5, cmp_float_tolerance);
    BOOST_TEST(avg_distances[2] == 10, cmp_float_tolerance);
}

BOOST_AUTO_TEST_SUITE_END()
