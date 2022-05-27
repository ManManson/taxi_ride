#include <arrow/dataset/dataset.h>
#include <arrow/scalar.h>
#include <arrow/type.h>
#include <arrow/util/logging.h>
#include <arrow/util/value_parsing.h>

#include <boost/log/trivial.hpp>
#include <boost/program_options/option.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

#include "average_distances.hh"
#include "load_dataset.hh"

int
main(int argc, char* argv[])
{
    namespace bp = boost::program_options;

    constexpr const char* program_description =
      R"(Examine the public NYC-TLC dataset hosted on S3 and calculate
mean value of trip distances grouped by passenger count.

Allowed command-line options)";

    bp::options_description cmd_line_opts(program_description);
    cmd_line_opts.add_options()("help,h", "print usage message")(
      "start-date",
      bp::value<std::string>(),
      "start date passed to the filter for the taxi rides data")(
      "end-date",
      bp::value<std::string>(),
      "end date passed to the filter for the taxi rides data");

    bp::variables_map vm;
    auto parsed_opts = bp::command_line_parser(argc, argv).options(cmd_line_opts).run();
    bp::store(parsed_opts, vm);
    if (vm.count("help") != 0) {
        std::cout << cmd_line_opts;
        return 0;
    }

    auto try_parse_datetime =
      [&vm](const char* arg) -> std::shared_ptr<arrow::TimestampScalar> {
        if (vm.count(arg) == 0) {
            return nullptr;
        }
        arrow::TimestampType::c_type raw;
        const std::string& dt_str = vm[arg].as<std::string>();
        arrow::internal::ParseValue<arrow::TimestampType>(
          arrow::TimestampType(arrow::TimeUnit::SECOND),
          dt_str.data(),
          dt_str.size(),
          &raw);
        return std::make_shared<arrow::TimestampScalar>(raw, arrow::TimeUnit::SECOND);
    };

    std::shared_ptr<arrow::TimestampScalar> start_date = try_parse_datetime("start-date"),
                                            end_date = try_parse_datetime("end-date");

    std::string fs_uri = "s3://nyc-tlc.s3.amazonaws.com?region=us-east-1";
    BOOST_LOG_TRIVIAL(info) << "Loading dataset from " << fs_uri;
    auto ds = LoadNycTlcDataset(std::move(fs_uri));
    BOOST_LOG_TRIVIAL(info) << "Dataset successfully loaded";

    AverageDistances dist_calc(*ds);
    BOOST_LOG_TRIVIAL(info) << "Aggregating mean distances grouped by passenger count";
    auto avg_map =
      dist_calc.GetAverageDistances(std::move(start_date), std::move(end_date));

    std::stringstream res_strm;
    res_strm << "Mean distances distribution among various passenger counts:\n";
    for (const auto& [k, v] : avg_map) {
        res_strm << "passenger_count: " << k << " => mean trip distance: " << v
                 << std::endl;
    }
    BOOST_LOG_TRIVIAL(info) << res_strm.str();

    return 0;
}
