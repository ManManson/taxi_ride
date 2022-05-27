#include <map>
#include <memory>
#include <string>
#include <vector>

namespace arrow {

class TimestampScalar;
class RecordBatch;

namespace dataset {
class Dataset;
} // namespace dataset

namespace compute {
class Expression;
} // namespace compute

} // namespace arrow

///
/// \brief Implementation of the "Scan -> Filter -> Projection -> Hash aggregation"
/// algorithm applied to the nyc-tlc dataset.
///
/// As the name of the class suggests, it calculates a distribution mean of trip
/// distance values, grouped by passenger count and filtered by the given
/// start and end date arguments.
///
/// The scan is projected onto the minimal required subset of columns,
/// that is needed for the algorithm to work, i.e.
///  * tpep_pickup_datetime
///  * tpep_dropoff_datetime
///  * passenger_count
///  * trip_distance
///
/// The class is initialized with a reference to a materialized `Dataset`
/// instance and contains one main routine to perform its function:
/// `GetAverageDistances(start_date, end_date)`.
///
/// Both arguments are optional and represent either a full scan, one-sided
/// filter or two-sided filter (bounded at both sides of the date-time range).
///
class AverageDistances
{
    using TimestampScalarPtr = std::shared_ptr<arrow::TimestampScalar>;

    arrow::dataset::Dataset& _ds;

  public:
    // <passenger count, mean trip distance> group mapping
    using ResultType = std::map<int, double>;

    explicit AverageDistances(arrow::dataset::Dataset& ds)
      : _ds(ds)
    {
    }

    ResultType GetAverageDistances(TimestampScalarPtr start_date,
                                   TimestampScalarPtr end_date);

  private:
    std::vector<std::string> GetProjectionColumns() const;
    arrow::compute::Expression MakeFilterExpression(TimestampScalarPtr start_date,
                                                    TimestampScalarPtr end_date) const;
    ResultType RecordBatchToMap(const arrow::RecordBatch& b) const;
};
