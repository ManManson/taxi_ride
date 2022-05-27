#include <memory>
#include <string>

namespace arrow::dataset {
class Dataset;
}

///
/// \brief Load the nyc-tlc dataset from the provided URI,
/// which can be either a local filesystem path to the directory
/// containing the Parquet files, or a URL pointing to a cloud
/// filesystem (i.e. S3 storage).
/// \return New `Dataset` object, a tabular-like structure, which can
/// handle larger-than-memory cases and provides a convenient abstraction
/// to the table interface.
///
std::shared_ptr<arrow::dataset::Dataset>
LoadNycTlcDataset(std::string uri);
