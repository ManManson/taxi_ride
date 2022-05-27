#include "load_dataset.hh"

#include <arrow/dataset/dataset.h>
#include <arrow/dataset/discovery.h>
#include <arrow/dataset/file_base.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/filesystem/api.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/path_util.h>
#include <arrow/util/logging.h>

#include <boost/log/trivial.hpp>

std::shared_ptr<arrow::dataset::Dataset>
LoadNycTlcDataset(std::string uri)
{
    auto fs = arrow::fs::FileSystemFromUri(uri).ValueOrDie();

    arrow::fs::FileSelector selector;
    selector.base_dir = "nyc-tlc/trip data";
    selector.recursive = true;
    arrow::fs::FileInfoVector discovered_files;

    const std::string glob_pattern = "yellow_tripdata_2020*";
    BOOST_LOG_TRIVIAL(debug)
      << "Looking for parquet files matching the following pattern: " << glob_pattern;
    arrow::fs::internal::Globber globber(glob_pattern);
    for (auto&& f : fs->GetFileInfo(selector).ValueOrDie()) {
        if (globber.Matches(f.base_name())) {
            discovered_files.emplace_back(std::move(f));
        }
    }
    assert(!discovered_files.empty());
    BOOST_LOG_TRIVIAL(debug) << "Filtered " << discovered_files.size() << " files";

    auto parquet_format = std::make_shared<arrow::dataset::ParquetFileFormat>();
    auto ds_factory =
      arrow::dataset::FileSystemDatasetFactory::Make(
        fs,
        discovered_files,
        std::static_pointer_cast<arrow::dataset::FileFormat>(parquet_format),
        {})
        .ValueOrDie();

    BOOST_LOG_TRIVIAL(debug) << "Materializing dataset from selected parquet files";
    return ds_factory->Finish().ValueOrDie();
}
