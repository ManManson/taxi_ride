#include <arrow/compute/api_aggregate.h>
#include <arrow/compute/exec/exec_plan.h>
#include <arrow/compute/exec/expression.h>
#include <arrow/compute/exec/options.h>
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/scanner.h>
#include <arrow/record_batch.h>
#include <arrow/util/async_generator.h>
#include <arrow/util/logging.h>
#include <arrow/util/vector.h>

#include "average_distances.hh"

#include <boost/log/trivial.hpp>

namespace compute = arrow::compute;

std::vector<std::string>
AverageDistances::GetProjectionColumns() const
{
    return { "tpep_pickup_datetime",
             "tpep_dropoff_datetime",
             "passenger_count",
             "trip_distance" };
}

compute::Expression
AverageDistances::MakeFilterExpression(TimestampScalarPtr start_date,
                                       TimestampScalarPtr end_date) const
{
    auto pickup_field_ref = compute::field_ref("tpep_pickup_datetime"),
         dropoff_field_ref = compute::field_ref("tpep_dropoff_datetime");
    if (start_date && end_date) {
        return compute::and_(
          compute::greater_equal(pickup_field_ref, compute::literal(start_date)),
          compute::less_equal(dropoff_field_ref, compute::literal(end_date)));
    }
    if (start_date) {
        return compute::greater_equal(pickup_field_ref, compute::literal(start_date));
    }
    if (end_date) {
        return compute::less_equal(dropoff_field_ref, compute::literal(end_date));
    }
    BOOST_LOG_TRIVIAL(fatal) << "unreachable";
    return {};
}

AverageDistances::ResultType
AverageDistances::RecordBatchToMap(const arrow::RecordBatch& b) const
{
    ResultType res;
    // GetValues is fed `1` as an argument because (maybe by convention, at least
    // I couldn't find exact mentions of it in the Arrow sources) the first internal
    // buffer at index 0 always contains a null bitmap. Values seem to be always stored in
    // the second buffer.
    auto mean_trip_data = b.column_data(0)->GetValues<double>(1);
    auto passenger_count = b.column_data(1)->GetValues<double>(1);
    for (size_t i = 0, end = b.num_rows(); i < end; ++i) {
        res.emplace(passenger_count[i], mean_trip_data[i]);
    }
    return res;
}

AverageDistances::ResultType
AverageDistances::GetAverageDistances(TimestampScalarPtr start_date,
                                      TimestampScalarPtr end_date)
{
    ResultType res;
    // Initiate a filtered and projected scan on the stored dataset reference.
    //
    // After that, apply a hash aggregate function to calculate the mean
    // values of `trip_distance`:s grouped by `passenger_count`.
    auto scan_builder = _ds.NewScan().ValueOrDie();

    assert(scan_builder->Project(GetProjectionColumns()).ok());
    auto projected_schema = scan_builder->projected_schema();
    if (start_date || end_date) {
        assert(
          scan_builder
            ->Filter(MakeFilterExpression(std::move(start_date), std::move(end_date)))
            .ok());
    }

    // Construct the scanner and convert it to RecordBatchReader since
    // it is an expected way to consume date from the
    // `arrow::compute::SourceNodeOptions`.
    auto ds_scanner = scan_builder->Finish().ValueOrDie();
    auto ds_reader = ds_scanner->ToRecordBatchReader().ValueOrDie();

    std::vector<compute::Declaration> decl_nodes;

    // Source node (filtered and projected)
    decl_nodes.emplace_back(
      compute::Declaration{ "source",
                            compute::SourceNodeOptions{
                              projected_schema,
                              compute::MakeReaderGenerator(
                                std::move(ds_reader), arrow::internal::GetCpuThreadPool())
                                .ValueOrDie() } });

    // Aggregation node
    std::vector<compute::internal::Aggregate> aggs = { { "hash_mean", nullptr } };
    compute::AggregateNodeOptions agg_node_opts(
      aggs, { "trip_distance" }, { "mean_trip_distance" }, { "passenger_count" });
    decl_nodes.emplace_back(compute::Declaration{ "aggregate", agg_node_opts });
    // Sink node
    arrow::AsyncGenerator<arrow::util::optional<arrow::compute::ExecBatch>> sink_gen;
    decl_nodes.emplace_back(
      compute::Declaration{ "sink", compute::SinkNodeOptions{ &sink_gen } });

    // Construct execution plan and assign compute declarations to it as a
    // sequence of nodes
    auto exec_plan = compute::ExecPlan::Make().ValueOrDie();

    auto nodes_seq = compute::Declaration::Sequence(std::move(decl_nodes));
    auto exec_node = nodes_seq.AddToPlan(exec_plan.get()).ValueOrDie();
    BOOST_LOG_TRIVIAL(debug) << "Validating execution plan";
    assert(exec_node->Validate().ok() && exec_plan->Validate().ok());
    BOOST_LOG_TRIVIAL(debug) << "Executing the plan";
    assert(exec_plan->StartProducing().ok());
    // Consume the data from the source and asynchronously push the processed
    // data to the sink.
    // Execution plan returns the result as optional<ExecBatch>:es,
    // so apply a transformation to get rid of optionals.
    auto gen_fut = arrow::CollectAsyncGenerator(sink_gen);
    auto start_and_collect =
      arrow::AllComplete({ exec_plan->finished(), arrow::Future<>(gen_fut) })
        .Then([gen_fut]() -> arrow::Result<std::vector<compute::ExecBatch>> {
            ARROW_ASSIGN_OR_RAISE(auto collected, gen_fut.result());
            return arrow::internal::MapVector(
              [](arrow::util::optional<compute::ExecBatch> batch) {
                  return std::move(*batch);
              },
              std::move(collected));
        });
    // Wait for the result to become available
    auto output = start_and_collect.MoveResult().ValueOrDie();
    BOOST_LOG_TRIVIAL(debug) << "Plan executed successfully, processing the results";
    if (output.empty()) {
        return {};
    }

    // Construct the schema for the result
    arrow::SchemaBuilder out_schema_builder;
    assert(
      out_schema_builder
        .AddField(std::make_shared<arrow::Field>("mean_trip_distance", arrow::float64()))
        .ok());
    assert(
      out_schema_builder.AddField(_ds.schema()->GetFieldByName("passenger_count")).ok());
    auto out_schema = out_schema_builder.Finish().ValueOrDie();

    for (const auto& e : output) {
        // Bind the ExecBatch to an explicit schema and convert to a more convenient
        // RecordBatch interface
        auto rec_batch = e.ToRecordBatch(out_schema).ValueOrDie();
        res.merge(RecordBatchToMap(*rec_batch));
    }

    return res;
}
