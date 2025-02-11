#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <base/types.h>

namespace DB
{
namespace Streaming
{
class ReplayStreamStep final : public ITransformingStep
{
public:
    ReplayStreamStep(const DataStream & input_stream_, Float32 replay_speed_, const String & replay_time_col_, std::vector<Int64> shards_last_sns_, std::optional<String> start_time = std::nullopt, std::optional<String> end_time = std::nullopt);
    String getName() const override { return "ReplayStep"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    Float32 replay_speed = 0;
    std::vector<Int64> shards_last_sns;
    const String replay_time_col;
    std::optional<String> start_time;
    std::optional<String> end_time;

};
}
}
