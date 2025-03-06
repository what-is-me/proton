#include <Storages/ExternalStream/StorageExternalStream.h>

#include <IO/Kafka/Connection.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Parsers/ASTCreateQuery.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Storages/ExternalStream/ExternalStreamSettings.h>
#include <Storages/ExternalStream/ExternalStreamTypes.h>
#include <Storages/ExternalStream/Kafka/Kafka.h>
#include <Storages/ExternalStream/Log/FileLog.h>
#include <Storages/ExternalStream/Pulsar/Pulsar.h>
#include <Storages/ExternalStream/StorageExternalStreamImpl.h>
#include <Storages/ExternalStream/Timeplus/Timeplus.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageFactory.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>

#include <string>
#include <re2/re2.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int INCORRECT_NUMBER_OF_COLUMNS;
extern const int INCORRECT_QUERY;
extern const int INVALID_SETTING_VALUE;
extern const int NOT_IMPLEMENTED;
extern const int TYPE_MISMATCH;
extern const int LICENSE_VIOLATED;
}

namespace
{
ExpressionActionsPtr buildShardingKeyExpression(ASTPtr sharding_key, ContextPtr context, const NamesAndTypesList & columns)
{
    auto syntax_result = TreeRewriter(context).analyze(sharding_key, columns);
    return ExpressionAnalyzer(sharding_key, syntax_result, context).getActions(true);
}

void validateEngineArgs(ContextPtr context, ASTs & engine_args, const ColumnsDescription & columns)
{
    if (engine_args.empty())
        return;

    auto sharding_expr = buildShardingKeyExpression(engine_args[0], context, columns.getAllPhysical());
    const auto & block = sharding_expr->getSampleBlock();
    if (block.columns() != 1)
        throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS, "Sharding expression must return exactly one column");

    auto type = block.getByPosition(0).type;

    if (!type->isValueRepresentedByInteger())
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Sharding expression has type {}, but should be one of integer type", type->getName());
}

StoragePtr createExternalStream(
    IStorage * storage,
    std::unique_ptr<ExternalStreamSettings> external_stream_settings,
    ContextPtr context [[maybe_unused]],
    const ASTs & engine_args,
    StorageInMemoryMetadata & storage_metadata,
    bool attach,
    ExternalStreamCounterPtr external_stream_counter,
    ContextPtr context_)
{
    auto type = external_stream_settings->type.value;

    if (type.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "External stream type is required in settings");


    if (type == StreamTypes::KAFKA || type == StreamTypes::REDPANDA)
        return std::make_unique<ExternalStream::Kafka>(
            storage,
            std::move(external_stream_settings),
            engine_args,
            storage_metadata,
            attach,
            std::move(external_stream_counter),
            std::move(context_));

    if (type == StreamTypes::TIMEPLUS)
        return std::make_unique<ExternalStream::Timeplus>(
            storage, storage_metadata, std::move(external_stream_settings), attach, std::move(context_));

    if (type == StreamTypes::LOG && context->getSettingsRef()._tp_enable_log_stream_expr.value)
        return std::make_unique<FileLog>(storage, std::move(external_stream_settings), std::move(context_));

#if USE_PULSAR
    if (type == StreamTypes::PULSAR)
        return std::make_unique<ExternalStream::Pulsar>(
            storage, std::move(external_stream_settings), attach, std::move(external_stream_counter), std::move(context_));

#endif
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unknown external stream type: {}", type);
}

}

UInt64 StorageExternalStream::readFailed(bool reset) const
{
    if (!external_stream_counter)
        return 0;
    return external_stream_counter->readFailed(reset);
}

UInt64 StorageExternalStream::writtenFailed(bool reset) const
{
    if (!external_stream_counter)
        return 0;
    return external_stream_counter->writtenFailed(reset);
}

StorageExternalStream::StorageExternalStream(
    const ASTs & engine_args,
    const StorageID & table_id_,
    ContextPtr context_,
    const ColumnsDescription & columns_,
    const String & comment,
    ASTStorage * storage_def,
    bool attach)
    : StorageProxy(table_id_), WithContext(context_->getGlobalContext()), external_stream_counter(std::make_shared<ExternalStreamCounter>())
{
    auto external_stream_settings = std::make_unique<ExternalStreamSettings>();
    external_stream_settings->loadFromQuery(*storage_def);
    external_stream_settings->loadFromConfigFile();

    if (columns_.empty() && external_stream_settings->type.value != StreamTypes::TIMEPLUS)
        /// This is the same error reported by InterpreterCreateQuery
        throw Exception(
            ErrorCodes::INCORRECT_QUERY, "Incorrect CREATE query: required list of column descriptions or AS section or SELECT.");

    external_stream_type = external_stream_settings->type.value;

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setComment(comment);

    if (!columns_.empty())
        storage_metadata.setColumns(columns_);

    if (storage_def->partition_by != nullptr)
    {
        ASTPtr partition_by_ast{storage_def->partition_by->clone()};
        storage_metadata.partition_key = KeyDescription::getKeyFromAST(partition_by_ast, columns_, context_);
    }

    /// Some external streams require the column information, thus we need to set the metadata before creating the stream.
    setInMemoryMetadata(storage_metadata);

    auto metadata = getInMemoryMetadata();
    auto stream = createExternalStream(
        this,
        std::move(external_stream_settings),
        context_,
        engine_args,
        metadata,
        attach,
        external_stream_counter,
        std::move(context_));
    external_stream.swap(stream);
    /// Some external streams fetch the structure in other ways, thus need to set the metadata again here in case it's updated.
    setInMemoryMetadata(metadata);
}

void StorageExternalStream::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    return getNested()->read(
        query_plan, column_names, storage_snapshot, query_info, context_, processed_stage, max_block_size, num_streams);
}

void registerStorageExternalStream(StorageFactory & factory)
{
    /** * ExternalStream engine arguments : ExternalStream(shard_by_expr)
    * - shard_by_expr
    **/
    auto creator_fn = [](const StorageFactory::Arguments & args) {
        validateEngineArgs(args.getLocalContext(), args.engine_args, args.columns);

        if (args.storage_def->settings != nullptr)
            return StorageExternalStream::create(
                args.engine_args, args.table_id, args.getContext(), args.columns, args.comment, args.storage_def, args.attach);
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "External stream requires correct settings setup");
    };

    factory.registerStorage(
        "ExternalStream",
        creator_fn,
        StorageFactory::StorageFeatures{
            .supports_settings = true,
            .supports_sort_order = true, // for partition by
            .supports_schema_inference = true,
        });
}

}
