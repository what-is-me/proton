#include <Storages/ExternalStream/Kafka/KafkaSink.h>

#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/Kafka/mapErrorCode.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/createBlockSelector.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Storages/ExternalStream/Kafka/Kafka.h>
#include <Common/ProtonCommon.h>

#include <boost/algorithm/string/predicate.hpp>

#include <numeric>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_WRITE_TO_KAFKA;
extern const int INVALID_SETTING_VALUE;
extern const int KAFKA_PRODUCER_STOPPED;
extern const int TYPE_MISMATCH;
}

namespace
{
ExpressionActionsPtr buildExpression(const Block & header, const ASTPtr & expr_ast, const ContextPtr & context)
{
    assert(expr_ast);

    auto syntax_result = TreeRewriter(context).analyze(const_cast<ASTPtr &>(expr_ast), header.getNamesAndTypesList());
    return ExpressionAnalyzer(expr_ast, syntax_result, context).getActions(false);
}
}

namespace ExternalStream
{

ChunkSharder::ChunkSharder(ExpressionActionsPtr sharding_expr_, const String & column_name)
    : sharding_expr(sharding_expr_), sharding_key_column_name(column_name)
{
}

ChunkSharder::ChunkSharder()
{
    random_sharding = true;
}

BlocksWithShard ChunkSharder::shard(Block block, UInt32 shard_cnt)
{
    /// no topics have zero partitions
    assert(shard_cnt > 0);

    if (shard_cnt == 1)
        return {BlockWithShard{Block(std::move(block)), 0}};

    if (random_sharding)
        return {BlockWithShard{Block(std::move(block)), static_cast<int32_t>(getNextShardIndex(shard_cnt))}};

    return doSharding(std::move(block), shard_cnt);
}

BlocksWithShard ChunkSharder::doSharding(Block block, UInt32 shard_cnt) const
{
    auto selector = createSelector(block, shard_cnt);

    Blocks partitioned_blocks{static_cast<size_t>(shard_cnt)};

    for (UInt32 i = 0; i < shard_cnt; ++i)
        partitioned_blocks[i] = block.cloneEmpty();

    for (size_t pos = 0; pos < block.columns(); ++pos)
    {
        MutableColumns partitioned_columns = block.getByPosition(pos).column->scatter(shard_cnt, selector);
        for (UInt32 i = 0; i < shard_cnt; ++i)
            partitioned_blocks[i].getByPosition(pos).column = std::move(partitioned_columns[i]);
    }

    BlocksWithShard blocks_with_shard;
    blocks_with_shard.reserve(partitioned_blocks.size());

    /// Filter out empty blocks
    for (size_t i = 0; i < partitioned_blocks.size(); ++i)
    {
        if (partitioned_blocks[i].rows() > 0)
            blocks_with_shard.emplace_back(std::move(partitioned_blocks[i]), i);
    }

    return blocks_with_shard;
}

IColumn::Selector ChunkSharder::createSelector(Block block, UInt32 shard_cnt) const
{
    std::vector<UInt64> slot_to_shard(shard_cnt);
    std::iota(slot_to_shard.begin(), slot_to_shard.end(), 0);

    sharding_expr->execute(block);

    const auto & key_column = block.getByName(sharding_key_column_name);

/// If key_column.type is DataTypeLowCardinality, do shard according to its dictionaryType
#define CREATE_FOR_TYPE(TYPE) \
    if (typeid_cast<const DataType##TYPE *>(key_column.type.get())) \
        return createBlockSelector<TYPE>(*key_column.column, slot_to_shard); \
    else if (auto * type_low_cardinality = typeid_cast<const DataTypeLowCardinality *>(key_column.type.get())) \
        if (typeid_cast<const DataType##TYPE *>(type_low_cardinality->getDictionaryType().get())) \
            return createBlockSelector<TYPE>(*key_column.column->convertToFullColumnIfLowCardinality(), slot_to_shard);

    CREATE_FOR_TYPE(UInt8)
    CREATE_FOR_TYPE(UInt16)
    CREATE_FOR_TYPE(UInt32)
    CREATE_FOR_TYPE(UInt64)
    CREATE_FOR_TYPE(Int8)
    CREATE_FOR_TYPE(Int16)
    CREATE_FOR_TYPE(Int32)
    CREATE_FOR_TYPE(Int64)

#undef CREATE_FOR_TYPE

    throw Exception{ErrorCodes::TYPE_MISMATCH, "Sharding key expression does not evaluate to an integer type"};
}

KafkaSink::KafkaSink(
    Kafka & kafka,
    const Block & header,
    const ASTPtr & message_key_ast,
    const DB::Kafka::ProducerPtr & producer_,
    ExternalStreamCounterPtr external_stream_counter_,
    Poco::Logger * logger_,
    ContextPtr context)
    : SinkToStorage(header, ProcessorID::ExternalTableDataSinkID)
    , producer(producer_)
    , partition_cnt(producer->getPartitionCount())
    , one_message_per_row(kafka.produceOneMessagePerRow())
    , topic_refresh_interval_ms(kafka.topicRefreshIntervalMs())
    , pending_data(context->getSettingsRef().kafka_max_message_size.value)
    , external_stream_counter(external_stream_counter_)
    , logger(logger_)
{
    pending_data.resize(0); /// no pending data at the beginning

    /// If the buffer_size (kafka_max_message_size) is reached, the buffer will be forced to flush.
    wb = std::make_unique<WriteBufferFromKafkaSink>(
        [this](char * pos, size_t len, size_t total_len) { addMessageToBatch(pos, len, total_len); },
        [this]() { tryCarryOverPendingData(); },
        /*buffer_size=*/context->getSettingsRef().kafka_max_message_size.value);

    const auto & data_format = kafka.dataFormat();
    assert(!data_format.empty());

    Block output_header = header;
    /// `_tp_message_key` should not be part of the message payload.
    if (auto pos = output_header.tryGetPositionByName(ProtonConsts::RESERVED_MESSAGE_KEY); pos)
    {
        assert(!message_key_ast);

        message_key_column_name = ProtonConsts::RESERVED_MESSAGE_KEY;
        output_header.erase(*pos);
    }

    if (message_key_ast)
    {
        message_key_expr = buildExpression(header, message_key_ast, context);
        const auto & sample_block = message_key_expr->getSampleBlock();
        /// The last column is the key column, the others are required columns (to be used to calculate the key value).
        message_key_column_name = sample_block.getColumnsWithTypeAndName().back().name;
    }

    if (one_message_per_row)
    {
        /// The callback allows `IRowOutputFormat` based formats produce one Kafka message per row.
        writer = FormatFactory::instance().getOutputFormat(
            data_format,
            *wb,
            output_header,
            context,
            [this](auto & /*column*/, auto /*row*/) {
                wb->markOffset();
                wb->next();
            },
            kafka.getFormatSettings(context));
        if (dynamic_cast<IRowOutputFormat *>(writer.get()) == nullptr)
            LOG_WARNING(
                logger, "Data format `{}` is not a row-based format, `one_message_per_row` setting will not be applied", data_format);
    }
    else
    {
        auto max_rows_per_message = context->getSettingsRef().max_insert_block_size.value;
        writer = FormatFactory::instance().getOutputFormat(
            data_format,
            *wb,
            output_header,
            context,
            [this, max_rows_per_message](auto & /*column*/, auto /*row*/) {
                wb->markOffset();
                if (++rows_in_current_message >= max_rows_per_message)
                {
                    external_stream_counter->addToMessagesByRow(1);
                    wb->next();
                }
            },
            kafka.getFormatSettings(context));
    }
    writer->setAutoFlush();

    if (kafka.hasCustomShardingExpr())
    {
        const auto & ast = kafka.shardingExprAst();
        partitioner = std::make_unique<ChunkSharder>(buildExpression(header, ast, context), ast->getColumnName());
    }
    else
        partitioner = std::make_unique<ChunkSharder>();

    /// Fetch partition count regularly, so that it can send data to new partitions.
    background_jobs.scheduleOrThrowOnError([this, refresh_interval_ms = static_cast<UInt64>(topic_refresh_interval_ms)]() {
        LOG_INFO(logger, "Start topic partition count refreshing job");
        auto metadata_refresh_stopwatch = Stopwatch();
        /// Use a small sleep interval to avoid blocking operation for a long just (in case refresh_interval_ms is big).
        auto sleep_ms = std::min(UInt64(500), refresh_interval_ms);
        while (true)
        {
            if (is_finished.test())
                break;

            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
            /// Fetch topic metadata for partition updates
            if (metadata_refresh_stopwatch.elapsedMilliseconds() < refresh_interval_ms)
                continue;

            metadata_refresh_stopwatch.restart();

            try
            {
                partition_cnt = producer->getPartitionCount();
            }
            catch (...) /// do not break the loop until finished
            {
                LOG_WARNING(logger, "Failed to describe topic, error code: {}", getCurrentExceptionMessage(true, true));
            }
        }
        LOG_INFO(logger, "Stopped topic partition count refreshing job");
    });
}

/// When this function is called, there could be two scenarios:
/// 1) len == total_len. That means the whole Chunk (or the whole row, when one_message_per_row is true) can fit into the buffer.
///    In this case, we just need to create a rd_kafka_message_t, and push it to current_batch.
///
/// 2) len < total_len. That means the Chunk (or a row) is too big to fit into the buffer. In this case, the data between
///    [pos + len, pos + total_len] will be copied into pending_data so that the bufer can accept more data until it gets
///    complete data. Then it can create a rd_kafka_message_t.
///
/// In this way, we limit the size of one single Kafka message, so that it can avoid hitting the max.message.size.
/// However, it's still possible that, one single row is still too big and it exceeds that limit. There is nothing we can do about it for now.
void KafkaSink::addMessageToBatch(char * pos, size_t len, size_t total_len)
{
    auto pending_size = pending_data.size();

    /// There are complete data to consume.
    if (len > 0)
    {
        StringRef key = keys_for_current_batch.empty() ? "" : keys_for_current_batch[current_batch_row++];

        /// Data at pos (which is in the WriteBuffer) will be overwritten, thus it must be kept somewhere else (in `batch_payload`).
        auto msg_size = pending_size + len;
        nlog::ByteVector payload{msg_size};
        payload.resize(msg_size); /// set the size to the right value
        if (pending_size != 0u)
            memcpy(payload.data(), pending_data.data(), pending_size);
        memcpy(payload.data() + pending_size, pos, len);

        current_batch.push_back(rd_kafka_message_t{
            .err = RD_KAFKA_RESP_ERR_NO_ERROR,
            .rkt = nullptr,
            .partition = next_partition,
            .payload = payload.data(),
            .len = msg_size,
            .key = const_cast<char *>(key.data),
            .key_len = key.size,
            .offset = 0, /// fixme (yokofly) is the `offset` no need settings? the old code use default value
            ._private = this,
        });

        batch_payload.push_back(std::move(payload));
        ++state.outstandings;

        pending_data.resize(0);
        pending_size = 0;
        rows_in_current_message = 0;
    }

    if (len == total_len)
        /// Nothing left
        return;

    /// There are some remaining incomplete data, copy them to pending_data.
    auto remaining = total_len - len;
    pending_data.resize(pending_size + remaining);
    memcpy(pending_data.data() + pending_size, pos + len, remaining);

    external_stream_counter->addToMessagesBySize(1);
}

void KafkaSink::tryCarryOverPendingData()
{
    /// If there are pending data and it can be fit into the buffer, then write the data back to the buffer,
    /// so that we can use the buffer to limit the message size.
    /// If the pending data are too big, that means we get a over-size row.
    if (!pending_data.empty() && pending_data.size() < wb->available())
    {
        wb->write(pending_data.data(), pending_data.size());
        pending_data.resize(0);
    }
}

void KafkaSink::consume(Chunk chunk)
{
    if (!chunk.hasRows())
        return;

    if (unlikely(is_finished.test()))
        throw Exception(
            ErrorCodes::KAFKA_PRODUCER_STOPPED,
            "KafkaSink cannot consume data because producer has stopped, likely the underlying external stream is gone");

    auto total_rows = chunk.rows();
    auto block = getHeader().cloneWithColumns(chunk.detachColumns());
    BlocksWithShard blocks;

    /// We do swap with empty std::vector here to avoid some big underlying memory hang out there forever.
    /// since std::vector::clear still holds on to its allocated memory
    if (!message_key_column_name.empty())
    {
        if (!keys_for_current_batch.empty())
        {
            std::vector<StringRef> keys;
            keys_for_current_batch.swap(keys);
        }
        keys_for_current_batch.reserve(chunk.rows());
        current_batch_row = 0;

        /// When message key is used, partitioning is based on message key.
        blocks = BlocksWithShard{{std::move(block), 0}};
    }
    else
        blocks = partitioner->shard(std::move(block), partition_cnt);

    if (!current_batch.empty())
    {
        std::vector<rd_kafka_message_t> batch;
        current_batch.swap(batch);
    }

    if (!batch_payload.empty())
    {
        std::vector<nlog::ByteVector> payload;
        batch_payload.swap(payload);
    }

    /// When one_message_per_row is set to true, one Kafka message will be generated for each row.
    /// Otherwise, all rows in the same block will be in the same kafka message.
    if (one_message_per_row)
    {
        current_batch.reserve(chunk.rows());
        batch_payload.reserve(chunk.rows());
    }
    else
    {
        current_batch.reserve(blocks.size());
        batch_payload.reserve(blocks.size());
    }

    for (auto & block_with_shard : blocks)
    {
        next_partition = message_key_column_name.empty() ? block_with_shard.shard : RD_KAFKA_PARTITION_UA;

        if (message_key_column_name.empty())
        {
            writer->write(block_with_shard.block);
            continue;
        }

        /// Compute and collect message keys.
        if (message_key_expr)
            message_key_expr->execute(block_with_shard.block);

        const auto & message_key_column{*block_with_shard.block.getByName(message_key_column_name).column};
        size_t rows{message_key_column.size()};
        for (size_t i = 0; i < rows; ++i)
        {
            if (message_key_column.isNullable())
            {
                const ColumnNullable & col = assert_cast<const ColumnNullable &>(message_key_column);
                if (col.isNullAt(i))
                    keys_for_current_batch.push_back("");
                else
                    keys_for_current_batch.push_back(col.getDataAt(0));
            }
            else
                keys_for_current_batch.push_back(message_key_column.getDataAt(i));
        }

        /// 1. After `message_key_expr->execute`, the columns in `block_with_shard.block` could be out-of-order.
        /// We have to make sure the the column order in `block_with_shard.block` exactly matches the order in header,
        /// otherwise, the output format writer will panic.
        /// 2. Remove the `_tp_message_key` values from the chunk (because it won't be in the payload).
        Block blk;
        blk.reserve(getHeader().columns() - (message_key_expr ? 0 : 1));
        for (const auto & col : getHeader())
            if (col.name != ProtonConsts::RESERVED_MESSAGE_KEY)
                blk.insert(std::move(block_with_shard.block.getByName(col.name)));

        writer->write(blk);
    }

    /// With `wb->setAutoFlush()`, it makes sure that all messages are generated for the chunk at this point.
    rd_kafka_produce_batch(
        producer->getTopicHandle(),
        RD_KAFKA_PARTITION_UA,
        RD_KAFKA_MSG_F_FREE | RD_KAFKA_MSG_F_PARTITION | RD_KAFKA_MSG_F_BLOCK,
        current_batch.data(),
        static_cast<int32_t>(current_batch.size()));

    rd_kafka_resp_err_t err{RD_KAFKA_RESP_ERR_NO_ERROR};
    for (size_t i = 0; i < current_batch.size(); ++i)
    {
        if (current_batch[i].err != RD_KAFKA_RESP_ERR_NO_ERROR)
        {
            err = current_batch[i].err;
            external_stream_counter->addWrittenFailed(1);
        }
        else
        {
            /// payload of messages which are succesfully handled by rd_kafka_produce_batch will be free'ed by librdkafka
            batch_payload[i].release();
            external_stream_counter->addWrittenBytes(current_batch[i].len);
        }
    }

    /// Clean up all the bookkeepings for the batch.
    std::vector<rd_kafka_message_t> batch;
    current_batch.swap(batch);

    std::vector<nlog::ByteVector> payload;
    batch_payload.swap(payload);

    if (!keys_for_current_batch.empty())
    {
        std::vector<StringRef> keys;
        keys_for_current_batch.swap(keys);
    }

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
        throw Exception(DB::Kafka::mapErrorCode(err), rd_kafka_err2str(err));
    else
        external_stream_counter->addWrittenRows(total_rows);
}

void KafkaSink::onFinish()
{
    if (is_finished.test_and_set())
        return;

    LOG_INFO(logger, "Stopping producing messages");

    producer->stop();

    background_jobs.wait();

    /// if there are no outstandings, no need to do flushing
    if (outstandingMessages() == 0)
        return;

    /// Make sure all outstanding requests are transmitted and handled.
    /// It should not block for ever here, otherwise, it will block proton from stopping the job
    /// or block proton from terminating.
    if (auto err = rd_kafka_flush(producer->getHandle(), 15000 /* time_ms */); err)
        LOG_ERROR(logger, "Failed to flush kafka producer, error={}", rd_kafka_err2str(err));

    if (auto err = lastSeenError(); err != RD_KAFKA_RESP_ERR_NO_ERROR)
        LOG_ERROR(logger, "Failed to send messages, last_seen_error={}", rd_kafka_err2str(err));

    /// if flush does not return an error, the delivery report queue should be empty
    if (outstandingMessages() > 0)
        LOG_ERROR(logger, "Not all messsages are sent successfully, expected={} actual={}", outstandings(), acked());
}

void KafkaSink::onMessageDelivery(rd_kafka_t * /* producer */, const rd_kafka_message_t * msg, void * /*opaque*/)
{
    auto * sink = static_cast<KafkaSink *>(msg->_private);
    sink->onMessageDelivery(msg->err);
}

void KafkaSink::onMessageDelivery(rd_kafka_resp_err_t err)
{
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        state.last_error_code.store(err);
        ++state.error_count;
    }
    else
        ++state.acked;
}

KafkaSink::~KafkaSink()
{
    onFinish();
}

void KafkaSink::checkpoint(CheckpointContextPtr context)
{
    do
    {
        if (auto err = lastSeenError(); err != RD_KAFKA_RESP_ERR_NO_ERROR)
            throw Exception(
                DB::Kafka::mapErrorCode(err),
                "Failed to send messages, error_count={} last_error={}",
                errorCount(),
                rd_kafka_err2str(err));

        auto outstanding_msgs = outstandingMessages();
        if (outstanding_msgs == 0)
            break;

        LOG_INFO(logger, "Waiting for {} outstandings on checkpointing", outstanding_msgs);

        if (is_finished.test())
        {
            /// for a final check, it should not wait for too long
            if (auto err = rd_kafka_flush(producer->getHandle(), 15000 /* time_ms */); err)
                throw Exception(DB::Kafka::mapErrorCode(err), "Failed to flush kafka producer, error={}", rd_kafka_err2str(err));

            if (auto err = lastSeenError(); err != RD_KAFKA_RESP_ERR_NO_ERROR)
                throw Exception(
                    DB::Kafka::mapErrorCode(err),
                    "Failed to send messages, error_count={} last_error={}",
                    errorCount(),
                    rd_kafka_err2str(err));

            if (outstandingMessages() > 0)
                throw Exception(
                    ErrorCodes::CANNOT_WRITE_TO_KAFKA,
                    "Not all messsages are sent successfully, expected={} actual={}",
                    outstandings(),
                    acked());

            break;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    } while (true);

    state.reset();
    IProcessor::checkpoint(context);
}

void KafkaSink::State::reset()
{
    outstandings.store(0);
    acked.store(0);
    error_count.store(0);
    last_error_code.store(0);
}

}

}
