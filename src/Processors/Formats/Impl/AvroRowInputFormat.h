#pragma once

#include "config.h"

#if USE_AVRO

#include <unordered_map>
#include <map>
#include <vector>

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Formats/FormatSchemaInfo.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>

#include <DataFile.hh>
#include <Decoder.hh>
#include <Schema.hh>
#include <ValidSchema.hh>

/// proton: starts
#include <Formats/KafkaSchemaRegistryForAvro.h>
#include <Processors/Formats/ISchemaWriter.h>
/// proton: ends

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

class InputStreamReadBufferAdapter;

class AvroDeserializer
{
public:
    AvroDeserializer(const Block & header, avro::ValidSchema schema, bool allow_missing_fields, bool null_as_default_);
    void deserializeRow(MutableColumns & columns, avro::Decoder & decoder, RowReadExtension & ext) const;

private:
    using DeserializeFn = std::function<bool(IColumn & column, avro::Decoder & decoder)>;
    using DeserializeNestedFn = std::function<bool(IColumn & column, avro::Decoder & decoder)>;

    using SkipFn = std::function<void(avro::Decoder & decoder)>;
    DeserializeFn createDeserializeFn(avro::NodePtr root_node, DataTypePtr target_type);
    SkipFn createSkipFn(avro::NodePtr root_node);
    DeserializeFn wrapSkipFn(SkipFn && skip_fn); /// proton: added

    struct Action
    {
        enum Type {Noop, Deserialize, Skip, Record, Union, Nested};
        Type type;
        /// Deserialize
        int target_column_idx;
        DeserializeFn deserialize_fn;
        /// Skip
        SkipFn skip_fn;
        /// Record | Union
        std::vector<Action> actions;
        /// For flattened Nested column
        std::vector<Int64> nested_column_indexes;
        std::vector<DeserializeFn> nested_deserializers;


        Action() : type(Noop) {}

        Action(int target_column_idx_, DeserializeFn deserialize_fn_)
            : type(Deserialize)
            , target_column_idx(target_column_idx_)
            , deserialize_fn(deserialize_fn_) {}

        explicit Action(SkipFn skip_fn_)
            : type(Skip)
            , skip_fn(skip_fn_) {}

        Action(std::vector<Int64> nested_column_indexes_, std::vector<DeserializeFn> nested_deserializers_)
            : type(Nested)
            , nested_column_indexes(nested_column_indexes_)
            , nested_deserializers(nested_deserializers_) {}

        static Action recordAction(std::vector<Action> field_actions) { return Action(Type::Record, field_actions); }

        static Action unionAction(std::vector<Action> branch_actions) { return Action(Type::Union, branch_actions); }


        void execute(MutableColumns & columns, avro::Decoder & decoder, RowReadExtension & ext) const
        {
            switch (type)
            {
                case Noop:
                    break;
                case Deserialize:
                    ext.read_columns[target_column_idx] = deserialize_fn(*columns[target_column_idx], decoder);
                    break;
                case Skip:
                    skip_fn(decoder);
                    break;
                case Record:
                    for (const auto & action : actions)
                        action.execute(columns, decoder, ext);
                    break;
                case Nested:
                    deserializeNested(columns, decoder, ext);
                    break;
                case Union:
                    auto index = decoder.decodeUnionIndex();
                    if (index >= actions.size())
                    {
                        throw Exception(ErrorCodes::INCORRECT_DATA, "Union index out of boundary");
                    }
                    actions[index].execute(columns, decoder, ext);
                    break;
            }
        }
    private:
        Action(Type type_, std::vector<Action> actions_)
            : type(type_)
            , actions(actions_) {}

        void deserializeNested(MutableColumns & columns, avro::Decoder & decoder, RowReadExtension & ext) const;
    };

    /// Populate actions by recursively traversing root schema
    AvroDeserializer::Action createAction(const Block & header, const avro::NodePtr & node, const std::string & current_path = "");

    /// Bitmap of columns found in Avro schema
    std::vector<bool> column_found;
    /// Deserialize/Skip actions for a row
    Action row_action;
    /// Map from name of named Avro type (record, enum, fixed) to SkipFn.
    /// This is to avoid infinite recursion when  Avro schema contains self-references. e.g. LinkedList
    std::map<avro::Name, SkipFn> symbolic_skip_fn_map;

    bool null_as_default = false;
};

class AvroRowInputFormat final : public IRowInputFormat
{
public:
    AvroRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_, const FormatSettings & format_settings_);

    String getName() const override { return "AvroRowInputFormat"; }

private:
    bool readRow(MutableColumns & columns, RowReadExtension & ext) override;
    void readPrefix() override;

    std::unique_ptr<avro::DataFileReaderBase> file_reader_ptr;
    std::unique_ptr<AvroDeserializer> deserializer_ptr;
    FormatSettings format_settings;
};

/// Confluent framing + Avro binary datum encoding. Mainly used for Kafka.
/// Uses 3 caches:
/// 1. global: schema registry cache (base_url -> SchemaRegistry)
/// 2. SchemaRegistry: schema cache (schema_id -> schema)
/// 3. AvroConfluentRowInputFormat: deserializer cache (schema_id -> AvroDeserializer)
/// This is needed because KafkaStorage creates a new instance of InputFormat per a batch of messages
class AvroConfluentRowInputFormat final : public IRowInputFormat
{
public:
    AvroConfluentRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_, const FormatSettings & format_settings_);
    String getName() const override { return "AvroConfluentRowInputFormat"; }
    /// proton: starts
    void resetParser() override;
    void setReadBuffer(ReadBuffer & buf) override;
    /// proton: ends

private:
    bool readRow(MutableColumns & columns, RowReadExtension & ext) override; /* proton: updated */

    bool allowSyncAfterError() const override { return true; }
    void syncAfterError() override;

    std::shared_ptr<KafkaSchemaRegistryForAvro> schema_registry;
    using SchemaId = uint32_t;
    std::unordered_map<SchemaId, AvroDeserializer> deserializer_cache;
    const AvroDeserializer & getOrCreateDeserializer(SchemaId schema_id);

    std::unique_ptr<InputStreamReadBufferAdapter> input_stream;
    avro::DecoderPtr decoder;
    FormatSettings format_settings;
};

/// proton: starts
/// Avro binary datum encoding. Mainly used for Kafka, and similar technologies.
class AvroSchemaRowInputFormat final : public IRowInputFormat
{
public:
    AvroSchemaRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_, const FormatSchemaInfo & schema_info, const FormatSettings & format_settings);
    String getName() const override { return "AvroSchemaRowInputFormat"; }
    void resetParser() override;
    void setReadBuffer(ReadBuffer & buf) override;

private:
    bool readRow(MutableColumns & columns, RowReadExtension & ext) override; /* proton: updated */

    bool allowSyncAfterError() const override { return true; }
    void syncAfterError() override;

    std::unique_ptr<InputStreamReadBufferAdapter> input_stream;
    AvroDeserializer deserializer;
    avro::DecoderPtr decoder;
};

class AvroSchemaWriter : public IExternalSchemaWriter
{
public:
    explicit AvroSchemaWriter(std::string_view schema_body_, const FormatSettings & settings_);

    void validate() override;
    bool write(bool replace_if_exist) override;

private:
    FormatSchemaInfo schema_info;
};
/// proton: ends

class AvroSchemaReader : public ISchemaReader
{
public:
    AvroSchemaReader(ReadBuffer & in_, bool confluent_, const FormatSettings & format_settings_);

    NamesAndTypesList readSchema() override;

private:
    DataTypePtr avroNodeToDataType(avro::NodePtr node);

    bool confluent;
    const FormatSettings format_settings;
};

}

#endif
