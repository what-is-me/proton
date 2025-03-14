#pragma once

#include <Core/Names.h>
#include <base/types.h>


namespace DB
{

/**
  * Various tweaks for input/output formats. Text serialization/deserialization
  * of data types also depend on some of these settings. It is different from
  * FormatFactorySettings in that it has all necessary user-provided settings
  * combined with information from context etc, that we can use directly during
  * serialization. In contrast, FormatFactorySettings' job is to reflect the
  * changes made to user-visible format settings, such as when tweaking the
  * the format for File engine.
  * NOTE Parameters for unrelated formats and unrelated data types are collected
  * in this struct - it prevents modularity, but they are difficult to separate.
  */
struct FormatSettings
{
    /// Format will be used for streaming. Not every formats support it
    /// Option means that each chunk of data need to be formatted independently. Also each chunk will be flushed at the end of processing.
    bool enable_streaming = false;

    bool skip_unknown_fields = false;
    bool with_names_use_header = false;
    bool with_types_use_header = false;
    bool write_statistics = true;
    bool import_nested_json = false;
    bool null_as_default = true;
    bool decimal_trailing_zeros = false;
    bool defaults_for_omitted_fields = true;

    bool seekable_read = true;
    UInt64 max_rows_to_read_for_schema_inference = 100;

    String column_names_for_schema_inference = "";
    bool try_infer_integers = false;
    bool try_infer_dates = false;
    bool try_infer_datetimes = false;

    enum class DateTimeInputFormat
    {
        Basic,      /// Default format for fast parsing: YYYY-MM-DD hh:mm:ss (ISO-8601 without fractional part and timezone) or NNNNNNNNNN unix timestamp.
        BestEffort,  /// Use sophisticated rules to parse whatever possible.
        BestEffortUS  /// Use sophisticated rules to parse American style: mm/dd/yyyy
    };

    DateTimeInputFormat date_time_input_format = DateTimeInputFormat::Basic;

    enum class DateTimeOutputFormat
    {
        Simple,
        ISO,
        UnixTimestamp
    };

    enum class EscapingRule
    {
        None,
        Escaped,
        Quoted,
        CSV,
        JSON,
        XML,
        Raw
    };

    DateTimeOutputFormat date_time_output_format = DateTimeOutputFormat::Simple;

    bool input_format_ipv4_default_on_conversion_error = false;
    bool input_format_ipv6_default_on_conversion_error = false;

    UInt64 input_allow_errors_num = 0;
    Float32 input_allow_errors_ratio = 0;

    UInt64 max_binary_string_size = 0;

    struct
    {
        UInt64 row_group_size = 1000000;
        bool low_cardinality_as_dictionary = false;
        bool import_nested = false;
        bool allow_missing_columns = false;
        bool skip_columns_with_unsupported_types_in_schema_inference = false;
        bool case_insensitive_column_matching = false;
    } arrow;

    struct
    {
        String schema_registry_url;
        String output_codec;
        UInt64 output_sync_interval = 16 * 1024;
        bool allow_missing_fields = false;
        String string_column_pattern;
        UInt64 output_rows_in_file = 1;
        bool null_as_default = false;
    } avro;

    String bool_true_representation = "true";
    String bool_false_representation = "false";

    struct CSV
    {
        char delimiter = ',';
        bool allow_single_quotes = true;
        bool allow_double_quotes = true;
        bool empty_as_default = false;
        bool crlf_end_of_line = false;
        bool enum_as_number = false;
        bool arrays_as_nested_csv = false;
        String null_representation = "\\N";
        char tuple_delimiter = ',';
        bool input_format_use_best_effort_in_schema_inference = true;
    } csv;

    struct HiveText
    {
        char fields_delimiter = '\x01';
        char collection_items_delimiter = '\x02';
        char map_keys_delimiter = '\x03';
        Names input_field_names;
    } hive_text;

    struct Custom
    {
        std::string result_before_delimiter;
        std::string result_after_delimiter;
        std::string row_before_delimiter;
        std::string row_after_delimiter;
        std::string row_between_delimiter;
        std::string field_delimiter;
        EscapingRule escaping_rule = EscapingRule::Escaped;
    } custom;

    struct
    {
        bool array_of_rows = false;
        bool quote_64bit_integers = true;
        bool quote_64bit_floats = false;
        bool quote_denormals = true;
        bool quote_decimals = false;
        bool escape_forward_slashes = true;
        bool named_tuples_as_objects = false;
        bool serialize_as_strings = false;
        bool read_bools_as_numbers = true;
        bool try_infer_numbers_from_strings = false;
        bool read_numbers_as_strings = true;
    } json;

    struct
    {
        UInt64 row_group_size = 1000000;
        bool import_nested = false;
        bool allow_missing_columns = false;
        bool skip_columns_with_unsupported_types_in_schema_inference = false;
        bool case_insensitive_column_matching = false;
    } parquet;

    struct Pretty
    {
        UInt64 max_rows = 10000;
        UInt64 max_column_pad_width = 250;
        UInt64 max_value_width = 10000;
        bool color = true;

        bool output_format_pretty_row_numbers = false;

        enum class Charset
        {
            UTF8,
            ASCII,
        };

        Charset charset = Charset::UTF8;
    } pretty;

    struct
    {
        bool input_flatten_google_wrappers = false;
        bool output_nullables_with_google_wrappers = false;
        /**
         * Some buffers (kafka / rabbit) split the rows internally using callback,
         * and always send one row per message, so we can push there formats
         * without framing / delimiters (like ProtobufSingle). In other cases,
         * we have to enforce exporting at most one row in the format output,
         * because Protobuf without delimiters is not generally useful.
         */
        bool allow_multiple_rows_without_delimiter = false;
        bool skip_fields_with_unsupported_types_in_schema_inference = false;
        bool use_autogenerated_schema = true;
        std::string google_protos_path;
    } protobuf;

    struct
    {
        uint32_t client_capabilities = 0;
        size_t max_packet_size = 0;
        uint8_t * sequence_id = nullptr; /// Not null if it's MySQLWire output format used to handle MySQL protocol connections.
    } mysql_wire;

    struct
    {
        std::string regexp;
        EscapingRule escaping_rule = EscapingRule::Raw;
        bool skip_unmatched = false;
    } regexp;

    struct
    {
        std::string format_schema;
        std::string format_schema_path;
        bool is_server = false;
        std::string output_format_schema;
    } schema;

    /// proton: starts
    struct
    {
        bool skip_cert_check = false;
        bool force_refresh_schema = false;
        std::string url;
        std::string credentials;
        std::string private_key_file;
        std::string certificate_file;
        std::string ca_location;
        std::string topic_name; /// for output to fetch the schema
    } kafka_schema_registry{};
    /// proton: ends

    struct
    {
        String resultset_format;
        String row_format;
        String row_between_delimiter;
    } template_settings;

    struct
    {
        bool empty_as_default = false;
        bool crlf_end_of_line = false;
        String null_representation = "\\N";
        bool enum_as_number = false;
        bool input_format_use_best_effort_in_schema_inference = true;
    } tsv;

    struct
    {
        bool interpret_expressions = true;
        bool deduce_templates_of_expressions = true;
        bool accurate_types_of_literals = true;
        /// proton: starts
        bool no_commas_between_rows = false;
        /// proton: ends
    } values;

    struct
    {
        bool import_nested = false;
        bool allow_missing_columns = false;
        int64_t row_batch_size = 100'000;
        bool skip_columns_with_unsupported_types_in_schema_inference = false;
        bool case_insensitive_column_matching = false;
        bool output_string_as_string = false;
    } orc;

    /// For capnProto format we should determine how to
    /// compare ClickHouse Enum and Enum from schema.
    enum class EnumComparingMode
    {
        BY_NAMES, // Names in enums should be the same, values can be different.
        BY_NAMES_CASE_INSENSITIVE, // Case-insensitive name comparison.
        BY_VALUES, // Values should be the same, names can be different.
    };

    struct
    {
        EnumComparingMode enum_comparing_mode = EnumComparingMode::BY_VALUES;
        bool skip_fields_with_unsupported_types_in_schema_inference = false;
        bool use_autogenerated_schema = true;
    } capn_proto;

    struct
    {
        UInt64 number_of_columns = 0;
    } msgpack;

    /// proton: starts
    struct
    {
        String rawstore_time_extraction_type;
        String rawstore_time_extraction_rule;
    } rawstore;
    /// proton: ends
};

}
