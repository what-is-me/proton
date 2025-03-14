#include "config.h"

#if USE_AWS_S3

#include <IO/S3Common.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionS3.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>
#include <Access/Common/AccessFlags.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/StorageS3.h>
#include <Storages/StorageURL.h>
#include <Formats/FormatFactory.h>
#include "registerTableFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


/// This is needed to avoid copy-pase. Because s3Cluster arguments only differ in additional argument (first) - cluster name
void TableFunctionS3::parseArgumentsImpl(const String & error_message, ASTs & args, ContextPtr context, StorageS3Configuration & s3_configuration)
{
    if (auto named_collection = getURLBasedDataSourceConfiguration(args, context))
    {
        auto [common_configuration, storage_specific_args] = named_collection.value();
        s3_configuration.set(common_configuration);
        StorageS3::processNamedCollectionResult(s3_configuration, storage_specific_args);
    }
    else
    {
        if (args.empty() || args.size() > 6)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, error_message);

        auto header_it = StorageURL::collectHeaders(args, s3_configuration, context);
        if (header_it != args.end())
            args.erase(header_it);

        for (auto & arg : args)
            arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

        /// Size -> argument indexes
        static auto size_to_args = std::map<size_t, std::map<String, size_t>>
        {
            {1, {{}}},
            {2, {{"format", 1}}},
            {5, {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}, {"structure", 4}}},
            {6, {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}, {"structure", 4}, {"compression_method", 5}}}
        };

        std::map<String, size_t> args_to_idx;
        /// For 4 arguments we support 2 possible variants:
        /// s3(source, format, structure, compression_method) and s3(source, access_key_id, access_key_id, format)
        /// We can distinguish them by looking at the 2-nd argument: check if it's a format name or not.
        if (args.size() == 4)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(args[1], "format/access_key_id");
            if (second_arg == "auto" || FormatFactory::instance().getAllFormats().contains(second_arg))
                args_to_idx = {{"format", 1}, {"structure", 2}, {"compression_method", 3}};

            else
                args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}};
        }
        /// For 3 arguments we support 2 possible variants:
        /// s3(source, format, structure) and s3(source, access_key_id, access_key_id)
        /// We can distinguish them by looking at the 2-nd argument: check if it's a format name or not.
        else if (args.size() == 3)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(args[1], "format/access_key_id");
            if (second_arg == "auto" || FormatFactory::instance().getAllFormats().contains(second_arg))
                args_to_idx = {{"format", 1}, {"structure", 2}};
            else
                args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}};
        }
        else
        {
            args_to_idx = size_to_args[args.size()];
        }

        /// This argument is always the first
        s3_configuration.url = checkAndGetLiteralArgument<String>(args[0], "url");

        if (args_to_idx.contains("format"))
            s3_configuration.format = checkAndGetLiteralArgument<String>(args[args_to_idx["format"]], "format");

        if (args_to_idx.contains("structure"))
            s3_configuration.structure = checkAndGetLiteralArgument<String>(args[args_to_idx["structure"]], "structure");

        if (args_to_idx.contains("compression_method"))
            s3_configuration.compression_method = checkAndGetLiteralArgument<String>(args[args_to_idx["compression_method"]], "compression_method");

        if (args_to_idx.contains("access_key_id"))
            s3_configuration.auth_settings.access_key_id = checkAndGetLiteralArgument<String>(args[args_to_idx["access_key_id"]], "access_key_id");

        if (args_to_idx.contains("secret_access_key"))
            s3_configuration.auth_settings.secret_access_key = checkAndGetLiteralArgument<String>(args[args_to_idx["secret_access_key"]], "secret_access_key");
    }

    if (s3_configuration.format == "auto")
        s3_configuration.format = FormatFactory::instance().getFormatFromFileName(s3_configuration.url, true);
}

void TableFunctionS3::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    /// Parse args
    ASTs & args_func = ast_function->children;

    const auto message = fmt::format(
        "The signature of table function {} could be the following:\n" \
        " - url\n" \
        " - url, format\n" \
        " - url, format, structure\n" \
        " - url, access_key_id, secret_access_key\n" \
        " - url, format, structure, compression_method\n" \
        " - url, access_key_id, secret_access_key, format\n" \
        " - url, access_key_id, secret_access_key, format, structure\n" \
        " - url, access_key_id, secret_access_key, format, structure, compression_method",
        getName());

    if (args_func.size() != 1)
        throw Exception("Table function '" + getName() + "' must have arguments.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    auto & args = args_func.at(0)->children;

    parseArgumentsImpl(message, args, context, configuration);
}

ColumnsDescription TableFunctionS3::getActualTableStructure(ContextPtr context) const
{
    if (configuration.structure == "auto")
    {
        context->checkAccess(getSourceAccessType());
        return StorageS3::getTableStructureFromData(configuration, false, std::nullopt, context);
    }

    return parseColumnsListFromString(configuration.structure, context);
}

StoragePtr TableFunctionS3::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    S3::URI s3_uri (configuration.url);

    ColumnsDescription columns;
    if (configuration.structure != "auto")
        columns = parseColumnsListFromString(configuration.structure, context);
    else if (!structure_hint.empty())
        columns = structure_hint;

    StoragePtr storage = StorageS3::create(
        configuration,
        StorageID(getDatabaseName(), table_name),
        columns,
        ConstraintsDescription{},
        String{},
        context,
        /// No format_settings for table function S3
        std::nullopt);

    storage->startup();

    return storage;
}


class TableFunctionGCS : public TableFunctionS3
{
public:
    static constexpr auto name = "gcs";
    std::string getName() const override
    {
        return name;
    }
private:
    const char * getStorageTypeName() const override { return "GCS"; }
};

class TableFunctionOSS : public TableFunctionS3
{
public:
    static constexpr auto name = "oss";
    std::string getName() const override
    {
        return name;
    }
private:
    const char * getStorageTypeName() const override { return "OSS"; }
};


void registerTableFunctionGCS(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionGCS>(
        {.documentation
         = {.description=R"(The table function can be used to read the data stored on Google Cloud Storage.)",
            .examples{{"gcs", "SELECT * FROM gcs(url, hmac_key, hmac_secret)", ""}},
            .categories{"DataLake"}},
         .allow_readonly = false});
}

void registerTableFunctionS3(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionS3>(
        {.documentation
         = {.description=R"(The table function can be used to read the data stored on AWS S3.)",
            .examples{{"s3", "SELECT * FROM s3(url, access_key_id, secret_access_key)", ""}},
            .categories{"DataLake"}},
         .allow_readonly = false});
}

void registerTableFunctionCOS(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionCOS>();
}

}

#endif
