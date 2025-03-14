#include <Databases/DDLDependencyVisitor.h>
#include <Dictionaries/getDictionaryConfigurationFromAST.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Poco/String.h>
/// proton: starts
#include <Common/parseRemoteDescription.h>
#include <Storages/ExternalStream/ExternalStreamSettings.h>
/// proton: ends

namespace DB
{

TableNamesSet getDependenciesSetFromCreateQuery(ContextPtr global_context, const QualifiedTableName & table, const ASTPtr & ast)
{
    assert(global_context == global_context->getGlobalContext());
    TableLoadingDependenciesVisitor::Data data;
    data.default_database = global_context->getCurrentDatabase();
    data.create_query = ast;
    data.global_context = global_context;
    TableLoadingDependenciesVisitor visitor{data};
    visitor.visit(ast);
    data.dependencies.erase(table);
    return data.dependencies;
}

void DDLDependencyVisitor::visit(const ASTPtr & ast, Data & data)
{
    /// Looking for functions in column default expressions and dictionary source definition
    if (const auto * function = ast->as<ASTFunction>())
        visit(*function, data);
    else if (const auto * dict_source = ast->as<ASTFunctionWithKeyValueArguments>())
        visit(*dict_source, data);
    else if (const auto * storage = ast->as<ASTStorage>())
        visit(*storage, data);
}

bool DDLDependencyVisitor::needChildVisit(const ASTPtr & node, const ASTPtr & child)
{
    if (node->as<ASTStorage>())
        return false;

    if (auto * create = node->as<ASTCreateQuery>())
    {
        if (child.get() == create->select)
            return false;
    }

    return true;
}

void DDLDependencyVisitor::visit(const ASTFunction & function, Data & data)
{
    if (function.name == "join_get" ||
        function.name == "dict_has" ||
        function.name == "dict_is_in" ||
        function.name.starts_with("dict_get"))
    {
        extractTableNameFromArgument(function, data, 0);
    }
    else if (Poco::toLower(function.name) == "in")
    {
        extractTableNameFromArgument(function, data, 1);
    }

}

void DDLDependencyVisitor::visit(const ASTFunctionWithKeyValueArguments & dict_source, Data & data)
{
    if (dict_source.name != "clickhouse")
        return;
    if (!dict_source.elements)
        return;

    auto config = getDictionaryConfigurationFromAST(data.create_query->as<ASTCreateQuery &>(), data.global_context);
    auto info = getInfoIfClickHouseDictionarySource(config, data.global_context);

    if (!info || !info->is_local)
        return;

    if (info->table_name.database.empty())
        info->table_name.database = data.default_database;
    data.dependencies.emplace(std::move(info->table_name));
}

/// proton: starts
namespace
{
bool hasLocalAddress(const String & hosts, bool secure)
{
    auto global_context = Context::getGlobalContextInstance();
    UInt16 default_port = secure ? global_context->getTCPPortSecure().value_or(0) : global_context->getTCPPort();
    auto addresses = parseRemoteDescriptionForExternalDatabase(hosts, /*max_addresses=*/ 10, /*default_port=*/ default_port);
    for (const auto & addr : addresses)
        if (isLocalAddress({addr.first, addr.second}, default_port))
            return true;

    return false;
}
}
/// proton: ends

void DDLDependencyVisitor::visit(const ASTStorage & storage, Data & data)
{
    if (!storage.engine)
        return;

    /// proton: starts
    /// Because Timeplus external streams need to get the structure of the target stream,
    /// it depends on the target stream. Thus, if a Timeplus external stream is pointing to
    /// a local stream, then add the target stream to dependencies, to make sure that the
    /// target stream is loaded before the external stream.
    if (storage.engine->name == "ExternalStream")
    {
        ExternalStreamSettings settings;
        settings.loadFromQuery(const_cast<ASTStorage &>(storage));
        if (settings.type.value == "timeplus" && hasLocalAddress(settings.hosts, settings.secure))
        {
            QualifiedTableName name{settings.db, settings.stream};
            data.dependencies.emplace(std::move(name));
        }

        return;
    }
    /// proton: ends

    if (storage.engine->name != "Dictionary")
        return;

    extractTableNameFromArgument(*storage.engine, data, 0);
}


void DDLDependencyVisitor::extractTableNameFromArgument(const ASTFunction & function, Data & data, size_t arg_idx)
{
    /// Just ignore incorrect arguments, proper exception will be thrown later
    if (!function.arguments || function.arguments->children.size() <= arg_idx)
        return;

    QualifiedTableName qualified_name;

    const auto * arg = function.arguments->as<ASTExpressionList>()->children[arg_idx].get();
    if (const auto * literal = arg->as<ASTLiteral>())
    {
        if (literal->value.getType() != Field::Types::String)
            return;

        auto maybe_qualified_name = QualifiedTableName::tryParseFromString(literal->value.get<String>());
        /// Just return if name if invalid
        if (!maybe_qualified_name)
            return;

        qualified_name = std::move(*maybe_qualified_name);
    }
    else if (const auto * identifier = dynamic_cast<const ASTIdentifier *>(arg))
    {
        /// ASTIdentifier or ASTTableIdentifier
        auto table_identifier = identifier->createTable();
        /// Just return if table identified is invalid
        if (!table_identifier)
            return;

        qualified_name.database = table_identifier->getDatabaseName();
        qualified_name.table = table_identifier->shortName();
    }
    else
    {
        assert(false);
        return;
    }

    if (qualified_name.database.empty())
    {
        /// It can be table/dictionary from default database or XML dictionary, but we cannot distinguish it here.
        qualified_name.database = data.default_database;
    }
    data.dependencies.emplace(std::move(qualified_name));
}

}
