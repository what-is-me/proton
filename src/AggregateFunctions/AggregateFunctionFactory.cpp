#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>

#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>

#include <Interpreters/Context.h>

#include <Common/StringUtils/StringUtils.h>
#include <Common/typeid_cast.h>
#include <Common/CurrentThread.h>

#include <Poco/String.h>

#include <Functions/FunctionFactory.h>

/// proton: starts
#include <Functions/UserDefined/UserDefinedFunctionFactory.h>
/// proton: ends

namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_AGGREGATION;
}

const String & getAggregateFunctionCanonicalNameIfAny(const String & name)
{
    return AggregateFunctionFactory::instance().getCanonicalNameIfAny(name);
}

void AggregateFunctionFactory::registerFunction(const String & name, Value creator_with_properties, CaseSensitiveness case_sensitiveness)
{
    if (creator_with_properties.creator == nullptr)
        throw Exception("AggregateFunctionFactory: the aggregate function " + name + " has been provided "
            " a null constructor", ErrorCodes::LOGICAL_ERROR);

    if (!aggregate_functions.emplace(name, creator_with_properties).second)
        throw Exception("AggregateFunctionFactory: the aggregate function name '" + name + "' is not unique",
            ErrorCodes::LOGICAL_ERROR);

    if (case_sensitiveness == CaseInsensitive)
    {
        auto key = Poco::toLower(name);
        if (!case_insensitive_aggregate_functions.emplace(key, creator_with_properties).second)
            throw Exception("AggregateFunctionFactory: the case insensitive aggregate function name '" + name + "' is not unique",
                ErrorCodes::LOGICAL_ERROR);
        case_insensitive_name_mapping[key] = name;
    }
}

static DataTypes convertLowCardinalityTypesToNested(const DataTypes & types)
{
    DataTypes res_types;
    res_types.reserve(types.size());
    for (const auto & type : types)
        res_types.emplace_back(recursiveRemoveLowCardinality(type));

    return res_types;
}

/// proton: starts
AggregateFunctionPtr AggregateFunctionFactory::get(
    const String & name,
    const DataTypes & argument_types,
    const Array & parameters,
    AggregateFunctionProperties & out_properties,
    bool is_changelog_input) const
/// proton: ends
{
    auto types_without_low_cardinality = convertLowCardinalityTypesToNested(argument_types);

    /// If one of the types is Nullable, we apply aggregate function combinator "_null".

    if (std::any_of(types_without_low_cardinality.begin(), types_without_low_cardinality.end(),
        [](const auto & type) { return type->isNullable(); }))
    {
        AggregateFunctionCombinatorPtr combinator = AggregateFunctionCombinatorFactory::instance().tryFindSuffix("_null");
        if (!combinator)
            throw Exception("Logical error: cannot find aggregate function combinator to apply a function to Nullable arguments.",
                ErrorCodes::LOGICAL_ERROR);

        DataTypes nested_types = combinator->transformArguments(types_without_low_cardinality);
        Array nested_parameters = combinator->transformParameters(parameters);

        bool has_null_arguments = std::any_of(types_without_low_cardinality.begin(), types_without_low_cardinality.end(),
            [](const auto & type) { return type->onlyNull(); });

        AggregateFunctionPtr nested_function = getImpl(
            name, nested_types, nested_parameters, out_properties, has_null_arguments, is_changelog_input);

        // Pure window functions are not real aggregate functions. Applying
        // combinators doesn't make sense for them, they must handle the
        // nullability themselves. Another special case is functions from Nothing
        // that are rewritten to AggregateFunctionNothing, in this case
        // nested_function is nullptr.
        if (!nested_function || !nested_function->isOnlyWindowFunction())
            return combinator->transformAggregateFunction(nested_function, out_properties, types_without_low_cardinality, parameters);
    }

    auto with_original_arguments = getImpl(name, types_without_low_cardinality, parameters, out_properties, false, is_changelog_input);

    if (!with_original_arguments)
        throw Exception("Logical error: AggregateFunctionFactory returned nullptr", ErrorCodes::LOGICAL_ERROR);
    return with_original_arguments;
}

/// proton: starts
AggregateFunctionPtr AggregateFunctionFactory::getImpl(
    const String & name_param,
    const DataTypes & argument_types,
    const Array & parameters,
    AggregateFunctionProperties & out_properties,
    bool has_null_arguments,
    bool is_changelog_input) const
/// proton: ends
{
    String name = getAliasToOrName(name_param);
    bool is_case_insensitive = false;
    Value found;

    /// Find by exact match.
    if (auto it = aggregate_functions.find(name); it != aggregate_functions.end())
    {
        found = it->second;
    }

    if (auto jt = case_insensitive_aggregate_functions.find(Poco::toLower(name)); jt != case_insensitive_aggregate_functions.end())
    {
        found = jt->second;
        is_case_insensitive = true;
    }

    ContextPtr query_context;
    if (CurrentThread::isInitialized())
        query_context = CurrentThread::get().getQueryContext();

    if (found.creator)
    {
        out_properties = found.properties;

        if (query_context && query_context->getSettingsRef().log_queries)
            query_context->addQueryFactoriesInfo(
                    Context::QueryLogFactories::AggregateFunction, is_case_insensitive ? Poco::toLower(name) : name);

        /// The case when aggregate function should return NULL on NULL arguments. This case is handled in "get" method.
        if (!out_properties.returns_default_when_only_null && has_null_arguments)
            return nullptr;

        const Settings * settings = query_context ? &query_context->getSettingsRef() : nullptr;
        return found.creator(name, argument_types, parameters, settings);
    }

    /// Combinators of aggregate functions.
    /// For every aggregate function 'agg' and combiner '-Comb' there is a combined aggregate function with the name 'aggComb',
    ///  that can have different number and/or types of arguments, different result type and different behaviour.

    if (AggregateFunctionCombinatorPtr combinator = AggregateFunctionCombinatorFactory::instance().tryFindSuffix(name))
    {
        const std::string & combinator_name = combinator->getName();

        if (combinator->isForInternalUsageOnly())
            throw Exception(ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION,
                "Aggregate function combinator '{}' is only for internal usage",
                combinator_name);

        if (query_context && query_context->getSettingsRef().log_queries)
            query_context->addQueryFactoriesInfo(Context::QueryLogFactories::AggregateFunctionCombinator, combinator_name);

        String nested_name = name.substr(0, name.size() - combinator_name.size());

        if (combinator_name == "_time_weighted")
        {
            if (nested_name == "avg")
                nested_name = "avg_weighted";
            else if (nested_name == "median")
                nested_name = "median_exact_weighted";           
            else
                throw Exception(ErrorCodes::ILLEGAL_AGGREGATION, "Unknown aggregate function '{}'", name);
        }

        /// Nested identical combinators (i.e. uniqCombinedIfIf) is not
        /// supported (since they don't work -- silently).
        ///
        /// But non-identical is supported and works. For example,
        /// uniqCombinedIfMergeIf is useful in cases when the underlying
        /// storage stores AggregateFunction(uniqCombinedIf) and in SELECT you
        /// need to filter aggregation result based on another column.

        if (!combinator->supportsNesting() && nested_name.ends_with(combinator_name))
        {
            throw Exception(ErrorCodes::ILLEGAL_AGGREGATION,
                "Nested identical combinator '{}' is not supported",
                combinator_name);
        }

        DataTypes nested_types = combinator->transformArguments(argument_types);
        Array nested_parameters = combinator->transformParameters(parameters);

        AggregateFunctionPtr nested_function = get(nested_name, nested_types, nested_parameters, out_properties);
        return combinator->transformAggregateFunction(nested_function, out_properties, argument_types, parameters);
    }

    /// proton: starts. Check user defined aggr function
    auto aggr = UserDefinedFunctionFactory::getAggregateFunction(name, argument_types, parameters, out_properties, is_changelog_input);
    if (aggr)
        return aggr;
    /// proton: ends

    String extra_info;
    if (FunctionFactory::instance().hasNameOrAlias(name))
        extra_info = ". There is an ordinary function with the same name, but aggregate function is expected here";

    auto hints = this->getHints(name);
    if (!hints.empty())
        throw Exception(ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION,
                        "Unknown aggregate function {}{}. Maybe you meant: {}", name, extra_info, toString(hints));
    else
        throw Exception(ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION, "Unknown aggregate function {}{}", name, extra_info);
}

/// proton: starts
AggregateFunctionPtr AggregateFunctionFactory::tryGet(
    const String & name,
    const DataTypes & argument_types,
    const Array & parameters,
    AggregateFunctionProperties & out_properties,
    bool is_changelog_input) const
/// proton: ends
{
    return isAggregateFunctionName(name)
        /// proton: starts
        ? get(name, argument_types, parameters, out_properties, is_changelog_input)
        /// proton: ends
        : nullptr;
}


std::optional<AggregateFunctionProperties> AggregateFunctionFactory::tryGetPropertiesImpl(const String & name_param) const
{
    String name = getAliasToOrName(name_param);
    Value found;

    /// Find by exact match.
    if (auto it = aggregate_functions.find(name); it != aggregate_functions.end())
    {
        found = it->second;
    }

    if (auto jt = case_insensitive_aggregate_functions.find(Poco::toLower(name)); jt != case_insensitive_aggregate_functions.end())
        found = jt->second;

    if (found.creator)
        return found.properties;

    /// Combinators of aggregate functions.
    /// For every aggregate function 'agg' and combiner '-Comb' there is a combined aggregate function with the name 'aggComb',
    ///  that can have different number and/or types of arguments, different result type and different behaviour.

    if (AggregateFunctionCombinatorPtr combinator = AggregateFunctionCombinatorFactory::instance().tryFindSuffix(name))
    {
        if (combinator->isForInternalUsageOnly())
            return {};

        String nested_name = name.substr(0, name.size() - combinator->getName().size());

        /// NOTE: It's reasonable to also allow to transform properties by combinator.
        return tryGetPropertiesImpl(nested_name);
    }

    return {};
}


std::optional<AggregateFunctionProperties> AggregateFunctionFactory::tryGetProperties(const String & name) const
{
    return tryGetPropertiesImpl(name);
}


bool AggregateFunctionFactory::isAggregateFunctionName(const String & name) const
{
    if (aggregate_functions.contains(name) || isAlias(name))
        return true;

    String name_lowercase = Poco::toLower(name);
    if (case_insensitive_aggregate_functions.contains(name_lowercase) || isAlias(name_lowercase))
        return true;

    if (AggregateFunctionCombinatorPtr combinator = AggregateFunctionCombinatorFactory::instance().tryFindSuffix(name))
        return isAggregateFunctionName(name.substr(0, name.size() - combinator->getName().size()));

    /// proton: starts
    try
    {
        return UserDefinedFunctionFactory::instance().isAggregateFunctionName(name);
    }
    catch (...)
    {
        /// We ignore the failure of user defined function checking
        return false;
    }
    /// proton: ends
}

/// proton: starts
bool AggregateFunctionFactory::hasNameOrAlias(const String & name) const
{
    if (hasBuiltInNameOrAlias(name))
        return true;

    try
    {
        return UserDefinedFunctionFactory::instance().isAggregateFunctionName(name);
    }
    catch (...)
    {
        /// We ignore the failure of user defined function checking
        return false;
    }
}
/// proton: ends

AggregateFunctionFactory & AggregateFunctionFactory::instance()
{
    static AggregateFunctionFactory ret;
    return ret;
}

}
