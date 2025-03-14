#include "AggregateFunctionUniq.h"

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeUUID.h>

namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


namespace Streaming
{


/** `DataForVariadic` is a data structure that will be used for `uniq` aggregate function of multiple arguments.
  * It differs, for example, in that it uses a trivial hash function, since `uniq` of many arguments first hashes them out itself.
  */
template <typename Data, template <bool, bool> typename DataForVariadic>
AggregateFunctionPtr
createAggregateFunctionUniq(const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    assertNoParameters(name, params);

    if (argument_types.size() < 2)
        throw Exception("Incorrect number of arguments for aggregate function " + name,
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    bool use_exact_hash_function = !isAllArgumentsContiguousInMemory(argument_types);

    if (argument_types.size() == 2)
    {
        const IDataType & argument_type = *argument_types[0];

        AggregateFunctionPtr res(createWithNumericType<AggregateFunctionUniq, Data>(*argument_types[0], argument_types));

        WhichDataType which(argument_type);
        if (res)
            return res;
        else if (which.isDate())
            return std::make_shared<AggregateFunctionUniq<DataTypeDate::FieldType, Data>>(argument_types);
        else if (which.isDate32())
            return std::make_shared<AggregateFunctionUniq<DataTypeDate32::FieldType, Data>>(argument_types);
        else if (which.isDateTime())
            return std::make_shared<AggregateFunctionUniq<DataTypeDateTime::FieldType, Data>>(argument_types);
        else if (which.isStringOrFixedString())
            return std::make_shared<AggregateFunctionUniq<String, Data>>(argument_types);
        else if (which.isUUID())
            return std::make_shared<AggregateFunctionUniq<DataTypeUUID::FieldType, Data>>(argument_types);
        else if (which.isTuple())
        {
            if (use_exact_hash_function)
                return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic<true, true>>>(argument_types);
            else
                return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic<false, true>>>(argument_types);
        }
    }

    /// "Variadic" method also works as a fallback generic case for single argument.
    if (use_exact_hash_function)
        return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic<true, false>>>(argument_types);
    else
        return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic<false, false>>>(argument_types);
}

template <bool is_exact, template <typename, bool> typename Data, template <bool, bool, bool> typename DataForVariadic, bool is_able_to_parallelize_merge>
AggregateFunctionPtr
createAggregateFunctionUniq(const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    assertNoParameters(name, params);

    if (argument_types.size() < 2)
        throw Exception("Incorrect number of arguments for aggregate function " + name,
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    /// We use exact hash function if the user wants it;
    /// or if the arguments are not contiguous in memory, because only exact hash function have support for this case.
    bool use_exact_hash_function = is_exact || !isAllArgumentsContiguousInMemory(argument_types);

    if (argument_types.size() == 2)
    {
        const IDataType & argument_type = *argument_types[0];

        AggregateFunctionPtr res(createWithNumericType<AggregateFunctionUniq, Data, is_able_to_parallelize_merge>(*argument_types[0], argument_types));

        WhichDataType which(argument_type);
        if (res)
            return res;
        else if (which.isDate())
            return std::make_shared<AggregateFunctionUniq<DataTypeDate::FieldType, Data<DataTypeDate::FieldType, is_able_to_parallelize_merge>>>(argument_types);
        else if (which.isDate32())
            return std::make_shared<AggregateFunctionUniq<DataTypeDate32::FieldType, Data<DataTypeDate32::FieldType, is_able_to_parallelize_merge>>>(argument_types);
        else if (which.isDateTime())
            return std::make_shared<AggregateFunctionUniq<DataTypeDateTime::FieldType, Data<DataTypeDateTime::FieldType, is_able_to_parallelize_merge>>>(argument_types);
        else if (which.isStringOrFixedString())
            return std::make_shared<AggregateFunctionUniq<String, Data<String, is_able_to_parallelize_merge>>>(argument_types);
        else if (which.isUUID())
            return std::make_shared<AggregateFunctionUniq<DataTypeUUID::FieldType, Data<DataTypeUUID::FieldType, is_able_to_parallelize_merge>>>(argument_types);
        else if (which.isTuple())
        {
            if (use_exact_hash_function)
                return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic<true, true, is_able_to_parallelize_merge>>>(argument_types);
            else
                return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic<false, true, is_able_to_parallelize_merge>>>(argument_types);
        }
    }

    /// "Variadic" method also works as a fallback generic case for single argument.
    if (use_exact_hash_function)
        return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic<true, false, is_able_to_parallelize_merge>>>(argument_types);
    else
        return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic<false, false, is_able_to_parallelize_merge>>>(argument_types);
}

void registerAggregateFunctionsUniqRetract(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = false };

    factory.registerFunction("__unique_retract",
        {createAggregateFunctionUniq<AggregateFunctionUniqUniquesHashSetData, AggregateFunctionUniqUniquesHashSetDataForVariadic>, properties});

    auto assign_bool_param = [](const std::string & name, const DataTypes & argument_types, const Array & params, const Settings * settings)
    {
        return createAggregateFunctionUniq<
            true, AggregateFunctionUniqExactData, AggregateFunctionUniqExactDataForVariadic, false /* is_able_to_parallelize_merge */>(name, argument_types, params, settings);
    };
    factory.registerFunction("__unique_exact_retract", {assign_bool_param, properties});

}
}
}
