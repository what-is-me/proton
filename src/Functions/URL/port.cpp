#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/StringUtils/StringUtils.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include "domain.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

template<bool conform_rfc>
struct FunctionPortImpl : public IFunction
{
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1 && arguments.size() != 2)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                            + std::to_string(arguments.size()) + ", should be 1 or 2",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!WhichDataType(arguments[0].type).isString())
            throw Exception("Illegal type " + arguments[0].type->getName() + " of first argument of function " + getName() + ". Must be string.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() == 2 && !WhichDataType(arguments[1].type).isUInt16())
            throw Exception("Illegal type " + arguments[1].type->getName() + " of second argument of function " + getName() + ". Must be uint16.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt16>();
    }


    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        UInt16 default_port = 0;
        if (arguments.size() == 2)
        {
            const auto * port_column = checkAndGetColumn<ColumnConst>(arguments[1].column.get());
            if (!port_column)
                throw Exception("Second argument for function " + getName() + " must be constant uint16", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            default_port = port_column->getValue<UInt16>();
        }

        const ColumnPtr url_column = arguments[0].column;
        if (const ColumnString * url_strs = checkAndGetColumn<ColumnString>(url_column.get()))
        {
            auto col_res = ColumnVector<UInt16>::create();
            typename ColumnVector<UInt16>::Container & vec_res = col_res->getData();
            vec_res.resize(url_column->size());

            vector(default_port, url_strs->getChars(), url_strs->getOffsets(), vec_res);
            return col_res;
        }
        else
            throw Exception(
                "Illegal column " + arguments[0].column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
}

private:
    static void vector(UInt16 default_port, const ColumnString::Chars & data, const ColumnString::Offsets & offsets, PaddedPODArray<UInt16> & res)
    {
        size_t size = offsets.size();

        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            res[i] = extractPort(default_port, data, prev_offset, offsets[i] - prev_offset - 1);
            prev_offset = offsets[i];
        }
}

    static UInt16 extractPort(UInt16 default_port, const ColumnString::Chars & buf, size_t offset, size_t size)
    {
        const char * p = reinterpret_cast<const char *>(buf.data()) + offset;
        const char * end = p + size;

        std::string_view host;
        if constexpr (conform_rfc)
            host = getURLHostRFC(p, size);
        else
            host = getURLHost(p, size);

        if (host.empty())
            return default_port;
        if (host.size() == size)
            return default_port;

        p = host.data() + host.size();
        if (*p++ != ':')
            return default_port;

        Int64 port = default_port;
        while (p < end)
        {
            if (*p == '/')
                break;
            if (!isNumericASCII(*p))
                return default_port;

            port = (port * 10) + (*p - '0');
            if (port < 0 || port > static_cast<UInt16>(-1))
                return default_port;
            ++p;
        }
        return port;
    }
};

struct FunctionPort : public FunctionPortImpl<false>
{
    static constexpr auto name = "port";
    String getName() const override { return name; }
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionPort>(); }
};

struct FunctionPortRFC : public FunctionPortImpl<true>
{
    static constexpr auto name = "port_rfc";
    String getName() const override { return name; }
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionPortRFC>(); }
};

REGISTER_FUNCTION(Port)
{
    factory.registerFunction<FunctionPort>(FunctionDocumentation
    {
        .description=R"(Returns the port or `default_port` if there is no port in the URL (or in case of validation error).)",
        .categories{"URL"}
    });
    factory.registerFunction<FunctionPortRFC>(FunctionDocumentation
    {
        .description=R"(Similar to `port`, but conforms to RFC 3986.)",
        .categories{"URL"}
    });
}

}
