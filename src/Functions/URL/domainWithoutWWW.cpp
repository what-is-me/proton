#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include "domain.h"

namespace DB
{

struct NameDomainWithoutWWW { static constexpr auto name = "domain_without_www"; };
using FunctionDomainWithoutWWW = FunctionStringToString<ExtractSubstringImpl<ExtractDomain<true, false>>, NameDomainWithoutWWW>;

struct NameDomainWithoutWWWRFC { static constexpr auto name = "domain_without_www_rfc"; };
using FunctionDomainWithoutWWWRFC = FunctionStringToString<ExtractSubstringImpl<ExtractDomain<true, true>>, NameDomainWithoutWWWRFC>;


REGISTER_FUNCTION(DomainWithoutWWW)
{
    factory.registerFunction<FunctionDomainWithoutWWW>(
    FunctionDocumentation{
        .description=R"(
Extracts the hostname from a URL, removing the leading "www." if present.

The URL can be specified with or without a scheme.
If the argument can't be parsed as URL, the function returns an empty string.
        )",
        .examples{{"domainWithoutWWW", "SELECT domain_without_www('https://www.clickhouse.com')", ""}},
        .categories{"URL"}
    });
    factory.registerFunction<FunctionDomainWithoutWWWRFC>(
    FunctionDocumentation{
        .description=R"(Similar to `domain_without_www` but follows stricter rules to be compatible with RFC 3986 and less performant.)",
        .examples{},
        .categories{"URL"}
    });
}

}
