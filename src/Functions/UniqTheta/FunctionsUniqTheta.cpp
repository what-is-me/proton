#include <Functions/FunctionFactory.h>

#include "FunctionsUniqTheta.h"

#if USE_DATASKETCHES

namespace DB
{

REGISTER_FUNCTION(UniqTheta)
{
    factory.registerFunction<FunctionUniqThetaIntersect>(
            FunctionDocumentation{.description = R"(
Two uniqThetaSketch objects to do intersect calculation(set operation ∩), the result is a new uniqThetaSketch.

A uniqThetaSketch object is to be constructed by aggregation function uniqTheta with -State.

UniqThetaSketch is a data structure storage of approximate values set.
For more information on RoaringBitmap, see: [Theta Sketch Framework](https://datasketches.apache.org/docs/Theta/ThetaSketchFramework.html).

Typical usage:
[example:typical]
)",
                    .examples{
                            {"typical", "select finalize_aggregation(uniq_theta_intersect(arrayReduce('uniq_theta_state',[1,2]), array_reduce('uniq_theta_state',[2,3,4])));", ""}},
                    .categories{"uniqTheta"}
            });

    factory.registerFunction<FunctionUniqThetaUnion>(
            FunctionDocumentation{.description = R"(
Two uniqThetaSketch objects to do union calculation(set operation ∪), the result is a new uniqThetaSketch.

A uniqThetaSketch object is to be constructed by aggregation function uniqTheta with -State.

UniqThetaSketch is a data structure storage of approximate values set.
For more information on RoaringBitmap, see: [Theta Sketch Framework](https://datasketches.apache.org/docs/Theta/ThetaSketchFramework.html).

Typical usage:
[example:typical]
)",
                    .examples{
                            {"typical", "select finalize_aggregation(uniq_theta_union(array_reduce('uniq_theta_state',[1,2]), array_reduce('uniq_theta_state',[2,3,4])));", ""}},
                    .categories{"uniqTheta"}
            });
    factory.registerFunction<FunctionUniqThetaNot>(
            FunctionDocumentation{.description = R"(
Two uniqThetaSketch objects to do a_not_b calculation(set operation ×), the result is a new uniqThetaSketch.

A uniqThetaSketch object is to be constructed by aggregation function uniqTheta with -State.

UniqThetaSketch is a data structure storage of approximate values set.
For more information on RoaringBitmap, see: [Theta Sketch Framework](https://datasketches.apache.org/docs/Theta/ThetaSketchFramework.html).

Typical usage:
[example:typical]
)",
                    .examples{
                            {"typical", "select finalize_aggregation(uniq_theta_not(array_reduce('uniq_theta_state',[1,2]), array_reduce('uniq_theta_state',[2,3,4])));", ""}},
                    .categories{"uniqTheta"}
            });
}

}

#endif
