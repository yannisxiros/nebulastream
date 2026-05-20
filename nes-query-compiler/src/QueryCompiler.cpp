/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/


#include <QueryCompiler.hpp>

#include <memory>
#include <vector>
#include <Configuration/WorkerConfiguration.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Phases/LowerToCompiledQueryPlanPhase.hpp>
#include <Phases/LowerToPhysicalOperators.hpp>
#include <Phases/PipeliningPhase.hpp>
#include <Util/DumpMode.hpp>
#include <CompiledQueryPlan.hpp>
#include <ErrorHandling.hpp>

namespace NES::QueryCompilation
{

/// This phase should be as dumb as possible and not further decisions should be made here.
std::unique_ptr<CompiledQueryPlan> QueryCompiler::compileQuery(std::unique_ptr<QueryCompilationRequest> request)
{
    auto lowerToCompiledQueryPlanPhase = LowerToCompiledQueryPlanPhase(request->dumpCompilationResult);

    std::vector<std::string> collectedConstants;
    FunctionProvider::beginConstantCollection(collectedConstants);
    auto queryPlan = LowerToPhysicalOperators::apply(request->queryPlan.getPlan(), defaultQueryExecution);
    FunctionProvider::endConstantCollection();

    auto pipelinedQueryPlan = PipeliningPhase::apply(queryPlan);
    auto compiled = lowerToCompiledQueryPlanPhase.apply(pipelinedQueryPlan);
    compiled->constantStrings = std::move(collectedConstants);
    return compiled;
}
}
