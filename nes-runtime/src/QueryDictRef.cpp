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

#include "Nautilus/DataTypes/DictVar.hpp"
#include "Nautilus/DataTypes/VariableSizedData.hpp"
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>

#include <nautilus/function.hpp>
#include <QueryDict.hpp>
#include <QueryDictRef.hpp>

namespace NES
{


QueryDictRef::QueryDictRef(nautilus::val<QueryDict*> queryDict)  : queryDict(queryDict)
{
}


/**
 * @brief Proxy function for dictionary insertion.
 * This function will be called at runtime.
 */
static int8_t* insertProxy(QueryDict* queryDict, int8_t* data, uint64_t size, uint64_t hash)
{
    return queryDict->insertRaw(data, size, hash);
}

DictVar QueryDictRef::insert(VariableSizedData varData, HashFunction& hashFunction) const
{
    auto hash = hashFunction.calculate(varData);
    auto out = nautilus::invoke(insertProxy, queryDict, varData.getContent(), varData.getSize(), hash);
    return DictVar(varData.getContent(), varData.getSize());
}

nautilus::val<uint32_t> QueryDictRef::getInserted() const
{
    return nautilus::invoke(+[](QueryDict* queryDict) { return queryDict->insNum; }, queryDict);
}

}
