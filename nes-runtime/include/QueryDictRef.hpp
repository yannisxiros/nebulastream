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

#pragma once
#include <cstdint>
#include <memory>

#include <nautilus/val.hpp>
#include "Nautilus/Interface/Hash/HashFunction.hpp"
#include "Nautilus/DataTypes/DictVar.hpp"
#include "Nautilus/DataTypes/VariableSizedData.hpp"

namespace NES
{
struct QueryDict;

/**
 * @brief Nautilus interface to the QueryDict struct.
 */
class QueryDictRef
{
public:
    explicit QueryDictRef(nautilus::val<QueryDict*> queryDict);

    /**
     * @brief Dummy insert function that will be implemented using invoke.
     * @param varData Variable sized data to insert.
     * @return DictVar representing the inserted data.
     */
    DictVar insert(VariableSizedData varData, HashFunction& hashFunc) const;

    /**
     * @brief Returns the current size of the dictionary.
     * @return Current dictionary size.
     */
    nautilus::val<uint32_t> getInserted() const;

private:
    nautilus::val<QueryDict*> queryDict;
};

}
