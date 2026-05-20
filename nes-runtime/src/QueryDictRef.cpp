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
    if (queryDict->dictSize + size > QueryDict::data_region_cap)
    {
        return  data;
    }

    // Insert into dictionary
    int8_t* dictEntryPtr = queryDict->dictDataPtr + queryDict->dictSize;


    // Update hash map (simple linear probing for demonstration)
    uint64_t index = hash % QueryDict::map_num_of_slots; // TODO optimize with &
    auto mapPtr = queryDict->dictMapPtr;
    for (auto hops = 0; hops < 3; hops++)
    {
        if (mapPtr[index] == 0) // Empty slot -> insert
        {
            std::memcpy(dictEntryPtr, data, size);
            auto offset = queryDict->dictSize;
            queryDict->dictSize += size + sizeof(hash);

            //allocate slot
            auto entry = hash & 0xFFFF;
            entry |= offset << 16;
            mapPtr[index] = entry;

            //write down hash then string
            std::memcpy(dictEntryPtr, &hash, sizeof(uint64_t));
            std::memcpy(dictEntryPtr + sizeof(uint64_t), data, size * sizeof(int8_t*));
            queryDict->insNum ++;
            return dictEntryPtr + sizeof(uint64_t); //skip the hash
        }
        if ((mapPtr[index] & 0xFFFF) == (hash & 0xFFFF)) // Hash matches, check string
        {
            auto existingOffset = mapPtr[index] >> 16;
            auto existingEntryPtr = queryDict->dictDataPtr + existingOffset;
            auto existingHash = *reinterpret_cast<uint64_t*>(existingEntryPtr);
            if (existingHash == hash && std::memcmp(existingEntryPtr + sizeof(uint64_t), data, size) == 0)  //TODO secure size check...
            {
                return existingEntryPtr + sizeof(uint64_t); // Found existing entry
            }
        }
        index = (index + 1) % QueryDict::map_num_of_slots;
    }

    return data;
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
