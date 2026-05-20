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

#include <QueryDict.hpp>
#include <cstring>

namespace NES
{

uint64_t hashBytes(void* data, uint64_t length);

int8_t* QueryDict::insertRaw(int8_t* data, uint64_t size, uint64_t hash)
{
    if (dictSize + size > data_region_cap)
        return data;

    // Insert into dictionary
    int8_t* dictEntryPtr = dictDataPtr + dictSize;

    // Update hash map (simple linear probing for demonstration)
    uint64_t index = hash & (map_num_of_slots - 1);
    auto mapPtr = dictMapPtr;
    for (auto hops = 0; hops < 3; hops++)
    {
        if (mapPtr[index] == 0) // Empty slot -> insert
        {
            auto offset = dictSize;
            dictSize += size + sizeof(hash);

            //allocate slot
            auto entry = hash & 0xFFFF;
            entry |= offset << 16;
            mapPtr[index] = entry;

            //write down hash then string
            std::memcpy(dictEntryPtr, &hash, sizeof(uint64_t));
            std::memcpy(dictEntryPtr + sizeof(uint64_t), data, size);
            insNum++;
            return dictEntryPtr + sizeof(uint64_t); //skip the hash
        }
        if ((mapPtr[index] & 0xFFFF) == (hash & 0xFFFF)) // Hash matches, check string
        {
            auto existingOffset = mapPtr[index] >> 16;
            auto existingEntryPtr = dictDataPtr + existingOffset;
            auto existingHash = *reinterpret_cast<uint64_t*>(existingEntryPtr);
            if (existingHash == hash && std::memcmp(existingEntryPtr + sizeof(uint64_t), data, size) == 0)  //TODO secure size check...
                return existingEntryPtr + sizeof(uint64_t); // Found existing entry
        }
        index = (index + 1) & (map_num_of_slots - 1);
    }
    return data;
}

void QueryDict::insertConstantStrings()
{
    for (const auto& str : constantStrings)
    {
        auto* ptr = const_cast<int8_t*>(reinterpret_cast<const int8_t*>(str.data()));
        const uint64_t hash = hashBytes(ptr, str.size());
        insertRaw(ptr, str.size(), hash);
    }
}

}
