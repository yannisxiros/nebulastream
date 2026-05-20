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
#include <string>
#include <vector>

#include "Nautilus/Interface/Hash/HashFunction.hpp"
#include "Nautilus/Interface/Hash/MurMur3HashFunction.hpp"
#include "Runtime/BufferManager.hpp"

namespace NES
{

/**
 * @brief Struct to hold dictionary information for a query.
 */
struct QueryDict
{
    static constexpr uint32_t data_region_cap = 512*1024;
    static constexpr uint32_t map_region_size = 256*1024;
    static constexpr uint32_t map_num_of_slots = map_region_size / sizeof(uint32_t);


    std::unique_ptr<TupleBuffer> dictionaryBuffer;
    std::unique_ptr<HashFunction> hashFunction;
    int8_t* dictDataPtr;
    uint32_t* dictMapPtr;
    uint32_t dictSize = 0, insNum = 0;
    std::vector<std::string> constantStrings;


    int8_t* insertRaw(int8_t* data, uint64_t size, uint64_t hash);
    void insertConstantStrings();

    void init(std::shared_ptr<AbstractBufferProvider> bufferManager)
    {
        hashFunction = std::make_unique<MurMur3HashFunction>();
        if (auto unpooledDict = bufferManager->getUnpooledBuffer(1024*1024))
        {
            dictionaryBuffer = std::make_unique<TupleBuffer>(std::move(*unpooledDict));
        }
        else
        {
            throw BufferAllocationFailure("Failed to allocate dictionary buffer for Query Engine.");
        }

        size_t A = 512 * 1024;     // Alignment: 512KB
        size_t S = 512 * 1024;     // Target Size: 512KB
        size_t EXTRA = 256 * 1024; // Extra needed: 256KB

        char* raw_mem = (char*)dictionaryBuffer->getAvailableMemoryArea().data();
        uintptr_t raw_addr = (uintptr_t)raw_mem;
        uintptr_t aligned_addr = (raw_addr + A - 1) & ~(A - 1);
        int8_t* aligned_mem = (int8_t*)aligned_addr;

        size_t prefix_size = (size_t)(aligned_addr - raw_addr);

        int8_t* extra_mem;

        if (prefix_size >= EXTRA) {
            extra_mem = aligned_mem - EXTRA;
        } else {
            extra_mem = aligned_mem + S;
        }

        dictDataPtr = aligned_mem;
        dictMapPtr = (uint32_t*)extra_mem;
        DictVar::dictAddr = (uint64_t)aligned_mem;
    }
};

}
