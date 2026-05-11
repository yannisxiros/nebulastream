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

#include <compare>
#include <cstdint>
#include <ostream>
#include <Util/Logger/Formatter.hpp>
#include <ErrorHandling.hpp>

#ifndef NES_STRING_EXTRABUF_SIZE
#define NES_STRING_EXTRABUF_SIZE 0
#endif

namespace NES
{
static constexpr size_t prefixSize = 4;

template <size_t ExtraBufSize = NES_STRING_EXTRABUF_SIZE>
struct StringEntryTemplate
{
    uint32_t size;
    std::byte prefix[prefixSize];
    int8_t* ptr;
    std::byte extrabuf[ExtraBufSize];

    static constexpr size_t getInlineBufSize() { return sizeof(StringEntryTemplate<ExtraBufSize>) - sizeof(uint32_t); }
};

template <>
struct StringEntryTemplate<0>
{
    uint32_t size;
    std::byte prefix[prefixSize];
    int8_t* ptr;

    static constexpr size_t getInlineBufSize() { return sizeof(StringEntryTemplate<0>) - sizeof(uint32_t); }
};

using StringEntry = StringEntryTemplate<NES_STRING_EXTRABUF_SIZE>;

static constexpr size_t inlineBufSize = StringEntry::getInlineBufSize(); //all the struct - size
static_assert(inlineBufSize >= sizeof(uint8_t*), "InlineBuf must store at least 8 bytes to hold a ptr");

}