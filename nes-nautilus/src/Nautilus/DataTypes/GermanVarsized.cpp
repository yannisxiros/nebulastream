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

#include <Nautilus/DataTypes/GermanVarsized.hpp>

#include <cstdint>
#include <ostream>
#include <utility>

#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <nautilus/std/cstring.h>
#include <nautilus/std/ostream.h>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <ErrorHandling.hpp>
#include <Runtime/StringEntry.hpp>

namespace NES
{

GermanVarsized::GermanVarsized(const nautilus::val<int8_t*>& reference)
    :ptrToHeader(reference)
{
}

GermanVarsized::GermanVarsized(const GermanVarsized& other) : ptrToHeader(other.ptrToHeader)
{
}

GermanVarsized& GermanVarsized::operator=(const GermanVarsized& other) noexcept
{
    if (this == &other)
    {
        return *this;
    }

    ptrToHeader = other.ptrToHeader;
    return *this;
}

GermanVarsized::GermanVarsized(GermanVarsized&& other) noexcept
    :ptrToHeader(std::move(other.ptrToHeader))
{
}

GermanVarsized& GermanVarsized::operator=(GermanVarsized&& other) noexcept
{
    if (this == &other)
    {
        return *this;
    }

    ptrToHeader = std::move(other.ptrToHeader);
    return *this;
}

nautilus::val<bool> operator==(const GermanVarsized& varSizedData, const nautilus::val<bool>& other)
{
    return varSizedData.isValid() == other;
}

nautilus::val<bool> operator==(const nautilus::val<bool>& other, const GermanVarsized& varSizedData)
{
    return varSizedData.isValid() == other;
}

nautilus::val<bool> GermanVarsized::isValid() const
{
    return getSize() > 0 && ptrToHeader != nullptr;
}

nautilus::val<bool> GermanVarsized::operator==(const GermanVarsized& rhs) const
{
    auto header1 = readValueFromMemRef<uint64_t>(ptrToHeader);
    auto header2 = readValueFromMemRef<uint64_t>(rhs.getReference());
    
    if (header1 != header2)
    {
        return {false};
    }

    const auto size = getSize();
    if (size <= prefixSize)
    {
        return {true};
    }

    const auto varSizedData = getContent();
    const auto rhsVarSizedData = rhs.getContent();
    const auto compareResult = (nautilus::memcmp(varSizedData + nautilus::val<uint32_t>(prefixSize), rhsVarSizedData + nautilus::val<uint32_t>(prefixSize), size - nautilus::val<uint32_t>(prefixSize)) == 0);
    return {compareResult};
}

nautilus::val<bool> GermanVarsized::operator==(const VariableSizedData& rhs) const
{
    const auto size = getSize();
    const auto rhsSize = rhs.getSize();
    if (size != rhsSize)
    {
        return {false};
    }

    //this is dumb should skip if both inline
    if (nautilus::memcmp(getPrefix(), rhs.getContent(), prefixSize) != 0)
        return {false};

    const auto varSizedData = getContent();
    const auto rhsVarSizedData = rhs.getContent();
    const auto compareResult = (nautilus::memcmp(varSizedData, rhsVarSizedData, size) == 0);
    return {compareResult};
}


nautilus::val<bool> GermanVarsized::operator!=(const GermanVarsized& rhs) const
{
    return !(*this == rhs);
}

nautilus::val<bool> GermanVarsized::operator!() const
{
    return !isValid();
}

[[nodiscard]] nautilus::val<uint32_t> GermanVarsized::getSize() const
{
    return readValueFromMemRef<uint32_t>(ptrToHeader);
}

[[nodiscard]] nautilus::val<int8_t*> GermanVarsized::getContent() const
{
    if (getSize() <= inlineBufSize)
        return getPrefix();
    auto ptr = getMemberWithOffset<int8_t**>(ptrToHeader, offsetof(StringEntry, ptr));
    return readValueFromMemRef<int8_t*>(ptr);
}

// get the whole structure
[[nodiscard]] nautilus::val<int8_t*> GermanVarsized::getReference() const
{
    return ptrToHeader;
}

[[nodiscard]] nautilus::val<int8_t*> GermanVarsized::getPrefix() const
{
    return getMemberWithOffset<int8_t*>(ptrToHeader, offsetof(StringEntry, prefix));
}

[[nodiscard]] nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& oss, const GermanVarsized& GermanVarsized)
{
    oss << "Size(" << GermanVarsized.getSize() << "): ";
    for (nautilus::val<uint32_t> i = 0; i < GermanVarsized.getSize(); ++i)
    {
        const nautilus::val<int> byte = readValueFromMemRef<int8_t>((GermanVarsized.getContent() + i)) & nautilus::val<int>(0xff);
        oss << nautilus::hex;
        oss.operator<<(byte);
        oss << " ";
    }
    return oss;
}
}
