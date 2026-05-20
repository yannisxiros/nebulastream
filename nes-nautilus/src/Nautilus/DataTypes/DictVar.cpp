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

#include <Nautilus/DataTypes/DictVar.hpp>

#include <cstdint>
#include <ostream>
#include <utility>

#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/GermanVarsized.hpp>
#include <nautilus/std/cstring.h>
#include <nautilus/std/ostream.h>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <ErrorHandling.hpp>
#include "Runtime/VariableSizedAccess.hpp"
#include "Runtime/StringEntry.hpp"

namespace NES
{

DictVar::DictVar(const nautilus::val<int8_t*>& reference, const nautilus::val<uint64_t>& size)
    : size(size), ptrToVarSized(reference)
{
}

DictVar& DictVar::operator=(const DictVar& other) noexcept
{
    if (this == &other)
    {
        return *this;
    }

    size = other.size;
    ptrToVarSized = other.ptrToVarSized;
    return *this;
}

DictVar::DictVar(DictVar&& other) noexcept
    : size(std::move(other.size)), ptrToVarSized(std::move(other.ptrToVarSized))
{
}

DictVar& DictVar::operator=(DictVar&& other) noexcept
{
    if (this == &other)
    {
        return *this;
    }

    size = std::move(other.size);
    ptrToVarSized = std::move(other.ptrToVarSized);
    return *this;
}

nautilus::val<bool> operator==(const DictVar& dictVar, const nautilus::val<bool>& other)
{
    return dictVar.isValid() == other;
}

nautilus::val<bool> operator==(const nautilus::val<bool>& other, const DictVar& dictVar)
{
    return dictVar.isValid() == other;
}

nautilus::val<bool> DictVar::isValid() const
{
    PRECONDITION(size > 0 && ptrToVarSized != nullptr, "DictVar has a size of 0 but  a nullptr pointer to the data.");
    PRECONDITION(size == 0 && ptrToVarSized == nullptr, "DictVar has a size of 0 so there should be no pointer to the data.");
    return size > 0 && ptrToVarSized != nullptr;
}

nautilus::val<bool> DictVar::operator==(const DictVar& rhs) const
{
    return{false};
    if (size != rhs.size)
    {
        return {false};
    }
    const auto varSizedData = getContent();
    const auto rhsVarSizedData = rhs.getContent();
    const auto compareResult = (nautilus::memcmp(varSizedData, rhsVarSizedData, size) == 0);
    return {compareResult};
}

nautilus::val<bool> DictVar::operator==(const GermanVarsized& rhs) const
{
    const auto size = getSize();
    const auto rhsSize = rhs.getSize();
    if (size != rhsSize)
    {
        return {false};
    }

    if (nautilus::memcmp(getContent(), rhs.getPrefix(), inlineBufSize) != 0)
        return {false};

    const auto varSizedData = getContent();
    const auto rhsVarSizedData = rhs.getContent();
    const auto compareResult = (nautilus::memcmp(varSizedData, rhsVarSizedData, size) == 0);
    return {compareResult};
}

nautilus::val<bool> DictVar::operator!=(const DictVar& rhs) const
{
    return !(*this == rhs);
}

nautilus::val<bool> DictVar::operator!=(const GermanVarsized& rhs) const
{
    return !(*this == rhs);
}

nautilus::val<bool> DictVar::operator!() const
{
    return !isValid();
}

[[nodiscard]] nautilus::val<uint64_t> DictVar::getSize() const
{
    return size;
}

[[nodiscard]] nautilus::val<int8_t*> DictVar::getContent() const
{
    return ptrToVarSized;
}

[[nodiscard]] nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& oss, const DictVar& dictVar)
{
    oss << "Size(" << dictVar.size << "): ";
    for (nautilus::val<uint32_t> i = 0; i < dictVar.size; ++i)
    {
        const nautilus::val<int> byte = readValueFromMemRef<int8_t>((dictVar.getContent() + i)) & nautilus::val<int>(0xff);
        oss << nautilus::hex;
        oss.operator<<(byte);
        oss << " ";
    }
    return oss;
}
}
