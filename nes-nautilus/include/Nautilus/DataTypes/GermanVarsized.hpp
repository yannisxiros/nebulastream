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
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <nautilus/val.hpp>

namespace NES
{

/// Forward declaring the class here, so that we can declare the operator==(const GermanVarsized, const nautilus::val<bool>) for it
class GermanVarsized;
nautilus::val<bool> operator==(const GermanVarsized& varSizedData, const nautilus::val<bool>& other);
nautilus::val<bool> operator==(const nautilus::val<bool>& other, const GermanVarsized& varSizedData);

/// We assume that the first 4 bytes of a int8_t* to any var sized data contains the length of the var sized data
/// This class should not be used as standalone. Rather it should be used via the VarVal class
class GermanVarsized
{
public:
    /// @param bufferBacked: If set to true the GermanVarsized object is backed by a tuple buffer.
    explicit GermanVarsized(const nautilus::val<int8_t*>& reference);
    // explicit GermanVarsized(const nautilus::val<int8_t*>& reference, const nautilus::val<uint64_t>& size, const nautilus::val<int8_t*>& prefix);
    GermanVarsized(const GermanVarsized& other);
    GermanVarsized& operator=(const GermanVarsized& other) noexcept;
    GermanVarsized(GermanVarsized&& other) noexcept;
    GermanVarsized& operator=(GermanVarsized&& other) noexcept;

    [[nodiscard]] nautilus::val<uint32_t> getSize() const;
    /// Returns the content of the variable sized data, this means the pointer to the actual variable sized data.
    /// In other words, this returns the pointer to the actual data, not the pointer to the size + data
    [[nodiscard]] nautilus::val<int8_t*> getContent() const;
    [[nodiscard]] nautilus::val<int8_t*> getPrefix() const;
    [[nodiscard]] nautilus::val<int8_t*> getReference() const;

    /// Declaring friend for it, so that we can access the members in it and do not have to declare getters for it
    // friend nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& oss, const GermanVarsized& GermanVarsized);
    friend nautilus::val<bool> operator==(const GermanVarsized& varSizedData, const nautilus::val<bool>& other);
    friend nautilus::val<bool> operator==(const nautilus::val<bool>& other, const GermanVarsized& varSizedData);


    nautilus::val<bool> operator==(const VariableSizedData& varSizedData) const;


    /// Performing an equality check between two GermanVarsized objects. Two GermanVarsized objects are equal if their size and
    /// content are byte-wise equal. To check the equality of the content, we compare the content byte-wise via a memcmp.
    nautilus::val<bool> operator==(const GermanVarsized&) const;
    nautilus::val<bool> operator!=(const GermanVarsized&) const;
    nautilus::val<bool> operator!() const;
    [[nodiscard]] nautilus::val<bool> isValid() const;

private:
    nautilus::val<int8_t*> ptrToHeader;
};


}
