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
#include <Nautilus/Interface/BufferRef/InlineTupleBufferRef.hpp>

#include <cstdint>
#include <memory>
#include <ranges>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <nautilus/val_ptr.hpp>
#include <static.hpp>
#include <val.hpp>
#include "Nautilus/DataTypes/DataTypesUtil.hpp"
#include <std/cstring.h>

namespace NES
{

InlineTupleBufferRef::InlineTupleBufferRef(std::vector<Field> fields, const uint64_t tupleSize, const uint64_t bufferSize)
    : TupleBufferRef(bufferSize / tupleSize, bufferSize, tupleSize), fields(std::move(fields))
{
}

namespace
{
nautilus::val<int8_t*> calculateFieldAddress(const nautilus::val<int8_t*>& recordAddress, const uint64_t fieldOffset)
{
    auto fieldAddress = recordAddress + nautilus::val<uint64_t>(fieldOffset);
    return fieldAddress;
}
}


std::span<std::byte> InlineTupleBufferRef::readString(const TupleBuffer& tupleBuffer, VariableSizedAccess variableSizedAccess)
{
    auto bufferToRead = variableSizedAccess.getIndex().getRawIndex() == -1U ? tupleBuffer : tupleBuffer.loadChildBuffer(variableSizedAccess.getIndex());
    const auto varSizedSpan = bufferToRead.getAvailableMemoryArea().subspan(variableSizedAccess.getOffset().getRawOffset());
    return varSizedSpan.subspan(0, variableSizedAccess.getSize().getRawSize());
}

Record InlineTupleBufferRef::readRecord(
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const RecordBuffer& recordBuffer,
    nautilus::val<uint64_t>& recordIndex) const
{
    Record record;
    record.size = tupleSize;
    const auto bufferAddress = recordBuffer.getMemArea();
    const auto recordAddress = bufferAddress + recordIndex;
    for (nautilus::static_val<uint64_t> i = 0; i < fields.size(); ++i)
    {
        const auto& [name, type, fieldOffset] = fields.at(i);
        if (not includesField(projections, name))
        {
            continue;
        }
        auto fieldAddress = calculateFieldAddress(recordAddress, fieldOffset);
        if (type.type != DataType::Type::FLINK)
        {
            record.write(name, VarVal::readVarValFromMemory(fieldAddress, type.type));
            continue;
        }

        auto variableSizedAccess = static_cast<nautilus::val<VariableSizedAccess*>>(fieldAddress);

        const auto varSizedPtr = invoke(
            +[](const TupleBuffer* tupleBuffer, const VariableSizedAccess* variableSizedAccessPtr)
            {
                INVARIANT(tupleBuffer != nullptr, "Tuplebuffer MUST NOT be null at this point");
                INVARIANT(variableSizedAccessPtr != nullptr, "VariableSizedAccess MUST NOT be null at this point");
                return readString(*tupleBuffer, *variableSizedAccessPtr).data();
            },
            recordBuffer.getReference(),
            variableSizedAccess);

        record.size += invoke(
            +[](const VariableSizedAccess* variableSizedAccessPtr)
            {
                INVARIANT(variableSizedAccessPtr != nullptr, "VariableSizedAccess MUST NOT be null at this point");
                if (variableSizedAccessPtr->getIndex().getRawIndex() == -1U)
                    return variableSizedAccessPtr->getSize().getRawSize();
                return uint64_t{0};
            },
            variableSizedAccess);

        const nautilus::val<uint64_t> size = *getMemberWithOffset<uint64_t>(variableSizedAccess, offsetof(VariableSizedAccess, size));
        record.write(name ,VariableSizedData(varSizedPtr, size));
    }
    return record;
}

namespace
{
TupleBuffer getNewBufferForVarSized(AbstractBufferProvider& tupleBufferProvider, const uint64_t newBufferSize)
{
    /// If the fixed size buffers are not large enough, we get an unpooled buffer
    if (tupleBufferProvider.getBufferSize() > newBufferSize)
    {
        if (auto newBuffer = tupleBufferProvider.getBufferNoBlocking(); newBuffer.has_value())
        {
            return newBuffer.value();
        }
    }
    const auto unpooledBuffer = tupleBufferProvider.getUnpooledBuffer(newBufferSize);
    if (not unpooledBuffer.has_value())
    {
        throw CannotAllocateBuffer("Cannot allocate unpooled buffer of size {}", newBufferSize);
    }

    return unpooledBuffer.value();
}

void copyVarSizedAndIncrementMetaData(
    TupleBuffer& childBuffer, const VariableSizedAccess::Offset offset, const std::span<const std::byte> varSizedValue)
{
    const auto spaceInChildBuffer = childBuffer.getAvailableMemoryArea().subspan(offset.getRawOffset());
    PRECONDITION(spaceInChildBuffer.size() >= varSizedValue.size(), "SpaceInChildBuffer must be larger than varSizedValue");
    std::ranges::copy(varSizedValue, spaceInChildBuffer.begin());
}
}



VariableSizedAccess writeString(
    TupleBuffer& tupleBuffer,
    AbstractBufferProvider& bufferProvider,
    const std::span<const std::byte> varSizedValue)
{
    const auto totalVarSizedLength = varSizedValue.size();
    const auto numberOfChildBuffers = tupleBuffer.getNumberOfChildBuffers();
    if (numberOfChildBuffers == 0)
    {
        auto newChildBuffer = getNewBufferForVarSized(bufferProvider, totalVarSizedLength);
        copyVarSizedAndIncrementMetaData(newChildBuffer, VariableSizedAccess::Offset{0}, varSizedValue);
        newChildBuffer.incMemSize(totalVarSizedLength);
        const VariableSizedAccess::Index childBufferIndex{tupleBuffer.storeChildBuffer(newChildBuffer)};
        return VariableSizedAccess{childBufferIndex, VariableSizedAccess::Size{totalVarSizedLength}};
    }

    const VariableSizedAccess::Index childIndex{numberOfChildBuffers - 1};
    auto lastChildBuffer = tupleBuffer.loadChildBuffer(childIndex);
    const auto usedMemorySize = lastChildBuffer.getMemSize();
    if (usedMemorySize + totalVarSizedLength > lastChildBuffer.getBufferSize())
    {
        auto newChildBuffer = getNewBufferForVarSized(bufferProvider, totalVarSizedLength);
        copyVarSizedAndIncrementMetaData(newChildBuffer, VariableSizedAccess::Offset{0}, varSizedValue);
        newChildBuffer.incMemSize(totalVarSizedLength);
        const VariableSizedAccess::Index childBufferIndex{tupleBuffer.storeChildBuffer(newChildBuffer)};
        return VariableSizedAccess{childBufferIndex, VariableSizedAccess::Size{totalVarSizedLength}};
    }

    /// There is enough space in the lastChildBuffer, thus, we copy the var sized into it
    const VariableSizedAccess::Offset childOffset{usedMemorySize};
    copyVarSizedAndIncrementMetaData(lastChildBuffer, childOffset, varSizedValue);
    lastChildBuffer.incMemSize(totalVarSizedLength);
    return VariableSizedAccess{childIndex, childOffset, VariableSizedAccess::Size{totalVarSizedLength}};
}

void InlineTupleBufferRef::writeRecord(
    nautilus::val<uint64_t>& recordOffset [[maybe_unused]],
    RecordBuffer& recordBuffer,
    const Record& rec,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider)
{
    const auto bufferAddress = recordBuffer.getMemArea();
    const auto recordAddress = bufferAddress + recordBuffer.getMemSize();
    nautilus::val<uint32_t> runningSize = tupleSize;
    for (nautilus::static_val<uint64_t> i = 0; i < fields.size(); ++i)
    {
        const auto& [name, type, fieldOffset] = fields.at(i);
        if (not rec.hasField(name))
        {
            /// Skipping any fields that are not part of the record
            continue;
        }
        auto fieldAddress = calculateFieldAddress(recordAddress, fieldOffset);
        const auto& value = rec.read(name);


        if (type.type != DataType::Type::FLINK)
        {
            /// We might have to cast the value to the correct type, e.g. VarVal could be a INT8 but the type we have to write is of type INT16
            /// We get the correct function to call via a unordered_map
            if (const auto storeFunction = storeValueFunctionMap.find(type.type); storeFunction != storeValueFunctionMap.end())
            {
                storeFunction->second(value, fieldAddress);
                continue;
            }
            throw UnknownDataType("Physical Type: {} is currently not supported", type);
        }

        const auto varSizedValue = value.cast<VariableSizedData>();
        if (runningSize + varSizedValue.getSize() <= recordBuffer.getMemSize())
        {
           nautilus::memcpy(recordAddress + runningSize, varSizedValue.getContent(), varSizedValue.getSize());
            *static_cast<nautilus::val<uint64_t*>>( fieldAddress + offsetof(VariableSizedAccess, size)) = varSizedValue.getSize();
            auto idxPtr = static_cast<nautilus::val<uint32_t*>>( fieldAddress + offsetof(VariableSizedAccess, index));
            *idxPtr = uint32_t {-1U};
            auto offtPtr = static_cast<nautilus::val<uint32_t*>>( fieldAddress + offsetof(VariableSizedAccess, offset));
            *offtPtr = recordBuffer.getMemSize() + runningSize;
            runningSize += varSizedValue.getSize();
            continue;
        }

        auto refToIndex = static_cast<nautilus::val<VariableSizedAccess*>>(fieldAddress);
        invoke(
            +[](TupleBuffer* tupleBuffer,
                AbstractBufferProvider* bufferProvider,
                const int8_t* varSizedPtr,
                const uint64_t varSizedValueLength,
                VariableSizedAccess* refToIndex)
            {
                INVARIANT(tupleBuffer != nullptr, "Tuplebuffer MUST NOT be null at this point");
                INVARIANT(bufferProvider != nullptr, "BufferProvider MUST NOT be null at this point");
                const std::span varSizedValueSpan{varSizedPtr, varSizedPtr + varSizedValueLength};
                const VariableSizedAccess writtenAccess = writeString(*tupleBuffer, *bufferProvider,
                    std::as_bytes(varSizedValueSpan));
                *refToIndex = writtenAccess;
            },
            recordBuffer.getReference(),
            bufferProvider,
            varSizedValue.getContent(),
            varSizedValue.getSize(),
            refToIndex);
    }
    recordBuffer.incMemSize(runningSize);
}

std::vector<Record::RecordFieldIdentifier> InlineTupleBufferRef::getAllFieldNames() const
{
    return fields | std::views::transform([](const Field& field) { return field.name; }) | std::ranges::to<std::vector>();
}

std::vector<DataType> InlineTupleBufferRef::getAllDataTypes() const
{
    return fields | std::views::transform([](const Field& field) { return field.type; }) | std::ranges::to<std::vector>();
}

}
