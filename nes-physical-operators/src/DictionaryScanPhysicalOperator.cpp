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


#include <DictionaryScanPhysicalOperator.hpp>

#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Util/StdInt.hpp>
#include <ExecutionContext.hpp>
#include <InputFormatterTupleBufferRef.hpp>
#include <PhysicalOperator.hpp>
#include <val.hpp>

#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/DataTypes/DictVar.hpp>

namespace NES
{

DictionaryScanPhysicalOperator::DictionaryScanPhysicalOperator(
    std::shared_ptr<TupleBufferRef> bufferRef, std::vector<Record::RecordFieldIdentifier> projections)
    : bufferRef(std::move(bufferRef))
    , projections(std::move(projections))
    , isRawScan(std::dynamic_pointer_cast<InputFormatterTupleBufferRef>(this->bufferRef) != nullptr)
{
}

void DictionaryScanPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    PhysicalOperatorConcept::setup(executionCtx, compilationContext);
}

void ovewriteRecordDict(Record& record)
{
    for (const auto& field : record.getAllFieldIdentifiers())
    {
        auto value = record.read(field);
        if (value.isVarsized())
        {
            auto varSizedData = value.cast<VariableSizedData>();
            auto dictVar = DictVar(varSizedData.getContent(), varSizedData.getSize());
            record.write(field, dictVar);
        }
    }
}

void DictionaryScanPhysicalOperator::rawScan(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    auto inputFormatterBufferRef = std::dynamic_pointer_cast<InputFormatterTupleBufferRef>(this->bufferRef);

    if (not inputFormatterBufferRef->indexBuffer(recordBuffer, executionCtx.pipelineMemoryProvider.arena))
    {
        executionCtx.setOpenReturnState(OpenReturnState::REPEAT);
        return;
    }

    /// call open on all child operators
    openChild(executionCtx, recordBuffer);

    /// process buffer
    const auto executeChildLambda = [this](ExecutionContext& executionCtx, Record& record)
    {
        /// Increment the counter at the start of the dictionary buffer
        nautilus::invoke(
            +[](int8_t* dictionaryBuffer)
            {
                if (dictionaryBuffer)
                {
                    auto* counter = reinterpret_cast<std::atomic<uint64_t>*>(dictionaryBuffer);
                    counter->fetch_add(1, std::memory_order_relaxed);
                }
            },
            executionCtx.dictionaryPtr);

        ovewriteRecordDict(record);
        executeChild(executionCtx, record);
    };
    inputFormatterBufferRef->readBuffer(executionCtx, recordBuffer, executeChildLambda);
}

void DictionaryScanPhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// initialize global state variables to keep track of the watermark ts and the origin id
    executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    executionCtx.originId = recordBuffer.getOriginId();
    executionCtx.currentTs = recordBuffer.getCreatingTs();
    executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
    executionCtx.chunkNumber = recordBuffer.getChunkNumber();
    executionCtx.lastChunk = recordBuffer.isLastChunk();

    DictVar::dict = 32;

    if (isRawScan)
    {
        rawScan(executionCtx, recordBuffer);
        return;
    }
    /// call open on all child operators
    openChild(executionCtx, recordBuffer);
    /// iterate over records in buffer
    auto numberOfRecords = recordBuffer.getNumRecords();
    for (nautilus::val<uint64_t> i = 0_u64; i < numberOfRecords; i = i + 1_u64)
    {
        auto record = bufferRef->readRecord(projections, recordBuffer, i);
        ovewriteRecordDict(record);
        executeChild(executionCtx, record);
    }
}

std::optional<PhysicalOperator> DictionaryScanPhysicalOperator::getChild() const
{
    return child;
}

void DictionaryScanPhysicalOperator::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

bool DictionaryScanPhysicalOperator::getIsRawScan() const
{
    return isRawScan;
}

}
