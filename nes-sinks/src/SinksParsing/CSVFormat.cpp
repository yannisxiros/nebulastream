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

#include <SinksParsing/CSVFormat.hpp>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <ostream>
#include <ranges>
#include <span>
#include <sstream>
#include <string>
#include <DataTypes/Schema.hpp>
#include <Nautilus/DataTypes/DictVar.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <Runtime/StringEntry.hpp>
#include <SinksParsing/Format.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>

#include <ErrorHandling.hpp>

namespace NES
{
CSVFormat::CSVFormat(const Schema& schema) : CSVFormat(schema, false)
{
}

CSVFormat::CSVFormat(const Schema& pSchema, const bool escapeStrings) : Format(pSchema), escapeStrings(escapeStrings)
{
    PRECONDITION(schema.getNumberOfFields() != 0, "Formatter expected a non-empty schema");
    size_t offset = 0;
    for (const auto& field : schema.getFields())
    {
        const auto physicalType = field.dataType;
        formattingContext.offsets.push_back(offset);
        offset += physicalType.getSizeInBytes();
        formattingContext.physicalTypes.emplace_back(physicalType);
    }
    formattingContext.schemaSizeInBytes = schema.getSizeOfSchemaInBytes();
}

std::string CSVFormat::getFormattedBuffer(const TupleBuffer& inputBuffer) const
{
    return tupleBufferToFormattedCSVString(inputBuffer, formattingContext);
}

std::string CSVFormat::tupleBufferToFormattedCSVString(TupleBuffer tbuffer, const FormattingContext& formattingContext) const
{
    std::stringstream ss;
    const auto numberOfTuples = tbuffer.getNumberOfTuples();
    const auto memSize = tbuffer.getMemSize() == 0 ? numberOfTuples * formattingContext.schemaSizeInBytes : tbuffer.getMemSize();
    const auto tupleSize = formattingContext.schemaSizeInBytes;
    const auto buffer = tbuffer.getAvailableMemoryArea().subspan(0, memSize);
    for (size_t i = 0; i != memSize; i+=tupleSize)
    {
        auto tuple = buffer.subspan(i, formattingContext.schemaSizeInBytes);
        auto fields
            = std::views::iota(static_cast<size_t>(0), formattingContext.offsets.size())
            | std::views::transform(
                  [&formattingContext, &tuple, &tbuffer, copyOfEscapeStrings = escapeStrings, &i](const auto& fieldIdx)
                  {
                      const auto physicalType = formattingContext.physicalTypes[fieldIdx];
                      if (physicalType.type == DataType::Type::FLINK || physicalType.type == DataType::Type::VARSIZED)
                      {
                          const auto base = formattingContext.offsets[fieldIdx];
                          uint64_t childIndex{};
                          uint64_t offset{};
                          uint64_t size{};
                          std::memcpy(&childIndex, &tuple[base + offsetof(VariableSizedAccess, index)], sizeof(uint32_t));
                          std::memcpy(&offset, &tuple[base + offsetof(VariableSizedAccess, offset)], sizeof(uint32_t));
                          std::memcpy(&size, &tuple[base + offsetof(VariableSizedAccess, size)], sizeof(uint64_t));
                          const VariableSizedAccess variableSizedAccess{
                              VariableSizedAccess::Index(childIndex), VariableSizedAccess::Offset(offset), VariableSizedAccess::Size(size)};
                          auto varSizedData = readVarSizedDataAsString(tbuffer, variableSizedAccess);
                          if (childIndex == static_cast<uint64_t>(-1U))
                          {
                              i += size;
                          }
                          if (copyOfEscapeStrings)
                          {
                              return "\"" + varSizedData + "\"";
                          }
                          return varSizedData;
                      }
                      if (physicalType.type == DataType::Type::DICTIONARY)
                      {
                          const auto base = formattingContext.offsets[fieldIdx];
                          const auto* vsa = std::bit_cast<const VariableSizedAccess*>(&tuple[base]);
                          const auto rawIndex = vsa->getIndex().getRawIndex();
                          const auto size = vsa->getSize().getRawSize();
                          std::string varSizedData;
                          if (rawIndex == std::numeric_limits<uint32_t>::max())
                          {
                              // In-region: slot number encodes position inside QueryDict data area
                              const uint32_t slotNum = vsa->getOffset().getRawOffset();
                              const auto* ptr = reinterpret_cast<const char*>(DictVar::dictAddr + static_cast<uint64_t>(slotNum) * 8);
                              varSizedData = std::string(ptr, size);
                          }
                          else
                          {
                              // Not in-region: stored in a child buffer, same path as VARSIZED
                              varSizedData = readVarSizedDataAsString(tbuffer, VariableSizedAccess{
                                  VariableSizedAccess::Index(rawIndex), vsa->getOffset(), VariableSizedAccess::Size(size)});
                          }
                          if (copyOfEscapeStrings)
                          {
                              return "\"" + varSizedData + "\"";
                          }
                          return varSizedData;
                      }
                      if (physicalType.type == DataType::Type::GERMAN_VARSIZED)
                      {
                          const StringEntry* entry = std::bit_cast<const StringEntry*>(&tuple[formattingContext.offsets[fieldIdx]]);
                          std::string varSizedData;
                          if (entry->size <= inlineBufSize)
                              varSizedData = std::string(reinterpret_cast<const char*>(&entry->prefix), entry->size);
                          else
                              varSizedData = std::string(reinterpret_cast<const char*>(entry->ptr), entry->size);
                          if (copyOfEscapeStrings)
                          {
                              return "\"" + varSizedData + "\"";
                          }
                          return varSizedData;
                      }
                      return physicalType.formattedBytesToString(&tuple[formattingContext.offsets[fieldIdx]]);
                  });
        ss << fmt::format("{}\n", fmt::join(fields, ","));
    }
    return ss.str();
}

std::ostream& operator<<(std::ostream& out, const CSVFormat& format)
{
    return out << fmt::format("CSVFormat(Schema: {})", format.schema);
}

}
