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

#include <SinksParsing/JSONFormat.hpp>

#include <cstddef>
#include <cstdint>
#include <ranges>
#include <span>
#include <sstream>
#include <string>
#include <cstring>
#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <SinksParsing/Format.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <ErrorHandling.hpp>

namespace NES
{

JSONFormat::JSONFormat(const Schema& pSchema) : Format(pSchema)
{
    PRECONDITION(schema.getNumberOfFields() != 0, "Formatter expected a non-empty schema");
    size_t offset = 0;
    for (const auto& field : schema.getFields())
    {
        const auto physicalType = field.dataType;
        formattingContext.offsets.push_back(offset);
        offset += physicalType.getSizeInBytes();
        formattingContext.physicalTypes.emplace_back(physicalType);
        formattingContext.names.emplace_back(field.name);
    }
    formattingContext.schemaSizeInBytes = schema.getSizeOfSchemaInBytes();
}

std::string JSONFormat::getFormattedBuffer(const TupleBuffer& inputBuffer) const
{
    return tupleBufferToFormattedJSONString(inputBuffer, formattingContext);
}

std::string JSONFormat::tupleBufferToFormattedJSONString(TupleBuffer tbuffer, const FormattingContext& formattingContext)
{
    std::stringstream ss;
    const auto numberOfTuples = tbuffer.getNumberOfTuples();
    // Use the running memSize like CSVFormat: if TupleBuffer reports memSize==0 fall back to numberOfTuples * schemaSizeInBytes
    const auto memSize = tbuffer.getMemSize() == 0 ? numberOfTuples * formattingContext.schemaSizeInBytes : tbuffer.getMemSize();
    const auto tupleSize = formattingContext.schemaSizeInBytes;
    const auto buffer = tbuffer.getAvailableMemoryArea().subspan(0, memSize);

    for (size_t i = 0; i != memSize; i += tupleSize)
    {
        auto tuple = buffer.subspan(i, formattingContext.schemaSizeInBytes);
        auto fields
            = std::views::iota(static_cast<size_t>(0), formattingContext.offsets.size())
            | std::views::transform(
                  [&formattingContext, &tuple, &tbuffer, &i](const auto& index)
                  {
                      auto type = formattingContext.physicalTypes[index];
                      auto offset = formattingContext.offsets[index];
                      if (type.type == DataType::Type::VARSIZED)
                      {
                          const auto base = offset;
                          uint64_t idx{};
                          uint64_t off{};
                          uint64_t size{};
                          auto indexAddress = &tuple[base + offsetof(VariableSizedAccess, index)];
                          auto offsetAddress = &tuple[base + offsetof(VariableSizedAccess, offset)];
                          auto sizeAddress = &tuple[base + offsetof(VariableSizedAccess, size)];
                          std::memcpy(&idx, indexAddress, sizeof(uint32_t));
                          std::memcpy(&off, offsetAddress, sizeof(uint32_t));
                          std::memcpy(&size, sizeAddress, sizeof(uint64_t));
                          const VariableSizedAccess variableSizedAccess{VariableSizedAccess{
                              VariableSizedAccess::Index(static_cast<uint32_t>(idx)), VariableSizedAccess::Offset(static_cast<uint32_t>(off)), VariableSizedAccess::Size(static_cast<uint64_t>(size))}};
                          auto varSizedData = readVarSizedDataAsString(tbuffer, variableSizedAccess);
                          // If the value was inlined (index == -1U) the actual memory contains the inline bytes after the fixed-size tuple area.
                          if (idx == static_cast<uint64_t>(-1))
                          {
                              i += size;
                          }
                          return fmt::format(R"("{}":"{}")", formattingContext.names.at(index), varSizedData);
                      }
                      return fmt::format("\"{}\":{}", formattingContext.names.at(index), type.formattedBytesToString(&tuple[offset]));
                  });

        ss << fmt::format("{{{}}}\n", fmt::join(fields, ","));
    }
    return ss.str();
}

std::ostream& operator<<(std::ostream& out, const JSONFormat& format)
{
    return out << fmt::format("JSONFormat(Schema: {})", format.schema);
}

}
