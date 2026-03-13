//
// Created by zyro on 3/13/26.
//
#pragma once
#include "DataTypes/Schema.hpp"

namespace NES
{
inline bool containsFlinkType(const Schema& schema)
{
    return std::ranges::any_of(schema, [&](const auto& fields)
    {
        return fields.dataType == DataType{DataType::Type::FLINK};
    });
}

}