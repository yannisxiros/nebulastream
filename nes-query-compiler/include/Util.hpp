//
// Created by zyro on 3/13/26.
//
#pragma once
#include "DataTypes/Schema.hpp"

namespace NES
{
inline bool containsType(const Schema& schema, DataType::Type innerType)
{
    return std::ranges::any_of(schema, [&](const auto& fields)
    {
        return fields.dataType == DataType{innerType};
    });
}

}