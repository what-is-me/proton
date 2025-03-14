#pragma once

#include <IO/WriteBuffer.h>
#include <Core/NamesAndTypes.h>

namespace DB
{

struct StructureToProtobufSchema
{
    static constexpr auto name = "structure_to_protobuf_schema";

    static void writeSchema(WriteBuffer & buf, const String & message_name, const NamesAndTypesList & names_and_types_);
};

}
