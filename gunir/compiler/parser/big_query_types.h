// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin <qinan@baidu.com>
//
#ifndef  GUNIR_COMPILER_PARSER_BIG_QUERY_TYPES_H
#define  GUNIR_COMPILER_PARSER_BIG_QUERY_TYPES_H

#include <string>

#include "thirdparty/protobuf/descriptor.h"
#include "thirdparty/protobuf/descriptor.pb.h"

namespace gunir {
namespace compiler {

typedef ::google::protobuf::FieldDescriptor PBFieldDescriptor;
typedef ::google::protobuf::FieldDescriptorProto PBFieldDescriptorProto;

// label of field: optional, required, repeated
typedef ::google::protobuf::FieldDescriptor::Label PBLabel;

PBFieldDescriptorProto::Label ConvertPBLabelToPBProtoLabel(PBLabel label);
PBLabel ConvertPBProtoLabelToPBLabel(PBFieldDescriptorProto::Label label);

namespace BigQueryType {

enum BigQueryCppType {
    BOOL = 1,
    INT32 = 2,
    UINT32 = 3,
    INT64 = 4,
    UINT64 = 5,
    FLOAT = 6,
    DOUBLE = 7,
    STRING = 8,
    ENUM = 9,
    MESSAGE = 10,
    BYTES = 11,
    MAX_TYPE = 11,
};

inline std::string EnumToString(BigQueryCppType type) {
    switch (type) {
    case BOOL:
        return "BOOL";
    case INT32:
        return "INT32";
    case UINT32:
        return "UINT32";
    case INT64:
        return "INT64";
    case UINT64:
        return "UINT64";
    case FLOAT:
        return "FLOAT";
    case DOUBLE:
        return "DOUBLE";
    case STRING:
        return "STRING";
    case ENUM:
        return "ENUM";
    case MESSAGE:
        return "MESSAGE";
    case BYTES:
        return "BYTES";
    default:
        return "";
    }
}

PBFieldDescriptorProto::Type ConvertBQTypeToPBProtoType(
    const BigQueryCppType type);

BigQueryCppType ConvertPBProtoTypeToBQType(const PBFieldDescriptorProto::Type);

BigQueryCppType CovertPBTypeToBQType(const PBFieldDescriptor::Type type);


} // namespace BigQueryType

typedef BigQueryType::BigQueryCppType BQType;

} // namespace compiler
} // namespace gunir
#endif  // GUNIR_COMPILER_PARSER_BIG_QUERY_TYPES_H
