// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin <qinan@baidu.com>
//
#include "gunir/compiler/parser/big_query_types.h"

#include "thirdparty/glog/logging.h"

namespace gunir {
namespace compiler {

PBFieldDescriptorProto::Label ConvertPBLabelToPBProtoLabel(
    PBLabel label) {

    switch (label) {
    case PBFieldDescriptor::LABEL_REQUIRED:
        return PBFieldDescriptorProto::LABEL_REQUIRED;

    case PBFieldDescriptor::LABEL_OPTIONAL:
        return PBFieldDescriptorProto::LABEL_OPTIONAL;

    case PBFieldDescriptor::LABEL_REPEATED:
        return PBFieldDescriptorProto::LABEL_REPEATED;

    default:
        LOG(FATAL) << "PBLabel " << label << " is illegal";
        return PBFieldDescriptorProto::LABEL_REPEATED;
    };
}

PBLabel ConvertPBProtoLabelToPBLabel(PBFieldDescriptorProto::Label label) {
    switch (label) {
    case PBFieldDescriptorProto::LABEL_REQUIRED:
        return PBFieldDescriptor::LABEL_REQUIRED;

    case PBFieldDescriptorProto::LABEL_OPTIONAL:
        return PBFieldDescriptor::LABEL_OPTIONAL;

    case PBFieldDescriptorProto::LABEL_REPEATED:
        return PBFieldDescriptor::LABEL_REPEATED;

    default:
        LOG(FATAL) << "PBLabel " << label << " is illegal";
        return PBFieldDescriptor::LABEL_REPEATED;
    };
}

namespace BigQueryType {

BQType ConvertPBProtoTypeToBQType(
    const PBFieldDescriptorProto::Type type) {

    switch (type) {
    case PBFieldDescriptorProto::TYPE_BOOL:
        return BOOL;

    case PBFieldDescriptorProto::TYPE_INT32:
        return INT32;

    case PBFieldDescriptorProto::TYPE_UINT32:
        return UINT32;

    case PBFieldDescriptorProto::TYPE_INT64:
        return INT64;

    case PBFieldDescriptorProto::TYPE_UINT64:
        return UINT64;

    case PBFieldDescriptorProto::TYPE_FLOAT:
        return FLOAT;

    case PBFieldDescriptorProto::TYPE_DOUBLE:
        return DOUBLE;

    case PBFieldDescriptorProto::TYPE_STRING:
        return STRING;

    case PBFieldDescriptorProto::TYPE_ENUM:
        return ENUM;

    case PBFieldDescriptorProto::TYPE_MESSAGE:
        return MESSAGE;

    case PBFieldDescriptorProto::TYPE_BYTES:
        return BYTES;

    default:
        LOG(FATAL) << "Try to covert invalid or void type:"
            << type << " to PBType";
        return BYTES;
    }
    return BYTES;
}

PBFieldDescriptorProto::Type ConvertBQTypeToPBProtoType(
    const BigQueryCppType type) {

    switch (type) {
    case BOOL:
        return PBFieldDescriptorProto::TYPE_BOOL;

    case INT32:
        return PBFieldDescriptorProto::TYPE_INT32;

    case UINT32:
        return PBFieldDescriptorProto::TYPE_UINT32;

    case INT64:
        return PBFieldDescriptorProto::TYPE_INT64;

    case UINT64:
        return PBFieldDescriptorProto::TYPE_UINT64;

    case FLOAT:
        return PBFieldDescriptorProto::TYPE_FLOAT;

    case DOUBLE:
        return PBFieldDescriptorProto::TYPE_DOUBLE;

    case STRING:
        return PBFieldDescriptorProto::TYPE_STRING;

    case ENUM:
        return PBFieldDescriptorProto::TYPE_ENUM;

    case BYTES:
        return PBFieldDescriptorProto::TYPE_BYTES;

    case MESSAGE:
        return PBFieldDescriptorProto::TYPE_MESSAGE;

    default:
        LOG(FATAL) << "Try to covert invalid or void type:"
            << type << " to PBType";
        return PBFieldDescriptorProto::TYPE_BYTES;
    }
}

BigQueryCppType CovertPBTypeToBQType(const PBFieldDescriptor::Type type) {
    switch (type) {
    case PBFieldDescriptor::TYPE_BOOL:
        return BigQueryType::BOOL;

    case PBFieldDescriptor::TYPE_INT32:
    case PBFieldDescriptor::TYPE_SINT32:
    case PBFieldDescriptor::TYPE_SFIXED32:
        return BigQueryType::INT32;

    case PBFieldDescriptor::TYPE_UINT32:
    case PBFieldDescriptor::TYPE_FIXED32:
        return BigQueryType::UINT32;

    case PBFieldDescriptor::TYPE_INT64:
    case PBFieldDescriptor::TYPE_SINT64:
    case PBFieldDescriptor::TYPE_SFIXED64:
        return BigQueryType::INT64;

    case PBFieldDescriptor::TYPE_UINT64:
    case PBFieldDescriptor::TYPE_FIXED64:
        return BigQueryType::UINT64;

    case PBFieldDescriptor::TYPE_FLOAT:
        return BigQueryType::FLOAT;

    case PBFieldDescriptor::TYPE_DOUBLE:
        return BigQueryType::DOUBLE;

    case PBFieldDescriptor::TYPE_STRING:
        return BigQueryType::STRING;

    case PBFieldDescriptor::TYPE_BYTES:
        return BigQueryType::BYTES;

    case PBFieldDescriptor::TYPE_ENUM:
        return BigQueryType::ENUM;

    case PBFieldDescriptor::TYPE_MESSAGE:
    case PBFieldDescriptor::TYPE_GROUP:
        return BigQueryType::MESSAGE;

    default:
        return BigQueryType::MESSAGE;
    }
}

} // BigQueryType
} // namespace compiler
} // namespace gunir

