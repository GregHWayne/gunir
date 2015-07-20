// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include "gunir/compiler/expression.h"

#include "gunir/compiler/function_expression.h"
#include "gunir/compiler/operator_expression.h"

#include "gunir/compiler/parser/plan.pb.h"

namespace gunir {
namespace compiler {

void ColumnExpression::CopyToProto(ExpressionProto* expr_proto) const {
    expr_proto->set_type(ExpressionProto::kColumn);
    ColumnExpressionProto* proto = expr_proto->mutable_column();

    proto->set_label(ConvertPBLabelToPBProtoLabel(m_column_info.m_label));
    proto->set_type(
        BigQueryType::ConvertBQTypeToPBProtoType(m_column_info.m_type));
    proto->set_column_number(m_column_info.m_column_number);
    proto->set_repeat_level(m_column_info.m_repeat_level);
    proto->set_define_level(m_column_info.m_define_level);
    proto->set_affect_id(m_affect_id);

    for (size_t i = 0; i < m_column_info.m_column_path.size(); i++) {
        std::string* s = (proto->mutable_column_path())->Add();
        *s = m_column_info.m_column_path[i];
    }
    proto->set_column_path_string(m_column_info.m_column_path_string);
    proto->set_table_name(m_table_name);
    proto->set_is_distinct(m_is_distinct);
}

void ColumnExpression::ParseFromProto(const ExpressionProto& proto,
                                      ExpressionInitializer* initializer) {
    CHECK(proto.type() == ExpressionProto::kColumn)
        << "ColumnExpression can't parse from proto:" << proto.DebugString();

    const ColumnExpressionProto& column_proto = proto.column();

    m_column_info.m_label = ConvertPBProtoLabelToPBLabel(column_proto.label());
    m_column_info.m_type =
        BigQueryType::ConvertPBProtoTypeToBQType(column_proto.type());
    m_column_info.m_column_number = column_proto.column_number();

    m_column_info.m_repeat_level = column_proto.repeat_level();
    m_column_info.m_define_level = column_proto.define_level();
    m_affect_id = column_proto.affect_id();
    m_table_name = column_proto.table_name();
    m_is_distinct = column_proto.is_distinct();

    for (int i = 0; i < column_proto.column_path_size(); ++i) {
        m_column_info.m_column_path.push_back(column_proto.column_path(i));
    }
    m_column_info.m_column_path_string = column_proto.column_path_string();
}

ConstExpression::ConstExpression(const ExpressionProto& proto)
    : m_datum_block(BigQueryType::ConvertPBProtoTypeToBQType(
            proto.const_().type())) {
        ParseFromProto(proto, NULL);
    }

void ConstExpression::CopyToProto(ExpressionProto* expr_proto) const {
    expr_proto->set_type(ExpressionProto::kConst);
    ConstExpressionProto* proto = expr_proto->mutable_const_();
    const Datum* datum = &m_datum_block.m_datum;

    BQType data_type = m_datum_block.GetType();
    proto->set_type(BigQueryType::ConvertBQTypeToPBProtoType(data_type));

    switch (data_type) {
    case BigQueryType::BOOL:
        proto->set_bool_(datum->_BOOL);
        break;

    case BigQueryType::INT32:
        proto->set_int32_(datum->_INT32);
        break;

    case BigQueryType::UINT32:
        proto->set_uint32_(datum->_UINT32);
        break;

    case BigQueryType::INT64:
        proto->set_int64_(datum->_INT64);
        break;

    case BigQueryType::UINT64:
        proto->set_uint64_(datum->_UINT64);
        break;

    case BigQueryType::FLOAT:
        proto->set_float_(datum->_FLOAT);
        break;

    case BigQueryType::DOUBLE:
        proto->set_double_(datum->_DOUBLE);
        break;

    case BigQueryType::STRING:
        proto->set_string_(*datum->_STRING);
        break;

    default:
        LOG(FATAL) << "Unkown to for const expression";
    }
}

void ConstExpression::ParseFromProto(const ExpressionProto& proto,
                                     ExpressionInitializer* initializer) {
    CHECK(proto.type() == ExpressionProto::kConst)
        << "ConstExpression can't parse from proto:" << proto.DebugString();

    m_datum_block.m_is_null = false;
    m_datum_block.m_rep_level = 0U;
    m_datum_block.m_def_level = 0U;

    const ConstExpressionProto& const_proto = proto.const_();
    BQType data_type =
        BigQueryType::ConvertPBProtoTypeToBQType(const_proto.type());
    Datum* datum = &m_datum_block.m_datum;

    switch (data_type) {
    case BigQueryType::BOOL:
        datum->_INT32 = const_proto.bool_();
        break;

    case BigQueryType::INT32:
        datum->_INT32 = const_proto.int32_();
        break;

    case BigQueryType::UINT32:
        datum->_UINT32 = const_proto.uint32_();
        break;

    case BigQueryType::INT64:
        datum->_INT64 = const_proto.int64_();
        break;

    case BigQueryType::UINT64:
        datum->_UINT64 = const_proto.uint64_();
        break;

    case BigQueryType::FLOAT:
        datum->_FLOAT = const_proto.float_();
        break;

    case BigQueryType::DOUBLE:
        datum->_DOUBLE = const_proto.double_();
        break;

    case BigQueryType::STRING:
        *datum->_STRING = const_proto.string_();
        break;

    case BigQueryType::BYTES:
        *datum->_STRING = const_proto.bytes_();
        break;

    default:
        LOG(FATAL) << "Unkown to for const expression";
    }
}

} // namespace compiler
} // namespace gunir

