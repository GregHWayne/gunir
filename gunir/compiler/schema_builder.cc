// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include <algorithm>

#include "toft/base/string/number.h"

#include "gunir/compiler/schema_builder.h"
#include "gunir/compiler/table_manager.h"

namespace gunir {
namespace compiler {

const char* SchemaBuilder::kQueryResultMessageName = "QueryResult";
const char* SchemaBuilder::kQueryTransMessageName = "QueryIntermediate";
const char* SchemaBuilder::kAggPrefix = "AGG_";
const char* SchemaBuilder::kInt32 = "Int32";
const char* SchemaBuilder::kUInt32 = "UInt32";
const char* SchemaBuilder::kInt64 = "Int64";
const char* SchemaBuilder::kUInt64 = "UInt64";
const char* SchemaBuilder::kFloat = "Float";
const char* SchemaBuilder::kDouble = "Double";
const char* SchemaBuilder::kString = "String";
const char* SchemaBuilder::kBytes = "Bytes";
const char* SchemaBuilder::kBool = "Bool";
const char* SchemaBuilder::kValue = "value";

typedef ::google::protobuf::DescriptorProto PBDescriptorProto;
typedef ::google::protobuf::FieldDescriptorProto PBFieldDescriptorProto;
typedef ::google::protobuf::FileDescriptorSet PBFieldDescriptorSet;

SchemaBuilder::SchemaBuilder(TableManager* table_manager)
    : m_table_manager(table_manager),
      m_has_join(false) {
}

bool SchemaBuilder::BuildSchema(
    const std::vector<std::shared_ptr<Target> >& target_list,
    bool has_groupby, bool has_join,
    std::vector<ColumnInfo>* result_column_info) {

    m_has_join = has_join;

    m_result_message_desc.set_name(kQueryResultMessageName);

    bool is_succeed = true;

    if (has_groupby) {
        is_succeed = BuildGroupBySchema(target_list);
    } else {
        is_succeed = BuildPlainSchema(target_list);
    }

    if (is_succeed) {
        m_message_descs[kQueryResultMessageName] = m_result_message_desc;
        GenerateFileDescriptorProto();
        SetResultColumnInfo(result_column_info);
    }
    return is_succeed;
}

bool SchemaBuilder::BuildGroupByTransferSchema(
    const std::vector<AffectedColumnInfo>& affected_columns,
    std::vector<ShrAggExpr> aggs) {

    std::string src_table_name;

    for (size_t i = 0; i < affected_columns.size(); ++i) {
        src_table_name = affected_columns[i].m_table_name;
        const ColumnInfo& column = affected_columns[i].m_column_info;

        std::vector<std::string> colume_path = column.m_column_path;
        *colume_path.rbegin() = src_table_name + "_" + *colume_path.rbegin();
        BuildField(colume_path,
                   src_table_name,
                   column.m_label,
                   column.m_type);
        m_new_column_list.push_back(column.m_column_path);
    }

    std::sort(aggs.begin(), aggs.end(), AggExprComparator());

    for (size_t i = 0; i < aggs.size(); ++i) {
        std::vector<std::string> column_path;
        CHECK_EQ(i, aggs[i]->GetAggId());
        column_path.push_back(std::string(kAggPrefix) + toft::NumberToString(i));
        BuildField(column_path,
                   src_table_name,
                   PBFieldDescriptor::LABEL_OPTIONAL,
                   aggs[i]->GetTransType());
        m_new_column_list.push_back(column_path);
    }

    m_result_message_desc.set_name(kQueryTransMessageName);
    m_message_descs[kQueryTransMessageName] = m_result_message_desc;
    GenerateFileDescriptorProto();
    return true;
}

bool SchemaBuilder::BuildPlainSchema(
    const std::vector<std::shared_ptr<Target> >& target_list) {
    std::vector<std::shared_ptr<Target> >::const_iterator iter;

    for (iter = target_list.begin(); iter != target_list.end(); ++iter) {
        m_new_column_path.clear();
        if (!BuildTarget(*iter)) {
            return false;
        }
        m_new_column_list.push_back(m_new_column_path);
    }
    return true;
}

bool SchemaBuilder::BuildTarget(const std::shared_ptr<Target>& target) {
    if (target->HasWithin()) {
        return BuildWithinTarget(target);
    }
    return BuildSimpleTarget(target);
}

bool SchemaBuilder::BuildSimpleTarget(
    const std::shared_ptr<Target>& target) {
    const std::shared_ptr<Expression>& e = target->GetExpression();
    BQType data_field_type = e->GetReturnType();
    DeepestColumnInfo deepest_column_info;

    // target is const expression
    if (!e->GetDeepestColumn(&deepest_column_info)) {
        return BuildDataField(
            PBFieldDescriptor::LABEL_REQUIRED, *(target->GetAlias()),
            data_field_type, &m_result_message_desc);
    }

    std::vector<std::string> column_path = *(deepest_column_info.m_column_path);
    if (target->GetAlias() != NULL) {
        *(column_path.rbegin()) = *(target->GetAlias());
    }

    PBLabel data_field_label = deepest_column_info.m_label;
    if (data_field_label == PBFieldDescriptor::LABEL_REQUIRED &&
        CanTargetDataFieldBeNull(target, deepest_column_info)) {
        data_field_label = PBFieldDescriptor::LABEL_OPTIONAL;
    }

    std::string src_table_name = *deepest_column_info.m_table_name;
    return BuildField(
        column_path, src_table_name, data_field_label, data_field_type);
}

bool SchemaBuilder::CanTargetDataFieldBeNull(
    const std::shared_ptr<Target>& target,
    const DeepestColumnInfo& deepest_column) {

    const std::string table_name = *deepest_column.m_table_name;
    const std::shared_ptr<Table> table =
        m_table_manager->GetTable(table_name);
    CHECK_NE(static_cast<Table*>(NULL), table.get());

    const std::shared_ptr<Expression>& e = target->GetExpression();
    std::vector<AffectedColumnInfo> affected_columns;
    e->GetAffectedColumns(&affected_columns);

    // check if exists some REPEATED or OPTIONAL field other than common
    // path in affected_column , if exists, target data field can be null
    for (size_t i = 0; i < affected_columns.size(); ++i) {
        const ColumnInfo& column = affected_columns[i].m_column_info;
        if (column.m_define_level == 0) {
            continue;
        }

        std::vector<std::string> common_path =
            GetCommonPath(*deepest_column.m_column_path, column.m_column_path);

        CHECK_NE(0, common_path.size()) << "Table name not equal, " <<
            " must be used in join, we are not supported yet";

        ColumnInfo common_path_info;
        uint32_t common_path_define_level;
        if (!table->GetColumnInfo(common_path, &common_path_info)) {
            common_path_define_level = 0;
        } else {
            common_path_define_level = common_path_info.m_define_level;
        }

        if (common_path_define_level < column.m_define_level) {
            return true;
        }
    }
    return false;
}

std::vector<std::string> SchemaBuilder::GetCommonPath(
    const std::vector<std::string>& p1, const std::vector<std::string>& p2) {
    std::vector<std::string> common_path;

    for (size_t i = 0; i < p1.size() && i < p2.size(); ++i) {
        if (p1[i] == p2[i]) {
            common_path.push_back(p1[i]);
        }
    }
    return common_path;
}

bool SchemaBuilder::BuildWithinTarget(
    const std::shared_ptr<Target>& target) {
    std::string src_table_name = target->GetWithinTableName();

    std::vector<std::string> column_path = target->GetWithinMessagePath();
    const char* alias_name = (target->GetAlias())->c_str();
    column_path.push_back(std::string(alias_name));

    PBLabel data_field_label = PBFieldDescriptor::LABEL_OPTIONAL;
    BQType data_field_type = target->GetTargetType();
    BuildField(column_path, src_table_name,
               data_field_label, data_field_type);
    return true;
}

bool SchemaBuilder::BuildField(const std::vector<std::string>& column_path,
                               const std::string& src_table_name,
                               PBLabel data_field_label,
                               BQType data_field_type) {
    std::vector<std::string> cur_path;
    const std::shared_ptr<Table> table =
        m_table_manager->GetTable(src_table_name);
    const TableSchema& schema = table->GetTableSchema();
    PBDescriptorProto* new_message_desc = &m_result_message_desc;

    std::string cur_path_string;
    for (size_t i = 0; i < column_path.size(); ++i) {
        cur_path.push_back(column_path[i]);

        // not data field
        if (cur_path.size() < column_path.size()) {
            const PBFieldDescriptor* old_field_desc = schema.GetField(cur_path);
            PBFieldDescriptorProto* new_field_desc =
                BuildMessageField(old_field_desc, new_message_desc);

            new_message_desc = GetDescriptorProto(new_field_desc->type_name());
            continue;
        }

        // the data field
        const std::string& field_name = column_path[i];
        if (DescriptorHasField(new_message_desc, field_name)) {
            LOG(ERROR) << "Message in result schema have got the same name:"
                << field_name;
            return false;
        }

        BuildDataField(
            data_field_label, field_name, data_field_type, new_message_desc);
    }
    return true;
}

PBFieldDescriptorProto* SchemaBuilder::BuildMessageField(
    const PBFieldDescriptor* old_field_desc,
    PBDescriptorProto* new_message_desc) {

    PBFieldDescriptorProto* new_field_desc = GetFieldProto(
        old_field_desc->name(), new_message_desc);

    new_field_desc->set_label(
        ConvertPBLabelToPBProtoLabel(old_field_desc->label()));
    new_field_desc->set_type_name((old_field_desc->message_type())->name());
    new_field_desc->set_type(PBFieldDescriptorProto::TYPE_MESSAGE);

    m_new_column_path.push_back(old_field_desc->name());
    return new_field_desc;
}

PBFieldDescriptorProto* SchemaBuilder::BuildDataField(
    PBLabel column_label, const std::string& column_name,
    BQType column_type, PBDescriptorProto* new_message_desc) {

    // join only support optional result schema
    if (m_has_join) {
        column_label = PBFieldDescriptor::LABEL_OPTIONAL;
    }

    if (column_label == PBFieldDescriptor::LABEL_REPEATED) {
        return BuildWarpPODField(column_label, column_name, column_type,
                                 new_message_desc);
    }

    PBFieldDescriptorProto* new_field_desc = GetFieldProto(
        column_name, new_message_desc);

    new_field_desc->set_label(ConvertPBLabelToPBProtoLabel(column_label));
    new_field_desc->set_type(
        BigQueryType::ConvertBQTypeToPBProtoType(column_type));

    m_new_column_path.push_back(column_name);
    return new_field_desc;
}

PBFieldDescriptorProto* SchemaBuilder::BuildWarpPODField(
    PBLabel column_label, const std::string& column_name,
    BQType column_type, PBDescriptorProto* new_message_desc) {

    std::string type_name = GetPODWarpDescriptorProto(column_type);
    PBFieldDescriptorProto* new_field_desc = GetFieldProto(
        column_name, new_message_desc);

    new_field_desc->set_label(ConvertPBLabelToPBProtoLabel(column_label));
    new_field_desc->set_type(PBFieldDescriptorProto::TYPE_MESSAGE);
    new_field_desc->set_type_name(type_name);

    m_new_column_path.push_back(column_name);
    m_new_column_path.push_back(kValue);
    return new_field_desc;
}

// aggregate is plate, only have data field
bool SchemaBuilder::BuildGroupBySchema(
    const std::vector<std::shared_ptr<Target> >& target_list) {
    std::vector<std::shared_ptr<Target> >::const_iterator iter;

    for (iter = target_list.begin(); iter != target_list.end(); ++iter) {

        m_new_column_path.clear();

        const std::shared_ptr<Target>& t = *iter;
        const std::shared_ptr<Expression>& e = t->GetExpression();


        PBLabel column_label;
        std::string column_name;
        BQType column_type = e->GetReturnType();

        // target must have a name either column name or alias
        CHECK(t->IsSingleColumn() || t->GetAlias() != NULL)
            << "Select target must have a name";

        if (e->IsSingleColumn()) {
            DeepestColumnInfo deepest_column_info;
            bool is_succeed = e->GetDeepestColumn(&deepest_column_info);

            CHECK(is_succeed) << "SingleColumn must have column";

            column_label = deepest_column_info.m_label;

            std::vector<std::string> temp_column_path =
                *deepest_column_info.m_column_path;

            if (t->GetAlias() != NULL) {
                column_name = *(t->GetAlias());
                temp_column_path.clear();
                temp_column_path.push_back(column_name);
            }

            if (!BuildField(temp_column_path,
                            *deepest_column_info.m_table_name,
                            column_label,
                            column_type)) {
                return false;
            }

        } else {
            column_label = PBFieldDescriptor::LABEL_OPTIONAL;
            column_name = *(t->GetAlias());

            if (DescriptorHasField(&m_result_message_desc, column_name)) {
                LOG(ERROR) << "Multiple field with the same name:" << column_name;
                return false;
            }

            BuildDataField(column_label,
                           column_name,
                           column_type,
                           &m_result_message_desc);
        }

        m_new_column_list.push_back(m_new_column_path);
    }
    return true;
}

bool SchemaBuilder::DescriptorHasField(const PBDescriptorProto* message_desc,
                                       const std::string& field_name) {
    for (int i = 0; i < message_desc->field_size(); ++i) {
        const PBFieldDescriptorProto& field = message_desc->field(i);

        if (field.name() == field_name) {
            return true;
        }
    }
    return false;
}

PBFieldDescriptorProto* SchemaBuilder::GetFieldProto(
    const std::string& field_name,
    PBDescriptorProto* message_desc) {

    for (int i = 0; i < message_desc->field_size(); ++i) {
        PBFieldDescriptorProto* field_desc = message_desc->mutable_field(i);
        if (field_name == field_desc->name()) {
            return field_desc;
        }
    }

    PBFieldDescriptorProto* field_desc = (message_desc->mutable_field())->Add();
    field_desc->set_name(field_name);

    int number = message_desc->field_size();
    field_desc->set_number(number);
    return field_desc;
}

PBDescriptorProto* SchemaBuilder::GetDescriptorProto(
    const std::string& message_name) {
    std::map<std::string, PBDescriptorProto>::iterator iter;

    iter = m_message_descs.find(message_name);
    if (iter != m_message_descs.end()) {
        return &(iter->second);
    }

    PBDescriptorProto* desc = &m_message_descs[message_name];
    desc->set_name(message_name);
    return desc;
}

std::string SchemaBuilder::GetPODWarpDescriptorProto(BQType type) {
    std::string type_name = std::string(GetPODWarpTypeName(type));
    PBDescriptorProto* message_desc = &m_message_descs[type_name];

    // is pod warp descriptor inited?
    if (message_desc->field_size() > 0) {
        return type_name;
    }

    // init pod warp descriptor
    message_desc->set_name(type_name);

    PBFieldDescriptorProto* new_field_desc =
        (message_desc->mutable_field())->Add();

    new_field_desc->set_label(
        ConvertPBLabelToPBProtoLabel(PBFieldDescriptor::LABEL_OPTIONAL));
    new_field_desc->set_type(
        BigQueryType::ConvertBQTypeToPBProtoType(type));
    new_field_desc->set_name(kValue);
    new_field_desc->set_number(1);

    return type_name;
}

const char* SchemaBuilder::GetPODWarpTypeName(BQType type) {
    switch (type) {
    case BigQueryType::BOOL:
        return kBool;

    case BigQueryType::INT32:
        return kInt32;
    case BigQueryType::UINT32:
        return kUInt32;

    case BigQueryType::INT64:
        return kInt64;
    case BigQueryType::UINT64:
        return kUInt64;

    case BigQueryType::FLOAT:
        return kFloat;
    case BigQueryType::DOUBLE:
        return kDouble;

    case BigQueryType::STRING:
        return kString;

    case BigQueryType::BYTES:
        return kBytes;

    default:
        LOG(FATAL) << "Not pod type:" << type;
        return NULL;
    }
}

void SchemaBuilder::SetResultColumnInfo(
    std::vector<ColumnInfo>* result_column_info) {

    TableSchema schema;
    CHECK(schema.InitSchemaFromFileDescriptorProto(
            GetResultSchema(),
            kQueryResultMessageName))
        << "Cannot init from file descriptor proto";

    for (size_t i = 0; i < m_new_column_list.size(); ++i) {
        ColumnInfo info;
        schema.GetColumnInfo(m_new_column_list[i], &info);
        result_column_info->push_back(info);
    }
}

PBFieldDescriptorProto::Label SchemaBuilder::ConvertPBLabelToPBProtoLabel(
    PBLabel label) {
    return ::gunir::compiler::ConvertPBLabelToPBProtoLabel(label);
}

void SchemaBuilder::GenerateFileDescriptorProto() {
    m_file_desc_proto.reset(new PBFileDescriptorProto());

    std::map<std::string, PBDescriptorProto>::iterator iter;
    for (iter = m_message_descs.begin();
         iter != m_message_descs.end();
         ++iter) {
        (m_file_desc_proto->mutable_message_type())->AddAllocated(
            new PBDescriptorProto(iter->second));
    }
    m_file_desc_proto->set_name(kQueryResultMessageName);
}

std::string SchemaBuilder::GetReadableResultSchema() const {
    CHECK(m_file_desc_proto != NULL) << "FileSchema does not exist!";
    return GetReadableResultSchema(*m_file_desc_proto);
}

std::string SchemaBuilder::GetReadableResultSchema(
    const PBFileDescriptorProto& file_proto) const {

    PBDescriptorPool pool;
    const PBFileDescriptor* desc = pool.BuildFile(file_proto);
    return desc->DebugString();
}

std::string SchemaBuilder::GetResultSchema() const {
    CHECK(m_file_desc_proto != NULL) << "FileDesc is null";

    PBFieldDescriptorSet proto_set;
    proto_set.add_file()->CopyFrom(*m_file_desc_proto);

    std::string schema_string;
    proto_set.SerializeToString(&schema_string);

    return schema_string;
}

std::string SchemaBuilder::GetReadableSchema(const std::string& schema) {
    PBFieldDescriptorSet proto_set;
    proto_set.ParseFromString(schema);

    std::string readable_schema;
    for (int i = 0; i < proto_set.file_size(); ++i) {
        const PBFileDescriptorProto& file_proto = proto_set.file(i);
        PBDescriptorPool pool;

        const PBFileDescriptor* desc = pool.BuildFile(file_proto);
        readable_schema += desc->DebugString() + "\n";
    }
    return readable_schema;
}

} // namespace compiler
} // namespace gunir

