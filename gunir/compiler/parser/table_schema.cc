// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include "gunir/compiler/parser/table_schema.h"

#include "thirdparty/glog/logging.h"
#include "thirdparty/protobuf/descriptor.h"

#include "gunir/compiler/parser/big_query_types.h"
#include "gunir/utils/proto_message.h"

namespace gunir {
namespace compiler {

typedef ::google::protobuf::Descriptor PBDescriptor;
typedef ::google::protobuf::FieldDescriptor PBFieldDescriptor;

TableSchema::TableSchema()
    : m_table_descriptor(NULL) {
}

TableSchema::~TableSchema() {}

bool TableSchema::InitSchemaFromFileDescriptorProto(
    const std::string& schema_string,
    const std::string& msg_entry_name) {

    m_message_entry_name = msg_entry_name;

    m_proto_message.reset(new ProtoMessage());

    if (!m_proto_message->CreateMessageByFileDescriptorSet(schema_string,
                                                           msg_entry_name)) {
        LOG(ERROR) << "Create message [ " << msg_entry_name << " ] error ";
        return false;
    }
    const PBDescriptor* descriptor = m_proto_message->GetDescriptor();
    if (descriptor == NULL) {
        LOG(ERROR) << "can't find message [ " << msg_entry_name
            << " ] in file_desc:\n" + schema_string;
        return false;
    }

    m_table_descriptor = descriptor;
    return true;
}

bool TableSchema::InitSchemaFromProtoFile(const std::string& proto_file,
                                          const std::string& msg_entry_name) {
    m_message_entry_name = msg_entry_name;

    m_proto_message.reset(new ProtoMessage());
    if (!m_proto_message->CreateMessageByProtoFile(proto_file, msg_entry_name)) {
        LOG(ERROR) << "Create message [ " << msg_entry_name << " ] error ";
        return false;
    }

    const PBDescriptor* descriptor = m_proto_message->GetDescriptor();
    if (descriptor == NULL) {
        LOG(ERROR) << "can't find message [ "
            << msg_entry_name << " ] in the file " << proto_file;
        return false;
    }

    m_table_descriptor = descriptor;
    return true;
}

const std::string TableSchema::GetTableSchemaString() const {
    return m_proto_message->GetFileDescriptorSetString();
}

bool TableSchema::GetColumnInfo(const std::vector<std::string>& path,
                                ColumnInfo* info) const {
    uint32_t repeat_level = 0;
    uint32_t define_level = 0;
    const PBFieldDescriptor* field = GetField(path, &repeat_level, &define_level);

    if (field == NULL) {
        return false;
    }

    info->m_label = field->label();
    info->m_type = ConvertToBQType(field->type());
    info->m_column_number = field->number();
    info->m_repeat_level = repeat_level;
    info->m_define_level = define_level;
    info->m_column_path = path;
    info->m_column_path_string = "";

    for (size_t i = 0; i < path.size(); ++i) {
        info->m_column_path_string = info->m_column_path_string + "." + path[i];
    }
    info->m_column_path_string =
        m_message_entry_name + info->m_column_path_string;
    return true;
}

bool TableSchema::GetRequiredFieldPath(std::vector<std::string>* path_list) const {
    return FindRequiredFieldPath(path_list, m_table_descriptor);
}

bool TableSchema::FindRequiredFieldPath(std::vector<std::string>* path_list,
                                        const PBDescriptor* desc) const {
    const PBFieldDescriptor* field = NULL;
    for (int32_t i = 0 ; i < desc->field_count() ; i++) {
        field = desc->field(i);
        if ((field->label() == PBFieldDescriptor::LABEL_REQUIRED)
            || (field->label() == PBFieldDescriptor::LABEL_OPTIONAL)) {
            path_list->push_back(field->name());
            if (IsGroupField(field)) {
                if (FindRequiredFieldPath(path_list, field->message_type())) {
                    return true;
                } else {
                    path_list->erase(path_list->end()-1);
                }
            } else {
                return true;
            }
        }
    }
    return false;
}

const PBFieldDescriptor* TableSchema::GetField(
    const std::vector<std::string>& path) const {

    uint32_t repeat_level;
    uint32_t define_level;
    return GetField(path, &repeat_level, &define_level);
}

const PBFieldDescriptor* TableSchema::GetField(
    const std::vector<std::string>& path,
    uint32_t* repeat_level,
    uint32_t* define_level) const {

    uint32_t repeat = 0;
    uint32_t define = 0;
    const PBFieldDescriptor* field = NULL;
    std::vector<std::string>::const_iterator iter;
    const PBDescriptor* desc = m_table_descriptor;

    for (iter = path.begin(); iter != path.end(); ++iter) {
        field = desc->FindFieldByName(*iter);

        if (field == NULL) {
            return NULL;
        }

        if (field->label() == PBFieldDescriptor::LABEL_REPEATED) {
            repeat++;
        }
        if (field->label() == PBFieldDescriptor::LABEL_REPEATED ||
            field->label() == PBFieldDescriptor::LABEL_OPTIONAL) {
            define++;
        }

        if (IsGroupField(field)) {
            desc = field->message_type();
        } else {
            *repeat_level = repeat;
            *define_level = define;
            return field;
        }
    }

    *repeat_level = repeat;
    *define_level = define;
    return field;
}

bool TableSchema::GetFieldType(const std::vector<std::string>& path_list,
                               BQType* type) const {
    std::vector<std::string>::const_iterator iter;

    const PBFieldDescriptor* field = GetField(path_list);

    if (field == NULL || IsGroupField(field)) {
        return false;
    }

    *type = ConvertToBQType(field->type());
    return true;
}

bool TableSchema::GetFieldLabel(const std::vector<std::string>& path,
                                PBLabel* label) const {
    const PBFieldDescriptor* field = GetField(path);

    if (field == NULL) {
        return false;
    }

    *label = field->label();
    return true;
}

std::vector<ColumnInfo> TableSchema::GetAllColumnInfo() const {
    std::vector<ColumnInfo> column_infos;

    GetAllColumnInfo(&column_infos);
    return column_infos;
}

void TableSchema::GetAllColumnInfo(std::vector<ColumnInfo>* column_infos) const {
    std::vector<std::string> column_path;

    GetAllColumnInfo(
        column_infos, m_table_descriptor, column_path, "", 0, 0);
}

void TableSchema::GetAllColumnInfo(
    std::vector<ColumnInfo>* info_list,
    const PBDescriptor* message_desc,
    const std::vector<std::string>& column_path,
    const std::string& column_path_string,
    int repeat_level, int define_level) const {

    for (int i = 0; i < message_desc->field_count(); ++i) {
        const PBFieldDescriptor* field_desc = message_desc->field(i);
        int new_repeat_level = repeat_level;
        int new_define_level = define_level;

        std::vector<std::string> new_column_path(column_path);
        new_column_path.push_back(field_desc->name());

        std::string new_column_path_string =
            column_path_string + "." + field_desc->name();

        if (field_desc->label() == PBFieldDescriptor::LABEL_REPEATED) {
            new_repeat_level++;
        }

        if (field_desc->label() == PBFieldDescriptor::LABEL_OPTIONAL ||
            field_desc->label() == PBFieldDescriptor::LABEL_REPEATED) {
            new_define_level++;
        }

        if (!IsGroupField(field_desc)) {
            ColumnInfo info;

            info.m_label = field_desc->label();
            info.m_type = ConvertToBQType(field_desc->type());
            info.m_column_number = field_desc->number();
            info.m_repeat_level = new_repeat_level;
            info.m_define_level = new_define_level;

            info.m_column_path_string =
                m_message_entry_name + new_column_path_string;
            info.m_column_path = new_column_path;

            info_list->push_back(info);
        } else {
            GetAllColumnInfo(info_list,
                             field_desc->message_type(),
                             new_column_path,
                             new_column_path_string,
                             new_repeat_level, new_define_level);
        }
    }
}

} // namespace compiler
} // namespace gunir
