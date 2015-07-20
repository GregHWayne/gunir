// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/utils/proto_message.h"

#include "toft/storage/path/path.h"
#include "thirdparty/glog/logging.h"

#include "gunir/utils/filename_tool.h"


namespace gunir {

ProtoMessage::ProtoMessage()
    : m_data_base(new Database()),
    m_des_pool(new DescriptorPool(m_data_base.get())),
    m_factory(new DynamicMessageFactory()),
    m_message(NULL),
    m_file_descriptor(NULL),
    m_descriptor(NULL) {}

ProtoMessage::~ProtoMessage() {}

bool ProtoMessage::GetSchemaColumnStat(
        std::vector<io::ColumnStaticInfo>* column_stats) {
    ParseColumn(m_descriptor, 0, 0, m_descriptor->name(), column_stats);
    return true;
}

void ProtoMessage::ParseColumn(const Descriptor* descriptor,
        uint32_t rlevel, uint32_t dlevel,
        const std::string& path,
        std::vector<io::ColumnStaticInfo>* column_stats) {
    for (int i = 0; i < descriptor->field_count(); i++) {
        const FieldDescriptor* field = descriptor->field(i);

        std::string next_path = path + "." + field->name();
        int next_rlevel = rlevel;
        int next_dlevel = dlevel;
        if (field->is_repeated()) {
            next_rlevel++;
            next_dlevel++;
        } else if (field->is_optional()) {
            next_dlevel++;
        }

        if (field->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE) {
            const Descriptor* child_descriptor = field->message_type();
            ParseColumn(child_descriptor, next_rlevel, next_dlevel,
                        next_path, column_stats);
        } else {
            io::ColumnStaticInfo new_column;
            new_column.set_column_index(column_stats->size());
            new_column.set_column_name(next_path);
            SetColumnType(*field, &new_column);
            new_column.set_max_repetition_level(next_rlevel);
            new_column.set_max_definition_level(next_dlevel);
            column_stats->push_back(new_column);
        }
    }
}

void ProtoMessage::SetColumnType(const FieldDescriptor& field,
                                 io::ColumnStaticInfo* column) {
#define SetType(Type)  \
        case FieldDescriptor::Type: {\
            column->set_column_type(io::Type);\
            break;\
        }\

    switch (field.type()) {

        SetType(TYPE_INT32);
        SetType(TYPE_SINT32);
        SetType(TYPE_FIXED32);
        SetType(TYPE_SFIXED32);
        SetType(TYPE_UINT32);
        SetType(TYPE_INT64);
        SetType(TYPE_SINT64);
        SetType(TYPE_FIXED64);
        SetType(TYPE_SFIXED64);
        SetType(TYPE_UINT64);
        SetType(TYPE_FLOAT);
        SetType(TYPE_DOUBLE);
        SetType(TYPE_BOOL);
        SetType(TYPE_STRING);
        SetType(TYPE_BYTES);

    default: {
            LOG(ERROR) << "non-exist field type";
            break;
        }
    }
}

bool ProtoMessage::CreateMessageByProtoFile(const std::string& proto_file,
                                            const std::string& msg_name) {
    const std::string local_proto_file = MoveXfsFileToLocalTmp(proto_file);
    if (local_proto_file == "") {
        return false;
    }
    m_message = NULL;
    m_file_descriptor = NULL;
    m_descriptor = NULL;
    m_descriptor_string = "";

    m_source_tree.reset(new DiskSourceTree());
    m_importer.reset(new Importer(m_source_tree.get(), NULL));
    m_source_tree->MapPath("", toft::Path::GetDirectory(local_proto_file));

    m_file_descriptor =
        m_importer->Import(toft::Path::GetBaseName(local_proto_file));
    if (NULL == m_file_descriptor) {
        LOG(ERROR) << "build file descriptor failed, proto file = [ "
            << proto_file << " ]";
        return false;
    }

    m_descriptor = m_file_descriptor->FindMessageTypeByName(msg_name);
    if (GenerateMessage(msg_name)) {
        GenerateDescriptorString(m_file_descriptor, &m_descriptor_string);
        return true;
    }
    return false;
}

bool ProtoMessage::GenerateMessage(const std::string& msg_name) {
    if (NULL == m_descriptor) {
        LOG(ERROR) << "can't find message [ "
            << msg_name << " ] ";
        return false;
    }

    m_message = m_factory->GetPrototype(m_descriptor);
    if (NULL == m_message) {
        LOG(ERROR) << "can't get message from descriptor";
        return false;
    }

    return true;
}

bool ProtoMessage::CreateMessageByFileDescriptorSet(
        const std::string& descriptor_string, const std::string& msg_name) {

    m_message = NULL;
    m_file_descriptor = NULL;
    m_descriptor = NULL;
    m_descriptor_string = "";

    google::protobuf::FileDescriptorSet proto;
    if (!proto.ParseFromString(descriptor_string)) {
        LOG(ERROR) << "parse from string error ";
        return false;
    }
    if (proto.file_size() == 0) {
        LOG(ERROR) << "proto file number == 0 ";
        return false;
    }

    for (int32_t i = 0; i < proto.file_size(); ++i) {
        m_data_base->Add(proto.file(i));
    }

    m_descriptor = m_des_pool->FindMessageTypeByName(msg_name);
    if (GenerateMessage(msg_name)) {
        m_descriptor_string = descriptor_string;
        return true;
    }
    return false;
}

const google::protobuf::Message* ProtoMessage::GetMessage() const {
    return m_message;
}

const google::protobuf::Descriptor* ProtoMessage::GetDescriptor() const {
    return m_descriptor;
}

std::string ProtoMessage::GetFileDescriptorSetString() const {
    return m_descriptor_string;
}

void ProtoMessage::GenerateDescriptorString(
        const FileDescriptor* file_descriptor,
        std::string* descriptor_string) {
    CHECK_NOTNULL(file_descriptor);

    std::map<std::string, const FileDescriptor*> file_map;
    file_map[file_descriptor->name()] = file_descriptor;
    GetAllFileDescriptor(&file_map, file_descriptor);

    google::protobuf::FileDescriptorSet proto_set;
    std::map<std::string, const FileDescriptor*>::iterator it;
    for (it = file_map.begin(); it != file_map.end(); ++it) {
        google::protobuf::FileDescriptorProto* proto = proto_set.add_file();
        it->second->CopyTo(proto);
    }

    proto_set.SerializeToString(descriptor_string);
}

void ProtoMessage::GetAllFileDescriptor(
        std::map<std::string, const FileDescriptor*>* file_map,
        const FileDescriptor* file_desc) {
    for (int i = 0; i < file_desc->dependency_count(); ++i) {
        const FileDescriptor* new_file = file_desc->dependency(i);
        if (file_map->find(new_file->name()) == file_map->end()) {
            (*file_map)[new_file->name()] = new_file;
            GetAllFileDescriptor(file_map, new_file);
        }
    }
}

} // namespace gunir
