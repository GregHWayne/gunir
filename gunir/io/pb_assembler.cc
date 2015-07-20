// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/io/pb_assembler.h"

#include <vector>

#include "toft/base/string/algorithm.h"
#include "toft/base/string/string_piece.h"

namespace gunir {
namespace io {

using std::string;
using std::vector;
using google::protobuf::Message;

PbRecordAssembler::PbRecordAssembler() {
}

PbRecordAssembler::~PbRecordAssembler() {
}

bool PbRecordAssembler::Init(TabletReader* tablet_reader) {
    m_tablet_reader = tablet_reader;
    m_slice_column_reader = NULL;

    TabletSchema tablet_schema;
    m_tablet_reader->GetTabletSchema(&tablet_schema);
    const SchemaDescriptor& schema_descriptor =
        tablet_schema.schema_descriptor();
    return InitProtoMessage(schema_descriptor);
}

bool PbRecordAssembler::Init(SliceColumnReader* slice_column_reader,
                             const SchemaDescriptor& schema_descriptor) {
    m_slice_column_reader = slice_column_reader;
    m_tablet_reader = NULL;
    return InitProtoMessage(schema_descriptor);
}

bool PbRecordAssembler::InitProtoMessage(
                            const SchemaDescriptor& schema_descriptor) {
    const string& message_name = schema_descriptor.record_name();
    const string& proto_content = schema_descriptor.description();

    m_proto_message.reset(new ProtoMessage());
    if (!m_proto_message->CreateMessageByFileDescriptorSet(proto_content,
                                                           message_name)) {
        LOG(ERROR) << "create message proto info failed";
        return false;
    }
    m_proto_message->GetSchemaColumnStat(&m_column_static_info);

    m_automaton.reset(new Automaton());
    const Descriptor* descriptor =
        m_proto_message->GetMessage()->GetDescriptor();
    if (!m_automaton->Init(descriptor)) {
        LOG(ERROR) << "automaton init failed";
        return false;
    }
    m_automaton->ConstructFSM();

    return true;
}

const google::protobuf::Message* PbRecordAssembler::GetProtoMessage() const {
    return m_proto_message->GetMessage();
}

bool PbRecordAssembler::AssembleRecord(google::protobuf::Message* message) {
    message->Clear();
    string field = m_automaton->GetRootField();
    bool is_back_edge = false;

    while (field != Automaton::kFinishState) {
        Block block;
        bool ret = ReadColumn(field, &block);

        // ret == false in such two situation
        // 1, the tablet has wrong format
        // 2, columes's point moves to end, but the whole records are not done
        if (!ret && field == m_automaton->GetRootField()) {
            return false;
        }

        if (!ret) {
            LOG(ERROR) << "read column error : " << field;
            return false;
        }

        int new_repeat = 0;
        if (is_back_edge) {
            new_repeat = RepLevel(block);
        }

        string name = GetName(field);
        string path = GetPath(field);
        Message* sub_message = MoveToLevel(message,
                                           path, DefLevel(block),
                                           new_repeat);

        // if user alias , DefLevel(block) may > MaxDefLevel(field),
        // so , change to judge IsNull will be more accurate
//         if (DefLevel(block) == MaxDefLevel(field)) {
        if (!block.IsNull()) {
            AppendValue(sub_message, name, block);
        }

        int next_rep_level = NextRepLevel(field);
        is_back_edge = m_automaton->IsBackEdge(field, next_rep_level);
        field = m_automaton->GetTransition(field, next_rep_level);
    }

    return true;
}

Message* PbRecordAssembler::MoveToLevel(Message* message,
                                        const string& full_path,
                                        int def_level,
                                        int new_repeat) {
    CHECK(def_level >= new_repeat);
    string path = StringRemovePrefix(full_path,
                                     message->GetDescriptor()->name());
    vector<string> fields;
    SplitString(path, Automaton::kDelimiter, &fields);

    int rep = 0;
    int def = 0;
    uint32_t i = 0;

    for (i = 0; i < fields.size() && def < def_level; i++) {
        const Descriptor* descriptor = message->GetDescriptor();
        const Reflection* reflection = message->GetReflection();
        const FieldDescriptor* field = descriptor->FindFieldByName(fields[i]);
        if (field->is_optional() || field->is_repeated())
            def++;
        if (field->is_repeated()) {
            rep++;
            int size = reflection->FieldSize(*message, field);
            if (rep == new_repeat || size == 0) {
                message = reflection->AddMessage(message, field);
            } else {
                message = reflection->MutableRepeatedMessage(message,
                                                             field,
                                                             size - 1);
            }
        } else {
            message = reflection->MutableMessage(message, field);
        }
    }
    //  change by alaxwang to avoid def>= del_level,
    //  but block is not null , still have required field
    if (def >= def_level) {
        for ( ; i < fields.size(); i++) {
            const Descriptor* descriptor = message->GetDescriptor();
            const Reflection* reflection = message->GetReflection();
            const FieldDescriptor* field = descriptor->FindFieldByName(fields[i]);
            //  is not required
            if (field->is_optional() || field->is_repeated())
                break;
            message = reflection->MutableMessage(message, field);
        }
    }
    return message;
}

void PbRecordAssembler::AppendValue(Message* message,
                                    const string& field_name,
                                    const Block& block) {
    // LOG(ERROR) << "field_name " << field_name;
    const Descriptor* descriptor = message->GetDescriptor();
    const Reflection* reflection = message->GetReflection();
    const FieldDescriptor* field = descriptor->FindFieldByName(field_name);
    StringPiece block_value = block.GetValue();
    const char* value_ptr = block_value.data();
    CHECK_NOTNULL(field);
    switch (field->cpp_type()) {
        case FieldDescriptor::CPPTYPE_INT32: {
            int32_t value = *reinterpret_cast<const int32_t*>(value_ptr);
            field->is_repeated()
                ? reflection->AddInt32(message, field, value)
                : reflection->SetInt32(message, field, value);
            break;
        }

        case FieldDescriptor::CPPTYPE_INT64: {
            int64_t value = *reinterpret_cast<const int64_t*>(value_ptr);
            field->is_repeated()
                ? reflection->AddInt64(message, field, value)
                : reflection->SetInt64(message, field, value);
            break;
        }

        case FieldDescriptor::CPPTYPE_UINT32: {
            uint32_t value = *reinterpret_cast<const uint32_t*>(value_ptr);
            field->is_repeated()
                ? reflection->AddUInt32(message, field, value)
                : reflection->SetUInt32(message, field, value);
            break;
        }

        case FieldDescriptor::CPPTYPE_UINT64: {
            uint64_t value = *reinterpret_cast<const uint64_t*>(value_ptr);
            field->is_repeated()
                ? reflection->AddUInt64(message, field, value)
                : reflection->SetUInt64(message, field, value);
            break;
        }

        case FieldDescriptor::CPPTYPE_FLOAT: {
            float value = *reinterpret_cast<const float*>(value_ptr);
            field->is_repeated()
                ? reflection->AddFloat(message, field, value)
                : reflection->SetFloat(message, field, value);
            break;
        }

        case FieldDescriptor::CPPTYPE_DOUBLE: {
            double value = *reinterpret_cast<const double*>(value_ptr);
            field->is_repeated()
                ? reflection->AddDouble(message, field, value)
                : reflection->SetDouble(message, field, value);
            break;
        }

        case FieldDescriptor::CPPTYPE_BOOL: {
            bool value = *reinterpret_cast<const bool*>(value_ptr);
            field->is_repeated()
                ? reflection->AddBool(message, field, value)
                : reflection->SetBool(message, field, value);
            break;
        }

        case FieldDescriptor::CPPTYPE_STRING: {
            string value = block_value.as_string();
            field->is_repeated()
                ? reflection->AddString(message, field, value)
                : reflection->SetString(message, field, value);
            break;
        }

        default: {
            LOG(ERROR) << "unknown field type : " << field->cpp_type();
        }
    }
}

bool PbRecordAssembler::ReadColumn(const string& field, Block* block) {
    if (m_tablet_reader) {
        return m_tablet_reader->ReadColumn(field, block);
    } else if (m_slice_column_reader) {
        int field_index = m_automaton->GetFieldIndex(field);
        return m_slice_column_reader->ReadColumn(field_index, block);
    }

    LOG(ERROR) << "use assembler without init!";
    return false;
}

int PbRecordAssembler::RepLevel(const Block& block) {
    return block.GetRepLevel();
}

int PbRecordAssembler::DefLevel(const Block& block) {
    return block.GetDefLevel();
}

int PbRecordAssembler::NextRepLevel(const string& field) {
    if (m_tablet_reader) {
        return m_tablet_reader->NextRepLevel(field);
    } else if (m_slice_column_reader) {
        int field_index = m_automaton->GetFieldIndex(field);
        return m_slice_column_reader->NextRepLevel(field_index);
    }

    LOG(ERROR) << "use assembler without init!";
    return 0;
}

int PbRecordAssembler::MaxDefLevel(const string& field) {
    uint32_t field_index = m_automaton->GetFieldIndex(field);
    return m_column_static_info[field_index].max_definition_level();
}

string PbRecordAssembler::GetName(const string& field) {
    return field.substr(field.rfind(Automaton::kDelimiter) + 1);
}

string PbRecordAssembler::GetPath(const string& field) {
    return field.substr(0, field.rfind(Automaton::kDelimiter));
}

} // namespace io
} // namespace gunir
