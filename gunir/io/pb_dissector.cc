// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
//
// Created: 03/21/12
// Description:

#include "gunir/io/pb_dissector.h"

#include "toft/base/string/string_piece.h"
#include "toft/storage/file/file.h"
#include "thirdparty/glog/logging.h"

#include "gunir/io/data_holder.h"
#include "gunir/io/block.h"
#include "gunir/utils/proto_message.h"

namespace gunir {
namespace io {

REGISTER_RECORD_IMPORTER(PB, PbRecordImporter);
REGISTER_RECORD_DISSECTOR(PB, PbRecordDissector);
static const uint32_t kXFSBufSize = 4 * 1024 * 1024;

// PbRecordImporter
PbRecordImporter::PbRecordImporter()
    : m_files(NULL),
      m_files_pos(0),
      m_eof(false),
      m_file(NULL),
      m_total_read_bytes(0) {
}

PbRecordImporter::~PbRecordImporter() {
}

bool PbRecordImporter::Open(const ::google::protobuf::RepeatedPtrField<URI>&
                            files) {
    m_files = &files;
    CHECK_GT(m_files->size(), 0);
    m_files_pos = 0;
    m_eof = false;

    return OpenSingleFile(m_files->Get(m_files_pos ++).uri());
}

bool PbRecordImporter::OpenSingleFile(const std::string& file) {
    if (!Close()) {
        return false;
    }
    m_file.reset(File::Open(file.c_str(), "r"));
    if (m_file.get() == NULL) {
        LOG(ERROR) << "Fail to open file " << file;
        m_eof = true;
        return false;
    }
    m_record_reader.reset(new RecordReader(m_file.get()));
    DCHECK(m_next_record.empty());
    m_eof = (m_record_reader->Next() != 1);
    if (!m_record_reader->ReadRecord(&m_next_record)) {
        LOG(ERROR) << "Fail to read record";
        m_eof = true;
        return false;
    }
    m_total_read_bytes += m_next_record.size();
    return true;
}

bool PbRecordImporter::Close() {
    m_record_reader.reset();
    if (m_file.get() != NULL) {
        m_file->Close();
        m_file.reset();
    }
    return true;
}

bool PbRecordImporter::ReadRecord(StringPiece *value) {
    DCHECK(m_record_reader.get() != NULL);
    if (!m_eof) {
        *value = m_next_record;
        return true;
    }
    return false;
}

bool PbRecordImporter::NextRecord() {
    DCHECK(m_record_reader.get() != NULL);
    m_next_record.clear();
    m_eof = (m_record_reader->Next() != 1);
    if (!m_record_reader->ReadRecord(&m_next_record)) {
        if (m_files_pos != m_files->size()) {
            // try to open next file & read first record in buffer
            if (!OpenSingleFile(m_files->Get(m_files_pos ++).uri())) {
                return false;
            }
        } else {
            LOG(INFO) << "No more record in files";
            m_next_record.clear();
            m_eof = true;
            return false;
        }
    } else {
        m_total_read_bytes += m_next_record.size();
    }
    return true;
}

// PbRecordDissector
using std::string;
using std::vector;
// using common::File;
using google::protobuf::Message;
using google::protobuf::Descriptor;
using google::protobuf::FieldDescriptor;
using google::protobuf::Reflection;

PbRecordDissector::PbRecordDissector()
    : m_column_count(0) {
}

PbRecordDissector::~PbRecordDissector() {
    ReleaseBlocks();
}

bool PbRecordDissector::Init(const SchemaDescriptor& schema_descriptor) {
    const string& message_name = schema_descriptor.record_name();
    const string& schema = schema_descriptor.description();
    if (!InitCharsetConverter(schema_descriptor.charset_encoding())) {
        return false;
    }

    m_proto_message.reset(new ProtoMessage());
    return m_proto_message->CreateMessageByFileDescriptorSet(schema,
                                                             message_name);
}

void PbRecordDissector::SetBuffer(char *buffer, uint32_t length) {
    m_data_holder.reset(new DataHolder(buffer, length));
}

bool PbRecordDissector::DissectRecord(const StringPiece& record,
                                      vector<const Block*>* output_blocks,
                                      vector<uint32_t>* indexes) {
    Message* message = m_proto_message->GetMessage()->New();

    if (!message->ParseFromArray(record.data(), record.size())) {
        LOG(ERROR) << "parse record error";
        return false;
    }

    ReleaseBlocks();
    m_data_holder->Reset();

    ParseMessage(*message, 0, 0, 1,
                 message->GetDescriptor()->name(),
                 output_blocks,
                 indexes);

    delete message;
    return true;
}

void PbRecordDissector::ReleaseBlocks() {
    for (size_t i = 0; i < m_block_vec.size(); ++i) {
        m_block_pool.Release(m_block_vec[i]);
    }
    m_block_vec.clear();
}

void PbRecordDissector::ParseMessage(const Message& message,
                                     int rlevel, int dlevel, int depth,
                                     string path,
                                     vector<const Block*>* output_blocks,
                                     vector<uint32_t>* indexes) {
    const Descriptor* descriptor = message.GetDescriptor();
    const Reflection* reflection = message.GetReflection();

    for (int i = 0; i < descriptor->field_count(); i++) {
        const FieldDescriptor* field = descriptor->field(i);
        string next_path = path + "." + field->name();

        int count = 0;
        if (field->is_repeated()) {
            count = reflection->FieldSize(message, field);
        } else if (reflection->HasField(message, field)) {
            count = 1;
        }

        if (count == 0) {  // deal with null field
            if (field->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE) {
                const Descriptor* child_descriptor = field->message_type();
                WalkTreeNullFields(child_descriptor,
                                   rlevel, dlevel,
                                   next_path,
                                   output_blocks,
                                   indexes);
            } else {
                Block *bobj = m_block_pool.Acquire();
                m_block_vec.push_back(bobj);
                bobj->SetRepLevel(rlevel);
                bobj->SetDefLevel(dlevel);
                bobj->SetValueType(Block::TYPE_NULL);

                output_blocks->push_back(bobj);
                indexes->push_back(m_column_index[next_path]);
            }
        }

        int next_rlevel = rlevel;
        for (int offset = 0; offset < count; offset++) {
            int next_depth = depth;
            if (field->is_repeated()) {
                next_depth++;
            }

            int next_dlevel = dlevel;
            if (field->is_optional() || field->is_repeated()) {
                next_dlevel++;
            }

            if (field->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE) {
                if (field->is_repeated()) {
                    ParseMessage(reflection->GetRepeatedMessage(
                            message,
                            field,
                            offset),
                        next_rlevel, next_dlevel, next_depth,
                        next_path, output_blocks, indexes);
                } else {
                    ParseMessage(reflection->GetMessage(message, field),
                                 next_rlevel, next_dlevel,
                                 next_depth, next_path, output_blocks, indexes);
                }
            } else {
                Block *bobj = m_block_pool.Acquire();
                m_block_vec.push_back(bobj);
                bobj->SetRepLevel(next_rlevel);
                bobj->SetDefLevel(next_dlevel);
                if (field->is_repeated()) {
                    SetRepeatedFieldValue(message, reflection,
                                          field, offset, bobj);
                } else {
                    SetFieldValue(message, reflection, field, bobj);
                }

                output_blocks->push_back(bobj);
                indexes->push_back(m_column_index[next_path]);
            }

            next_rlevel = depth;
        }
    }
}

void PbRecordDissector::WalkTreeNullFields(const Descriptor* descriptor,
                                           int rlevel, int dlevel,
                                           string path,
                                           vector<const Block*>* output_blocks,
                                           vector<uint32_t>* indexes) {
    for (int i = 0; i < descriptor->field_count(); i++) {
        const FieldDescriptor* field = descriptor->field(i);
        string next_path = path + "." + field->name();

        if (field->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE) {
            const Descriptor* child_descriptor = field->message_type();
            WalkTreeNullFields(child_descriptor, rlevel, dlevel,
                               next_path, output_blocks, indexes);
        } else {
            Block *bobj = m_block_pool.Acquire();
            m_block_vec.push_back(bobj);
            bobj->SetValueType(Block::TYPE_NULL);
            bobj->SetRepLevel(rlevel);
            bobj->SetDefLevel(dlevel);

            output_blocks->push_back(bobj);
            indexes->push_back(m_column_index[next_path]);
        }
    }
}

void PbRecordDissector::SetStringBlockValue(Block* bobj,
                                            const std::string& value) {
    std::string convert;
    char* data_ptr = NULL;
    uint32_t size = 0;
    if (Convert(value, &convert)) {
        data_ptr = m_data_holder->Write(convert.c_str(), convert.length());
        size = convert.length();
    } else {
        data_ptr = m_data_holder->Write(value.c_str(), value.length());
        size = value.length();
    }
    CHECK_NOTNULL(data_ptr);
    bobj->SetValue(StringPiece(data_ptr, size));
}

template<class Type>
void PbRecordDissector::SetNormalBlockValue(Block* bobj,
                                            const Type& value,
                                            Block::VALUE_TYPE value_type) {
    const char* source_ptr = reinterpret_cast<const char*>(&value);
    uint32_t length = Block::kValueTypeLength[value_type];
    char *data_ptr = m_data_holder->Write(source_ptr, length);
    CHECK_NOTNULL(data_ptr);
    bobj->SetValue(StringPiece(data_ptr, length));
}

#define SetValueBase(Type1, Type2, Func1, Func2)  \
        case FieldDescriptor::Type1: {\
            Type2 value = reflection->Func1;\
            bobj->SetValueType(Block::Type1);\
            Func2;\
            break;\
        }\

// for required or optional
#define SetNormalValue(Type1, Type2, Type3)  \
        SetValueBase(Type1, Type2, \
                     Get##Type3(message, field), \
                     SetNormalBlockValue(bobj, value, Block::Type1))

#define SetStringValue(Type1, Type2, Type3)  \
        SetValueBase(Type1, Type2, \
                     Get##Type3(message, field), \
                     SetStringBlockValue(bobj, value))

// for repeated
#define SetRNormalValue(Type1, Type2, Type3)  \
        SetValueBase(Type1, Type2, \
                     GetRepeated##Type3(message, field, index), \
                     SetNormalBlockValue(bobj, value, Block::Type1))

#define SetRStringValue(Type1, Type2, Type3)  \
        SetValueBase(Type1, Type2, \
                     GetRepeated##Type3(message, field, index), \
                     SetStringBlockValue(bobj, value))

void PbRecordDissector::SetFieldValue(const Message& message,
                                      const Reflection* reflection,
                                      const FieldDescriptor* field,
                                      Block* bobj) {
    switch (field->type()) {

        SetNormalValue(TYPE_INT32, int32_t, Int32);
        SetNormalValue(TYPE_SINT32, int32_t, Int32);
        SetNormalValue(TYPE_FIXED32, int32_t, UInt32);
        SetNormalValue(TYPE_SFIXED32, int32_t, Int32);
        SetNormalValue(TYPE_UINT32, uint32_t, UInt32);
        SetNormalValue(TYPE_INT64, int64_t, Int64);
        SetNormalValue(TYPE_SINT64, int64_t, Int64);
        SetNormalValue(TYPE_FIXED64, int64_t, UInt64);
        SetNormalValue(TYPE_SFIXED64, int64_t, Int64);
        SetNormalValue(TYPE_UINT64, uint64_t, UInt64);
        SetNormalValue(TYPE_FLOAT, float, Float);
        SetNormalValue(TYPE_DOUBLE, double, Double);
        SetNormalValue(TYPE_BOOL, bool, Bool);
        SetStringValue(TYPE_STRING, string, String);
        SetStringValue(TYPE_BYTES, string, String);

    default: {
            LOG(ERROR) << "non-exist field type : " << field->DebugString();
            break;
        }
    }
}

void PbRecordDissector::SetRepeatedFieldValue(const Message& message,
                                              const Reflection* reflection,
                                              const FieldDescriptor* field,
                                              int index,
                                              Block* bobj) {
    switch (field->type()) {

        SetRNormalValue(TYPE_INT32, int32_t, Int32);
        SetRNormalValue(TYPE_SINT32, int32_t, Int32);
        SetRNormalValue(TYPE_FIXED32, int32_t, UInt32);
        SetRNormalValue(TYPE_SFIXED32, int32_t, Int32);
        SetRNormalValue(TYPE_UINT32, uint32_t, UInt32);
        SetRNormalValue(TYPE_INT64, int64_t, Int64);
        SetRNormalValue(TYPE_SINT64, int64_t, Int64);
        SetRNormalValue(TYPE_FIXED64, int64_t, UInt64);
        SetRNormalValue(TYPE_SFIXED64, int64_t, Int64);
        SetRNormalValue(TYPE_UINT64, uint64_t, UInt64);
        SetRNormalValue(TYPE_FLOAT, float, Float);
        SetRNormalValue(TYPE_DOUBLE, double, Double);
        SetRNormalValue(TYPE_BOOL, bool, Bool);
        SetRStringValue(TYPE_STRING, string, String);
        SetRStringValue(TYPE_BYTES, string, String);

    default: {
            LOG(ERROR) << "non-exist field type : " << field->DebugString();
            break;
        }
    }
}

bool PbRecordDissector::GetSchemaColumnStat(vector<ColumnStaticInfo>*
                                            column_stats) {
    m_column_index.clear();
    m_column_count = 0;

    bool ret = m_proto_message->GetSchemaColumnStat(column_stats);
    m_column_count = column_stats->size();
    for (uint32_t i = 0; i < m_column_count; ++i) {
        const std::string& next_path = (*column_stats)[i].column_name();
        m_column_index[next_path] = i;
    }

    return ret;
}

} // namespace io
} // namespace gunir
