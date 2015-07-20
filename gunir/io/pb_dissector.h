// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
//
// Description:

#ifndef GUNIR_IO_PB_DISSECTOR_H
#define GUNIR_IO_PB_DISSECTOR_H

#include <map>
#include <string>
#include <vector>

#include "toft/base/object_pool.h"
#include "toft/base/scoped_ptr.h"
#include "toft/base/string/string_piece.h"
#include "toft/storage/file/file.h"
#include "toft/storage/recordio/recordio.h"
#include "thirdparty/protobuf/descriptor.h"
#include "thirdparty/protobuf/message.h"

#include "gunir/io/column_metadata.pb.h"
#include "gunir/io/dissector.h"
#include "gunir/io/table_options.pb.h"
#include "gunir/io/tablet_schema.pb.h"

namespace gunir {
class DataHolder;
class ProtoMessage;

namespace io {
class Block;

// RecordImporter for protobuf-specific record data.
// Interfaces for operating data are specific for protobuf-defined
// records, opening record file, reading record data from file,
// preseting read process, etc.
class PbRecordImporter : public RecordImporter {
public:
    PbRecordImporter();
    virtual ~PbRecordImporter();

    // Open record file and hold reader pointer
    virtual bool Open(const ::google::protobuf::RepeatedPtrField<URI>& files);

    // Clear & close file handler
    virtual bool Close();

    // Read from file and back with record data
    virtual bool ReadRecord(StringPiece *value);

    // Ship to next record
    virtual bool NextRecord();

    // Check next record exist
    virtual bool HasRecord() {
        return !m_eof;
    }

    virtual int64_t GetReadBytes() const {
        return m_total_read_bytes;
    }

private:
    bool OpenSingleFile(const std::string& file);

private:
    const ::google::protobuf::RepeatedPtrField<URI>* m_files;
    int64_t m_files_pos;
    bool m_eof;
    scoped_ptr<toft::File> m_file;
    scoped_ptr<RecordReader> m_record_reader;

    StringPiece m_next_record;

    uint64_t m_total_read_bytes;
};

// RecordDissector for protobuf-specific record data.
// Record data is parsed with its protobuf schema, and calculate
// repetition & definition level. The return of dissector is
// followed the unique format, agreed with FieldProductor.
class PbRecordDissector : public RecordDissector {
public:
    PbRecordDissector();
    virtual ~PbRecordDissector();

    virtual bool Init(const SchemaDescriptor& schema_descriptor);
    virtual void SetBuffer(char *buffer, uint32_t len);

    // Dissect record data and back with columnar-specific data
    // all fields in record will be appended to output_fields
    virtual bool DissectRecord(const StringPiece& record,
                               std::vector<const Block*>* output_blocks,
                               std::vector<uint32_t>* indexes);

    // Parse schema and back with column static info
    // Try not to call this method multiple times
    // but at least once before any DissectRecord
    // all column in schema will be appended to column_stats
    virtual bool GetSchemaColumnStat(std::vector<ColumnStaticInfo>*
                                     column_stats);

    void ReleaseBlocks();

private:
    typedef google::protobuf::Message Message;
    typedef google::protobuf::Descriptor Descriptor;
    typedef google::protobuf::FieldDescriptor FieldDescriptor;
    typedef google::protobuf::Reflection Reflection;

    // Analyze record data and prepare repetition level,
    // definition level, and field value.
    void ParseMessage(const Message& message,
                      int rlevel, int dlevel, int depth, std::string path,
                      std::vector<const Block*>* output_blocks,
                      std::vector<uint32_t>* indexes);

    void WalkTreeNullFields(const Descriptor* descriptor,
                            int rlevel, int dlevel, std::string path,
                            std::vector<const Block*>* output_blocks,
                            std::vector<uint32_t>* indexes);

    // Parse FieldDescriptor's value and write to buffer,
    // then set Block's value
    template<class Type>
    void SetNormalBlockValue(Block* bobj, const Type& value,
                             Block::VALUE_TYPE type);

    void SetStringBlockValue(Block* bobj, const std::string& value);

    void SetFieldValue(const Message& message,
                       const Reflection* reflection,
                       const FieldDescriptor* field,
                       Block* output);

    void SetRepeatedFieldValue(const Message& message,
                               const Reflection* reflection,
                               const FieldDescriptor* field, int index,
                               Block* output);

private:
    scoped_ptr<ProtoMessage> m_proto_message;
    scoped_ptr<DataHolder> m_data_holder;
    ObjectPool<Block> m_block_pool;
    std::vector<Block*> m_block_vec;
    std::map<std::string, uint32_t> m_column_index;
    uint32_t m_column_count;
};

} // namespace io
} // namespace gunir

#endif // GUNIR_IO_PB_DISSECTOR_H
