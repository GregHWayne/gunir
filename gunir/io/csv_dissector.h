// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_IO_CSV_DISSECTOR_H
#define  GUNIR_IO_CSV_DISSECTOR_H

#include <string>
#include <vector>

#include "toft/base/object_pool.h"
#include "toft/base/scoped_ptr.h"
#include "toft/base/string/string_piece.h"
#include "toft/storage/file/file.h"

#include "gunir/io/dissector.h"
#include "gunir/utils/proto_message.h"
#include "gunir/utils/encoding/charset_converter.h"

namespace gunir {
class DataHolder;

namespace io {
class Block;

// RecordImporter for CSV record data.
// Interfaces for operating data are specific for CSV
// records, opening record file, reading record data from file,
// preseting read process, etc.
class CsvRecordImporter : public RecordImporter {
public:
    CsvRecordImporter();
    virtual ~CsvRecordImporter();

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

    StringPiece m_next_record;
    std::string m_line;

    uint64_t m_total_read_bytes;
};

// RecordDissector for CSV record data.
// Record data is parsed with its CSV schema, and calculate
// repetition & definition level. The return of dissector is
// followed the unique format, agreed with FieldProductor.
class CsvRecordDissector : public RecordDissector {
public:
    CsvRecordDissector();
    virtual ~CsvRecordDissector();

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
    void SetStringBlockValue(Block* bobj, const std::string& value);
    void SetColumnType(ColumnType ctype, Block::VALUE_TYPE* btype);

private:
    scoped_ptr<ProtoMessage> m_proto_message;
    scoped_ptr<DataHolder> m_data_holder;
    ObjectPool<Block> m_block_pool;
    std::vector<Block*> m_block_vec;
    uint32_t m_column_count;
    std::vector<Block::VALUE_TYPE> m_column_type;
};

}  // namespace io
}  // namespace gunir

#endif  // GUNIR_IO_CSV_DISSECTOR_H
