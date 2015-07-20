// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
//
// Description:

#include "gunir/io/field_productor.h"

#include <string>
#include <vector>

#include "toft/base/string/string_piece.h"
#include "thirdparty/glog/logging.h"

#include "gunir/io/dissector.h"
#include "gunir/io/tablet_schema.pb.h"

namespace gunir {
namespace io {

FieldProductor::FieldProductor()
    : m_dissector(NULL),
      m_importer(NULL) {
}

FieldProductor::FieldProductor(const ::google::protobuf::RepeatedPtrField<URI>& file_paths,
                               const SchemaDescriptor& schema_descriptor,
                               RecordImporter* importer,
                               RecordDissector* dissector) {
    m_importer.reset(importer);
    m_dissector.reset(dissector);

    m_importer->Open(file_paths);
    m_dissector->Init(schema_descriptor);
}

FieldProductor::~FieldProductor() {
}

void FieldProductor::Reset(const ::google::protobuf::RepeatedPtrField<URI>& file_paths,
                           const SchemaDescriptor& schema_descriptor) {
    if (m_importer.get() != NULL) {
        m_importer->Close();
    }

    const std::string schema_type = schema_descriptor.type();
    m_importer.reset(CREATE_RECORD_IMPORTER_OBJECT(schema_type));
    if (m_importer.get() == NULL) {
        LOG(ERROR) << "create importer fail : " << schema_type;
    }
    m_importer->Open(file_paths);

    m_dissector.reset(CREATE_RECORD_DISSECTOR_OBJECT(schema_type));
    if (m_dissector.get() == NULL) {
        LOG(ERROR) << "create dissector fail : " << schema_type;
    }
    m_dissector->Init(schema_descriptor);
}

void FieldProductor::SetBuffer(char *buffer, uint32_t length) {
    CHECK_NOTNULL(m_dissector.get());
    m_dissector->SetBuffer(buffer, length);
}

bool FieldProductor::GetSchemaColumnStat(std::vector<ColumnStaticInfo>*
                                         column_stats) {
    return m_dissector->GetSchemaColumnStat(column_stats);
}

bool FieldProductor::HasRecord() {
    return m_importer->HasRecord();
}

bool FieldProductor::NextRecordFields(std::vector<const Block*>* output_blocks,
                                      std::vector<uint32_t>* indexes) {
    StringPiece record_data;
    if (!m_importer->ReadRecord(&record_data)) {
        LOG(ERROR) << "Error to read record data from file";
        return false;
    }
    if (!m_dissector->DissectRecord(record_data, output_blocks, indexes)) {
        LOG(ERROR) << "Error to dissect record data";
        return false;
    }
    if (HasRecord()) {
        m_importer->NextRecord();
    }
    return true;
}

} // namespace io
} // namespace gunir
