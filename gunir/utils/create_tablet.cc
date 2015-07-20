// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/utils/create_tablet.h"

#include <vector>

#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"

#include "gunir/io/dissector.h"
#include "gunir/io/tablet_writer.h"

DECLARE_uint64(gunir_tablet_file_size);
DECLARE_uint64(gunir_max_record_size);

namespace gunir {
CreateTablet::CreateTablet() {}
CreateTablet::~CreateTablet() {}

bool CreateTablet::Setup(const std::string& schema_string,
                         const std::string& tablet_name,
                         const std::string& output_prefix) {
    // Init dissector.
    io::SchemaDescriptor schema_descriptor;
    schema_descriptor.ParseFromString(schema_string);
    m_dissector.reset(
        CREATE_RECORD_DISSECTOR_OBJECT(schema_descriptor.type()));
    if (m_dissector.get() == NULL) {
        return false;
    }
    m_buffer.reset(new char[FLAGS_gunir_tablet_file_size + FLAGS_gunir_max_record_size]);
    m_dissector->SetBuffer(m_buffer.get(), FLAGS_gunir_max_record_size);
    m_dissector->Init(schema_descriptor);

    std::vector<io::ColumnStaticInfo> column_static_infos;
    m_dissector->GetSchemaColumnStat(&column_static_infos);

    // Init tablet writer.
    m_tablet_writer.reset(new io::TabletWriter());
    m_tablet_writer->SetBuffer(m_buffer.get() + FLAGS_gunir_max_record_size,
                               FLAGS_gunir_tablet_file_size);
    if (!m_tablet_writer->BuildTabletSchema(tablet_name,
                                            schema_descriptor,
                                            column_static_infos)) {
        LOG(ERROR) << "construct tablet schema failed";
        return false;
    }

    if (!m_tablet_writer->Open(output_prefix)) {
        LOG(ERROR) << "open column writer failed";
        return false;
    }
    return true;
}

bool CreateTablet::Flush() {
    if (!m_tablet_writer->Close()) {
        LOG(ERROR) << "close tablet writer fail";
        return false;
    }
    m_dissector.reset();
    m_tablet_writer.reset();
    m_buffer.reset();
    return true;
}

bool CreateTablet::WriteMessage(const StringPiece& value) {
    std::vector<const io::Block*> blocks;
    std::vector<uint32_t> indexes;
    if (!m_dissector->DissectRecord(value, &blocks, &indexes)) {
        LOG(ERROR) << "Error to dissect record data";
        return false;
    }
    if (!m_tablet_writer->Write(blocks, indexes)) {
        LOG(ERROR) << "write field failed";
        return false;
    }
    return true;
}

}  // namespace gunir
