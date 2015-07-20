// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Created 03/21/12

#include "gunir/io/table_builder.h"

#include "thirdparty/glog/logging.h"

#include "gunir/io/block.h"
#include "gunir/io/column_metadata.pb.h"
#include "gunir/io/field_productor.h"
#include "gunir/io/tablet_schema.pb.h"
#include "gunir/io/tablet_writer.h"

DECLARE_uint64(gunir_tablet_file_size);
DECLARE_uint64(gunir_max_record_size);

namespace gunir {
namespace io {

TableBuilder::TableBuilder() {
    m_tablet_writer.reset(new TabletWriter());
    m_field_productor.reset(new FieldProductor());
    m_buffer.reset(new char[FLAGS_gunir_tablet_file_size
                   + FLAGS_gunir_max_record_size]);
}

TableBuilder::~TableBuilder() {
}

TableBuilder::TableBuilder(TabletWriter* tablet_writer,
                           FieldProductor* field_productor) {
    m_tablet_writer.reset(tablet_writer);
    m_field_productor.reset(field_productor);
}

bool TableBuilder::CreateTable(const TableOptions& options) {
    std::vector<std::string> output_files;
    return CreateTable(options, &output_files);
}

bool TableBuilder::CreateTable(const TableOptions& options,
                               std::vector<std::string>* output_files) {
    if (!InitFileSetting(options)) {
        LOG(ERROR) << "init files for FieldProductor/TabletWriter fail";
        return false;
    }
    if (!ProcessRecords()) {
        m_tablet_writer->Close();
        LOG(ERROR) << "process records fail";
        return false;
    }
    if (!m_tablet_writer->Close()) {
        LOG(ERROR) << "close tablet writer fail";
        return false;
    }
    m_tablet_writer->GetOutputFileList(output_files);
    return true;
}

bool TableBuilder::InitFileSetting(const TableOptions& options) {
    std::vector<ColumnStaticInfo> column_static_info;

    m_field_productor->Reset(options.input_files(),
                             options.schema_descriptor());
    m_field_productor->SetBuffer(m_buffer.get(), FLAGS_gunir_max_record_size);

    if (!m_field_productor->GetSchemaColumnStat(&column_static_info)) {
        LOG(ERROR) << "Parse schema failed";
        return false;
    }

    if (!m_tablet_writer->BuildTabletSchema(options.output_table(),
                                            options.schema_descriptor(),
                                            column_static_info)) {
        LOG(ERROR) << "construct tablet schema failed";
        return false;
    }

    m_tablet_writer->SetBuffer(m_buffer.get() + FLAGS_gunir_max_record_size,
                               FLAGS_gunir_tablet_file_size);
    if (!m_tablet_writer->Open(options.output_file())) {
        m_tablet_writer->Close();
        LOG(ERROR) << "open column writer failed";
        return false;
    }

    return true;
}

bool TableBuilder::ProcessRecords() {
    std::vector<const Block*> blocks;
    std::vector<uint32_t> indexes;
    while (m_field_productor->HasRecord()) {
        blocks.clear();
        indexes.clear();
        if (!m_field_productor->NextRecordFields(&blocks, &indexes)) {
            LOG(ERROR) << "get field data failed";
            return false;
        }

        if (!m_tablet_writer->Write(blocks, indexes)) {
            LOG(ERROR) << "write field failed";
            return false;
        }
    }

    return true;
}

}  // namespace io
}  // namespace gunir
