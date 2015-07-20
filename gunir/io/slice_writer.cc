// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//

#include "gunir/io/slice_writer.h"

#include "gunir/io/slice.h"
#include "gunir/io/tablet_writer.h"

DECLARE_uint64(gunir_tablet_file_size);

namespace gunir {
namespace io {

SliceWriter::SliceWriter() : m_buffer(new char[FLAGS_gunir_tablet_file_size]) {
    SetTabletWriter(new TabletWriter());
}

SliceWriter::~SliceWriter() {
    delete [] m_buffer;
}

void SliceWriter::SetTabletWriter(TabletWriter *writer) {
    m_tablet_writer.reset(writer);
    m_tablet_writer->SetBuffer(m_buffer, FLAGS_gunir_tablet_file_size);
}

bool SliceWriter::Open(const std::string& table_name,
                       const SchemaDescriptor& schema_descriptor,
                       const std::string& file_name_prefix) {
    if (!m_tablet_writer->BuildTabletSchema(table_name,
                                            schema_descriptor)) {
        LOG(ERROR) << "Slice writer build tablet schema error.";
        return false;
    }
    if (!m_tablet_writer->Open(file_name_prefix)) {
        LOG(ERROR) << "Slice Writer open file to write error.";
        return false;
    }

    return true;
}

bool SliceWriter::Close(std::vector<std::string> *file_list) {
    if (!m_tablet_writer->Close()) {
        LOG(ERROR) << "Slice writer close error.";
        return false;
    }
    m_tablet_writer->GetOutputFileList(file_list);

    return true;
}

bool SliceWriter::Write(const std::vector<Slice>& slices) {
    std::vector<const Block*> blocks;
    std::vector<uint32_t> indexes;
    uint32_t size = slices.size();
    for (uint32_t i = 0; i < size; ++i) {
        for (uint32_t j = 0; j < slices[i].GetCount(); ++j) {
            if (slices[i].HasBlock(j)) {
                blocks.push_back(slices[i].GetBlock(j));
                indexes.push_back(j);
            }
        }
    }
    if (!m_tablet_writer->Write(blocks, indexes)) {
        LOG(ERROR) << "Slice write slice of vector error.";
        return false;
    }
    return true;
}

} // namespace io
} // namespace gunir
