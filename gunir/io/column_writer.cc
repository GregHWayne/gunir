// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//

#include "gunir/io/column_writer.h"

#include "thirdparty/glog/logging.h"

namespace gunir {
namespace io {

ColumnWriter::ColumnWriter(ColumnDynamicInfo* column_info)
    : m_column_info(column_info) {
}

ColumnWriter::~ColumnWriter() {
}

void ColumnWriter::Write(const char *block_buf, uint32_t buf_len) {
    m_data_blocks.push_back(StringPiece(block_buf, buf_len));
}

bool ColumnWriter::Flush(File* file) {
    // Get the column data's start position in tablet file.
    int64_t start_position = file->Tell();
    if (start_position == -1) {
        LOG(ERROR) << "Column writer use the File's Tell error.";
        return false;
    }

    uint32_t length = 0;
    // Write the mempool's data into file.
    while (!m_data_blocks.empty()) {
        const StringPiece& sp = m_data_blocks.front();
        if (!WriteFile(file, sp.data(), sp.length())) {
            return false;
        }
        length += sp.length();
        m_data_blocks.pop_front();
    }

    // Update the column dynamic info.
    m_column_info->set_start_position(static_cast<uint64_t>(start_position));
    m_column_info->set_length(length);
    return true;
}

bool ColumnWriter::WriteFile(File *file, const char *buffer, uint32_t len) {
    uint32_t write_size = 0;

    while (write_size != len) {
        int64_t ret = file->Write(buffer + write_size, len - write_size);
        if (ret == -1) {
            LOG(ERROR) << "Column writer flush error.";
            return false;
        }
        write_size += static_cast<uint32_t>(ret);
    }

    return true;
}

}  // namespace io
}  // namespace gunir
