// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
// ColumnWriter is the Interface for operating the Tablet data format writing.

#ifndef GUNIR_IO_COLUMN_WRITER_H
#define GUNIR_IO_COLUMN_WRITER_H

#include <stdint.h>

#include <deque>
#include <string>

#include "toft/base/string/string_piece.h"
#include "toft/base/uncopyable.h"
#include "toft/storage/file/file.h"

#include "gunir/io/column_metadata.pb.h"

using namespace toft;

namespace gunir {
namespace io {

// Write the column block data into tablet data file.
class ColumnWriter {
    DECLARE_UNCOPYABLE(ColumnWriter);

public:
    explicit ColumnWriter(ColumnDynamicInfo* column_info);
    ~ColumnWriter();

    void Write(const char *block_buf, uint32_t buf_len);
    bool Flush(File *file);

private:
    bool WriteFile(File *file, const char *buffer, uint32_t len);

private:
    ColumnDynamicInfo* m_column_info;
    std::deque<StringPiece> m_data_blocks;
};

}  // namespace io
}  // namespace gunir

#endif  // GUNIR_IO_COLUMN_WRITER_H
