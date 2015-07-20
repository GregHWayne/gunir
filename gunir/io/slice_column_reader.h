// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//

#ifndef GUNIR_IO_SLICE_COLUMN_READER_H
#define GUNIR_IO_SLICE_COLUMN_READER_H

#include <vector>

#include "gunir/io/slice.h"

namespace gunir {
namespace io {

class SliceColumnReader {
public:
    explicit SliceColumnReader(const std::vector<Slice>& slices);
    ~SliceColumnReader();

    bool ReadColumn(int index, Block* block);
    int NextRepLevel(int index);

private:
    const std::vector<Slice>* m_slices;
    std::vector<uint32_t> m_cursor;
};

}  // namespace io
}  // namespace gunir

#endif  // GUNIR_IO_SLICE_COLUMN_READER_H
