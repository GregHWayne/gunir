// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
//
//
#ifndef GUNIR_IO_SHORT_DISPLAY_READER_H
#define GUNIR_IO_SHORT_DISPLAY_READER_H

#include <string>
#include <vector>

#include "toft/base/scoped_ptr.h"
#include "toft/system/memory/mempool.h"

#include "gunir/io/slice.h"
#include "gunir/io/tablet_schema.pb.h"

namespace gunir {
namespace io {

class TabletReader;

class ShortDisplayReader {
public:
    ShortDisplayReader();
    ~ShortDisplayReader();
    // Read all columns.
    bool Init(const std::string& table_name,
              const std::string& tablet_file);
    bool Close();
    bool Next();
    Slice* GetSlice();
    void GetFieldCount(std::vector<uint32_t> *field_count);
    void GetFieldName(std::vector<std::string> *field_name);
    void GetTabletSchema(TabletSchema *tablet_schema);

private:
    void SwapNextSliceToCurSlice();

private:
    scoped_ptr<MemPool> m_mempool;
    scoped_ptr<TabletReader> m_reader;
    scoped_ptr<Slice> m_slice;
    scoped_ptr<Slice> m_next_slice;
    std::vector<uint32_t> m_field_count;
};

inline void ShortDisplayReader::SwapNextSliceToCurSlice() {
    m_slice.reset(m_next_slice.release());
}

} // namespace io
} // namespace gunir

#endif  // GUNIR_IO_SHORT_DISPLAY_READER_H
