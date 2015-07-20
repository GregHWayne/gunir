// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_IO_SLICE_SCANNER_H
#define  GUNIR_IO_SLICE_SCANNER_H

#include <stdint.h>

#include "toft/base/scoped_ptr.h"
#include "toft/base/string/string_piece.h"

#include "gunir/io/scanner.h"

namespace gunir {
namespace io {
class Slice;

class SliceScanner : public Scanner {
public:
    explicit SliceScanner(const toft::StringPiece& result);
    ~SliceScanner();
    bool NextSlice(Slice** slice);

private:
    const char* m_buffer;
    const char* m_end_buffer;
    const char* m_cur_buffer;
    toft::scoped_ptr<Slice> m_slice;
};

} // namespace io
} // namespace gunir

#endif  // GUNIR_IO_SLICE_SCANNER_H
