// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
//
// Description:

#include "gunir/io/slice_scanner.h"

#include "thirdparty/glog/logging.h"

#include "gunir/io/slice.h"

namespace gunir {
namespace io {

SliceScanner::SliceScanner(const toft::StringPiece& result)
    : m_buffer(result.data()),
      m_cur_buffer(m_buffer),
      m_slice(new Slice()){
    if (m_buffer == NULL) {
        m_end_buffer = NULL;
    } else {
        m_end_buffer = m_buffer + result.size();
    }
}

SliceScanner::~SliceScanner() {}

bool SliceScanner::NextSlice(Slice** slice) {
    if (m_cur_buffer == m_end_buffer) {
        return false;
    }
    m_slice->Reset();
    *slice = m_slice.get();
    uint32_t length = m_end_buffer - m_cur_buffer;
    uint32_t read_len = 0;
    ErrorCode code = (*slice)->LoadFromString(m_cur_buffer, length, &read_len);
    m_cur_buffer += read_len;
    return code == kOk;
}

} // namespace io
} // namespace gunir
