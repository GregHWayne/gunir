// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include <memory.h>

#include "gunir/io/data_holder.h"

namespace gunir {
DataHolder::DataHolder(char* buffer, uint32_t len)
    : m_start_buffer(buffer), m_cur_buffer(buffer),
      m_end_buffer(m_start_buffer + len) {}

DataHolder::~DataHolder() {}

void DataHolder::Reset() {
    m_cur_buffer = m_start_buffer;
}

char* DataHolder::Write(const char *data, uint32_t data_len) {
    char* data_ptr = Reserve(data_len);
    if (data_ptr == NULL) {
        return data_ptr;
    }

    memcpy(data_ptr, data, data_len);
    return data_ptr;
}

char* DataHolder::Reserve(uint32_t buffer_len) {
    if (m_cur_buffer + buffer_len >  m_end_buffer) {
        return NULL;
    }
    char* data_ptr = m_cur_buffer;
    m_cur_buffer += buffer_len;
    return data_ptr;
}

} // namespace gunir
