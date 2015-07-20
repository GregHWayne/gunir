// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: Hui Li <huili@tencent.com>
//
//

#ifndef  GUNIR_COMMON_BASE_DATA_HOLDER_H
#define  GUNIR_COMMON_BASE_DATA_HOLDER_H

// #include "toft/base/inttypes.h"
#include <stdint.h>

namespace gunir {
class DataHolder {
public:
    DataHolder(char* buffer, uint32_t len);
    ~DataHolder();

    void Reset();
    // Write the data into DataHolder, data_ptr holds the pointer to data
    char* Write(const char *data, uint32_t data_len);

    // cut buffer_len length buffer from dataholder
    char* Reserve(uint32_t buffer_len);

    char* GetStart() {
        return m_start_buffer;
    }

    uint32_t GetLength() {
        return m_end_buffer - m_start_buffer;
    }

private:
    char* m_start_buffer;
    char* m_cur_buffer;
    char* m_end_buffer;
};

} // namespace gunir

#endif  // GUNIR_COMMON_BASE_DATA_HOLDER_H
