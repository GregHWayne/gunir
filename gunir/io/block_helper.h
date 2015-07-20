// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
#ifndef  GUNIR_IO_BLOCK_HELPER_H
#define  GUNIR_IO_BLOCK_HELPER_H

#include "gunir/io/data_holder.h"
#include "gunir/io/block.h"

namespace gunir {

class DataHolder;

namespace io {

class BlockHelper {
public:
    explicit BlockHelper(uint32_t data_holder_size);
    ~BlockHelper();

public:
    void Reset();
    bool SetBlockValue(Block* block, Block::VALUE_TYPE type,
                       const char* value, int32_t length);
private:
    char* m_buffer;
    DataHolder m_holder;
};

}  // namespace io
}  // namespace gunir


#endif  // GUNIR_IO_BLOCK_HELPER_H

