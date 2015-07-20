// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description: BitMap

#ifndef  GUNIR_UTILS_BIT_MAP_H
#define  GUNIR_UTILS_BIT_MAP_H

#include <stdint.h>
#include <stdlib.h>


namespace gunir {

class BitMap {
public:
    explicit BitMap(size_t bit_size);
    ~BitMap();

    void Set(size_t bit_index);
    void Reset(size_t bit_index);
    void Clear();

    bool Test(size_t bit_index) const;
    size_t GetCount() const;

private:
    void InitialClear();
    void Initialize(size_t size);
    void Destroy();
    void InitMemory(size_t);

private:
    uint8_t*    m_bitmap;
    size_t      m_num_bits;
//     size_t      m_num_bytes;
    size_t      m_set_count;
};
}  // namespace gunir
#endif  // GUNIR_UTILS_BIT_MAP_H
