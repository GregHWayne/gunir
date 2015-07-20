// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description: BitMap

// #include <stdlib.h>

#include <limits.h>
#include <string.h>
#include <exception>
#include <stdexcept>

#include "gunir/utils/bit_map.h"

namespace gunir {

BitMap::BitMap(size_t num_bits)
    : m_bitmap(NULL),
      m_num_bits(0),
      m_set_count(0) {
    Initialize(num_bits);
}

BitMap::~BitMap() {
    Destroy();
}

void BitMap::Initialize(size_t num_bits) {
    m_num_bits = num_bits;
    size_t num_bytes = (num_bits + CHAR_BIT - 1) / CHAR_BIT;
    InitMemory(num_bytes);
}

void BitMap::InitMemory(size_t num_bytes) {
    m_bitmap = static_cast<uint8_t *>(calloc(num_bytes, 1));
    if (!m_bitmap) {
        throw std::bad_alloc();
    }
}

void BitMap::Destroy() {
    free(m_bitmap);
    m_bitmap = NULL;
}

void BitMap::Set(size_t bit_index) {
    if (bit_index >= m_num_bits) {
        return;
    }

    uint8_t mask = (1 << (bit_index % CHAR_BIT));
    m_set_count += (m_bitmap[bit_index / CHAR_BIT] & mask) == 0;
    m_bitmap[bit_index / CHAR_BIT] |= mask;
}

void BitMap::Reset(size_t bit_index) {
    if (bit_index >= m_num_bits) {
        return;
    }

    uint8_t mask = (1 << (bit_index % CHAR_BIT));
    m_set_count -= (m_bitmap[bit_index / CHAR_BIT] & mask) != 0;
    m_bitmap[bit_index / CHAR_BIT] &= ~mask;
}

void BitMap::Clear() {
    size_t num_bytes = (m_num_bits + CHAR_BIT - 1) / CHAR_BIT;
    memset(m_bitmap, 0, num_bytes);
    m_set_count = 0;
}

bool BitMap::Test(size_t bit_index) const {
    if (bit_index >= m_num_bits) {
        return false;
    }
    uint8_t mask = (1 << (bit_index % CHAR_BIT));
    return (m_bitmap[bit_index / CHAR_BIT] & mask) != 0;
}

size_t BitMap::GetCount() const {
    return m_set_count;
}
}  // namespace gunir
