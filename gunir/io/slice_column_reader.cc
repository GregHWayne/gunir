// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Created: 06/12/12

#include "gunir/io/slice_column_reader.h"

namespace gunir {
namespace io {

SliceColumnReader::SliceColumnReader(const std::vector<Slice>& slices) {
    m_slices = &slices;
    if (m_slices->size() > 0) {
        m_cursor.resize((*m_slices)[0].GetCount());
        for (uint32_t i = 0; i < m_cursor.size(); i++) {
            m_cursor[i] = 0;
        }
    }
}

SliceColumnReader::~SliceColumnReader() {
}

bool SliceColumnReader::ReadColumn(int index, Block* block) {
    uint32_t& cursor = m_cursor[index];
    while (cursor < m_slices->size()) {
        cursor++;
        if ((*m_slices)[cursor - 1].HasBlock(index)) {
            *block = *(*m_slices)[cursor - 1].GetBlock(index);
            return true;
        }
    }
    return false;
}

int SliceColumnReader::NextRepLevel(int index) {
    uint32_t cursor = m_cursor[index];
    Block block;
    while (cursor < m_slices->size()) {
        if ((*m_slices)[cursor].HasBlock(index)) {
            block = *(*m_slices)[cursor].GetBlock(index);
            return block.GetRepLevel();
        }
        cursor++;
    }
    return 0;
}

} // namespace io
} // namespace gunir
