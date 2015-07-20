// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
//
#include "gunir/io/short_display_reader.h"

#include "gunir/io/column_metadata.pb.h"
#include "gunir/io/tablet_reader.h"
#include "gunir/io/tablet_schema.pb.h"

namespace gunir {
namespace io {

ShortDisplayReader::ShortDisplayReader() {
    m_mempool.reset(new MemPool(MemPool::MAX_UNIT_SIZE));
    m_reader.reset(new TabletReader(m_mempool.get()));
}

ShortDisplayReader::~ShortDisplayReader() {
}

bool ShortDisplayReader::Init(const std::string& table_name,
                              const std::string& tablet_file) {
    if (!m_reader->Init(tablet_file)) {
        return false;
    }

    if (m_reader->Next()) {
        Slice *slice = m_reader->GetSlice();
        m_next_slice.reset(new Slice(*slice));
    }

    return true;
}

bool ShortDisplayReader::Close() {
    return m_reader->Close();
}

bool ShortDisplayReader::Next() {
    if (m_next_slice.get() == NULL) {
        return false;
    }
    m_field_count.clear();
    m_field_count.assign(m_next_slice->GetCount(), 0);

    for (uint32_t i = 0; i < m_next_slice->GetCount(); ++i) {
        if (m_next_slice->HasBlock(i)) {
            const Block *block = m_next_slice->GetBlock(i);
            if (!block->IsNull()) {
                ++m_field_count[i];
            }
        }
    }

    while (m_reader->Next()) {
        Slice *slice = m_reader->GetSlice();

        if (slice->GetSelectLevel() == 0) {
            SwapNextSliceToCurSlice();
            m_next_slice.reset(new Slice(*slice));
            return true;
        }

        for (uint32_t i = 0; i < slice->GetCount(); ++i) {
            if (slice->HasBlock(i)) {
                const Block *block = slice->GetBlock(i);
                if (!block->IsNull()) {
                    ++m_field_count[i];
                }
            }
        }
    }

    SwapNextSliceToCurSlice();
    return true;
}

Slice* ShortDisplayReader::GetSlice() {
    return m_slice.get();
}

void ShortDisplayReader::GetFieldCount(std::vector<uint32_t> *field_count) {
    field_count->clear();
    for (uint32_t i = 0; i < m_field_count.size(); ++i) {
        field_count->push_back(m_field_count[i]);
    }
}

void ShortDisplayReader::GetFieldName(std::vector<std::string> *field_name) {
    TabletSchema ts;
    m_reader->GetTabletSchema(&ts);

    field_name->clear();
    for (int i = 0; i < ts.column_metadatas_size(); ++i) {
        field_name->push_back(
            ts.column_metadatas(i).static_info().column_name());
    }
}

void ShortDisplayReader::GetTabletSchema(TabletSchema *tablet_schema) {
    m_reader->GetTabletSchema(tablet_schema);
}

} // namespace io
} // namespace gunir
