// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_DATUM_BLOCK_H
#define  GUNIR_COMPILER_DATUM_BLOCK_H

#include <string>

#include "gunir/compiler/datum.h"

namespace gunir {
namespace compiler {

struct DatumBlock {
public:
    DatumBlock(const DatumBlock& that) {
        Copy(that);
    }

    DatumBlock(BQType type, size_t size) : m_type(type) {
        m_datum._BYTES = new BytesStorage(size);
        m_rep_level = 0U;
        m_def_level = 0U;
        m_is_null = true;
        m_has_block = false;
    }

    explicit DatumBlock(BQType type) : m_type(type) {
        if (m_type == BigQueryType::STRING) {
            m_datum._STRING = new std::string();
        } else if (m_type == BigQueryType::BYTES) {
            m_datum._BYTES = new BytesStorage(0);
        }
        m_rep_level = 0U;
        m_def_level = 0U;
        m_is_null = true;
        m_has_block = false;
    }

    DatumBlock(BQType type, void* data) : m_type(type) {
        if (m_type == BigQueryType::STRING) {
            m_datum._STRING = new std::string();
        } else if (m_type == BigQueryType::BYTES) {
            m_datum._BYTES = new BytesStorage(0);
        }
        m_datum.SetData(data, type);
        m_rep_level = 0U;
        m_def_level = 0U;
        m_is_null = false;
        m_has_block = false;
    }

    const DatumBlock& operator=(const DatumBlock& that) {
        Clean();
        Copy(that);
        return *this;
    }

    void Clean() {
        if (m_type == BigQueryType::STRING) {
            delete m_datum._STRING;
        }
        if (m_type == BigQueryType::BYTES) {
            delete m_datum._BYTES;
        }
    }

    void Copy(const DatumBlock& that) {
        m_type = that.m_type;
        m_is_null = that.m_is_null;
        m_has_block = that.m_has_block;
        m_rep_level = that.m_rep_level;
        m_def_level = that.m_def_level;
        if (m_type == BigQueryType::STRING) {
            m_datum._STRING = new std::string(*(that.m_datum._STRING));
        } else if (m_type == BigQueryType::BYTES) {
            m_datum._BYTES = new BytesStorage(*(that.m_datum._BYTES));
        } else {
            m_datum = that.m_datum;
        }
    }

    BQType GetType() const { return m_type; }

    ~DatumBlock() {
        Clean();
    }

    bool operator < (const DatumBlock& that) const {
         return CompareDatumWith(that) < 0;
    }

    int CompareDatumWith(const DatumBlock& that) const {
        if (this->m_is_null) {
            if (that.m_is_null) {
                return 0;
            } else {
                return -1;
            }
        } else {
            if (that.m_is_null) {
                return 1;
            } else {
                return (this->m_datum).CompareWith(that.m_datum, m_type);
            }
        }
    }

public:
    Datum m_datum;
    bool m_is_null;
    bool m_has_block;
    uint32_t m_rep_level;
    uint32_t m_def_level;

private:
    BQType m_type;
};

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_DATUM_BLOCK_H

