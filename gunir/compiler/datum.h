// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_DATUM_H
#define  GUNIR_COMPILER_DATUM_H

#include <functional>
#include <string>
#include <vector>

#include "toft/base/string/compare.h"

#include "gunir/compiler/parser/big_query_types.h"
#include "gunir/compiler/parser/select_stmt.pb.h"
#include "gunir/io/column_metadata.pb.h"

namespace gunir {
namespace compiler {
const double kFloatCompareThreshold = 1e-10;

template <class T>
int CompareDatumValue(T v1, T v2) {
    if (std::greater<T>()(v1, v2)) {
        return 1;
    }

    if (std::less<T>()(v1, v2)) {
        return -1;
    }
    return 0;
}

struct BytesStorage {
public:
    size_t m_size;
    void* m_data;

    explicit BytesStorage(size_t size) {
        m_size = size;
        m_data = static_cast<void*>(new char[m_size]);
        memset(m_data, 0, m_size);
    }

    BytesStorage(size_t size, const void* content) {
        m_size = size;
        m_data = static_cast<void*>(new char[m_size]);
        memcpy(m_data, content, m_size);
    }

    BytesStorage(const BytesStorage& that) {
        m_size = that.m_size;
        m_data = static_cast<void*>(new char[m_size]);
        memcpy(m_data, that.m_data, m_size);
    }

    const BytesStorage& operator=(const BytesStorage& that) {
        if (m_size != that.m_size) {
            m_size = that.m_size;
            delete static_cast<char*>(m_data);
            m_data = static_cast<void*>(new char[m_size]);
        }

        memcpy(m_data, that.m_data, m_size);
        return that;
    }

    ~BytesStorage() {
        delete static_cast<char*>(m_data);
        m_data = NULL;
    }

    int compare(const BytesStorage& that) const {
        return toft::CompareByteString(m_data, m_size,
                                       that.m_data, that.m_size);
    }

    bool operator<(const BytesStorage& that) const {
        return this->compare(that) < 0;
    }

    bool operator>(const BytesStorage& that) const {
        return that < *this;
    }

    bool operator<=(const BytesStorage& that) const {
        return !(*this > that);
    }

    bool operator>=(const BytesStorage& that) const {
        return !(*this < that);
    }

    bool operator==(const BytesStorage& that) const {
        return (m_size == that.m_size)
            && toft::MemoryEqual(m_data, that.m_data, m_size);
    }

    bool operator!=(const BytesStorage& that) {
        return !(*this == that);
    }
};

union Datum {
    bool _BOOL;

    int32_t _INT32;
    uint32_t _UINT32;

    int64_t _INT64;
    uint64_t _UINT64;

    float _FLOAT;
    double _DOUBLE;

    std::string* _STRING;
    BytesStorage* _BYTES;

    Datum() {
        _DOUBLE = 0;
    }

    int CompareWith(const Datum& that, BQType type) const {
        switch (type) {
        case BigQueryType::INT32:
            return CompareDatumValue(this->_INT32, that._INT32);

        case BigQueryType::UINT32:
            return CompareDatumValue(this->_UINT32, that._UINT32);

        case BigQueryType::INT64:
            return CompareDatumValue(this->_INT64, that._INT64);

        case BigQueryType::UINT64:
            return CompareDatumValue(this->_UINT64, that._UINT64);

        case BigQueryType::FLOAT:
            return CompareDatumValue(this->_FLOAT, that._FLOAT);

        case BigQueryType::DOUBLE:
            return CompareDatumValue(this->_DOUBLE, that._DOUBLE);

        case BigQueryType::BOOL:
            return CompareDatumValue(this->_BOOL, that._BOOL);

        case BigQueryType::STRING:
            return this->_STRING->compare(*that._STRING);

        case BigQueryType::BYTES:
            return this->_BYTES->compare(*that._BYTES);

        default:
            return CompareDatumValue(this->_INT32, that._INT32);
        }
    }

    void* GetData(BQType type) {
        Datum& data = *this;
        switch (type) {
        case BigQueryType::INT32:
            return static_cast<void*>(&data._INT32);

        case BigQueryType::UINT32:
            return static_cast<void*>(&data._UINT32);

        case BigQueryType::INT64:
            return static_cast<void*>(&data._INT64);

        case BigQueryType::UINT64:
            return static_cast<void*>(&data._UINT64);

        case BigQueryType::FLOAT:
            return static_cast<void*>(&data._FLOAT);

        case BigQueryType::DOUBLE:
            return static_cast<void*>(&data._DOUBLE);

        case BigQueryType::BOOL:
            return static_cast<void*>(&data._BOOL);

        case BigQueryType::STRING:
            return static_cast<void*>(&data._STRING);

        case BigQueryType::BYTES:
            return static_cast<void*>(&data._BYTES);

        default:
            return static_cast<void*>(&data._BYTES);
        }
    }

    void SetData(void* data, BQType type) {
        switch (type) {
        case BigQueryType::INT32:
            _INT32 = *static_cast<int32_t*>(data);
            break;

        case BigQueryType::UINT32:
            _UINT32 = *static_cast<uint32_t*>(data);
            break;

        case BigQueryType::INT64:
            _INT64 = *static_cast<int64_t*>(data);
            break;

        case BigQueryType::UINT64:
            _UINT64 = *static_cast<uint64_t*>(data);
            break;

        case BigQueryType::FLOAT:
            _FLOAT = *static_cast<float*>(data);
            break;

        case BigQueryType::DOUBLE:
            _DOUBLE = *static_cast<double*>(data);
            break;

        case BigQueryType::BOOL:
            _BOOL = *static_cast<bool*>(data);
            break;

        case BigQueryType::STRING:
            *_STRING = *static_cast<std::string*>(data);
            break;

        case BigQueryType::BYTES:
            *_BYTES = *static_cast<BytesStorage*>(data);

        default:
            LOG(FATAL) << "Can't set data to unknown type or bytes type"
                << type;
        }
    }

    void CopyFrom(const Datum& that, BQType type) {
        if (type == BigQueryType::STRING) {
            delete _STRING;
            _STRING = new std::string(*(that._STRING));
        } else if (type == BigQueryType::BYTES) {
            delete _BYTES;
            _BYTES = new BytesStorage(*(that._BYTES));
        } else {
            *this = that;
        }
    }
};

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_DATUM_H

