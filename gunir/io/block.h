// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
//

#ifndef GUNIR_IO_BLOCK_H_
#define GUNIR_IO_BLOCK_H_

#include <stdint.h>
#include <string>

#include "toft/base/scoped_ptr.h"
#include "toft/base/string/string_piece.h"

#include "gunir/proto/error_code.pb.h"

using namespace toft;

namespace gunir {
namespace io {

class Block {
public:
    enum VALUE_TYPE {
        // fixed type
        TYPE_INT32 = 0,
        TYPE_INT64 = 1,
        TYPE_UINT32 = 2,
        TYPE_UINT64 = 3,
        TYPE_SINT32 = 4,
        TYPE_SINT64 = 5,
        TYPE_FIXED32 = 6,
        TYPE_FIXED64 = 7,
        TYPE_SFIXED32 = 8,
        TYPE_SFIXED64 = 9,
        TYPE_FLOAT = 10,
        TYPE_DOUBLE = 11,
        TYPE_BOOL = 12,
        // non-fixed type
        TYPE_STRING = 13,
        TYPE_BYTES = 14,
        TYPE_UNDEFINED = 15,
        TYPE_NULL = 16,
        // mark only
        TYPE_MAX_MARK = 17,
    };

    static const int kMaxLevelBit = 8;  // max level value is: 2^8 = 256
    static const int kMaxValueBit = 27; // max value length is: 2^27 = 64MB
    static const uint32_t kValueTypeLength[TYPE_MAX_MARK];

    Block();
    Block(const Block& bobj);
    ~Block();
    Block& operator =(const Block& block);

    void Reset();

    bool SerializeToString(char *buf, uint32_t *value_len) const;
    ErrorCode LoadFromString(const char *buf,
                             uint32_t buf_len,
                             uint32_t *read_len);

    bool IsNull() const;

    int GetRepLevel() const;
    void SetRepLevel(int rep_level);

    int GetDefLevel() const;
    void SetDefLevel(int def_level);

    StringPiece GetValue() const;
    void SetValue(const StringPiece& value);
    std::string DebugString() const;

    VALUE_TYPE GetValueType() const;
    void SetValueType(VALUE_TYPE type);

    uint32_t GetBlockSerializedSize() const;

private:
    VALUE_TYPE m_type;
    int m_rep_level;
    int m_def_level;
    StringPiece m_value;
};

inline bool Block::IsNull() const {
    return m_type == TYPE_NULL;
}

inline int Block::GetRepLevel() const {
    return m_rep_level;
}

inline void Block::SetRepLevel(int rep_level) {
    m_rep_level = rep_level;
}

inline int Block::GetDefLevel() const {
    return m_def_level;
}

inline void Block::SetDefLevel(int def_level) {
    m_def_level = def_level;
}

inline StringPiece Block::GetValue() const {
    return m_value;
}

inline Block::VALUE_TYPE Block::GetValueType() const {
    return m_type;
}

inline void Block::SetValueType(VALUE_TYPE type) {
    m_type = type;
}

} // namespace io
} // namespace gunir

#endif  // GUNIR_IO_BLOCK_H
