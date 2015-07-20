// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
//

#include "gunir/io/block.h"

#include "toft/base/string/compare.h"
#include "toft/base/string/number.h"
#include "thirdparty/glog/logging.h"

namespace gunir {
namespace io {

const uint32_t Block::kValueTypeLength[TYPE_MAX_MARK] = {
    // fixed type
    sizeof(int32_t),    // TYPE_INT32 = 0, // NO_LINT
    sizeof(int64_t),    // TYPE_INT64 = 1, // NO_LINT
    sizeof(uint32_t),   // TYPE_UINT32 = 2, // NO_LINT
    sizeof(uint64_t),   // TYPE_UINT64 = 3, // NO_LINT
    sizeof(int32_t),    // TYPE_SINT32 = 4, // NO_LINT
    sizeof(int64_t),    // TYPE_SINT64 = 5, // NO_LINT
    sizeof(int32_t),    // TYPE_FIXED32 = 6, // NO_LINT
    sizeof(int64_t),    // TYPE_FIXED64 = 7, // NO_LINT
    sizeof(int32_t),    // TYPE_SFIXED32 = 8, // NO_LINT
    sizeof(int64_t),    // TYPE_SFIXED64 = 9, // NO_LINT
    sizeof(float),      // TYPE_FLOAT = 10, // NO_LINT
    sizeof(double),     // TYPE_DOUBLE = 11, // NO_LINT
    sizeof(bool),       // TYPE_BOOL = 12, // NO_LINT
    // non-fixed type
    0, // TYPE_STRING = 13,
    0, // TYPE_BYTES = 14,
    0, // TYPE_UNDEFINED = 15,
    0, // TYPE_NULL = 16,
};

Block::Block()
    : m_type(TYPE_NULL), m_rep_level(0), m_def_level(0) {}

Block::Block(const Block& bobj)
    : m_type(bobj.m_type), m_rep_level(bobj.m_rep_level),
      m_def_level(bobj.m_def_level), m_value(bobj.m_value) {}

Block::~Block() {
}

Block& Block::operator =(const Block& block) {
    m_type = block.m_type;
    m_rep_level = block.m_rep_level;
    m_def_level = block.m_def_level;
    m_value = block.m_value;
    return *this;
}

void Block::Reset() {
    // for reuse
    m_type = TYPE_NULL;
    m_rep_level = 0;
    m_def_level = 0;
    m_value.clear();
}

bool Block::SerializeToString(char *buf, uint32_t *value_len) const {
    int max_level = 1 << kMaxLevelBit;
    // r, d, value
    if (m_rep_level > max_level || m_def_level > max_level) {
        LOG(ERROR) << "R: " << m_rep_level << " or D: " << m_def_level
                   << " is too big, Max level is " << max_level;
        return false;
    }
    *buf++ = (0xff & m_rep_level);
    *buf++ = (0xff & m_def_level);
    *buf++ = (0xff & m_type);

    *value_len = 3;

    // skip null block's value
    if (m_type == TYPE_NULL) {
        return true;
    }

    // value
    uint32_t max_value_size = 1 << kMaxValueBit;
    uint32_t size = m_value.size();
    if (size > max_value_size) {
        LOG(ERROR) << "value size " << size << " exceed the max size"
                   << max_value_size;
        return false;
    }
    switch (m_type) {
        case TYPE_INT32:
        case TYPE_INT64:
        case TYPE_UINT32:
        case TYPE_UINT64:
        case TYPE_SINT32:
        case TYPE_SINT64:
        case TYPE_FIXED32:
        case TYPE_FIXED64:
        case TYPE_SFIXED32:
        case TYPE_SFIXED64:
        case TYPE_FLOAT:
        case TYPE_BOOL:
        case TYPE_DOUBLE:
            DCHECK(kValueTypeLength[m_type] == size) << "Value length error.";
            memcpy(buf, m_value.data(), size);
            *value_len += size;
            break;
        case TYPE_STRING:
        case TYPE_BYTES:
        case TYPE_UNDEFINED:
            memcpy(buf, &size, sizeof(size));
            memcpy(buf + sizeof(size), m_value.data(), size);
            *value_len += (sizeof(size) + size);
            break;
        default:
            LOG(ERROR) << "Type [" << m_type << "] is not implemented.";
            return false;
    }

    return true;
}

ErrorCode Block::LoadFromString(const char *buf,
                                uint32_t buf_len,
                                uint32_t *read_len) {
    if (buf_len < 3) {
        return kBufNotEnough;
    }

    int max_level = 1 << kMaxLevelBit;
    // r, d, value
    m_rep_level = 0 | (*buf++ & 0xff);
    m_def_level = 0 | (*buf++ & 0xff);
    m_type = (VALUE_TYPE)(*buf++ & 0xff);
    if (m_rep_level > max_level || m_def_level > max_level) {
        LOG(ERROR) << "R: " << m_rep_level << " or D: " << m_def_level
                   << " is too big, Max level is " << max_level;
        return kParserError;
    }

    *read_len = 3;

    // skip null block's value
    if (m_type == TYPE_NULL) {
        return kOk;
    }

    // value
    uint32_t size = 0;
    switch (m_type) {
        case TYPE_INT32:
        case TYPE_INT64:
        case TYPE_UINT32:
        case TYPE_UINT64:
        case TYPE_SINT32:
        case TYPE_SINT64:
        case TYPE_FIXED32:
        case TYPE_FIXED64:
        case TYPE_SFIXED32:
        case TYPE_SFIXED64:
        case TYPE_FLOAT:
        case TYPE_BOOL:
        case TYPE_DOUBLE:
            // read value
            if (buf_len < *read_len + kValueTypeLength[m_type]) {
                return kBufNotEnough;
            }
            m_value.set(buf, kValueTypeLength[m_type]);
            *read_len += m_value.size();
            break;
        case TYPE_STRING:
        case TYPE_BYTES:
        case TYPE_UNDEFINED:
            // read size
            if (buf_len < *read_len + sizeof(size)) {
                return kBufNotEnough;
            }
            size = *(reinterpret_cast<const uint32_t*>(buf));
            *read_len += sizeof(size);

            // read value
            if (buf_len < *read_len + size) {
                return kBufNotEnough;
            }
            m_value.set(buf + sizeof(size), size);
            *read_len += size;
            break;
        default:
            LOG(ERROR) << "Type [" << m_type << "] is not implemented.";
            return kParserError;
    }

    return kOk;
}

uint32_t Block::GetBlockSerializedSize() const {
    uint32_t block_size = 3; // for R, D, type, each for 1 Byte.

    if (IsNull()) {
        return block_size;
    }

    uint32_t size = m_value.size();
    if (kValueTypeLength[GetValueType()] == 0) {
        // for non-fixed block type.
        block_size += sizeof(size);
    }

    return (block_size + size);
}

void Block::SetValue(const StringPiece& value) {
    DCHECK_NE(m_type, TYPE_NULL);
    m_value = value;
}

#ifndef BLOCKDEBUGSTRING
#define APPENDVALUE(value_type, type) \
    case value_type: { \
        value.append("Value Type: "); \
        value.append(#value_type); \
        value.append("\n"); \
        \
        value.append("Value: "); \
        type value_content; \
        memcpy(&value_content, m_value.data(), kValueTypeLength[value_type]); \
        value.append(NumberToString(value_content)); \
        value.append("\n"); \
        break; \
    }

#define APPENDSTRINGVALUE(value_type) \
    case value_type: { \
        value.append("Value Type: "); \
        value.append(#value_type); \
        value.append("\n"); \
        \
        value.append("Value: "); \
        value.append(m_value.data(), m_value.size()); \
        value.append("\n"); \
        break; \
    }

#endif

std::string Block::DebugString() const {
    std::string value;
    value.append("Repeated Level: " + NumberToString(m_rep_level) + "\n");
    value.append("Definition Level: " + NumberToString(m_def_level) + "\n");
    switch (m_type) {
        APPENDVALUE(TYPE_INT32, int32_t);
        APPENDVALUE(TYPE_INT64, int64_t);
        APPENDVALUE(TYPE_UINT32, uint32_t);
        APPENDVALUE(TYPE_UINT64, uint64_t);
        APPENDVALUE(TYPE_SINT32, int32_t);
        APPENDVALUE(TYPE_SINT64, int64_t);
        APPENDVALUE(TYPE_FIXED32, int32_t);
        APPENDVALUE(TYPE_FIXED64, int64_t);
        APPENDVALUE(TYPE_SFIXED32, int32_t);
        APPENDVALUE(TYPE_SFIXED64, int64_t);

        APPENDVALUE(TYPE_FLOAT, float);
        APPENDVALUE(TYPE_DOUBLE, double);

        APPENDSTRINGVALUE(TYPE_STRING);
        APPENDSTRINGVALUE(TYPE_BYTES);
        APPENDSTRINGVALUE(TYPE_UNDEFINED);

        case TYPE_BOOL: {
            value.append("Value Type: TYPE_BOOL\n");
            bool value_content = *(m_value.data());
            if (value_content) {
                value.append("Value: True\n");
            } else {
                value.append("Value: False\n");
            }
            break;
        }
        case TYPE_NULL: {
            value.append("Value Type: TYPE_NULL\n");
            value.append("Value: NULL\n");
            break;
        }
        default:
            LOG(ERROR) << "Type [" << m_type << "] is not implemented.";
    }
    return value;
}

} // namespace io
} // namespace gunir
