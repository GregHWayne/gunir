// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
//
//

#include "gunir/io/slice.h"

#include "toft/base/string/number.h"
#include "thirdparty/glog/logging.h"

namespace gunir {
namespace io {

const uint32_t Slice::kSliceStructureTypeFlag[3] = {
    0x00000000, // TYPE_UNKOWN = 0, // 0000
    0x10000000, // TYPE_NON_NESTED = 1, // 0001
    0x20000000, // TYPE_PB = 2, // 0010
};

const char Slice::kSliceBlockInfoFlag[8] = {
    0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02, 0x01,
};

Slice::Slice(uint32_t count)
    : m_type(TYPE_UNKOWN) {
    SetCount(count);
}

Slice::Slice(const Slice& sobj)
    : m_type(sobj.m_type),
      m_count(sobj.m_count),
      m_blocks(sobj.m_blocks),
      m_has_blocks(sobj.m_has_blocks) {
}

Slice::~Slice() {
}

void Slice::Reset() {
    // for reuse slice
    m_type = TYPE_UNKOWN;
    for (size_t i = 0; i < m_has_blocks.size(); ++i) {
        m_has_blocks[i] = false;
        m_blocks[i].Reset();
    }
}

uint32_t Slice::ByteSize() const {
    uint32_t block_info_bytes = (kMaxBoolBit * m_count + 8 - 1) / 8;
    uint32_t head_bytes = 4 + block_info_bytes; // 4 for slice type and count
    uint32_t blocks_bytes = 0;
    for (uint32_t i = 0; i < m_has_blocks.size(); ++i) {
        if (m_has_blocks[i]) {
            blocks_bytes += m_blocks[i].GetBlockSerializedSize();
        }
    }

    return head_bytes + blocks_bytes;
}

bool Slice::SerializeToString(char *buf,
                              uint32_t buf_len,
                              uint32_t *value_len) const {
    uint32_t block_info_bytes = (kMaxBoolBit * m_count + 8 - 1) / 8;
    uint32_t head_bytes = 4 + block_info_bytes; // 4 for slice type and count
    uint32_t blocks_bytes = 0;
    for (uint32_t i = 0; i < m_has_blocks.size(); ++i) {
        if (m_has_blocks[i]) {
            blocks_bytes += m_blocks[i].GetBlockSerializedSize();
        }
    }

    if (buf_len < head_bytes + blocks_bytes) {
        LOG(ERROR) << "Buffer is not enough.";
        return false;
    }

    // init the memory
    memset(buf, 0, head_bytes + blocks_bytes);
    char *data = buf;

    // slice type and count
    uint32_t max_count = 1 << kMaxCountBit;
    if (m_count > max_count) {
        LOG(ERROR) << "Count " << m_count << " is too big."
                   << "Max count is " << max_count;
        return false;
    }

    uint32_t *temp_ptr = reinterpret_cast<uint32_t*>(data);
    *temp_ptr |= kSliceStructureTypeFlag[GetSliceStructureType()];
    *temp_ptr |= m_count;
    data += sizeof(uint32_t); // NO_LINT

    // block info
    for (uint32_t i = 0, count = 0; i < block_info_bytes; ++i) {
        for (uint32_t j = 0; j < 8 && count < m_count; ++j, ++count) {
            if (m_has_blocks[count]) {
                *data |= kSliceBlockInfoFlag[j];
            }
        }
        ++data;
    }

    // blocks
    for (uint32_t i = 0; i < m_blocks.size(); ++i) {
        if (!m_has_blocks[i]) {
            continue;
        }
        uint32_t value_len = 0;
        if (!m_blocks[i].SerializeToString(data, &value_len)) {
            LOG(ERROR) << "Serialize block failed.";
            return false;
        }
        data += value_len;
        if (data - buf > head_bytes + blocks_bytes) {
            LOG(ERROR) << "Serialize blocks error.";
            return false;
        }
    }
    CHECK(head_bytes + blocks_bytes == data - buf)
        << "Serialize slice to string error."
        << "The buf size used is " << data - buf
        << " while the theory data size is " << head_bytes + blocks_bytes
        << " (head is " <<  head_bytes << ", block is " << blocks_bytes << ").";

    *value_len = head_bytes + blocks_bytes;
    return true;
}

ErrorCode Slice::LoadFromString(const char *buf, uint32_t buf_len, uint32_t *read_len) {
    DCHECK(buf != NULL) << "buf is not valid.";

    const char *data = buf;
    uint32_t head_bytes = 4;
    if (buf_len < head_bytes) {
        LOG(ERROR) << "This is not a valid slice serialized string";
        return kBufNotEnough;
    }

    // slice type and count
    const uint32_t *temp_ptr = reinterpret_cast<const uint32_t*>(data);
    uint32_t slice_type = *temp_ptr & 0xf0000000;
    uint32_t count = *temp_ptr & 0x0effffff;
    data += sizeof(uint32_t); // NO_LINT

    uint32_t slice_type_flag_index = slice_type >> 28;
    if (slice_type_flag_index >=
        sizeof(kSliceStructureTypeFlag) / sizeof(uint32_t)) { // NO_LINT
        LOG(ERROR) << "Invalid slice type index " << slice_type_flag_index;
        return kParserError;
    } else {
        SetSliceStructureType((SLICE_STRUCTURE_TYPE)slice_type_flag_index);
    }

    uint32_t max_count = 1 << kMaxCountBit;
    if (count > max_count) {
        LOG(ERROR) << "Invalid count " << count;
        return kParserError;
    }

    SetCount(count);

    uint32_t block_info_bytes = (kMaxBoolBit * m_count + 8 - 1) / 8;
    head_bytes += block_info_bytes;

    // block info
    for (uint32_t i = 0, count = 0; i < block_info_bytes; ++i) {
        for (uint32_t j = 0; j < 8 && count < m_count; ++j, ++count) {
            if ((*data & kSliceBlockInfoFlag[j]) == kSliceBlockInfoFlag[j]) {
                m_has_blocks[count] = true;
            }
        }
        ++data;
    }

    // blocks
    for (uint32_t i = 0; i < m_count; ++i) {
        if (!m_has_blocks[i]) {
            continue;
        }
        uint32_t read_len = 0;
        ErrorCode code = m_blocks[i].LoadFromString(data, buf + buf_len - data,
                                                    &read_len);
        if (kOk != code) {
            return code;
        }
        data += read_len;
        DCHECK_LE(data, buf_len + buf);
    }

    CHECK_GE(buf_len, data - buf) << "LoadFromString for slice error. "
        << "The buffer len is " << buf_len << ". "
        << "while read " << data - buf << " bytes.";
    *read_len = data - buf;
    return kOk;
}

void Slice::SetCount(uint32_t count) {
    m_count = count;

    m_blocks.resize(m_count);
    m_has_blocks.resize(m_count, false);
}

uint32_t Slice::GetCount() const {
    return m_count;
}

bool Slice::HasBlock(uint32_t index) const {
    DCHECK(index < m_count) << "block index exceeds slice count.";
    return m_has_blocks[index];
}

void Slice::SetHasBlock(uint32_t index, bool has_block) {
    DCHECK_LT(index, m_count) << "block index exceeds slice count.";
    m_has_blocks[index] = has_block;
}

Block *Slice::MutableBlock(uint32_t index) {
    DCHECK_LT(index, m_count) << "block index exceeds slice count.";
    if (m_has_blocks[index]) {
        return &m_blocks[index];
    }
    return NULL;
}

const Block *Slice::GetBlock(uint32_t index) const {
    DCHECK_LT(index, m_count) << "block index exceeds slice count.";
    if (m_has_blocks[index]) {
        return &m_blocks[index];
    }
    return NULL;
}

Slice::SLICE_STRUCTURE_TYPE Slice::GetSliceStructureType() const {
    return m_type;
}

void Slice::SetSliceStructureType(SLICE_STRUCTURE_TYPE type) {
    m_type = type;
}

std::string Slice::DebugString() const {
    std::string value = "----Slice DebugString----\n";
    for (uint32_t i = 0; i < m_blocks.size(); ++i) {
        value += "Block " + NumberToString(i) + ":\n";
        value += m_blocks[i].DebugString();
        if (i != m_blocks.size() - 1) {
            value += "--------\n";
        }
    }
    value += "----End DebugString----\n";
    return value;
}

} // namespace io
} // namespace gunir
