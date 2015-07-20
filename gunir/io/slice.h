// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
//
// Description: The slice class.

// Slice is the data carrier for gunir. Its format is like this:
// --------------------------------------------------------------------
// | Head content | Block 1 for column 1 | ... | Block n for column n |
// --------------------------------------------------------------------
// All you see is just a string produced by SerializeToString() method.

// Slice head schema:
// ---------------------------------------------------------------
// slice type(4bits) | unused(1bit) | count(27bits) | block_info |
// ---------------------------------------------------------------

// block_info schema:
// -----------------------------------------
// has_block(1bit) | has_block(1bit) | ... |
// -----------------------------------------

// block(for null) schema:
// ------------------------------------------
// rep_level(1B) | def_level(1B) | type(1B) |
// ------------------------------------------

// block(for fixed type) schema:
// --------------------------------------------------------
// rep_level(1B) | def_level(1B) | type(1B) | value(1-8B) |
// --------------------------------------------------------

// block(for non-fixed type) schema:
// -------------------------------------------------------------------------
// rep_level(1B) | def_level(1B) | type(1B) | length(4B) | value(length B) |
// -------------------------------------------------------------------------

#ifndef GUNIR_IO_SLICE_H_
#define GUNIR_IO_SLICE_H_

#include <stdint.h>
#include <string>
#include <vector>

#include "toft/base/scoped_ptr.h"
#include "toft/base/uncopyable.h"

#include "gunir/proto/error_code.pb.h"
#include "gunir/io/block.h"

namespace gunir {
namespace io {

class Slice {
public:
    static const int kMaxBoolBit = 1;
    static const int kMaxCountBit = 27; // max count number is: 2^27 = 64MB
    static const uint32_t kSliceStructureTypeFlag[3];
    static const char kSliceBlockInfoFlag[8];

    enum SLICE_STRUCTURE_TYPE { // Use 4 bit to represent below type.
        TYPE_UNKOWN = 0,        // 0000
        TYPE_NON_NESTED = 1,    // 0001
        TYPE_PB = 2,            // 0010
    };

    explicit Slice(uint32_t count = 0);
    Slice(const Slice& sobj);
    ~Slice();

    void Reset();

    uint32_t ByteSize() const;
    bool SerializeToString(char *buf,
                           uint32_t buf_len,
                           uint32_t *value_len) const;
    ErrorCode LoadFromString(const char *buf, uint32_t buf_len,
                             uint32_t *read_len);

    uint32_t GetCount() const;

    bool HasBlock(uint32_t index) const;
    void SetHasBlock(uint32_t index, bool has_block = true);

    const Block *GetBlock(uint32_t index) const;
    Block *MutableBlock(uint32_t index);

    SLICE_STRUCTURE_TYPE GetSliceStructureType() const;
    void SetSliceStructureType(SLICE_STRUCTURE_TYPE type);
    std::string DebugString() const;

    int GetSelectLevel() {
        int select_level = 0;
        for (size_t i = 0; i < m_blocks.size(); ++i) {
            if (select_level < m_blocks[i].GetRepLevel())
                select_level = m_blocks[i].GetRepLevel();
        }
        return select_level;
    }

private:
    void SetCount(uint32_t count);
    uint32_t GetBlockSize(const Block *block_obj);

private:
    SLICE_STRUCTURE_TYPE m_type;
    uint32_t m_count;
    std::vector<Block> m_blocks;
    std::vector<bool> m_has_blocks;
};

} // namespace io
} // namespace gunir

#endif  // GUNIR_IO_SLICE_H
