// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include <vector>

#include "toft/base/scoped_ptr.h"
#include "thirdparty/gtest/gtest.h"

#include "gunir/io/slice.h"
#include "gunir/io/slice_scanner.h"
#include "gunir/utils/test_helper.h"

namespace gunir {
namespace io {

class SliceScannerTest : public ::testing::Test {
protected:
    virtual void SetUp() {
        m_buffer = new char[1024 * 1024];
        m_length = 1024 * 1024;
        InitTestData();
        m_scanner.reset(new SliceScanner(m_string_piece));
    }

    virtual void TearDown() {
        delete [] m_buffer;
    }

    void InitTestData() {
        char* buffer = m_buffer;
        uint32_t length = m_length;
        uint32_t value_len = 0;
        for (uint32_t i = 0; i < kVectorSize; ++i) {
            for (uint32_t j = 0; j < kSliceNum; ++j) {
                Slice *sobj = new Slice(1);
                sobj->SetHasBlock(0);
                Block *bobj = sobj->MutableBlock(0);
                bobj->SetRepLevel(j);
                CHECK(sobj->SerializeToString(buffer, length, &value_len));
                delete sobj;
                buffer += value_len;
                length -= value_len;
            }
        }
        m_string_piece.set(m_buffer, buffer - m_buffer);
    }

protected:
    static const uint32_t kVectorSize = 10;
    static const uint32_t kSliceNum = 10;
    char* m_buffer;
    uint32_t m_length;
    scoped_ptr<SliceScanner> m_scanner;
    StringPiece m_string_piece;
};

TEST_F(SliceScannerTest, EmptyScanner) {
    Slice* slice;
    m_scanner.reset(new SliceScanner(StringPiece()));
    EXPECT_FALSE(m_scanner->NextSlice(&slice));
}

TEST_F(SliceScannerTest, NextSlice) {
    int32_t count = 0;
    Slice* slice;
    while (m_scanner->NextSlice(&slice)) {
        const Block *block = slice->GetBlock(0);
        EXPECT_EQ(count % 10, block->GetRepLevel());
        count++;
    }
    EXPECT_EQ(100, count);
}

}  // namespace io
}  // namespace gunir
