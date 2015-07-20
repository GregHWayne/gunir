// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
// brief Performance test of SliceScanner

#include <vector>

#include "toft/base/scoped_ptr.h"
#include "toft/system/time/clock.h"
#include "thirdparty/gtest/gtest.h"
#include "thirdparty/perftools/profiler.h"

#include "gunir/io/slice.h"
#include "gunir/io/slice_scanner.h"

namespace gunir {
namespace io {

class SliceScannerPerformanceTest : public ::testing::Test {
protected:
    virtual void SetUp() {
        m_length = 16 * MByte;
        m_buffer = new char[16 * MByte];
        InitTestdata();
    }

    virtual void TearDown() {
        delete [] m_buffer;
    }

    void InitTestdata() {
        uint32_t cur_buffer_len = m_length;
        char* cur_buffer = m_buffer;
        uint32_t serilize_len = 0;

        std::string data_str = "hello world, hello world";
        Slice slice(TYPE_MAX_MARK);
        ConstructSlice(data_str, &slice);

        int64_t start_tm = RealtimeClock.MicroSeconds();

        while (slice.SerializeToString(cur_buffer, cur_buffer_len, &serilize_len)) {
            cur_buffer += serilize_len;
            cur_buffer_len -= serilize_len;
        }

        int64_t end_tm = RealtimeClock.MicroSeconds();
        int64_t total_cost_us = end_tm - start_tm;
        uint64_t total_data_size = cur_buffer - m_buffer;

        // data size(Byte) per second
        uint64_t data_size_persecond = (total_data_size * 1000 * 1000) / (uint64_t)total_cost_us;
        std::cout << "Serialize: total_cost_us=" << total_cost_us
            << " total_data_size(M)=" << total_data_size / MByte
            << " data_size_persecond(M)=" << data_size_persecond / MByte << std::endl;

        m_string_piece.set(m_buffer, cur_buffer - m_buffer);
        m_slice_scanner.reset(new SliceScanner(m_string_piece));
    }

    void ConstructSlice(const std::string& data_str,
                        Slice* slice) {
        CHECK_GE(data_str.length(), sizeof(uint64_t));
        slice->SetSliceStructureType(Slice::TYPE_PB);

        for (uint32_t j = 0; j < slice->GetCount(); ++j) {
            slice->SetHasBlock(j, true);
            Block* block = slice->MutableBlock(j);
            CHECK_NOTNULL(block);
            block->SetRepLevel(j);
            block->SetDefLevel(j);
            StringPiece value;

            switch (j) {
            case TYPE_BOOL:
                value.set(data_str.data(), sizeof(bool));
                block->SetValueType(Block::TYPE_BOOL);
                block->SetValue(value);
                break;
            case TYPE_FLOAT:
                value.set(data_str.data(), sizeof(float));
                block->SetValueType(Block::TYPE_FLOAT);
                block->SetValue(value);
                break;
            case TYPE_UINT32:
                value.set(data_str.data(), sizeof(uint32_t));
                block->SetValueType(Block::TYPE_UINT32);
                block->SetValue(value);
                break;
            case TYPE_UINT64:
                value.set(data_str.data(), sizeof(uint64_t));
                block->SetValueType(Block::TYPE_UINT64);
                block->SetValue(value);
                break;
            case TYPE_STRING:
                value.set(data_str.data(), data_str.length());
                block->SetValueType(Block::TYPE_STRING);
                block->SetValue(value);
                break;
            case TYPE_NULL:
            default:
                block->SetValueType(Block::TYPE_NULL);
                break;
            }
        }
    }

    enum BlockValueType {
        TYPE_NULL = 0,
        TYPE_BOOL = 1,
        TYPE_FLOAT = 2,
        TYPE_UINT32 = 3,
        TYPE_UINT64 = 4,
        TYPE_STRING = 5,
        TYPE_MAX_MARK = 6,
    };

protected:
    static const uint32_t MByte = 1024 * 1024;
    char* m_buffer;
    uint32_t m_length;
    scoped_ptr<SliceScanner> m_slice_scanner;
    StringPiece m_string_piece;
};

TEST_F(SliceScannerPerformanceTest, NextSlice)
{
    uint32_t count = 0;
    Slice* slice = NULL;

    int64_t start_tm = RealtimeClock.MicroSeconds();

    while (m_slice_scanner->NextSlice(&slice))
    {
        EXPECT_TRUE(slice != NULL);
        slice = NULL;
        ++count;
    }

    int64_t end_tm = RealtimeClock.MicroSeconds();
    int64_t total_cost_us = end_tm - start_tm;
    uint64_t total_data_size = m_string_piece.length();

    // data size per second
    uint64_t data_size_persecond = (total_data_size * 1000 * 1000) / (uint64_t)total_cost_us;
    std::cout << "Parse: count=" << count << " total_cost_us=" << total_cost_us
        << " total_data_size(M)=" << total_data_size / MByte
        << " data_size_persecond(M)=" << data_size_persecond / MByte << std::endl;
}

} // namespace io
} // namespace gunir
