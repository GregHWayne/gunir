// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//

#include <stdint.h>
#include <vector>

#include "toft/base/scoped_ptr.h"
#include "thirdparty/gtest/gtest.h"

#include "gunir/io/block.h"
#include "gunir/io/column_metadata.pb.h"
#include "gunir/io/column_writer.h"

namespace gunir {
namespace io {

class ColumnWriterTest : public ::testing::Test {
protected:
    virtual void SetUp() {
        m_column_writer = new ColumnWriter(&m_colomn_metadata);
    }
    virtual void TearDown() {
        delete m_column_writer;
    }

    std::string GenerateBlockString() {
        const char* value = "test";
        Block block;
        block.SetDefLevel(3);
        block.SetRepLevel(3);
        block.SetValueType(Block::TYPE_STRING);
        block.SetValue(StringPiece(value));

        char buffer[1024];
        uint32_t len = 0;
        CHECK(block.SerializeToString(buffer, &len));
        CHECK_GT(len, 0);
        return std::string(buffer, len);
    }

    void CheckBlockString(const std::string& result, uint32_t* read_len) {
        Block block;
        CHECK_EQ(kOk, block.LoadFromString(result.c_str(), result.length(),
                                           read_len));
        EXPECT_EQ(block.GetDefLevel(), 3);
        EXPECT_EQ(block.GetRepLevel(), 3);
        EXPECT_EQ(block.IsNull(), false);
        EXPECT_EQ(block.GetValueType(), Block::TYPE_STRING);
        StringPiece sp = block.GetValue();
        EXPECT_EQ(std::string(sp.data(), sp.length()), std::string("test"));
    }

protected:
    ColumnWriter *m_column_writer;
    ColumnDynamicInfo m_colomn_metadata;
};

TEST_F(ColumnWriterTest, Flush) {
    std::string block = GenerateBlockString();

    uint32_t times = 10;
    for (uint32_t i = 0; i < times; ++i) {
        m_column_writer->Write(block.c_str(), block.length());
    }

    scoped_ptr<File> file;
    file.reset(File::Open("column_writer_test.output", "w"));
    CHECK_NOTNULL(file.get());

    EXPECT_TRUE(m_column_writer->Flush(file.get()));
    EXPECT_TRUE(file->Close());
    EXPECT_EQ(block.length() * times, m_colomn_metadata.length());

//     std::string result;
//     EXPECT_TRUE(File::LoadToString("column_writer_test.output",
//                                    &result));
//     EXPECT_EQ(block.length() * times, result.length());
//     uint32_t read_len = 0;
//     for (uint32_t i = 0; i < times; ++i) {
//         CheckBlockString(result, &read_len);
//         result = result.substr(read_len, result.length() - read_len);
//     }
}

}  // namespace io
}  // namespace gunir
