// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//

#include <stdint.h>

#include <vector>

// #include "toft/base/stdext/shared_ptr.h"
#include "toft/system/memory/mempool.h"
#include "thirdparty/glog/logging.h"
#include "thirdparty/gtest/gtest.h"

#include "gunir/io/column_metadata.pb.h"
#include "gunir/io/column_reader.h"

DECLARE_int32(gunir_column_read_block_size);

namespace gunir {
namespace io {

class ColumnReaderTest : public ::testing::Test {
protected:
    virtual void SetUp() {
//         File::Init();
        FLAGS_gunir_column_read_block_size = 10;
        m_mempool = new MemPool(MemPool::MAX_UNIT_SIZE);
        m_column_filename = "column_file";
        m_error_column_filename = "error_column_file";

        m_buffer.reset(new char[1024 * 1024]);
        m_buffer_error_data.reset(new char[1024 * 1024]);

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
        m_block_string = std::string(buffer, len);

        char *ptr = m_buffer.get();
        for (uint32_t i = 0; i < 30; ++i) {
            memcpy(ptr, m_block_string.c_str(), len);
            ptr += m_block_string.size();
        }

        char *ptr2 = m_buffer_error_data.get();
        std::string error_data = "error_data\0\r\t\n";
        for (uint32_t i = 0; i < 10; ++i) {
            memcpy(ptr2, m_block_string.c_str(), len);
            ptr2 += m_block_string.size();
        }
        for (uint32_t i = 10; i < 30; ++i) {
            memcpy(ptr2, error_data.c_str(), error_data.size());
            ptr2 += error_data.size();
        }

        m_buffer_len = ptr - m_buffer.get();
        m_buffer_error_len = ptr2 - m_buffer_error_data.get();

        DumpColumnData(m_buffer.get(), m_buffer_len, m_column_filename);
        DumpColumnData(m_buffer_error_data.get(),
                       m_buffer_error_len,
                       m_error_column_filename);

        ColumnMetaData column_metadata;
        column_metadata.mutable_dynamic_info()->set_start_position(0);
        column_metadata.mutable_dynamic_info()->set_length(m_buffer_len);

        m_column_name = "col1";
        column_metadata.mutable_static_info()->set_column_name(m_column_name);
        m_column_type = TYPE_STRING;
        column_metadata.mutable_static_info()->set_column_type(m_column_type);

        m_column_reader
            = new ColumnReader(column_metadata.static_info(), m_mempool);
    }

    virtual void TearDown() {
//         File::CleanUp();
        delete m_column_reader;
        delete m_mempool;
    }

    bool DumpColumnData(const char* buffer,
                        uint32_t size,
                        const std::string& filename) {
        scoped_ptr<File> file;
        file.reset(File::Open(filename.c_str(),
                              "w"));
        if (file.get() == NULL) {
            LOG(ERROR) << "Open file error";
            return false;
        }
        int64_t ret1 = file->Write(buffer, static_cast<int64_t>(size));
        if (ret1 < 0) {
            LOG(ERROR) << "Write file error.";
            return false;
        }

        if (!file->Close()) {
            LOG(ERROR) << "Close tablet file error.";
            return false;
        }
        return true;
    }

protected:
    ColumnReader *m_column_reader;
    scoped_ptr<char> m_buffer;
    scoped_ptr<char> m_buffer_error_data;
    uint32_t m_buffer_len;
    uint32_t m_buffer_error_len;
    std::string m_block_string;
    std::string m_column_name;
    ColumnType m_column_type;

    MemPool* m_mempool;
    std::string m_column_filename;
    std::string m_error_column_filename;
};

TEST_F(ColumnReaderTest, Open) {
    EXPECT_TRUE(m_column_reader->Open(m_column_filename, 0, m_buffer_len));
}

TEST_F(ColumnReaderTest, NextBlock) {
    EXPECT_TRUE(m_column_reader->Open(m_column_filename, 0, m_buffer_len));
    EXPECT_TRUE(m_column_reader->NextBlock());
}

TEST_F(ColumnReaderTest, GetBlock) {
    EXPECT_TRUE(m_column_reader->Open(m_column_filename, 0, m_buffer_len));
    EXPECT_TRUE(m_column_reader->NextBlock());

    Block block;
    m_column_reader->GetBlock(&block);

    char buffer[1024];
    uint32_t len = 0;
    CHECK(block.SerializeToString(buffer, &len));
    std::string str = std::string(buffer, len);

    EXPECT_EQ(m_block_string, str);
}

TEST_F(ColumnReaderTest, GetBlockAll) {
    EXPECT_TRUE(m_column_reader->Open(m_column_filename, 0, m_buffer_len));

    for (uint32_t i = 0; i < 30; ++i) {
        EXPECT_TRUE(m_column_reader->NextBlock());

        Block block;
        m_column_reader->GetBlock(&block);

        char buffer[1024];
        uint32_t len = 0;
        CHECK(block.SerializeToString(buffer, &len));
        std::string str = std::string(buffer, len);

        EXPECT_EQ(m_block_string, str);
    }

    EXPECT_FALSE(m_column_reader->NextBlock());
}

TEST_F(ColumnReaderTest, ReadBlockError) {
    EXPECT_TRUE(m_column_reader->Open(m_error_column_filename,
                                      0,
                                      m_buffer_error_len));

    for (uint32_t i = 0; i < 9; ++i) {
        EXPECT_TRUE(m_column_reader->NextBlock());
    }

    for (uint32_t i = 9; i < 30; ++i) {
        EXPECT_FALSE(m_column_reader->NextBlock());
    }
}

TEST_F(ColumnReaderTest, Level) {
    EXPECT_TRUE(m_column_reader->Open(m_column_filename, 0, m_buffer_len));
    int ll = 3;

    for (uint32_t i = 0; i < 30; ++i) {
        EXPECT_TRUE(m_column_reader->NextBlock());

        int level = m_column_reader->GetRepetitionLevel();
        EXPECT_EQ(level, ll);

        level = m_column_reader->GetDefinitionLevel();
        EXPECT_EQ(level, ll);
    }

    EXPECT_FALSE(m_column_reader->NextBlock());
}

TEST_F(ColumnReaderTest, NextRepetitionLevel) {
    EXPECT_TRUE(m_column_reader->Open(m_column_filename, 0, m_buffer_len));

    int ll = 3;
    int level = m_column_reader->NextRepetitionLevel();
    EXPECT_EQ(level, ll);

    for (uint32_t i = 0; i < 29; ++i) {
        EXPECT_TRUE(m_column_reader->NextBlock());

        level = m_column_reader->NextRepetitionLevel();
        EXPECT_EQ(level, ll);
    }

    EXPECT_TRUE(m_column_reader->NextBlock());
    EXPECT_FALSE(m_column_reader->NextBlock());
}

TEST_F(ColumnReaderTest, GetColumnType) {
    EXPECT_TRUE(m_column_reader->Open(m_column_filename, 0, m_buffer_len));

    ColumnType type;
    EXPECT_TRUE(m_column_reader->GetColumnType(&type));

    EXPECT_EQ(type, m_column_type);
}

TEST_F(ColumnReaderTest, GetColumnName) {
    EXPECT_TRUE(m_column_reader->Open(m_column_filename, 0, m_buffer_len));

    std::string name;
    EXPECT_TRUE(m_column_reader->GetColumnName(&name));

    EXPECT_EQ(name, m_column_name);
}

}  // namespace io
}  // namespace gunir
