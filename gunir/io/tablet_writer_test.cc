// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//

#include <vector>
#include "thirdparty/glog/logging.h"
#include "thirdparty/gtest/gtest.h"

#include "gunir/io/block.h"
#include "gunir/io/column_metadata.pb.h"
#include "gunir/io/tablet_schema.pb.h"
#include "gunir/io/tablet_writer.h"

namespace gunir {
namespace io {

class TabletWriterTest : public ::testing::Test {
protected:
    virtual void SetUp() {
        m_tablet_writer = new TabletWriter();

        m_table_name = "table_name";

        m_sd.set_type("pb");
        m_sd.set_description("null");
        m_sd.set_record_name("message_name");

        ColumnStaticInfo csc1;
        csc1.set_column_index(0);
        csc1.set_column_name("col_1");
        csc1.set_column_type(TYPE_INT32);
        csc1.set_max_repetition_level(3);
        csc1.set_max_definition_level(3);

        ColumnStaticInfo csc2;
        csc2.set_column_index(1);
        csc2.set_column_name("col_1");
        csc2.set_column_type(TYPE_INT32);
        csc2.set_max_repetition_level(3);
        csc2.set_max_definition_level(3);

        m_csis.push_back(csc1);
        m_csis.push_back(csc2);

        m_file_name = "test_output_file";
    }
    virtual void TearDown() {
        delete m_tablet_writer;
    }

protected:
    TabletWriter *m_tablet_writer;
    SchemaDescriptor m_sd;
    std::string m_table_name;
    std::vector<ColumnStaticInfo> m_csis;
    std::string m_file_name;
};

TEST_F(TabletWriterTest, BuildTabletSchema) {
    EXPECT_TRUE(m_tablet_writer->BuildTabletSchema(m_table_name, m_sd, m_csis));
}

TEST_F(TabletWriterTest, Open) {
    EXPECT_TRUE(m_tablet_writer->BuildTabletSchema(m_table_name, m_sd, m_csis));
    EXPECT_TRUE(m_tablet_writer->Open(m_file_name));
    m_tablet_writer->Close();
}

TEST_F(TabletWriterTest, Close) {
    EXPECT_TRUE(m_tablet_writer->BuildTabletSchema(m_table_name, m_sd, m_csis));
    EXPECT_TRUE(m_tablet_writer->Open(m_file_name));
    EXPECT_TRUE(m_tablet_writer->Close());
}

TEST_F(TabletWriterTest, WriteBlockVector) {
    EXPECT_TRUE(m_tablet_writer->BuildTabletSchema(m_table_name, m_sd, m_csis));
    EXPECT_TRUE(m_tablet_writer->Open(m_file_name));
    char buffer[200];
    uint32_t length = 200;
    m_tablet_writer->SetBuffer(buffer, length);

    std::vector<const Block*> blocks;
    std::vector<uint32_t> indexes;
    for (uint32_t i = 0; i < 3; ++i) {
        Block *bobj = new Block();
        bobj->SetRepLevel(i);
        bobj->SetDefLevel(i);
        StringPiece sp;
        sp.set(&i, sizeof(i));
        bobj->SetValueType(Block::TYPE_UINT32);
        bobj->SetValue(sp);

        blocks.push_back(bobj);
        indexes.push_back(i);
    }
    EXPECT_FALSE(m_tablet_writer->Write(blocks, indexes));

    indexes[2] = 0;
    EXPECT_TRUE(m_tablet_writer->Write(blocks, indexes));
    EXPECT_TRUE(m_tablet_writer->Close());

    for (uint32_t i = 0; i < blocks.size(); ++i) {
        delete blocks[i];
    }
}

TEST_F(TabletWriterTest, GetOutputFileList) {
    EXPECT_TRUE(m_tablet_writer->BuildTabletSchema(m_table_name, m_sd, m_csis));
    EXPECT_TRUE(m_tablet_writer->Open(m_file_name));
    char buffer[200];
    uint32_t length = 200;
    m_tablet_writer->SetBuffer(buffer, length);

    std::vector<const Block*> blocks;
    std::vector<uint32_t> indexes;
    for (uint32_t i = 0; i < 10; ++i) {
        Block *bobj = new Block();
        bobj->SetRepLevel(i);
        bobj->SetDefLevel(i);
        StringPiece sp;
        sp.set(&i, sizeof(i));
        bobj->SetValueType(Block::TYPE_UINT32);
        bobj->SetValue(sp);

        blocks.push_back(bobj);
        indexes.push_back(i % 2);
    }

    for (int i = 0; i < 100; ++i) {
        EXPECT_TRUE(m_tablet_writer->Write(blocks, indexes));
    }
    EXPECT_TRUE(m_tablet_writer->Close());

    for (uint32_t i = 0; i < blocks.size(); ++i) {
        delete blocks[i];
    }

    std::vector<std::string> names;
    m_tablet_writer->GetOutputFileList(&names);
    for (uint32_t i = 0; i < names.size(); ++i) {
        EXPECT_TRUE(File::Exists(names[i].c_str()));
//         EXPECT_LE(File::GetSize(names[i].c_str()), length);
    }
}

}  // namespace io
}  // namespace gunir
