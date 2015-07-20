// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include "gunir/io/slice_writer.h"

#include "toft/base/scoped_ptr.h"
#include "thirdparty/glog/logging.h"
#include "thirdparty/gtest/gtest.h"

#include "gunir/io/slice.h"
#include "gunir/io/tablet_writer.h"
#include "gunir/utils/proto_message.h"

namespace gunir {
namespace io {

class SliceWriterTest : public ::testing::Test {
protected:
    virtual void SetUp() {
        m_tablet_writer = new TabletWriter();
        m_slice_writer.reset(new SliceWriter());
        m_slice_writer->SetTabletWriter(m_tablet_writer);

        m_table_name = "slice_writer_test_table";
        m_file_name_prefix = "tablet_";

        ProtoMessage proto_message;
        proto_message.CreateMessageByProtoFile("./testdata/document.proto",
                                               "Document");

        m_schema_descriptor.set_type("PB");
        m_schema_descriptor.set_description(
            proto_message.GetFileDescriptorSetString());
        m_schema_descriptor.set_record_name("Document");
    }

protected:
    TabletWriter* m_tablet_writer;
    scoped_ptr<SliceWriter> m_slice_writer;
    scoped_array<char> m_buffer;
    std::string m_table_name;
    SchemaDescriptor m_schema_descriptor;
    std::string m_file_name_prefix;
};

TEST_F(SliceWriterTest, Open) {
    EXPECT_TRUE(m_slice_writer->Open(m_table_name,
                                     m_schema_descriptor,
                                     m_file_name_prefix));
}

TEST_F(SliceWriterTest, OpenFail) {
    m_schema_descriptor.set_record_name("test");
    EXPECT_FALSE(m_slice_writer->Open(m_table_name,
                                      m_schema_descriptor,
                                      m_file_name_prefix));
}

TEST_F(SliceWriterTest, Close) {
    EXPECT_TRUE(m_slice_writer->Open(m_table_name,
                                     m_schema_descriptor,
                                     m_file_name_prefix));
    std::vector<std::string> files;
    EXPECT_TRUE(m_slice_writer->Close(&files));
}

TEST_F(SliceWriterTest, WriteSlices) {
    Slice slice(6);
    slice.SetHasBlock(0);
    slice.SetHasBlock(1);
    slice.SetHasBlock(2);
    slice.SetHasBlock(3);
    slice.SetHasBlock(4);
    slice.SetHasBlock(5);

    StringPiece sp;
    uint64_t value = 1;

    Block *bobj = slice.MutableBlock(0);
    bobj->SetValueType(Block::TYPE_INT64);
    sp.set(&value, sizeof(value));
    bobj->SetValue(sp);

    bobj = slice.MutableBlock(1);
    bobj->SetValueType(Block::TYPE_INT64);
    sp.set(&value, sizeof(value));
    bobj->SetValue(sp);

    bobj = slice.MutableBlock(2);
    bobj->SetValueType(Block::TYPE_INT64);
    sp.set(&value, sizeof(value));
    bobj->SetValue(sp);

    bobj = slice.MutableBlock(3);
    bobj->SetValueType(Block::TYPE_STRING);
    sp.set("code");
    bobj->SetValue(sp);

    bobj = slice.MutableBlock(4);
    bobj->SetValueType(Block::TYPE_STRING);
    sp.set("country");
    bobj->SetValue(sp);

    bobj = slice.MutableBlock(5);
    bobj->SetValueType(Block::TYPE_STRING);
    sp.set("url");
    bobj->SetValue(sp);

    std::vector<Slice> slices;
    slices.push_back(slice);
    slices.push_back(slice);

    EXPECT_TRUE(m_slice_writer->Open(m_table_name,
                                     m_schema_descriptor,
                                     m_file_name_prefix));
    EXPECT_TRUE(m_slice_writer->Write(slices));
}

TEST_F(SliceWriterTest, WriteSlicesFail) {
    Slice slice(6);
    slice.SetHasBlock(0);

    StringPiece sp;
    // The Column Type is not right.
    Block *bobj = slice.MutableBlock(0);
    bobj->SetValueType(Block::TYPE_STRING);
    sp.set("country");
    bobj->SetValue(sp);

    std::vector<Slice> slices;
    slices.push_back(slice);
    slices.push_back(slice);

//     EXPECT_TRUE(m_slice_writer->Open(m_table_name,
//                                      m_schema_descriptor,
//                                      m_file_name_prefix));
    EXPECT_FALSE(m_slice_writer->Write(slices));
}

} // namespace io
} // namespace gunir
