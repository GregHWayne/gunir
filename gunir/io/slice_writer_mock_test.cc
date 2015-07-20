// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include "gunir/io/slice_writer.h"

#include "toft/base/scoped_ptr.h"
#include "thirdparty/glog/logging.h"
#include "thirdparty/gmock/gmock.h"
#include "thirdparty/gtest/gtest.h"

#include "gunir/io/mock_tablet_writer.h"

namespace gunir {
namespace io {

using ::testing::_;
using ::testing::Return;
using ::testing::NiceMock;

class SliceWriterTest : public ::testing::Test {
protected:
    virtual void SetUp() {
        m_tablet_writer = new MockTabletWriter();
        m_slice_writer.reset(new SliceWriter());
        m_slice_writer->SetTabletWriter(m_tablet_writer);

        ON_CALL(*m_tablet_writer, BuildTabletSchema(_, _))
            .WillByDefault(Return(true));
        ON_CALL(*m_tablet_writer, Open(_))
            .WillByDefault(Return(true));
        ON_CALL(*m_tablet_writer, Close())
            .WillByDefault(Return(true));
        ON_CALL(*m_tablet_writer, Write(_, _))
            .WillByDefault(Return(true));
    }

protected:
    MockTabletWriter* m_tablet_writer;
    scoped_ptr<SliceWriter> m_slice_writer;
};

TEST_F(SliceWriterTest, Open) {
    EXPECT_CALL(*m_tablet_writer, BuildTabletSchema(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*m_tablet_writer, Open(_))
        .WillOnce(Return(true));

    SchemaDescriptor schema_descriptor;
    EXPECT_TRUE(m_slice_writer->Open("", schema_descriptor, ""));
}

TEST_F(SliceWriterTest, OpenFail) {
    EXPECT_CALL(*m_tablet_writer, BuildTabletSchema(_, _))
        .WillOnce(Return(false));

    SchemaDescriptor schema_descriptor;
    EXPECT_FALSE(m_slice_writer->Open("", schema_descriptor, ""));
}

TEST_F(SliceWriterTest, OpenFail2) {
    EXPECT_CALL(*m_tablet_writer, BuildTabletSchema(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*m_tablet_writer, Open(_))
        .WillOnce(Return(false));

    SchemaDescriptor schema_descriptor;
    EXPECT_FALSE(m_slice_writer->Open("", schema_descriptor, ""));
}

TEST_F(SliceWriterTest, OpenFail3) {
    EXPECT_CALL(*m_tablet_writer, BuildTabletSchema(_, _))
        .WillOnce(Return(false));

    SchemaDescriptor schema_descriptor;
    EXPECT_FALSE(m_slice_writer->Open("", schema_descriptor, ""));
}

TEST_F(SliceWriterTest, Close) {
    EXPECT_CALL(*m_tablet_writer, Close())
        .WillOnce(Return(true));

    std::vector<std::string> files;
    EXPECT_TRUE(m_slice_writer->Close(&files));
}

TEST_F(SliceWriterTest, CloseFail) {
    EXPECT_CALL(*m_tablet_writer, Close())
        .WillOnce(Return(false));

    std::vector<std::string> files;
    EXPECT_FALSE(m_slice_writer->Close(&files));
}

TEST_F(SliceWriterTest, WriteSlices) {
    EXPECT_CALL(*m_tablet_writer, Write(_, _)).WillOnce(Return(true));

    Slice slice(1);
    std::vector<Slice> slices;
    slices.push_back(slice);
    slices.push_back(slice);
    EXPECT_TRUE(m_slice_writer->Write(slices));
}

TEST_F(SliceWriterTest, WriteSlicesFail) {
    EXPECT_CALL(*m_tablet_writer, Write(_, _)).WillOnce(Return(false));

    Slice slice(1);
    std::vector<Slice> slices;
    slices.push_back(slice);
    slices.push_back(slice);
    EXPECT_FALSE(m_slice_writer->Write(slices));
}

} // namespace io
} // namespace gunir
