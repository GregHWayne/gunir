// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//

#include <string>
#include "toft/storage/file/file.h"
#include "thirdparty/gtest/gtest.h"

#include "gunir/io/io_test_helper.h"
#include "gunir/io/slice.h"
#include "gunir/io/slice_writer.h"
#include "gunir/io/tablet_reader.h"
#include "gunir/io/tablet_schema.pb.h"

namespace gunir {
namespace io {

class SliceReadWriteTest : public ::testing::Test {
protected:
    virtual void SetUp() {
        m_mempool.reset(new MemPool(MemPool::MAX_UNIT_SIZE));
        m_tablet_file_name_from_slice_for_tablet_io =
            "m_tablet_file_name_from_slice_for_tablet_io.tablet";
        m_tablet_file_name_from_slice_for_slice_io =
            "m_tablet_file_name_from_slice_for_slice_io.tablet";
        m_table_name = "Document";

        IOTestHelper helper;
        helper.BuildTabletTestFile(m_table_name, &m_output_files);
    }
    virtual void TearDown() {
    }

    bool IsRecordStartSlice(const Slice& slice) {
        for (uint32_t i = 0; i < slice.GetCount(); ++i) {
            if (!slice.HasBlock(i)) {
                return false;
            }
            if (slice.GetBlock(i)->GetRepLevel() != 0) {
                return false;
            }
        }
        return true;
    }

protected:
    scoped_ptr<MemPool> m_mempool;
    scoped_ptr<TabletReader> m_tablet_reader;
    scoped_ptr<SliceWriter> m_slice_writer;
    std::vector<std::string> m_output_files;
    std::string m_tablet_file_name_from_slice_for_tablet_io;
    std::string m_tablet_file_name_from_slice_for_slice_io;
    std::string m_table_name;
};

TEST_F(SliceReadWriteTest, SliceTestWithSliceIO) {
    for (uint32_t i = 0; i < m_output_files.size(); ++i) {
        m_tablet_reader.reset(new TabletReader(m_mempool.get()));
        EXPECT_TRUE(m_tablet_reader->Init(m_output_files[i]));

        TabletSchema tablet_schema;
        m_tablet_reader->GetTabletSchema(&tablet_schema);

        m_slice_writer.reset(new SliceWriter());
        EXPECT_TRUE(m_slice_writer->Open(
                m_table_name, tablet_schema.schema_descriptor(),
                m_tablet_file_name_from_slice_for_slice_io));

        uint32_t count = 6;
        std::vector<Slice> slice_vec;
        while (m_tablet_reader->Next()) {
            Slice *slice = m_tablet_reader->GetSlice();
            EXPECT_EQ(slice->GetCount(), count);
            if (IsRecordStartSlice(*slice)) {
                EXPECT_TRUE(m_slice_writer->Write(slice_vec));
                slice_vec.clear();
            }
            slice_vec.push_back(Slice(*slice));
        }
        EXPECT_TRUE(m_slice_writer->Write(slice_vec));
        slice_vec.clear();

        EXPECT_TRUE(m_tablet_reader->Close());

        std::vector<std::string> output_files;
        EXPECT_TRUE(m_slice_writer->Close(&output_files));

        uint32_t file_count = 1;
        EXPECT_EQ(output_files.size(), file_count);

        for (uint32_t i = 0; i < output_files.size(); ++i) {
            uint32_t digest_1 = 0;
            uint32_t digest_2 = 0;
            EXPECT_TRUE(File::GetDigest(output_files[i].c_str(), &digest_1));
            EXPECT_TRUE(File::GetDigest(m_output_files[i].c_str(), &digest_2));
            EXPECT_EQ(digest_1, digest_2);
        }
    }
}

} // namespace io
} // namespace gunir
