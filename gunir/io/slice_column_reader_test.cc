// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//

#include "gunir/io/slice_column_reader.h"

#include <iostream>
#include <string>

#include "toft/storage/file/file.h"
#include "toft/storage/recordio/recordio.h"
#include "thirdparty/glog/logging.h"
#include "thirdparty/gtest/gtest.h"

#include "gunir/io/slice.h"
#include "gunir/io/table_builder.h"
#include "gunir/io/tablet_reader.h"
#include "gunir/io/test_data.h"
#include "gunir/utils/proto_message.h"

namespace gunir {
namespace io {

const int kRecordNumber = 1000;

class SliceColumnReaderTest : public ::testing::Test {
public:
    SliceColumnReaderTest() {}
};

TEST_F(SliceColumnReaderTest, Reading) {
    ProtoMessage proto_message;
    proto_message.CreateMessageByProtoFile("testdata/document.proto",
                                           "Document");
    TestData data;
    data.Reset(proto_message.GetMessage());
    data.GenerateMessages(kRecordNumber);

    File* file = File::Open("document.msg", "w");
    RecordWriter* writer = new RecordWriter(file);
    for (int i = 0; i < data.size(); i++) {
        writer->WriteMessage(data[i]);
    }

//     writer->Flush();
    delete writer;
    file->Close();
    delete file;

    TableOptions opt;
    opt.add_input_files();
    opt.mutable_input_files(0)->set_uri("document.msg");
    opt.mutable_schema_descriptor()->set_type("PB");

    opt.mutable_schema_descriptor()->set_description(
        proto_message.GetFileDescriptorSetString());

    opt.mutable_schema_descriptor()->set_record_name("Document");
    opt.set_output_file("document.col");
    opt.set_output_table("Document");

    TableBuilder builder;
    std::vector<std::string> output_files;
    builder.CreateTable(opt, &output_files);

    std::vector<Slice> slices;
    for (uint32_t i = 0; i < output_files.size(); i++) {
        MemPool mempool(MemPool::MAX_UNIT_SIZE);
        TabletReader reader(&mempool);
        EXPECT_TRUE(reader.Init(output_files[i]));
        while (reader.Next()) {
            slices.push_back(*reader.GetSlice());
        }
        reader.Close();
    }

    SliceColumnReader reader(slices);
    Block block;
    for (uint32_t i = 0; i < slices[0].GetCount(); i++) {
        while (reader.ReadColumn(i, &block)) {
        }
    }
}

} // namespace io
} // namespace gunir
