// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Created 03/21/12

#include "gunir/io/pb_assembler.h"

#include <iostream>
#include <string>
#include <vector>

#include "toft/storage/file/file.h"
#include "toft/storage/recordio/recordio.h"
#include "thirdparty/glog/logging.h"
#include "thirdparty/gtest/gtest.h"

#include "gunir/io/pb_dissector.h"
#include "gunir/io/slice.h"
#include "gunir/io/table_builder.h"
#include "gunir/io/tablet_reader.h"
#include "gunir/io/test_data.h"

using namespace google;
using namespace toft;

namespace gunir {
namespace io {

const int kRecordNumber = 1000;

class AssemblerTest : public ::testing::Test {
public:
    AssemblerTest() {}

    void InitTestData(const std::string& proto_file,
                      const std::string& message_name,
                      TestData* data_ptr,
                      ProtoMessage* proto_message,
                      std::vector<std::string>* output_files) {
        proto_message->CreateMessageByProtoFile(proto_file, message_name);

        TestData& data = *data_ptr;
        data.Reset(proto_message->GetMessage());
        data.GenerateMessages(kRecordNumber);
        EXPECT_EQ(kRecordNumber, data.size());

        std::string input_file = message_name + ".msg";
        File* file = File::Open(input_file.c_str(),
                                "w");
        RecordWriter* writer = new RecordWriter(file);
        for (int i = 0; i < data.size(); i++) {
            writer->WriteMessage(data[i]);
        }

//         writer->Flush();
        delete writer;
        file->Close();
        delete file;

        TableOptions opt;
        opt.add_input_files();
        opt.mutable_input_files(0)->set_uri(input_file);
        opt.mutable_schema_descriptor()->set_type("PB");
        opt.mutable_schema_descriptor()->set_description(
            proto_message->GetFileDescriptorSetString());
        opt.mutable_schema_descriptor()->set_record_name(message_name);
        opt.set_output_file(message_name + ".col");
        opt.set_output_table(message_name);

        TableBuilder builder;
        builder.CreateTable(opt, output_files);
    }
};

TEST_F(AssemblerTest, AllTypes) {
    ProtoMessage proto_message;
    TestData data;
    std::vector<std::string> output_files;
    InitTestData("testdata/all_types.proto", "AllTypes",
                 &data, &proto_message, &output_files);

    int index = 0;
    for (uint32_t i = 0; i < output_files.size(); i++) {
        MemPool mempool(MemPool::MAX_UNIT_SIZE);
        TabletReader reader(&mempool);
        EXPECT_TRUE(reader.Init(output_files[i]));

        PbRecordAssembler assembler;
        EXPECT_TRUE(assembler.Init(&reader));

        scoped_ptr<protobuf::Message> message;
        message.reset(assembler.GetProtoMessage()->New());
        while (assembler.AssembleRecord(message.get())) {
            EXPECT_TRUE(message->IsInitialized());
            EXPECT_EQ(data[index].DebugString(), message->DebugString());
            index++;
        }
        reader.Close();
    }
    EXPECT_EQ(data.size(), index);
}

TEST_F(AssemblerTest, TabletAssembling) {
    ProtoMessage proto_message;
    TestData data;
    std::vector<std::string> output_files;
    InitTestData("testdata/document.proto", "Document",
                 &data, &proto_message, &output_files);

    int index = 0;
    for (uint32_t i = 0; i < output_files.size(); i++) {
        MemPool mempool(MemPool::MAX_UNIT_SIZE);
        TabletReader reader(&mempool);
        EXPECT_TRUE(reader.Init(output_files[i]));

        PbRecordAssembler assembler;
        EXPECT_TRUE(assembler.Init(&reader));

        scoped_ptr<protobuf::Message> message;
        message.reset(assembler.GetProtoMessage()->New());
        while (assembler.AssembleRecord(message.get())) {
            EXPECT_TRUE(message->IsInitialized());
            EXPECT_EQ(data[index].DebugString(), message->DebugString()) << index;
            index++;
        }
        reader.Close();
    }
    EXPECT_EQ(data.size(), index);
}

TEST_F(AssemblerTest, SliceAssembling) {
    ProtoMessage proto_message;
    TestData data;
    std::vector<std::string> output_files;
    InitTestData("testdata/document.proto", "Document",
                 &data, &proto_message, &output_files);

    int index = 0;
    for (uint32_t i = 0; i < output_files.size(); i++) {
        MemPool mempool(MemPool::MAX_UNIT_SIZE);
        TabletReader reader(&mempool);
        EXPECT_TRUE(reader.Init(output_files[i]));

        std::vector<Slice> slices;
        while (reader.Next()) {
            slices.push_back(*reader.GetSlice());
        }
        SliceColumnReader slice_column_reader(slices);

        TabletSchema tablet_schema;
        reader.GetTabletSchema(&tablet_schema);

        PbRecordAssembler assembler;
        EXPECT_TRUE(assembler.Init(&slice_column_reader,
                                   tablet_schema.schema_descriptor()));

        scoped_ptr<protobuf::Message> message;
        message.reset(assembler.GetProtoMessage()->New());
        while (assembler.AssembleRecord(message.get())) {
            EXPECT_TRUE(message->IsInitialized());
            EXPECT_EQ(data[index].DebugString(), message->DebugString());
            index++;
        }
        reader.Close();
    }
    EXPECT_EQ(data.size(), index);
}

}  // namespace io
}  // namespace gunir
