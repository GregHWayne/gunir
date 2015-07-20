// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//

#include <algorithm>
#include <set>
#include <string>

#include "toft/base/scoped_ptr.h"
#include "toft/base/string/algorithm.h"
#include "toft/base/string/string_number.h"
#include "toft/storage/file/file.h"
#include "toft/storage/file/recordio/recordio.h"
#include "toft/system/time/stopwatch.h"
#include "thirdparty/glog/logging.h"
#include "thirdparty/gtest/gtest.h"

#include "gunir/compiler/parser/select_stmt.pb.h"
#include "gunir/io/io_test_helper.h"
#include "gunir/io/pb_assembler.h"
#include "gunir/io/table_builder.cc"
#include "gunir/io/table_options.pb.h"
#include "gunir/io/tablet_reader.h"
#include "gunir/io/tablet_schema.pb.h"
#include "gunir/io/tablet_writer.h"
#include "gunir/io/testdata/document.pb.h"
#include "gunir/utils/create_table_tool.h"
#include "gunir/utils/filename_tool.h"

DEFINE_int64(pb_record_number, 1000, "record number to generate PB data");
DECLARE_string(gunir_create_table_tool);
DECLARE_string(identity);
DECLARE_string(role);

namespace gunir {
namespace io {

void ConstructRandomRecordIOFile(const std::string& file_name) {
    scoped_ptr<File> file;
    file.reset(File::Open(file_name.c_str(), File::ENUM_FILE_OPEN_MODE_W));
    CHECK_NOTNULL(file.get());

    scoped_ptr<RecordWriter> writer;
    writer.reset(new RecordWriter(file.get()));
    CHECK_NOTNULL(writer.get());

    IOTestHelper io_test_helper;
    for (int32_t i = 0; i < FLAGS_pb_record_number; ++i) {
        EXPECT_TRUE(writer->WriteMessage(io_test_helper.RandDocument()));
    }

    EXPECT_TRUE(writer->Flush());
    EXPECT_EQ(0, file->Close());
}

Stopwatch assemble_stop_watch(false);
int64_t assemble_consume_time = 0;

void AssembleTabletFile(const std::string& file_name,
                        const std::vector<std::string>& tablets) {
    assemble_stop_watch.Reset();
    assemble_stop_watch.Start();

    scoped_ptr<File> file_assemble;
    file_assemble.reset(File::Open(file_name.c_str(),
                                   File::ENUM_FILE_OPEN_MODE_W));
    CHECK_NOTNULL(file_assemble.get());

    scoped_ptr<RecordWriter> writer_assemble;
    writer_assemble.reset(new RecordWriter(file_assemble.get()));
    CHECK_NOTNULL(writer_assemble.get());

    MemPool m_mempool(MemPool::MAX_UNIT_SIZE);
    for (uint32_t i = 0; i < tablets.size(); ++i) {
        TabletReader reader(&m_mempool);
        EXPECT_TRUE(reader.Init(tablets[i]));

        PbRecordAssembler assembler;
        CHECK(assembler.Init(&reader));

        scoped_ptr<protobuf::Message> message;
        message.reset(assembler.GetProtoMessage()->New());

        while (assembler.AssembleRecord(message.get())) {
            EXPECT_TRUE(writer_assemble->WriteMessage(*(message.get())));
        }

        reader.Close();
    }

    EXPECT_TRUE(writer_assemble->Flush());
    EXPECT_EQ(0, file_assemble->Close());

    assemble_stop_watch.Stop();
    assemble_consume_time = assemble_stop_watch.ElapsedMilliSeconds();
}

void ReadMessages(const std::string& filename,
                  std::set<std::string>* messages) {
    scoped_ptr<File> file;
    file.reset(File::Open(filename.c_str(), File::ENUM_FILE_OPEN_MODE_R));
    scoped_ptr<RecordReader> reader;
    reader.reset(new RecordReader(file.get()));

    ProtoMessage proto_message;
    EXPECT_TRUE(proto_message.CreateMessageByProtoFile(
            "./testdata/document.proto", "Document"));
    scoped_ptr<protobuf::Message> message;
    message.reset(proto_message.GetMessage()->New());

    while (reader->ReadMessage(message.get())) {
        messages->insert(message->ShortDebugString());
    }
    file->Close();
}

void CheckFiles(const std::string& file1, const std::string& file2) {
    std::set<std::string> messages1;
    ReadMessages(file1, &messages1);
    std::set<std::string> messages2;
    ReadMessages(file2, &messages2);
    EXPECT_EQ(messages1.size(), messages2.size());
    std::set<std::string>::iterator iter1;
    std::set<std::string>::iterator iter2;
    for (iter1 = messages1.begin(), iter2 = messages2.begin();
         iter1 != messages1.end() && iter2 != messages2.end();
         ++iter1, ++iter2) {
        EXPECT_EQ(*iter1, *iter2);
    }
}

class IOTest : public ::testing::Test {
};

TEST_F(IOTest, IODataOperationValidate) {
    // Construct pb data of recordio format
    ConstructRandomRecordIOFile("./random.document.recordio.msg.1");

    LOG(INFO) << "[IO Statistics] recordio file szie: "
        << File::GetSize("./random.document.recordio.msg.1") << " byte.";

    // Construct tablet file
    TableOptions opt;
    opt.add_input_files();
    opt.mutable_input_files(0)->set_uri("./random.document.recordio.msg.1");

    opt.mutable_schema_descriptor()->set_type("PB");

    ProtoMessage proto_message;
    EXPECT_TRUE(proto_message.CreateMessageByProtoFile(
            "./testdata/document.proto", "Document"));

    opt.mutable_schema_descriptor()->set_description(
        proto_message.GetFileDescriptorSetString());

    opt.mutable_schema_descriptor()->set_record_name("Document");

    std::string tablet_file = "./tablet_file.tablet";
    opt.set_output_file(tablet_file);
    opt.set_output_table("Document");

    Stopwatch create_tablet_stop_watch(true);

    TableBuilder builder;
    std::vector<std::string> output_files;
    EXPECT_TRUE(builder.CreateTable(opt, &output_files));

    create_tablet_stop_watch.Stop();
    LOG(INFO) << "[IO Statistics] create tablet file consume time: "
        << create_tablet_stop_watch.ElapsedMilliSeconds() << " ms.";

    uint64_t total_file_size = 0;
    for (uint32_t i = 0; i < output_files.size(); ++i) {
        total_file_size += File::GetSize(output_files[i].c_str());
    }
    LOG(INFO) << "[IO Statistics] tablet file szie: "
        << total_file_size << " byte.";
    LOG(INFO) << "[IO Statistics] tablet file generate speed: "
        << static_cast<double>(total_file_size) /
        create_tablet_stop_watch.ElapsedMilliSeconds() << " KB/second.";

    // Assemble
    AssembleTabletFile("./assemble.document.recordio.msg.1", output_files);

    LOG(INFO) << "[IO Statistics] assemble consume time: "
        << assemble_stop_watch.ElapsedMilliSeconds() << " ms.";
    LOG(INFO) << "[IO Statistics] assemble speed: "
        << static_cast<double>(total_file_size) /
        assemble_stop_watch.ElapsedMilliSeconds() << " KB/second.";

    // Cheche the data consistency
    CheckFiles("./random.document.recordio.msg.1",
               "./assemble.document.recordio.msg.1");
}

bool FileNameCompare(const std::string& name1, const std::string& name2) {
    std::vector<std::string> name_1_vector;
    std::vector<std::string> name_1_result;
    SplitString(name1, "/", &name_1_vector);
    if (name_1_vector.size() > 1) {
        SplitString(name_1_vector[name_1_vector.size() - 1],
                    "_", &name_1_result);
    } else {
        SplitString(name1, "_", &name_1_result);
    }

    std::vector<std::string> name_2_vector;
    std::vector<std::string> name_2_result;
    SplitString(name2, "/", &name_2_vector);
    if (name_2_vector.size() > 1) {
        SplitString(name_2_vector[name_2_vector.size() - 1],
                    "_", &name_2_result);
    } else {
        SplitString(name2, "_", &name_2_result);
    }

    EXPECT_EQ(static_cast<uint32_t>(3), name_1_result.size());
    EXPECT_EQ(static_cast<uint32_t>(3), name_2_result.size());

    int name_1_1 = 0;
    StringToNumber(name_1_result[1], &name_1_1, 10);
    int name_1_2 = 0;
    StringToNumber(name_1_result[2], &name_1_2, 10);

    int name_2_1 = 0;
    StringToNumber(name_2_result[1], &name_2_1, 10);
    int name_2_2 = 0;
    StringToNumber(name_2_result[2], &name_2_2, 10);

    if (name_1_1 < name_2_1) {
        return true;
    } else if (name_1_1 == name_2_1) {
        if (name_1_2 < name_2_2) {
            return true;
        }
    }

    return false;
}

TEST_F(IOTest, MapReduceOperationValidate) {
    FLAGS_gunir_create_table_tool = "../../../utils/create_table_mapreduce";
    // Construct pb data of recordio format
    ConstructRandomRecordIOFile("./random.document.recordio.msg.2");

    LOG(INFO) << "[IO Statistics] recordio file szie: "
        << File::GetSize("./random.document.recordio.msg.2") << " byte.";

    // Construct MapReduce Job
    ProtoMessage proto_message;
    EXPECT_TRUE(proto_message.CreateMessageByProtoFile(
            "./testdata/document.proto", "Document"));

    compiler::CreateTableStmt create_table_stmt;
    create_table_stmt.mutable_table_name()->set_char_string("Document");
    create_table_stmt.mutable_input_path()->set_char_string(
        "./random.document.recordio.msg.2");
    create_table_stmt.mutable_table_schema()->set_char_string(
        proto_message.GetFileDescriptorSetString());
    create_table_stmt.mutable_message_name()->set_char_string("Document");
    create_table_stmt.mutable_table_type()->set_char_string("PB");


    std::string tablet_file_prefix = "./mapreduce_tablet_file/";

    Stopwatch create_tablet_stop_watch(true);

    FLAGS_identity = "anthonyqin";
    FLAGS_role = "anthonyqin";

    CreateTableTool create_table(&create_table_stmt);
    EXPECT_TRUE(create_table.LocalRun(tablet_file_prefix));

    create_tablet_stop_watch.Stop();
    LOG(INFO) << "[IO Statistics] create tablet file consume time: "
        << create_tablet_stop_watch.ElapsedMilliSeconds() << " ms.";

    const std::string file_pattern = tablet_file_prefix + "*";
    std::vector<std::string> output_files;
    int64_t total_size = 0;
    EXPECT_TRUE(GetFilesByPattern(file_pattern.c_str(),
                                  &output_files, &total_size));

    sort(output_files.begin(), output_files.end(), FileNameCompare);

    LOG(INFO) << "[IO Statistics] tablet file szie: "
        << total_size << " byte.";
    LOG(INFO) << "[IO Statistics] tablet file generate speed: "
        << static_cast<double>(total_size) /
        create_tablet_stop_watch.ElapsedMilliSeconds() << " KB/second.";

    // Assemble
    AssembleTabletFile("./assemble.document.recordio.msg.2", output_files);

    LOG(INFO) << "[IO Statistics] assemble consume time: "
        << assemble_stop_watch.ElapsedMilliSeconds() << " ms.";
    LOG(INFO) << "[IO Statistics] assemble speed: "
        << static_cast<double>(total_size) /
        assemble_stop_watch.ElapsedMilliSeconds() << " KB/second.";

    CheckFiles("./random.document.recordio.msg.2",
               "./assemble.document.recordio.msg.2");
}
} // namespace io
} // namespace gunir
