// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include <vector>

#include "toft/base/scoped_ptr.h"
#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"
#include "thirdparty/gmock/gmock.h"
#include "thirdparty/gtest/gtest.h"
#include "thirdparty/protobuf/descriptor.h"
#include "thirdparty/protobuf/dynamic_message.h"
#include "thirdparty/protobuf/message.h"

#include "gunir/io/column_metadata.pb.h"
#include "gunir/utils/proto_message.h"

namespace gunir {

class ProtoMessageTest : public ::testing::Test {
};

TEST_F(ProtoMessageTest, CreateMessageByNotExistProtoFile) {
    ProtoMessage proto_message;
    EXPECT_FALSE(proto_message.CreateMessageByProtoFile(
            "not_exist_file", "message"));
}

TEST_F(ProtoMessageTest, CreateMessageByExistProtoFileNotExistMessage) {
    ProtoMessage proto_message;
    EXPECT_FALSE(proto_message.CreateMessageByProtoFile(
            "proto_message_test.proto", "not_exist_message"));
}

TEST_F(ProtoMessageTest, CreateMessageByProtoFileSucceed) {
    ProtoMessage proto_message;
    EXPECT_TRUE(proto_message.CreateMessageByProtoFile(
            "testdata/proto_message_test.proto", "TestMessage"));

    const google::protobuf::Message* msg = proto_message.GetMessage();
    ASSERT_TRUE(NULL != msg) << "Create Message Failed";
    EXPECT_EQ(msg->GetTypeName(), "TestMessage");

    const google::protobuf::Descriptor* descriptor = msg->GetDescriptor();
    ASSERT_FALSE(NULL == descriptor);
    const google::protobuf::FieldDescriptor* text_field
        = descriptor->FindFieldByName("test_string");
    ASSERT_FALSE(NULL == text_field);
}

// proto file can import other proto files
TEST_F(ProtoMessageTest, CreateMessageByProtoFileImportNotExistFile) {
    ProtoMessage proto_message;
    EXPECT_FALSE(proto_message.CreateMessageByProtoFile(
            "testdata/proto_message_include_bad_file_test.proto",
            "TestMessage"));
}

TEST_F(ProtoMessageTest, CreateMessageByProtoFileImportFileSucceed) {
    ProtoMessage proto_message;
    EXPECT_TRUE(proto_message.CreateMessageByProtoFile(
            "testdata/proto_message_include_file_test.proto", "TestMessage2"));

    const google::protobuf::Message* msg = proto_message.GetMessage();
    ASSERT_TRUE(NULL != msg);
    EXPECT_EQ(msg->GetTypeName(), "TestMessage2");

    const google::protobuf::Descriptor* descriptor = msg->GetDescriptor();
    ASSERT_FALSE(NULL == descriptor);
    const google::protobuf::FieldDescriptor* text_field
        = descriptor->FindFieldByName("test_string");
    ASSERT_FALSE(NULL == text_field);
}

TEST_F(ProtoMessageTest, CreateMessageByFileDescriptorStringSucceed) {
    ProtoMessage proto_message;
    EXPECT_TRUE(proto_message.CreateMessageByProtoFile(
            "testdata/proto_message_include_file_test.proto", "TestMessage2"));

    std::string des_string = proto_message.GetFileDescriptorSetString();

    ProtoMessage des_message;
    EXPECT_TRUE(des_message.CreateMessageByFileDescriptorSet(
            des_string, "TestMessage2"));

    const google::protobuf::Message* msg = des_message.GetMessage();
    ASSERT_TRUE(NULL != msg);
    EXPECT_EQ(msg->GetTypeName(), "TestMessage2");

    const google::protobuf::Descriptor* descriptor = msg->GetDescriptor();
    ASSERT_FALSE(NULL == descriptor);
    const google::protobuf::FieldDescriptor* text_field
        = descriptor->FindFieldByName("test_string");
    ASSERT_FALSE(NULL == text_field);
}

TEST_F(ProtoMessageTest, GetSchemaColumnStat) {
    ProtoMessage proto_message;
    EXPECT_TRUE(proto_message.CreateMessageByProtoFile(
            "testdata/proto_message_include_file_test.proto", "TestMessage2"));

    std::string des_string = proto_message.GetFileDescriptorSetString();

    ProtoMessage des_message;
    EXPECT_TRUE(des_message.CreateMessageByFileDescriptorSet(
            des_string, "TestMessage2"));
    std::vector<io::ColumnStaticInfo> column_stats;
    EXPECT_TRUE(des_message.GetSchemaColumnStat(&column_stats));
    LOG(INFO) << "SchemaColumnStat size : " << column_stats.size();
    for (uint32_t i = 0; i < column_stats.size(); i++) {
         LOG(INFO) << "column info: " << column_stats[i].ShortDebugString();
    }
}
}  // namespace gunir
