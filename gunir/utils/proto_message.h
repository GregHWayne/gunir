// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//      ProtoMessage is used to create Message Object by Proto Descript Files
//      Usage :
//          ProtoMessage proto_message;
//          proto_message.CreateMessageByProtoFile("test.proto", "TestType");
//          Message* message = proto_message.GetMessage();
//
//      message is a object of class TestType which is defined in test.proto

#ifndef  GUNIR_UTILS_PROTO_MESSAGE_H
#define  GUNIR_UTILS_PROTO_MESSAGE_H

#include <map>
#include <string>
#include <vector>

#include "toft/base/scoped_ptr.h"
#include "thirdparty/protobuf/compiler/importer.h"
#include "thirdparty/protobuf/descriptor.h"
#include "thirdparty/protobuf/dynamic_message.h"
#include "thirdparty/protobuf/message.h"

#include "gunir/io/column_metadata.pb.h"


namespace gunir {
class ProtoMessage {
private:
    typedef google::protobuf::Descriptor Descriptor;
    typedef google::protobuf::DynamicMessageFactory DynamicMessageFactory;
    typedef google::protobuf::FileDescriptor FileDescriptor;
    typedef google::protobuf::FieldDescriptor FieldDescriptor;
    typedef google::protobuf::DescriptorPool DescriptorPool;
    typedef google::protobuf::Message Message;
    typedef google::protobuf::compiler::DiskSourceTree DiskSourceTree;
    typedef google::protobuf::compiler::Importer Importer;
    typedef google::protobuf::SimpleDescriptorDatabase Database;

public:
    ProtoMessage();
    ~ProtoMessage();

    bool CreateMessageByProtoFile(const std::string& proto_file,
                                  const std::string& msg_name);
    bool CreateMessageByFileDescriptorSet(
        const std::string& descriptor_string,
        const std::string& msg_name);

    bool GetSchemaColumnStat(std::vector<io::ColumnStaticInfo>* column_stats);

    // return the same prototype pointer for multi-call,
    // use GetMessage()->New() to get your own mutable object
    const google::protobuf::Message* GetMessage() const;
    const google::protobuf::Descriptor* GetDescriptor() const;
    std::string GetFileDescriptorSetString() const;
    static void GenerateDescriptorString(
        const FileDescriptor* file_descriptor,
        std::string* descriptor_string);

private:
    bool GenerateMessage(const std::string& msg_name);

    static void GetAllFileDescriptor(
        std::map<std::string, const FileDescriptor*>* file_map,
        const FileDescriptor* file_desc);

    void ParseColumn(const Descriptor* descriptor,
                     uint32_t rlevel, uint32_t dlevel,
                     const std::string& path,
                     std::vector<io::ColumnStaticInfo>* column_stats);
    void SetColumnType(const FieldDescriptor& field,
                       io::ColumnStaticInfo* column);

private:
    toft::scoped_ptr<DiskSourceTree>          m_source_tree;
    toft::scoped_ptr<Importer>                m_importer;
    toft::scoped_ptr<Database>                m_data_base;
    toft::scoped_ptr<DescriptorPool>          m_des_pool;
    toft::scoped_ptr<DynamicMessageFactory>   m_factory;

    const Message*                      m_message;
    const FileDescriptor*               m_file_descriptor;
    const Descriptor*                   m_descriptor;
    std::string                         m_descriptor_string;
};
}  // namespace gunir

#endif  // GUNIR_UTILS_PROTO_MESSAGE_H
