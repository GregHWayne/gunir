// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_PARSER_TABLE_SCHEMA_H
#define  GUNIR_COMPILER_PARSER_TABLE_SCHEMA_H

#include <string>
#include <vector>

#include "toft/base/scoped_ptr.h"
#include "thirdparty/protobuf/compiler/importer.h"

#include "gunir/compiler/parser/big_query_types.h"
#include "gunir/compiler/parser/column_info.h"

namespace gunir {
class ProtoMessage;
namespace compiler {

class TableSchema {
private:
    typedef ::google::protobuf::FieldDescriptor PBFieldDescriptor;
    typedef ::google::protobuf::Descriptor PBDescriptor;

public:
    TableSchema();
    ~TableSchema();

    bool InitSchemaFromProtoFile(const std::string& proto_file,
                                 const std::string& msg_name);

    bool InitSchemaFromFileDescriptorProto(
        const std::string& schema_string, const std::string& msg_name);

    bool GetFieldType(const std::vector<std::string>& path, BQType* type) const;

    bool GetFieldLabel(
        const std::vector<std::string>& path, PBLabel* label) const;

    bool GetColumnInfo(
        const std::vector<std::string>& path, ColumnInfo* info) const;

    bool GetRequiredFieldPath(std::vector<std::string>* path_list) const;

    const PBFieldDescriptor* GetField(
        const std::vector<std::string>& path) const;
    const PBFieldDescriptor* GetField(
        const std::vector<std::string>& path,
        uint32_t* repeat_level,
        uint32_t* define_level) const;

    const std::string GetTableSchemaString() const;

    std::string GetEntryName() const {
        return m_message_entry_name;
    }

    void GetAllColumnInfo(std::vector<ColumnInfo>* column_infos) const;
    std::vector<ColumnInfo> GetAllColumnInfo() const;

    void GetAllColumnInfo(
        std::vector<ColumnInfo>* info_list,
        const PBDescriptor* message_desc,
        const std::vector<std::string>& column_path,
        const std::string& path,
        int repeat_level, int define_level) const;

private:
    bool FindRequiredFieldPath(std::vector<std::string>* path_list,
                               const PBDescriptor* des) const;
    inline BQType ConvertToBQType(const PBFieldDescriptor::Type type) const;
    inline bool IsGroupField(const PBFieldDescriptor* field) const;

private:
    std::string m_message_entry_name;
    const PBDescriptor* m_table_descriptor;
    toft::scoped_ptr<ProtoMessage> m_proto_message;
};

bool TableSchema::IsGroupField(const PBFieldDescriptor* field) const {
    return (field->cpp_type() == PBFieldDescriptor::CPPTYPE_MESSAGE);
}

BQType TableSchema::ConvertToBQType(
    const PBFieldDescriptor::Type type) const {
    return BigQueryType::CovertPBTypeToBQType(type);
}

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_PARSER_TABLE_SCHEMA_H
