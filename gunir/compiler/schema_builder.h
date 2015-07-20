// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_SCHEMA_BUILDER_H
#define  GUNIR_COMPILER_SCHEMA_BUILDER_H

#include <map>
#include <string>
#include <vector>

#include "toft/base/shared_ptr.h"

#include "gunir/compiler/expression.h"
#include "gunir/compiler/parser/big_query_types.h"
#include "gunir/compiler/parser/table_schema.h"
#include "gunir/compiler/table_entry.h"
#include "gunir/compiler/target.h"

#include "thirdparty/glog/logging.h"
#include "thirdparty/protobuf/descriptor.h"
#include "thirdparty/protobuf/descriptor.pb.h"

namespace gunir {
namespace compiler {
class TableManager;

class SchemaBuilder {
private:
    typedef ::google::protobuf::DescriptorPool PBDescriptorPool;
    typedef ::google::protobuf::DescriptorProto PBDescriptorProto;
    typedef ::google::protobuf::FieldDescriptorProto PBFieldDescriptorProto;
    typedef ::google::protobuf::FileDescriptor PBFileDescriptor;
    typedef ::google::protobuf::FileDescriptorProto PBFileDescriptorProto;
    typedef std::shared_ptr<TableEntry> ShrTableEntry;
    typedef std::shared_ptr<Target> ShrTarget;
    typedef std::shared_ptr<Expression> ShrExpr;
    typedef std::shared_ptr<AggregateFunctionExpression> ShrAggExpr;

public:
    explicit SchemaBuilder(TableManager* table_manager);

    bool BuildSchema(
        const std::vector<std::shared_ptr<Target> >& target_list,
        bool has_groupby, bool has_join,
        std::vector<ColumnInfo>* result_column_info);

    bool BuildGroupByTransferSchema(
        const std::vector<AffectedColumnInfo>& affected_columns,
        std::vector<ShrAggExpr> aggs);

    std::map<std::string, PBDescriptorProto> GetDescriptorProto() {
        return m_message_descs;
    }

    std::string GetReadableResultSchema() const;

    std::string GetReadableResultSchema(
        const PBFileDescriptorProto& file_proto) const;

    std::string GetResultSchema() const;

    static std::string GetReadableSchema(
        const std::string& schema);

public:
    static const char* kQueryResultMessageName;
    static const char* kQueryTransMessageName;
    static const char* kAggPrefix;
    static const char* kInt32;
    static const char* kUInt32;
    static const char* kInt64;
    static const char* kUInt64;
    static const char* kFloat;
    static const char* kDouble;
    static const char* kString;
    static const char* kBytes;
    static const char* kBool;
    static const char* kValue;

private:
    bool BuildPlainSchema(
        const std::vector<std::shared_ptr<Target> >& target_list);

    bool BuildGroupBySchema(
        const std::vector<std::shared_ptr<Target> >& target_list);

    bool BuildTarget(const std::shared_ptr<Target>& target);
    bool BuildSimpleTarget(const std::shared_ptr<Target>& target);
    bool BuildWithinTarget(const std::shared_ptr<Target>& target);

    bool CanTargetDataFieldBeNull(
        const std::shared_ptr<Target>& target,
        const DeepestColumnInfo& deepest_column);

    static std::vector<std::string> GetCommonPath(
        const std::vector<std::string>& p1, const std::vector<std::string>& p2);

    void PrintFileDesc();
    bool BuildField(const std::vector<std::string>& column_path,
                    const std::string& src_table_name,
                    PBLabel data_field_label,
                    BQType data_field_type);

    PBFieldDescriptorProto* BuildMessageField(
        const PBFieldDescriptor* old_field_desc,
        PBDescriptorProto* message_desc);

    PBFieldDescriptorProto* BuildDataField(
        PBLabel column_label, const std::string& column_name,
        BQType column_type, PBDescriptorProto* message_desc);

    PBFieldDescriptorProto* BuildWarpPODField(
        PBLabel column_label, const std::string& column_name,
        BQType column_type, PBDescriptorProto* new_message_desc);

    bool DescriptorHasField(
        const PBDescriptorProto* message_desc, const std::string& field_name);

    void SetResultColumnInfo(
        std::vector<ColumnInfo>* result_column_info);

    PBFieldDescriptorProto* GetFieldProto(
        const std::string& field_name, PBDescriptorProto* message_desc);

    PBDescriptorProto* GetDescriptorProto(const std::string& message_name);
    std::string GetPODWarpDescriptorProto(BQType type);
    const char* GetPODWarpTypeName(BQType type);

    PBFieldDescriptorProto::Label ConvertPBLabelToPBProtoLabel(PBLabel label);

    void GenerateFileDescriptorProto();

private:
    PBDescriptorProto m_result_message_desc;
    std::map<std::string, PBDescriptorProto> m_message_descs;
    toft::scoped_ptr<PBFileDescriptorProto> m_file_desc_proto;

    TableManager* m_table_manager;

    // one to one with target
    std::vector<std::vector<std::string> > m_new_column_list;
    std::vector<std::string> m_new_column_path;

    bool m_has_join;
};

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_SCHEMA_BUILDER_H

