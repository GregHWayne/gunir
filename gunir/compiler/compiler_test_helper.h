// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description: help to do tests in compiler
//
#ifndef  GUNIR_COMPILER_COMPILER_TEST_HELPER_H
#define  GUNIR_COMPILER_COMPILER_TEST_HELPER_H

#include <climits>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include "gunir/compiler/executor.h"
#include "gunir/compiler/parser/big_query_parser.h"
#include "gunir/compiler/schema_builder.h"
#include "gunir/compiler/select_query.h"
#include "gunir/compiler/simple_planner.h"
#include "gunir/io/slice.h"
#include "gunir/io/table_builder.h"
#include "gunir/io/tablet_reader.h"
#include "gunir/proto/table.pb.h"
#include "gunir/compiler/testdata/document.pb.h"
#include "gunir/utils/proto_message.h"

#include "toft/base/scoped_ptr.h"
#include "toft/storage/file/file.h"
#include "toft/storage/recordio/recordio.h"
#include "toft/system/memory/mempool.h"

#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"
#include "thirdparty/gtest/gtest.h"
#include "thirdparty/protobuf/descriptor.h"
#include "thirdparty/protobuf/descriptor.pb.h"

namespace gunir {
namespace compiler {

typedef ::google::protobuf::Descriptor PBDescriptor;
typedef ::google::protobuf::DescriptorPool PBDescriptorPool;
typedef ::google::protobuf::DescriptorProto PBDescriptorProto;
typedef ::google::protobuf::FieldDescriptor PBFieldDescriptor;
typedef ::google::protobuf::FieldDescriptorProto PBFieldDescriptorProto;
typedef ::google::protobuf::FileDescriptor PBFileDescriptor;
typedef ::google::protobuf::FileDescriptorProto PBFileDescriptorProto;

class LocalTabletScanner : public io::Scanner {
public:
    LocalTabletScanner() {
        m_mempool.reset(new MemPool(MemPool::MAX_UNIT_SIZE));
        m_reader.reset(new io::TabletReader(m_mempool.get()));
    }

    bool Init(const std::string& table_name,
              const std::string& tablet_file_name,
              const std::vector<std::string>& columns) {
        return m_reader->Init(tablet_file_name, columns);
    }

    bool NextSlice(io::Slice** slice) {
        if (!m_reader->Next())
            return false;

        *slice = m_reader->GetSlice();
        return true;
    }

private:
    toft::scoped_ptr<MemPool> m_mempool;
    toft::scoped_ptr<io::TabletReader> m_reader;
};

std::vector<TableInfo>
GetTableInfos(std::vector<std::string> tables,
              const std::string& proto_file,
              const std::string& pb_data_file);
io::Scanner* GetScanner(
    const SelectQuery& select_query, const TableInfo& table_info);

bool ParseQuery(const std::string& query_string,
                const std::string& proto_file,
                const std::string& pb_data_file,
                SelectQuery** select_query);
bool ProcessQuery(const std::string& query_string,
                  const std::string& proto_file,
                  const std::string& pb_data_file,
                  Executor* executor,
                  std::vector<io::Scanner*>* scanners);

typedef ::google::protobuf::Message PBMessage;
typedef PBMessage* (* SrcGenerator)();
typedef PBMessage* (* ResultGenerator)(const PBMessage* input);

PBMessage* RandDocument();

void CreateTestData(const std::string& test_file_prefix,
                    const std::string& input_data_proto_file,
                    const std::string& input_data_proto_message,
                    const std::string& output_data_proto_file,
                    const std::string& output_data_proto_message,
                    SrcGenerator src_generator,
                    ResultGenerator generator);

bool IsSliceEqual(io::Slice* slice1, io::Slice* slice2);
std::string SliceDebugInfo(const io::Slice* s1, const io::Slice* s2);

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_COMPILER_TEST_HELPER_H
