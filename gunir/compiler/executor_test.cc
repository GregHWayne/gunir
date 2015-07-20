// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include <string>
#include <vector>

#include "thirdparty/gflags/gflags.h"
#include "thirdparty/gmock/gmock.h"
#include "thirdparty/gtest/gtest.h"

#include "gunir/proto/table.pb.h"
#include "gunir/common/server_test_helper.h"
#include "gunir/common/task_container.h"
#include "gunir/compiler/executor.h"
#include "gunir/compiler/job_plan.h"
#include "gunir/compiler/parser/big_query_parser.h"
#include "gunir/compiler/schema_builder.h"
#include "gunir/compiler/select_query.h"
#include "gunir/io/slice.h"
#include "gunir/io/tablet_reader.h"
#include "gunir/io/tablet_scanner.h"
#include "gunir/io/tablet_schema.pb.h"
#include "gunir/utils/test_helper.h"

#include "thirdparty/protobuf/descriptor.h"
#include "thirdparty/protobuf/descriptor.pb.h"

DECLARE_bool(gunir_local_run);

using ::testing::Invoke;
using ::testing::Return;
using ::testing::_;

namespace gunir {
namespace compiler {

typedef ::google::protobuf::Descriptor PBDescriptor;
typedef ::google::protobuf::DescriptorPool PBDescriptorPool;
typedef ::google::protobuf::DescriptorProto PBDescriptorProto;
typedef ::google::protobuf::FieldDescriptor PBFieldDescriptor;
typedef ::google::protobuf::FieldDescriptorProto PBFieldDescriptorProto;
typedef ::google::protobuf::FileDescriptor PBFileDescriptor;
typedef ::google::protobuf::FileDescriptorProto PBFileDescriptorProto;

class MockTaskContainer : public TaskContainer {
public:
    MockTaskContainer() : TaskContainer() {}
public:
    MOCK_METHOD1(Insert, bool(const std::vector<io::Slice>& slices));

    bool Insert(const io::Slice& slice) {return true;}
    // MOCK_METHOD1(Insert, bool(const io::Slice& slices));
};

TEST(ExecutorTest, test_executor) {
    FLAGS_gunir_local_run = true;
    MockTaskContainer m_container;

    EXPECT_CALL(m_container, Insert(_)).WillRepeatedly(Return(true));

    const char* query = "SELECT docid FROM Document;";
    LeafTaskSpec m_spec = GenerateLeafTaskSpec(
        query, "Document",
        "testdata/Document.proto",
        "testdata/executor_test/Document.dat");

    const std::string& filename = m_spec.task_input().scanner_input(0).tablet().name();
    std::vector<std::string> column_names;
    std::string column_name_string;
    toft::scoped_ptr<io::TabletReader> m_tablet_reader;
    toft::scoped_ptr<io::TabletScanner> m_scanner;
    toft::scoped_ptr<MemPool> m_mempool;

    for (int i = 0; i < m_spec.task_input().scanner_input(0).column_names_size(); ++i) {
        column_names.push_back(m_spec.task_input().scanner_input(0).column_names(i));
        column_name_string = column_name_string + column_names[i];
    }

    m_mempool.reset(new MemPool(MemPool::MAX_UNIT_SIZE));
    m_tablet_reader.reset(new io::TabletReader(m_mempool.get()));

    EXPECT_TRUE(m_tablet_reader->Init(filename, column_names));

    LOG(ERROR) << " reader open success ";

    m_scanner.reset(new io::TabletScanner(m_tablet_reader.get()));

    Executor executor;

    executor.Init(m_spec.task_input().exec_plan(),
                  m_spec.task_input().result_proto(),
                  m_spec.task_input().result_message());

    std::vector<io::Scanner*> scanners;
    scanners.push_back(m_scanner.get());

    executor.SetScanner(scanners);
    executor.SetContainer(&m_container);
    EXPECT_TRUE(executor.Run());
    EXPECT_TRUE(m_tablet_reader->Close());
}

} // namespace gunir
} // namespace compiler
