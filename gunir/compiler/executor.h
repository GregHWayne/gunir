// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_EXECUTOR_H
#define  GUNIR_COMPILER_EXECUTOR_H

#include <string>
#include <vector>

#include "toft/base/scoped_ptr.h"

#include "gunir/io/data_holder.h"
#include "gunir/compiler/container.h"
#include "gunir/compiler/big_query_functions.h"
#include "gunir/compiler/parser/big_query_types.h"
#include "gunir/compiler/parser/column_info.h"
#include "gunir/compiler/parser/table_schema.h"
#include "gunir/compiler/plan.h"
#include "gunir/io/block_helper.h"
#include "gunir/io/scanner.h"

namespace gunir {
namespace io {
class Slice;
}  // namespace io

namespace compiler {

class Executor : public io::Scanner {
private:
    typedef ::google::protobuf::FileDescriptorProto PBFileDescriptorProto;

public:
    Executor();
    ~Executor();

    bool Init(const PlanProto& plan_proto,
              const std::string& result_schema,
              const std::string& message_name);

    bool Init(Plan* plan,
              const std::string& result_schema,
              const std::string& message_name);

    void SetScanner(const std::vector<io::Scanner*>& scanners) {
        m_plan->SetScanner(scanners);
    }

    void SetContainer(Container* container) {
        m_container = container;
    }

    virtual bool NextSlice(io::Slice** slice);

    bool Run();

private:
    void PrintTuple();

    bool CopyToSlice(io::Slice* slice);

private:
    toft::scoped_ptr<Plan> m_plan;
    std::vector<ColumnInfo> m_column_infos;

    std::vector<DatumBlock*> m_tuple;
    toft::scoped_ptr<io::Slice> m_slice;
    Container* m_container;
    io::BlockHelper m_block_helper;
};

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_EXECUTOR_H
