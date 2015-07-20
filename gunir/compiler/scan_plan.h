// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_SCAN_PLAN_H
#define  GUNIR_COMPILER_SCAN_PLAN_H

#include <string>
#include <vector>

#include "toft/base/scoped_ptr.h"

#include "gunir/compiler/big_query_functions.h"
#include "gunir/compiler/parser/column_info.h"
#include "gunir/compiler/plan.h"
#include "gunir/compiler/scan_reader.h"

namespace gunir {
namespace compiler {

class ScanPlan : public Plan {
public:
    ScanPlan(const std::string& table_name,
             const TabletInfo& tablet_info,
             const std::vector<ColumnInfo>& column_infos,
             const std::vector<uint64_t>& affect_ids);

    ScanPlan(const std::vector<ColumnInfo>& column_infos,
             const std::vector<uint64_t>& affect_ids);

    explicit ScanPlan(const PlanProto& proto);

    ~ScanPlan() {}

    bool GetNextTuple(const std::vector<DatumBlock*>& tuple,
                      uint32_t* select_level);

    void CopyToProto(PlanProto* proto) const;

    void ParseFromProto(const PlanProto& proto);

    void SetScanner(const std::vector<io::Scanner*>& scanners) {
        m_reader->Reset(scanners[0], m_column_types.size());
    }

private:
    // TODO(codingliu): inter scan plan do not have table name
    std::string m_table_name;
    TabletInfo m_tablet_info;
    std::vector<std::string> m_column_name_list;
    std::vector<BQType> m_column_types;
    std::vector<uint64_t> m_affect_ids;
    toft::scoped_ptr<ScanReader> m_reader;
};

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_SCAN_PLAN_H

