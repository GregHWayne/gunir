// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include <cstdio>

#include "thirdparty/gtest/gtest.h"

#include "gunir/compiler/limit_plan.h"
#include "gunir/compiler/scan_plan.h"

namespace gunir {
namespace compiler {

class MockScanPlan : public ScanPlan {
public:
    MockScanPlan()
        : ScanPlan("mock_table_name",
                   TabletInfo(),
                   std::vector<ColumnInfo>(),
                   std::vector<uint64_t>()),
        m_returned(0) {
}

    bool GetNextTuple(const std::vector<DatumBlock*>& tuple,
                      uint32_t* select_level) {
        *select_level = 0;
        if (m_returned >= kMaxReturnNumber) {
            return false;
        }
        m_returned++;
        return true;
    }

    int m_returned;
    static const int kMaxReturnNumber;
};
const int MockScanPlan::kMaxReturnNumber = 1000;

int GetRecordNumber(Plan* plan) {
    int count = 0;
    uint32_t select_level;
    std::vector<DatumBlock*> tuple;
    bool hasmore = plan->GetNextTuple(tuple, &select_level);

    while (hasmore) {
        count++;
        hasmore = plan->GetNextTuple(tuple, &select_level);
    }
    return count;
}

TEST(LimitPlanTest, limit) {
    MockScanPlan* scan_plan1 = new MockScanPlan();
    toft::scoped_ptr<LimitPlan> plan1(new LimitPlan(scan_plan1, 0, 100));
    ASSERT_EQ(100, GetRecordNumber(plan1.get()));

    MockScanPlan* scan_plan2 = new MockScanPlan();
    toft::scoped_ptr<LimitPlan> plan2(new LimitPlan(scan_plan2, 1, 1));
    ASSERT_EQ(1, GetRecordNumber(plan2.get()));

    MockScanPlan* scan_plan3 = new MockScanPlan();
    toft::scoped_ptr<LimitPlan> plan3(new LimitPlan(scan_plan3, 1, 0));
    ASSERT_EQ(0, GetRecordNumber(plan3.get()));

    MockScanPlan* scan_plan4 = new MockScanPlan();
    toft::scoped_ptr<LimitPlan> plan4(new LimitPlan(scan_plan4, 0, 0));
    ASSERT_EQ(0, GetRecordNumber(plan4.get()));

    MockScanPlan* scan_plan5 = new MockScanPlan();
    toft::scoped_ptr<LimitPlan> plan5(
        new LimitPlan(scan_plan5, 0, MockScanPlan::kMaxReturnNumber + 100));
    ASSERT_EQ(MockScanPlan::kMaxReturnNumber, GetRecordNumber(plan5.get()));
}

} // namespace compiler
} // namespace gunir

