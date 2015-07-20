// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include <cstdio>
#include <vector>

#include "thirdparty/gtest/gtest.h"

#include "gunir/proto/table.pb.h"
#include "gunir/compiler/executor.h"
#include "gunir/compiler/parser/big_query_parser.h"
#include "gunir/compiler/schema_builder.h"
#include "gunir/compiler/select_query.h"
#include "gunir/compiler/simple_planner.h"
#include "gunir/compiler/simple_planner_test.h"
#include "gunir/io/slice.h"

#include "thirdparty/protobuf/descriptor.h"
#include "thirdparty/protobuf/descriptor.pb.h"

namespace gunir {
namespace compiler {

Plan* TransForm(Plan* plan) {
    PlanProto plan_proto;
    plan->CopyToProto(&plan_proto);
    // LOG(ERROR) << plan_proto.DebugString();
    delete plan;
    return Plan::InitPlanFromProto(plan_proto);
}

/*
 * before filter
 * docid docid_1 docid_2 suburl code_country
 * 0    1   2 Hello null
 * 1    2   4 Hello code_1country_1
 * ...
 * 9    10  20 Hello code_9country_9
 *
 * after filter docid > 5
 * docid docid_1 docid_2 suburl code_country
 * 6    7   14 Hello null
 * 7    8   16 Hello code_7country_7
 * 8    9   18 Hello null
 * 9    10  20 Hello code_9country_9
 *
 * after filter docid_2 > 15 && docid_2 < 19
 * docid docid_1 docid_2 suburl code_country
 * 7    8   16 Hello code_7country_7
 * 8    9   18 Hello null
 */

TEST(SimplePlannerTest, test) {
    const char* query =
        " SELECT "
        " distinct docid, "
        " docid + 1 AS docid_1, "
        " (docid + 1) * 2 AS docid_2,"
        " SUBSTR(name.url, 0, 5) AS suburl, "
        " CONCAT(name.language.code, name.language.country) AS code_country, "
        " CONCAT(name.url, name.language.country) AS url_country "
        " FROM Document "
        " WHERE ((docid + 1) * 2 > 15 && (docid + 1) * 2 < 22)"
        " LIMIT 0, 5;";

    SelectQuery select_query = AnalyzeSelectQuery(query);

    // Plan
    SimplePlanner planer(select_query);
    // LOG(ERROR) << select_query.GetReadableResultSchema();
    TaskPlanProto plan_proto;
    ASSERT_TRUE(planer.GenerateExecutePlan(&plan_proto));

    // Proto test
    Plan* plan = Plan::InitPlanFromProto(plan_proto.exec_plan());

    // Set Scanners
    toft::scoped_ptr<io::Scanner> scanner1(
        new MockDocumentScanner(select_query.GetAffectedColumnInfo().size()));
    std::vector<io::Scanner*> scanners;
    scanners.push_back(scanner1.get());
    plan->SetScanner(scanners);

    // Run
    int record = 0;
    uint32_t select_level;
    std::vector<DatumBlock*> datum_row = AllocDatumRow(select_query);
    while (plan->GetNextTuple(datum_row, &select_level)) {
        record++;
        Datum* data0 = &(datum_row[0]->m_datum);
        Datum* data1 = &(datum_row[1]->m_datum);
        Datum* data2 = &(datum_row[2]->m_datum);
        Datum* data3 = &(datum_row[3]->m_datum);
        Datum* data4 = &(datum_row[4]->m_datum);
        Datum* data5 = &(datum_row[5]->m_datum);

        std::cout << data0->_INT64 << " | "
            << data1->_DOUBLE << " | "
            << data2->_DOUBLE << " | "
            << *data3->_STRING << " | ";

        if  (datum_row[4]->m_is_null)
            std::cout << "null | ";
        else
            std::cout << *data4->_STRING << " | ";

        if  (datum_row[5]->m_is_null)
            std::cout << "null" << std::endl;
        else
            std::cout << *data5->_STRING << std::endl;

        if (record == 1) {
            EXPECT_EQ(7, static_cast<int64_t>(data0->_INT64));
            EXPECT_EQ(8, static_cast<int64_t>(data1->_DOUBLE));
            EXPECT_EQ(16, static_cast<int64_t>(data2->_DOUBLE));

            EXPECT_STREQ("Hello", (data3->_STRING)->c_str());
            EXPECT_EQ(0U, datum_row[3]->m_rep_level);
            EXPECT_EQ(2U, datum_row[3]->m_def_level);

            EXPECT_STREQ("code_7country_7", (data4->_STRING)->c_str());
            EXPECT_EQ(0U, datum_row[4]->m_rep_level);
            EXPECT_EQ(3U, datum_row[4]->m_def_level);

            EXPECT_STREQ("Hello Worldcountry_7", (data5->_STRING)->c_str());
            EXPECT_EQ(0U, datum_row[5]->m_rep_level);
            EXPECT_EQ(3U, datum_row[5]->m_def_level);
        }

        if (record == 2) {
            EXPECT_EQ(8, static_cast<int64_t>(data0->_INT64));
            EXPECT_EQ(9, static_cast<int64_t>(data1->_DOUBLE));
            EXPECT_EQ(18, static_cast<int64_t>(data2->_DOUBLE));

            EXPECT_STREQ("Hello", (data3->_STRING)->c_str());
            EXPECT_EQ(1U, datum_row[3]->m_rep_level);
            EXPECT_EQ(2U, datum_row[3]->m_def_level);

            EXPECT_TRUE(datum_row[4]->m_is_null);
            EXPECT_EQ(2U, datum_row[4]->m_rep_level);
            EXPECT_EQ(2U, datum_row[4]->m_def_level);

            EXPECT_TRUE(datum_row[5]->m_is_null);
            EXPECT_EQ(2U, datum_row[5]->m_rep_level);
            EXPECT_EQ(1U, datum_row[5]->m_def_level);
        }
    }

    ASSERT_EQ(3, record);
    ReleaseDatumRow(datum_row);
    delete plan;
}

} // namespace compiler
} // namespace gunir

