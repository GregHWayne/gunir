// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
#include <algorithm>

#include "gunir/compiler/compiler_test_helper.h"
#include "gunir/compiler/compiler_test_helper.pb.h"

DECLARE_int32(record_number);

namespace gunir {
namespace compiler {

/*
 * Department generate result for the following query
 * SELECT
 * department_id, institute_id,
 * cnt_student, sum_id, avg_id, sum_age, avg_age, max_age, min_age,
 * COUNT(student_id) AS CNT_STUDENT,
 * SUM(student_id) AS SUM_ID,
 * AVG(student_id) AS AVG_ID,
 * SUM(student_id) + 2 * AVG(student_id) AS ALL_ID,
 * SUM(student_age) AS SUM_AGE,
 * AVG(student_age) AS AVG_AGE,
 * MAX(student_age) AS MAX_AGE,
 * MIN(student_age) AS MIN_AGE,
 * AVG(student_age) + MAX(student_age) * MIN(student_age) - SUM(student_age) AS
 * ALL_AGE
 * FROM Department
 * GROUPBY department_id, institute_id,
 * cnt_student, sum_id, avg_id, sum_age, avg_age, max_age, min_age;
 */

struct CppDepartment {
    int64_t department_id;
    int64_t institute_id;
    std::vector<int64_t> student_id;
    std::vector<int32_t> student_age;

    bool operator<(const CppDepartment& that) const {
        if (this->department_id != that.department_id) {
            return this->department_id < that.department_id;
        }
        return (this->institute_id < that.institute_id);
    }

    bool operator==(const CppDepartment& that) const {
        return (this->department_id == that.department_id &&
                this->institute_id == that.institute_id);
    }
};

struct AggregateResult {
    int64_t cnt_student;
    int64_t sum_id;
    int64_t avg_id;
    int64_t avg_age;
    int64_t sum_age;
    int64_t max_age;
    int64_t min_age;

    AggregateResult() {
        cnt_student = 0;
        sum_id = 0;
        avg_id = 0;
        avg_age = 0;
        sum_age = 0;
        max_age = -1;
        min_age = INT32_MAX;
    }
};

AggregateResult g_aggregate_result[100][100];

int64_t LocalRandNumber() {
    return rand() % 100000; // NOLINT
}

void AddToAggregate(const CppDepartment& cpp_department,
                    AggregateResult* agg_result) {
    for (size_t i = 0; i < cpp_department.student_id.size(); ++i) {
        agg_result->cnt_student += 1;

        agg_result->sum_id += cpp_department.student_id[i];
        agg_result->sum_age += cpp_department.student_age[i];

        if (agg_result->max_age < cpp_department.student_age[i]) {
            agg_result->max_age = cpp_department.student_age[i];
        }

        if (agg_result->min_age > cpp_department.student_age[i]) {
            agg_result->min_age = cpp_department.student_age[i];
        }
    }
}

CppDepartment* g_cpp_department = NULL;

void InitDepartment() {
    static const int kRecordNumber = FLAGS_record_number;
    CppDepartment* cpp_department = new CppDepartment[FLAGS_record_number];
    g_cpp_department = new CppDepartment[FLAGS_record_number];

    for (int i = 0; i < kRecordNumber; ++i) {
        cpp_department[i].department_id = LocalRandNumber() % 20;
        cpp_department[i].institute_id = LocalRandNumber() % 10;

        int student_number = LocalRandNumber() % 50 + 1;
        for (int j = 0; j < student_number; ++j) {
            cpp_department[i].student_id.push_back(LocalRandNumber());
            cpp_department[i].student_age.push_back(LocalRandNumber());
        }
    }

    for (int i = 0; i < kRecordNumber; ++i) {
        g_cpp_department[i] = cpp_department[i];
    }

    std::sort(cpp_department, cpp_department + FLAGS_record_number);

    int count = 0;
    do {
        int64_t department_id = cpp_department[count].department_id;
        int64_t institute_id = cpp_department[count].institute_id;
        AggregateResult agg_result;

        do {
            AddToAggregate(cpp_department[count], &agg_result);
            count++;
        } while (count < kRecordNumber &&
                 cpp_department[count - 1] == cpp_department[count]);

        agg_result.avg_id = agg_result.sum_id / agg_result.cnt_student;
        agg_result.avg_age = agg_result.sum_age / agg_result.cnt_student;

        g_aggregate_result[department_id][institute_id] = agg_result;
    } while (count < kRecordNumber);

    delete[] cpp_department;
}

PBMessage* RandDepartment() {
    if (g_cpp_department == NULL) {
        InitDepartment();
    }
    static int count = 0;
    const CppDepartment& cpp_dpmt = g_cpp_department[count];

    Department dpmt;

    AggregateResult agg_result =
        g_aggregate_result[cpp_dpmt.department_id][cpp_dpmt.institute_id];

    dpmt.set_department_id(cpp_dpmt.department_id);
    dpmt.set_institute_id(cpp_dpmt.institute_id);

    dpmt.set_cnt_student(agg_result.cnt_student);
    dpmt.set_avg_id(agg_result.avg_id);
    dpmt.set_sum_id(agg_result.sum_id);
    dpmt.set_avg_age(agg_result.avg_age);
    dpmt.set_max_age(agg_result.max_age);
    dpmt.set_min_age(agg_result.min_age);
    dpmt.set_sum_age(agg_result.sum_age);

    for (size_t i = 0; i < cpp_dpmt.student_id.size(); ++i) {
        dpmt.add_student_id(cpp_dpmt.student_id[i]);
        dpmt.add_student_age(cpp_dpmt.student_age[i]);
    }

    count++;
    if (count == FLAGS_record_number) {
        delete[] g_cpp_department;
    }
    return new Department(dpmt);
}

PBMessage* AggregateDepartment(const PBMessage* input) {
    return NULL;
}

void CreateAggregateTestData() {
    CreateTestData("aggregate_test",
                   "./testdata/aggregate_test/Department.proto",
                   "Department",
                   "./testdata/aggregate_test/Department.proto",
                   "Department",
                   RandDepartment,
                   AggregateDepartment);
}

} // namespace compiler
} // namespace gunir

int main(int argc, char** argv) {
    google::ParseCommandLineFlags(&argc, &argv, false);

    ::gunir::compiler::CreateAggregateTestData();
    return 0;
}
