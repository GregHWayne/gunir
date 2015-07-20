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
 * Studuent generate result for the following query
 * SELECT
 * student_id, grade, department_id, score
 * FROM Document
 * ORDER BY student_id, grade DESC, department_id ASC, score;
 */

struct CppStudent {
    int student_id;
    int grade;
    int department_id;
    int score;

    bool operator<(const CppStudent& that) const {
        if (this->student_id < that.student_id) {
            return true;
        }
        if (this->student_id > that.student_id) {
            return false;
        }

        if (this->grade > that.grade) {
            return true;
        }
        if (this->grade < that.grade) {
            return false;
        }

        if (this->department_id < that.department_id) {
            return true;
        }
        if (this->department_id > that.department_id) {
            return false;
        }

        if (this->score < that.score) {
            return true;
        }
        if (this->score > that.score) {
            return false;
        }
        return true;
    }
};

CppStudent* students = NULL;
CppStudent* sorted_students = NULL;

int SortRandNumber() {
    return rand(); // NOLINT
}

void InitStudents() {
    students = new CppStudent[FLAGS_record_number];

    for (int i = 0; i < FLAGS_record_number; ++i) {
        students[i].student_id = SortRandNumber() % 100;
        students[i].grade = SortRandNumber() % 100;
        students[i].department_id = SortRandNumber() % 100;
        students[i].score = SortRandNumber() % 100;
    }
}

PBMessage* RandStudent() {
    if (students == NULL) {
        InitStudents();
    }

    static int count = 0;
    const CppStudent& cpps = students[count];
    Student s;

    s.set_student_id(cpps.student_id);
    s.set_grade(cpps.grade);
    s.set_department_id(cpps.department_id);
    s.set_score(cpps.score);

    count++;
    if (count == FLAGS_record_number) {
        delete[] students;
    }
    return new Student(s);
}

void SortStudents() {
    sorted_students = new CppStudent[FLAGS_record_number];
    for (int i = 0; i < FLAGS_record_number; ++i) {
        sorted_students[i] = students[i];
    }

    std::stable_sort(sorted_students, sorted_students + FLAGS_record_number);
}

PBMessage* StudentSorter(const PBMessage* input) {
    if (sorted_students == NULL) {
        SortStudents();
    }

    static int count = 0;
    const CppStudent& cpps = sorted_students[count];
    Student s;

    s.set_student_id(cpps.student_id);
    s.set_grade(cpps.grade);
    s.set_department_id(cpps.department_id);
    s.set_score(cpps.score);

    count++;
    if (count == FLAGS_record_number) {
        delete[] sorted_students;
    }
    return new Student(s);
}

void CreateSortTestData() {
    CreateTestData("sort_test",
                   "./testdata/sort_test/Student.proto",
                   "Student",
                   "./testdata/sort_test/Student.proto",
                   "Student",
                   RandStudent,
                   StudentSorter);
}

} // namespace compiler
} // namespace gunir

int main(int argc, char** argv) {
    google::ParseCommandLineFlags(&argc, &argv, false);

    ::gunir::compiler::CreateSortTestData();
    return 0;
}
