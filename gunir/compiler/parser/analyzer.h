// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#ifndef GUNIR_PARSER_ANALYZER_H
#define GUNIR_PARSER_ANALYZER_H

#include <map>
#include <set>
#include <string>
#include <vector>

#include "gunir/compiler/parser/select_stmt.pb.h"

namespace gunir {
namespace compiler {

class Analyzer {
public:
    Analyzer(const SelectStmt& stmt);
    ~Analyzer();

    bool CheckField(const std::string& field, const std::string& value);
    void Reset();
    bool IsOk();

    int64_t MaxLimit();
    bool IsTarget(const std::string& field);

    void GetFieldFromWhereClause(std::vector<std::string>* cond_fields);

private:
    bool Analyze();
    void AnalyzeWhereClause(const RawExpression& exp, int32_t* index);
    void AnalyzeTargetList(const RawTargetList& target_stmt);
    void AnalyzeLimit(const Limit& limit);

    bool CheckGreaterEqual(const std::string& lvalue,
                           const RawAtomicExpression& rexp);
    bool CheckGreater(const std::string& lvalue,
                      const RawAtomicExpression& rexp);
    bool CheckLessEqual(const std::string& lvalue,
                        const RawAtomicExpression& rexp);
    bool CheckLess(const std::string& lvalue,
                   const RawAtomicExpression& rexp);
    bool CheckEqual(const std::string& lvalue,
                    const RawAtomicExpression& rexp);
    bool CheckNotEqual(const std::string& lvalue,
                       const RawAtomicExpression& rexp);

private:
    const SelectStmt& m_select_stmt;
    std::map<std::string, RawExpression> m_atomic_exp;
    std::vector<bool> m_atomic_result;
    std::map<std::string, int32_t> m_atomic_index;
    std::vector<Operators> m_agg_opt;

    int64_t m_limit;
    std::set<std::string> m_target_list;
};

} // namespace compiler
} // namespace gunir

#endif // GUNIR_PARSER_ANALYZER_H
