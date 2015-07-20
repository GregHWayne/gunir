// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/compiler/parser/analyzer.h"

namespace gunir {
namespace compiler {

Analyzer::Analyzer(const SelectStmt& stmt)
    : m_select_stmt(stmt) {
    Analyze();
}

Analyzer::~Analyzer() {}

bool Analyzer::CheckField(const std::string& field, const std::string& value) {
    std::map<std::string, RawExpression>::iterator it = m_atomic_exp.find(field);
    if (it == m_atomic_exp.end()) {
        return false;
    }

    bool ret = false;
    const RawExpression& exp = it->second;
    LOG(INFO) << "exp = " << exp.DebugString();
    switch (exp.op()) {
    case kGreaterEqual:
        ret = CheckGreaterEqual(value, exp.right().atomic());
        break;
    case kGreater:
        ret = CheckGreater(value, exp.right().atomic());
        break;
    case kLessEqual:
        ret = CheckLessEqual(value, exp.right().atomic());
        break;
    case kLess:
        ret = CheckLess(value, exp.right().atomic());
        break;
    case kEqual:
        ret = CheckEqual(value, exp.right().atomic());
        break;
    case kNotEqual:
        ret = CheckNotEqual(value, exp.right().atomic());
        break;
    default:
        LOG(ERROR) << "not support operator: " << exp.op();
    }

    LOG(INFO) << "index of '" << field << "' is: " << m_atomic_index[field]
        << ", result.size() = " << m_atomic_result.size();
    m_atomic_result[m_atomic_index[field]] = ret;
    return ret;
}

void Analyzer::Reset() {
    m_atomic_result.clear();
    m_atomic_result.resize(m_atomic_exp.size());
}

bool Analyzer::IsTarget(const std::string& field) {
    std::set<std::string>::iterator it = m_target_list.find(field);
    if (it != m_target_list.end()) {
        return true;
    }
    return false;
}

int64_t Analyzer::MaxLimit() {
    return m_limit;
}

bool Analyzer::IsOk() {
    uint32_t data_index = 0;
    uint32_t opt_index = 0;

    LOG(INFO) << "m_atomic_result = " << m_atomic_result[data_index] << ", " << m_atomic_result[data_index + 1];
    while (data_index + 1 < m_atomic_result.size()
           && opt_index < m_agg_opt.size()) {
        switch (m_agg_opt[opt_index]) {
        case kLogicalAnd:
            m_atomic_result[data_index + 1] =
                (m_atomic_result[data_index] && m_atomic_result[data_index + 1]);
            break;
        case kLogicalOr:
            m_atomic_result[data_index + 1] =
                (m_atomic_result[data_index] || m_atomic_result[data_index + 1]);
            break;
        default:
            LOG(ERROR) << "not support logic operator: " << m_agg_opt[opt_index];
        }
        data_index++;
        opt_index++;
    }
    LOG(INFO) << "m_atomic_result = " << m_atomic_result[data_index] << ", " << m_atomic_result[data_index + 1];

    if (opt_index == m_agg_opt.size()) {
        return m_atomic_result[data_index];
    }
    return false;
}

void Analyzer::GetFieldFromWhereClause(std::vector<std::string>* cond_fields) {
    std::map<std::string, RawExpression>::iterator it = m_atomic_exp.begin();
    for (; it != m_atomic_exp.end(); ++it) {
        cond_fields->push_back(it->first);
    }
}

bool Analyzer::Analyze() {
    if (!m_select_stmt.has_where_clause()) {
        return true;
    }
    const RawExpression& where_clause = m_select_stmt.where_clause();
    int32_t index = 0;
    AnalyzeWhereClause(where_clause, &index);
    Reset();
    AnalyzeTargetList(m_select_stmt.target_list());
    if (m_select_stmt.has_limit()) {
        AnalyzeLimit(m_select_stmt.limit());
    }
    return true;
}

void Analyzer::AnalyzeWhereClause(const RawExpression& exp, int32_t* index) {
    if (exp.has_left() && exp.has_right() && exp.has_op()) {
        if (exp.left().has_atomic() && exp.right().has_atomic()) {
            std::string field_name = exp.left().atomic().column().field_list(0).char_string();
            m_atomic_exp[field_name] = exp;
            m_atomic_index[field_name] = *index;
            *index += 1;
        } else {
            AnalyzeWhereClause(exp.left(), index);
            AnalyzeWhereClause(exp.right(), index);
            m_agg_opt.push_back(exp.op());
        }
    } else {
        LOG(WARNING) << "invalid where expresion, expr = {"
            << exp.ShortDebugString() << "}";
    }
}

void Analyzer::AnalyzeTargetList(const RawTargetList& target_stmt) {
    LOG(INFO) << "target_stmt = " << target_stmt.DebugString();
    for (int32_t i = 0; i < target_stmt.target_list_size(); ++i) {
        m_target_list.insert(target_stmt.target_list(i)
                             .expression().atomic().column()
                             .field_list(0).char_string());
    }
}

void Analyzer::AnalyzeLimit(const Limit& limit) {
    m_limit = limit.number();
}

#define MULTI_TYPE_COMPARE(opt)                         \
    if (rexp.has_integer()) {                           \
        int64_t data = 0;                               \
        memcpy(&data, lvalue.c_str(), sizeof(int64_t)); \
        return data opt rexp.integer();                 \
    } else if (rexp.has_floating()) {                   \
        double data = 0.0;                              \
        memcpy(&data, lvalue.c_str(), sizeof(double));  \
        return data opt rexp.floating();                \
    } else if (rexp.has_boolean()) {                    \
        bool data = false;                              \
        memcpy(&data, lvalue.c_str(), sizeof(bool));    \
        return data opt rexp.boolean();                 \
    } else if (rexp.has_char_string()) {                \
        return lvalue opt rexp.char_string().char_string();   \
    } else {                                            \
        LOG(ERROR) << "unsupport value type";           \
    }                                                   \
    return false;

bool Analyzer::CheckGreaterEqual(const std::string& lvalue,
                                 const RawAtomicExpression& rexp) {
    MULTI_TYPE_COMPARE(>=);
//     if (rexp.has_integer()) {
//         int64_t data = 0;
//         memcpy(&data, lvalue.c_str(), sizeof(int64_t));
//         return data >= rexp.integer();
//     } else if (rexp.has_floating()) {
//         double data = 0.0;
//         memcpy(&data, lvalue.c_str(), sizeof(double));
//         return data >= rexp.floating();
//     } else if (rexp.has_boolean()) {
//         bool data = false;
//         memcpy(&data, lvalue.c_str(), sizeof(bool));
//         return data >= rexp.boolean();
//     } else if (rexp.has_char_string()) {
//         return lvalue >= rexp.char_string().char_string();
//     } else {
//         LOG(ERROR) << "unsupport value type";
//     }
//     return false;
}

bool Analyzer::CheckGreater(const std::string& lvalue,
                            const RawAtomicExpression& rexp) {
    LOG(INFO) << "exp = " << rexp.DebugString();
    MULTI_TYPE_COMPARE(>);
}

bool Analyzer::CheckLessEqual(const std::string& lvalue,
                              const RawAtomicExpression& rexp) {
    MULTI_TYPE_COMPARE(<=);
}

bool Analyzer::CheckLess(const std::string& lvalue,
                         const RawAtomicExpression& rexp) {
    LOG(INFO) << "exp = " << rexp.DebugString();
    MULTI_TYPE_COMPARE(<);
}

bool Analyzer::CheckEqual(const std::string& lvalue,
                          const RawAtomicExpression& rexp) {
    MULTI_TYPE_COMPARE(==);
}

bool Analyzer::CheckNotEqual(const std::string& lvalue,
                             const RawAtomicExpression& rexp) {
    MULTI_TYPE_COMPARE(!=);
}


} // namespace compiler
} // namespace gunir
