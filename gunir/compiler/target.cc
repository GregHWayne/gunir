// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include "gunir/compiler/target.h"

namespace gunir {
namespace compiler {

bool Target::IsCompatibleWithGroupBy(
    const std::vector<GroupByColumn>& groupby_columns) const {
    std::vector<AffectedColumnInfo> all_affected;
    std::vector<AffectedColumnInfo> not_in_agg;
    std::vector<AffectedColumnInfo> in_agg;

    // Get all affected columns
    m_expr->GetAffectedColumns(&all_affected);

    // Get columns in agg
    for (size_t i = 0; i < m_agg_exprs.size(); ++i) {
        m_agg_exprs[i]->GetAffectedColumns(&in_agg);
    }

    // Get columns not in agg
    for (size_t i = 0; i < all_affected.size(); ++i) {
        const AffectedColumnInfo& column_in_all = all_affected[i];
        AddNotInAggregate(column_in_all, in_agg, &not_in_agg);
    }

    // Columns not in agg must be in groupby list
    for (size_t i = 0; i < not_in_agg.size(); ++i) {
        if (!IsInGroupByColumns(not_in_agg[i], groupby_columns)) {
            return false;
        }
    }
    return true;
}

void Target::GetAggAndNotAggColumn(std::vector<AffectedColumnInfo>* in_agg,
                                   std::vector<AffectedColumnInfo>* not_in_agg) {
    std::vector<AffectedColumnInfo> all_affected;

    // Get all affected columns
    m_expr->GetAffectedColumns(&all_affected);

    // Get columns in agg
    for (size_t i = 0; i < m_agg_exprs.size(); ++i) {
        m_agg_exprs[i]->GetAffectedColumns(in_agg);
    }

    // Get columns not in agg
    for (size_t i = 0; i < all_affected.size(); ++i) {
        const AffectedColumnInfo& column_in_all = all_affected[i];
        AddNotInAggregate(column_in_all, *in_agg, not_in_agg);
    }
}

void Target::AddNotInAggregate(
    const AffectedColumnInfo& column_in_all,
    const std::vector<AffectedColumnInfo>& in_agg,
    std::vector<AffectedColumnInfo>* not_in_agg) const {

    for (size_t i = 0; i < in_agg.size(); ++i) {
        const AffectedColumnInfo& column_in_agg = in_agg[i];

        if (column_in_all.m_column_info.m_column_path_string !=
            column_in_agg.m_column_info.m_column_path_string) {
            continue;
        }

        int count =
            column_in_all.m_affect_times - column_in_agg.m_affect_times;

        // Not all affected columns are in agg, add into not_in_agg
        if (count > 0) {
            size_t index = not_in_agg->size();
            not_in_agg->push_back(column_in_all);
            (*not_in_agg)[index].m_affect_times = count;
        }
        return;
    }

    not_in_agg->push_back(column_in_all);
}

bool Target::IsInGroupByColumns(
    const AffectedColumnInfo& column,
    const std::vector<GroupByColumn>& groupby_columns) const {

    for (size_t i = 0; i < groupby_columns.size(); ++i) {
        if (IsPathSame(column.m_column_info.m_column_path,
                       groupby_columns[i].m_column_info.m_column_path)) {
            return true;
        }
    }

    // LOG(ERROR) <<" false name: " << column.m_column_info.m_column_path_string;

    return false;
}

} // compiler
} // gunir

