// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include "gunir/compiler/select_query.h"

#include <algorithm>
#include <set>

#include "toft/base/singleton.h"
#include "toft/base/string/algorithm.h"
#include "toft/base/string/number.h"

#include "gunir/compiler/function_expression.h"
#include "gunir/compiler/function_resolver.h"
#include "gunir/compiler/operator_expression.h"
#include "gunir/compiler/parser/plan.pb.h"
#include "gunir/compiler/parser/table_schema.h"

DEFINE_int64(gunir_compiler_join_table_size, 8*1024*1024,
             "limit size of join table");

DEFINE_int64(gunir_compiler_join_tablet_num, 1,
             "limit tablet number of join table");

namespace gunir {
namespace compiler {

SelectQuery::SelectQuery(const SelectStmt& stmt)
    : m_limit_start(-1),
    m_limit_number(-1),
    m_join_table_start(-1),
    m_aggregate_type(kNoneAgg),
    m_agg_number(0),
    m_select_stmt(stmt),
    m_has_join(false),
    m_select_star(false) {
}

bool SelectQuery::Init(const std::vector<TableInfo>& table_infos) {
    std::string failed_table_name;
    if (!m_table_manager.Init(table_infos, &failed_table_name)) {
        m_err = "Init table [ " + failed_table_name + " ] failed";
        LOG(ERROR) << m_err;
        return false;
    }
    return true;
}

std::vector<std::string> SelectQuery::GetQueryTables() const {
    std::vector<std::string> table_names;
    const RawTableList& from_list = m_select_stmt.from_list();

    for (int i = 0; i < from_list.table_list_size(); ++i) {
        const RawTable& raw_table = from_list.table_list(i);
        if (raw_table.has_table_name())  {
            table_names.push_back(raw_table.table_name().char_string());
        }
    }

    // ADD table from Join table
    if (m_select_stmt.has_join()) {
        const RawTable& raw_table = m_select_stmt.join().partner();
        table_names.push_back(raw_table.table_name().char_string());
    }

    return table_names;
}

bool SelectQuery::Analyze() {
    const RawTableList& raw_tables = m_select_stmt.from_list();

    m_stage = kConvertQuery;
    if (!m_query_convertor.Convert(&m_select_stmt, &m_err)) {
        return false;
    }

    m_stage = kFromList;
    if (!AnalyzeTableEntryList(raw_tables)) {
        return false;
    }

    m_stage = kWhereClause;
    if (m_select_stmt.has_where_clause() &&
        !AnalyzeWhereClause(m_select_stmt.where_clause())) {
        return false;
    }

    m_stage = kJoin;
    if (m_select_stmt.has_join() &&
        !AnalyzeJoin(m_select_stmt.join())) {
        return false;
    }

    m_stage = kTargets;
    if (!AnalyzeTargets(m_select_stmt.target_list())) {
        return false;
    }

    m_stage = kGroupBy;
    if (m_select_stmt.has_groupby() &&
        !AnalyzeGroupBy(m_select_stmt.groupby())) {
        return false;
    }

    m_stage = kConstruct;
    if (!ConstructResultSchema()) {
        return false;
    }

    m_stage = kDistinct;
    if (!AnalyzeDistinct()) {
        return false;
    }

    m_stage = kHaving;
    if (m_select_stmt.has_having() &&
        !AnalyzeHaving(m_select_stmt.having())) {
        return false;
    }

    m_stage = kOrderBy;
    if (m_select_stmt.has_orderby() &&
        !AnalyzeOrderBy(m_select_stmt.orderby())) {
        return false;
    }

    m_stage = kLimit;
    if (m_select_stmt.has_limit() &&
        !AnalyzeLimit(m_select_stmt.limit())) {
        return false;
    }

    m_stage = kAll;
    if (!AnalyzeOthers()) {
        return false;
    }
    return true;
}

bool SelectQuery::AnalyzeOthers() {
    for (size_t i = 0; i < m_target_list.size(); ++i) {
        const ShrTarget& target = m_target_list[i];
        if (m_aggregate_type == kGroupByAgg &&
            !target->IsCompatibleWithGroupBy(m_groupby_columns)) {
            m_err = std::string("columns affected in target:") +
                toft::NumberToString(i + 1) +
                std::string(" must be in aggregate function or groupby list");
            LOG(ERROR) << m_err;
            return false;
        }
    }
    return true;
}

bool SelectQuery::AnalyzeTableEntry(const RawTable& raw_table) {

    ShrTableEntry entry(new TableEntry());
    if (!entry->InitTableEntry(&m_table_manager,
                               raw_table,
                               &m_err)) {
        m_err += "InitTableEntry error ";
        LOG(ERROR) << m_err;
        return false;
    }

    const std::string& table_name = entry->GetTableName();
    if (m_used_entry_name.find(table_name) != m_used_entry_name.end()) {
        m_err = std::string("Multiple table entry has same table name: ")
            + table_name;
        LOG(ERROR) << m_err;
        return false;
    }
    m_used_entry_name.insert(table_name);

    std::string table_alias_name;
    if (entry->GetTableAliasName(&table_alias_name)) {
        if (m_used_entry_name.find(table_alias_name) !=
            m_used_entry_name.end()) {
            m_err = std::string("Multiple tables have same name or alias:")
                + table_alias_name;
            LOG(ERROR) << m_err;
            return false;
        }
        m_used_entry_name.insert(table_alias_name);
    }

    m_table_entrys.push_back(entry);
    return true;
}

bool SelectQuery::AnalyzeTableEntryList(const RawTableList& raw_tables) {

    for (int i = 0; i < raw_tables.table_list_size(); ++i) {
        if (!AnalyzeTableEntry(raw_tables.table_list(i))) {
            return false;
        }
    }

    if (m_table_entrys.size() > 1) {
        m_err = "Only one table is supported after from in this version";
        LOG(ERROR) << m_err;
        return false;
    }

    return true;
}

bool SelectQuery::AnalyzeTargets(const RawTargetList& targets) {
    if (targets.target_is_star()) {
        m_select_star = true;
        return AffectAllColumns();
    }

    for (int i = 0; i < targets.target_list_size(); ++i) {
        ShrTarget target = AnalyzeTarget(targets.target_list(i), i);
        if (target == NULL) {
            return false;
        }

        target->SetAggregateFunctionExpression(m_agg_exprs);
        m_target_list.push_back(target);
        m_agg_exprs.clear();
    }

    if (m_affected_column_info.size() == 0) {
        m_err = "Error, no column is affected in target expression";
        LOG(ERROR) << m_err;
        return false;
    }

    return true;
}

bool SelectQuery::AffectAllColumns() {
    if (m_table_entrys.size() > 1) {
        m_err = "Can't SELECT* from multiple tables";
        LOG(ERROR) << m_err;
        return false;
    }

    std::vector<ColumnInfo> column_infos;
    const std::string table_name = m_table_entrys[0]->GetTableName();

    m_table_entrys[0]->GetAllColumnInfo(&column_infos);

    for (size_t i = 0; i < column_infos.size(); ++i) {
        const ColumnInfo& info = column_infos[i];
        if (m_has_join) {
            if (info.m_repeat_level != 0) {
                m_err = std::string("Cannot join on a repeated field:")
                    + info.m_column_path_string;
                LOG(ERROR) << m_err;
                return false;
            }
        }
        size_t affect_id = this->AddAffectedColumnInfo(info, table_name, false);
        m_table_entrys[0]->AddAffectedColumnInfo(info, affect_id, false);

        ShrExpr e = ShrExpr(new ColumnExpression(info, table_name, affect_id, false));

        ShrTarget t(new Target());

        t->SetExpression(e);
        m_target_list.push_back(t);
    }
    return true;
}

std::shared_ptr<Target> SelectQuery::AnalyzeTarget(
    const RawTarget& raw_target, int i) {
    const RawExpression& raw_expr = raw_target.expression();
    ShrExpr expr = AnalyzeExpression(raw_expr);

    if (expr == NULL) {
        m_err += "\n"+std::string("Analyze expression failed, ")
            + "in target " + toft::NumberToString(i + 1);
        LOG(ERROR) << m_err;
        return ShrTarget(static_cast<Target*>(NULL));
    }

    // Target is an expression, but it does not have alias
    if (!expr->IsSingleColumn() && !raw_target.has_alias()) {
        RawTarget* new_target = const_cast<RawTarget*>(&raw_target);
        new_target->mutable_alias()->set_char_string("Target_"+ toft::NumberToString(i + 1));
    }

    if (expr->IsSingleColumn() && raw_target.has_alias()) {
        ColumnExpression* col_expr = static_cast<ColumnExpression*>(expr.get());
        const std::string& col_alias = raw_target.alias().char_string();
        if (m_alias_to_path.find(col_alias) != m_alias_to_path.end()) {
            m_err += "\n Targets alias " + col_alias + "is exist";
            LOG(ERROR) << m_err;
            return ShrTarget(static_cast<Target*>(NULL));
        }
        const ColumnInfo& info = col_expr->GetColumnInfo();
        m_alias_to_path[col_alias] = info.m_column_path_string;
        m_alias_to_target_id[col_alias] = i;
        m_path_to_alias[info.m_column_path_string] = col_alias;
    } else if (raw_target.has_alias()) {
        m_alias_to_target_id[raw_target.alias().char_string()] = i;
    }

    // Check within and groupby
    if (!CheckWithinAndGroupBy(raw_target, expr, i)) {
        return ShrTarget(static_cast<Target*>(NULL));
    }

    ShrTarget t(new Target());
    t->SetExpression(expr);
    if (raw_target.has_alias()) {
        t->SetAlias(raw_target.alias().char_string());
    }

    if (!raw_target.has_within()) {
        return t;
    }

    if (m_has_join) {
        m_err = "Within can't not be used with join";
        return ShrTarget(static_cast<Target*>(NULL));
    }

    if (!AnalyzeWithin(raw_target.within(), t)) {
        m_err = std::string("Analyze within failed in target ") +
            toft::NumberToString(i);
        LOG(ERROR) << m_err;
        return ShrTarget(static_cast<Target*>(NULL));
    }
    return t;
}

bool SelectQuery::CheckWithinAndGroupBy(const RawTarget& raw_target,
                                        const ShrExpr& expr,
                                        int target_id) {
    if (!expr->HasAggregate()) {
        return true;
    }

    if (m_aggregate_type == kGroupByAgg && raw_target.has_within()) {
        m_err = std::string("WITHIN and GROUPBY can't be used in a query")
            + ", error at target:" + toft::NumberToString(target_id + 1);
        LOG(ERROR) << m_err;
        return false;
    }

    if (m_aggregate_type == kWithinAgg && !raw_target.has_within()) {
        m_err = std::string("Target:") + toft::NumberToString(target_id)
            + " must be WITHIN Aggregate";
        LOG(ERROR) << m_err;
        return false;
    }

    if (!raw_target.has_within()) {
        m_aggregate_type = kGroupByAgg;
    } else {
        m_aggregate_type = kWithinAgg;
    }
    return true;
}

bool SelectQuery::AnalyzeWithin(const RawWithin& within, ShrTarget t) {
    if (m_table_entrys.size() > 1) {
        m_err = "Within should not be used with join";
        LOG(ERROR) << m_err;
        return false;
    }

    if (t->GetAlias() == NULL) {
        m_err = "Within target must have an alias";
        LOG(ERROR) << m_err;
        return false;
    }

    t->SetWithinTableName(m_table_entrys[0]->GetTableName());
    if (within.is_record()) {
        t->SetWithinLevel(0U);
        return true;
    }

    CHECK_EQ(true, within.has_column());

    std::vector<std::string> column_path;
    std::string column_path_string;
    GetFieldListFromColumn(within.column(), &column_path, &column_path_string);

    // resolve column in table entrys
    ColumnInfo info;
    if (!m_table_entrys[0]->GetColumnInfo(column_path, &info)) {
        m_err = std::string("Can't find within path:")
            + column_path_string + " in any table";
        LOG(ERROR) << m_err;
        return false;
    }

    if (info.m_type != BigQueryType::MESSAGE) {
        m_err = std::string("WITHIN clause must within RECORD or Message, ")
            + column_path_string + " is not Message type";
        LOG(ERROR) << m_err;
        return false;
    }

    // info.m_column_path do not have table name as first field
    t->SetWithinMessagePath(info.m_column_path);
    t->SetWithinLevel(info.m_repeat_level);

    if (!IsAllAffectedColumnInWithinPath(t, info)) {
        return false;
    }
    return true;
}

bool SelectQuery::IsAllAffectedColumnInWithinPath(
    ShrTarget t, const ColumnInfo& within_column) {

    std::vector<AffectedColumnInfo> target_affected_columns;
    (t->GetExpression())->GetAffectedColumns(&target_affected_columns);

    const std::vector<std::string> within_column_path =
        within_column.m_column_path;
    for (size_t i = 0; i < target_affected_columns.size(); ++i) {
        const std::vector<std::string> affected_column_path =
            target_affected_columns[i].m_column_info.m_column_path;
        const std::string affected_column_path_string =
            target_affected_columns[i].m_column_info.m_column_path_string;

        if (affected_column_path.size() < within_column_path.size()) {
            m_err = affected_column_path_string + " is not in within path:"
                + within_column.m_column_path_string;
            LOG(ERROR) << m_err;
            return false;
        }

        size_t j = 0;
        for (j = 0; j < within_column_path.size(); ++j) {
            if (affected_column_path[j] != within_column_path[j]) {
                break;
            }
        }

        if (j != within_column_path.size()) {
            m_err = affected_column_path_string + " is not in within path:"
                + within_column.m_column_path_string;
            LOG(ERROR) << m_err;
            return false;
        }
    }
    return true;
}

bool SelectQuery::ConstructResultSchema() {
    bool has_groupby = HasGroupBy();

    if (!ConstructFinalSchema(has_groupby)) {
        return false;
    }

    if (has_groupby && !ConstructTransSchema()) {
        return false;
    }
    return true;
}

bool SelectQuery::ConstructFinalSchema(bool has_groupby) {
    SchemaBuilder builder(&m_table_manager);

    if (!builder.BuildSchema(
            m_target_list, has_groupby, m_has_join, &m_result_column_info)) {
        return false;
    }

    m_final_schema = builder.GetResultSchema();

    CHECK_EQ(m_result_column_info.size(), m_target_list.size())
        << "result_column size:" << m_result_column_info.size()
        << ", target_list size:" << m_target_list.size() << " not match";

    for (size_t i = 0; i < m_target_list.size(); ++i) {
        m_target_list[i]->SetResultColumnInfo(m_result_column_info[i]);
    }

    // sort the targets as the schema.GetAllColumnInfo seq
    TableSchema schema;
    CHECK(schema.
          InitSchemaFromFileDescriptorProto
          (m_final_schema, SchemaBuilder::kQueryResultMessageName))
        << "select query parse schema error";

    std::vector<ColumnInfo> final_colume_info = schema.GetAllColumnInfo();

    for (size_t i = 0 ; i < final_colume_info.size() ; i++) {
        for (size_t j = i+1 ; j < m_target_list.size() ; j++) {
            if (m_target_list[j]->GetResultColumnInfo().m_column_path_string
                == final_colume_info[i].m_column_path_string) {
                swap(m_target_list[j], m_target_list[i]);
                break;
            }
        }
    }

    LOG(INFO) << "final schema:\n" << GetReadableResultSchema();
    return true;
}

bool SelectQuery::ConstructTransSchema() {
    std::vector<ShrAggExpr> aggs = GetAggregateExpressionsInQuery();
    SchemaBuilder builder(&m_table_manager);
    builder.BuildGroupByTransferSchema(m_affected_column_info,
                                       aggs);
    m_trans_schema = builder.GetResultSchema();

    LOG(INFO) << "trans schema:\n" << GetReadableTransSchema();
    return true;
}

bool SelectQuery::AnalyzeWhereClause(const RawExpression& expr) {

    // alias is not support in whereClause
    ShrExpr e = AnalyzeExpression(expr);
    if (e == NULL) {
        return false;
    }

    if (e->GetReturnType() != BigQueryType::BOOL) {
        m_err = "Where clause must return bool";
        LOG(ERROR) << m_err;
        return false;
    }

    m_where = e;
    return true;
}

bool SelectQuery::AnalyzeJoin(const RawJoin& join) {
    m_has_join = true;

    if (!AnalyzeTableEntry(join.partner())) {
        return false;
    }

    const ShrTableEntry& entry = m_table_entrys[m_table_entrys.size() - 1];
    const std::string& table_name = entry->GetTableName();

    // check small table_size and tablet file_size <= 1
    int64_t join_table_size = m_table_manager.GetTable(table_name)->GetTableSize();
    int64_t tablet_file_num = (m_table_manager.GetTable(table_name)->GetTabletFile()).size();
    if (join_table_size > FLAGS_gunir_compiler_join_table_size
        ||  tablet_file_num > FLAGS_gunir_compiler_join_tablet_num) {
        m_err = "table size is out of limit "
            + toft::NumberToString(join_table_size) + " > "
            + toft::NumberToString(FLAGS_gunir_compiler_join_table_size)
            + " GetTabletFile size = "
            + toft::NumberToString(tablet_file_num);
        LOG(ERROR) << m_err;
        return false;
    }

    m_join_table_start = m_table_entrys.size();

    ShrExpr e = AnalyzeExpression(join.expression());

    if (e == NULL) {
        return false;
    }

    if (e->GetReturnType() != BigQueryType::BOOL) {
        m_err = "JOIN Codition must return bool";
        LOG(ERROR) << m_err;
        return false;
    }

    m_join_exprs.push_back(e);
    m_join_ops.push_back(join.op());

    return true;
}

bool SelectQuery::AnalyzeGroupBy(const ColumnPathList& column_list) {

    CHECK_GE(2U, m_table_entrys.size());
    for (int i = 0; i < column_list.path_list_size(); ++i) {
        const ColumnPath& column = column_list.path_list(i);
        std::vector<std::string> column_path;
        std::string column_path_string;
        ShrTableEntry affected_table_entry;
        size_t affect_id;
        ColumnInfo info;

        GetFieldListFromColumn(column, &column_path, &column_path_string);

        // replace alias to real path
        if (m_alias_to_path.find(column_path_string) != m_alias_to_path.end()) {
            GetFieldListFromString(m_alias_to_path[column_path_string],
                                   &column_path,
                                   &column_path_string);
        }

        if (!GetAffectColumnInfo(column_path, &info,
                                 &affected_table_entry,
                                 column_path_string)) {
            m_err += "\nCan't find group by column path in Targets";
            LOG(ERROR) << m_err;
            return false;
        }

        for (size_t i = 0; i < m_groupby_columns.size(); ++i) {
            if (info.m_column_path_string ==
                m_groupby_columns[i].m_column_info.m_column_path_string) {
                m_err = std::string("GroupBy duplicate path:")
                    + column_path_string;
                LOG(ERROR) << m_err;
                return false;
            }
        }

        if (info.m_type == BigQueryType::MESSAGE) {
            m_err = std::string("Cannot groupby Message Type, ")
                + column_path_string + " is Message type";
            LOG(ERROR) << m_err;
            return false;
        }

        if (info.m_repeat_level != 0) {
            m_err = std::string("Cannot groupby a repeated field:")
                + column_path_string;
            LOG(ERROR) << m_err;
            return false;
        }

        affect_id = AddAffectedColumnInfo(info,
                                          affected_table_entry->GetTableName(),
                                          false);

        GroupByColumn groupby_column;
        groupby_column.m_column_info = info;
        groupby_column.m_affect_id = affect_id;

        m_groupby_columns.push_back(groupby_column);
    }

    if (m_groupby_columns.size() > 0) {
        m_aggregate_type = kGroupByAgg;
    }

    if (m_select_star && m_groupby_columns.size() > 0) {
        m_err = "Groupby is used, should not SELECT *";
        LOG(ERROR) << m_err;
        return false;
    }

    return true;
}

bool SelectQuery::AnalyzeHaving(const RawExpression& raw_expr) {
    ShrExpr expr = AnalyzeExpression(raw_expr);
    if (expr == NULL) {
        return false;
    }

    if (expr->GetReturnType() != BigQueryType::BOOL) {
        m_err = "Having clause should return bool";
        LOG(ERROR) << "Having clause should return bool";
        return false;
    }

    if (expr->HasAggregate()) {
        m_err = "Having clause should not use aggregate";
        LOG(ERROR) << m_err;
        return false;
    }

    m_having = expr;
    return true;
}

bool SelectQuery::AnalyzeOrderBy(const OrderColumnPathList& order_column_list) {
    CHECK_LT(0, order_column_list.path_list_size());

    for (int i = 0; i < order_column_list.path_list_size(); ++i) {
        const OrderColumnPath& raw_orderby_column =
            order_column_list.path_list(i);

        OrderByColumn orderby_column;
        if (!AnalyzeOrderByColumn(raw_orderby_column, &orderby_column)) {
            m_err = "analyze orderby expression error";
            LOG(ERROR) << m_err;
            return false;
        }

        m_orderby_columns.push_back(orderby_column);
    }
    return true;
}

bool SelectQuery::AnalyzeOrderByColumn(
    const OrderColumnPath& raw_orderby_column,
    OrderByColumn* orderby_column) {

    std::vector<std::string> column_path;
    std::string column_path_string;
    GetFieldListFromColumn(raw_orderby_column.path(), &column_path, &column_path_string);

    if (m_alias_to_target_id.find(column_path_string)
        != (m_alias_to_target_id.end())) {
        int target_id = m_alias_to_target_id[column_path_string];
        m_target_list[target_id]->GetResultPath(&column_path, HasGroupBy());
    } else if (m_path_to_alias.find(column_path_string) != m_path_to_alias.end()) {
        // replace real path to alias
        GetFieldListFromString(m_path_to_alias[column_path_string],
                               &column_path,
                               &column_path_string);
    }

    if (!GetColumnInfoInResultSchema(column_path,
                                     &(orderby_column->m_column_info),
                                     &(orderby_column->m_target_id))) {
        m_err = std::string("Can't find column path path:")
            + column_path_string + " in result schema";
        LOG(ERROR) << m_err;
        return false;
    }

    if ((orderby_column->m_column_info).m_type == BigQueryType::MESSAGE) {
        m_err = std::string("Can't orderby Message type, ")
            + column_path_string + " is not Message type";
        LOG(ERROR) << m_err;
        return false;
    }

    if ((orderby_column->m_column_info).m_repeat_level >= 1) {
        m_err = std::string("Can't orderby a repeated field, ")
            + "[" + column_path_string + " ]";
        LOG(ERROR) << m_err;
        return false;
    }

    orderby_column->m_type = raw_orderby_column.type();
    return true;
}

bool SelectQuery::AnalyzeDistinct() {
    bool has_aggregate = false;
    for (size_t j = 0; j < m_target_list.size(); ++j) {
        const ShrTarget& target = m_target_list[j];
        std::vector<AffectedColumnInfo> in_agg;
        std::vector<AffectedColumnInfo> not_in_agg;
        target->GetAggAndNotAggColumn(&in_agg, &not_in_agg);

        int has_distinct_cnt = 0;
        for (size_t i = 0 ; i < not_in_agg.size() ; i++) {
            if (not_in_agg[i].m_is_distinct) {
                has_distinct_cnt++;
            }
        }

        if (has_distinct_cnt > 1) {
            m_err = "distinct can only appear once in one Target";
            LOG(ERROR) << m_err;
            return false;
        }

        if (has_distinct_cnt > 0) {
            std::vector<std::string> column_path;
            DistinctColumn dis_column;
            if (!target->GetResultPath(&column_path, HasGroupBy())) {
                m_err = "can not distinct a const expression";
                LOG(ERROR) << m_err;
                return false;
            }

            CHECK(GetColumnInfoInResultSchema(column_path,
                                              &(dis_column.m_column_info),
                                              &(dis_column.m_affect_id)));
            m_distinct_columns.push_back(dis_column);
        }

        bool has_distinct = false;
        for (size_t i = 0 ; i < in_agg.size() ; i++) {
            has_aggregate = true;
            if (in_agg[i].m_is_distinct) {
                has_distinct = true;
            }
        }

        if (has_distinct && (in_agg.size() + not_in_agg.size()) > 1) {
            m_err = "distinct do not support both in aggregate and not aggregate expression";
            LOG(ERROR) << m_err;
            return false;
        }

        if (has_distinct) {
            DistinctColumn dis_column;
            dis_column.m_affect_id = in_agg[0].m_affect_id;
            dis_column.m_column_info = in_agg[0].m_column_info;
            m_dis_agg_columns.push_back(dis_column);
        }
    }

    if (has_aggregate && m_distinct_columns.size() > 0) {
        m_err = "distinct can only appear in aggregation function when has groupby";
        LOG(ERROR) << m_err;
        return false;
    }

    if (m_dis_agg_columns.size() > 1) {
        m_err = "distinct can only appear in one aggregation in this version";
        LOG(ERROR) << m_err;
        return false;
    }

    for (size_t i = 0 ; i < m_distinct_columns.size() ; i++) {
        if (m_distinct_columns[i].m_column_info.m_repeat_level != 0) {
            m_err = "can not distinct a repeated field";
            LOG(ERROR) << m_err;
            return false;
        }
    }

    for (size_t i = 0 ; i < m_dis_agg_columns.size() ; i++) {
        if (m_dis_agg_columns[i].m_column_info.m_repeat_level != 0) {
            m_err = "can not distinct a repeated field";
            LOG(ERROR) << m_err;
            return false;
        }
    }

    return true;
}

bool SelectQuery::AnalyzeLimit(const Limit& limit) {
    if (limit.has_start()) {
        m_limit_start = limit.start();
    } else {
        m_limit_start = 0;
    }

    m_limit_number = limit.number();
    return true;
}

std::shared_ptr<Expression> SelectQuery::AnalyzeExpression(
    const RawExpression& expr) {

    if (expr.has_op()) {
        return AnalyzeOperatorExpression(expr);
    }

    return AnalyzeAtomicExpression(expr.atomic());
}

std::shared_ptr<Expression> SelectQuery::AnalyzeOperatorExpression(
    const RawExpression& raw_expr) {

    std::vector<ShrExpr> expr_list;
    std::vector<BQType> arg_type_list;

    CHECK(raw_expr.has_left());
    ShrExpr left = AnalyzeExpression(raw_expr.left());

    if (left == NULL) {
        return ShrExpr(static_cast<Expression*>(NULL));
    }
    arg_type_list.push_back(left->GetReturnType());
    expr_list.push_back(left);

    if (raw_expr.has_right()) {
        ShrExpr right = AnalyzeExpression(raw_expr.right());

        if (right == NULL) {
            return ShrExpr(static_cast<Expression*>(NULL));
        }

        arg_type_list.push_back(right->GetReturnType());
        expr_list.push_back(right);
    }

    std::vector<BQType> cast_info;
    const OperatorFunctionInfo* info =
        toft::Singleton<FunctionResolver>::Instance()->ResolveOperatorFunction(
            raw_expr.op(), arg_type_list, &cast_info);

    if (info == NULL ||
        static_cast<size_t>(info->m_arg_number) != expr_list.size()) {
        m_err = "No function matches the operator or arg number not "
            + std::string("match, expression_arg_number:")
            + toft::NumberToString(expr_list.size());
        LOG(ERROR) << m_err;
        return ShrExpr(static_cast<Expression*>(NULL));
    }

    // Operator function arguments may need to cast
    if (!AddCastFunctionExpression(cast_info, arg_type_list, &expr_list)) {
        return ShrExpr(static_cast<Expression*>(NULL));
    }

    OperatorExpression* expr = new OperatorExpression();

    expr->SetOperator(raw_expr.op(), info);
    expr->SetLeft(expr_list[0]);
    if (expr_list[0]->IsDistinct()) {
        expr->SetDistinct(true);
    }

    if (expr_list.size() > 1) {
        expr->SetRight(expr_list[1]);
        if (expr_list[1]->IsDistinct()) {
            expr->SetDistinct(true);
        }
    }

    return ShrExpr(expr);
}

std::shared_ptr<Expression> SelectQuery::AnalyzeAtomicExpression(
    const RawAtomicExpression& atomic) {

    // function expression
    if (atomic.has_function()) {
        return AnalyzeFunction(atomic.function());
    }

    // constant expression
    if (atomic.has_integer() ||
        atomic.has_floating() ||
        atomic.has_boolean() ||
        atomic.has_char_string()) {
        return AnalyzeConstExpression(atomic);
    }

    // column expression
    if (atomic.has_column()) {
        return AnalyzeColumnExpression(atomic.column());
    }

    // nested expression
    if (atomic.has_expression()) {
        return AnalyzeExpression(atomic.expression());
    }

    m_err = "AtomicExpression is not set!!!";
    LOG(ERROR) << m_err;
    return ShrExpr(static_cast<Expression*>(NULL));
}

std::shared_ptr<Expression> SelectQuery::AnalyzeConstExpression(
    const RawAtomicExpression& atomic) {

    if (atomic.has_integer()) {
        int64_t value = atomic.integer();

        DatumBlock block(BigQueryType::INT64, static_cast<void*>(&value));
        ConstExpression* e = new ConstExpression(block);

        return ShrExpr(e);
    }

    if (atomic.has_floating()) {
        double value = atomic.floating();

        DatumBlock block(BigQueryType::DOUBLE, static_cast<void*>(&value));
        ConstExpression* e = new ConstExpression(block);

        return ShrExpr(e);
    }

    if (atomic.has_boolean()) {
        bool value = atomic.boolean();

        DatumBlock block(BigQueryType::BOOL, static_cast<void*>(&value));
        ConstExpression* e = new ConstExpression(block);

        return ShrExpr(e);
    }

    if (atomic.has_char_string()) {
        const std::string& s = atomic.char_string().char_string();

        DatumBlock block(BigQueryType::STRING,
                         static_cast<void*>(const_cast<std::string*>(&s)));
        ConstExpression* e = new ConstExpression(block);
        return ShrExpr(e);
    }

    return ShrExpr(static_cast<Expression*>(NULL));
}

std::shared_ptr<Expression> SelectQuery::AnalyzeFunction(
    const RawFunction& raw_function) {
    std::string fn_name = raw_function.function_name().char_string();
    toft::StringToUpper(&fn_name);

    std::vector<ShrExpr> argument_expr_list;

    if (!AnalyzeFunctionArguments(raw_function.args(), &argument_expr_list)) {
        return ShrExpr(static_cast<Expression*>(NULL));
    }

    std::vector<BQType> argument_type_list;
    std::vector<ShrExpr>::iterator iter;
    bool is_argument_agg = false;

    for (iter = argument_expr_list.begin();
         iter != argument_expr_list.end();
         ++iter) {
        argument_type_list.push_back((*iter)->GetReturnType());
        if ((*iter)->HasAggregate()) {
            is_argument_agg = true;
        }
    }

    const AggregateFunctionInfo* agg_fn_info = NULL;
    const PlainFunctionInfo* plain_fn_info = NULL;

    // agg function can only be used in targets
    std::vector<BQType> argument_type_cast_info;
    if (m_stage == kTargets) {
        agg_fn_info =
            toft::Singleton<FunctionResolver>::Instance()->ResolveAggregateFunction(
                fn_name, argument_type_list[0], &argument_type_cast_info);
    }

    plain_fn_info =
        toft::Singleton<FunctionResolver>::Instance()->ResolvePlainFunction(
            fn_name, argument_type_list, &argument_type_cast_info);

    if (agg_fn_info == NULL && plain_fn_info == NULL) {
        m_err = "Can't resolve function:" + fn_name;
        LOG(ERROR) << m_err;
        return ShrExpr(static_cast<Expression*>(NULL));
    }

    if (agg_fn_info != NULL && plain_fn_info != NULL) {
        m_err = "Multiple functions has same name:" + fn_name;
        LOG(ERROR) << m_err;
        return ShrExpr(static_cast<Expression*>(NULL));
    }

    if (agg_fn_info != NULL && is_argument_agg) {
        m_err = "Nested aggregate function in function ["
            + fn_name + "] is not allowed";
        LOG(ERROR) << m_err;
        return ShrExpr(static_cast<Expression*>(NULL));
    }

    // Function arguments may need to cast
    if (!AddCastFunctionExpression(
            argument_type_cast_info, argument_type_list, &argument_expr_list)) {
        return ShrExpr(static_cast<Expression*>(NULL));
    }

    std::shared_ptr<FunctionExpression> e;
    if (agg_fn_info != NULL) {
        ShrAggExpr f(new
                     AggregateFunctionExpression(m_agg_number++,
                                                 argument_expr_list[0]->IsDistinct()));
        f->SetAggFunction(agg_fn_info);
        m_agg_exprs.push_back(f);
        e = f;
    } else {
        bool dis_distinct = false;
        if (argument_expr_list.size() > 0) {
            dis_distinct = argument_expr_list[0]->IsDistinct();
        }
        e = std::shared_ptr<FunctionExpression>(new FunctionExpression(dis_distinct));
        e->SetPlainFunction(plain_fn_info);
    }

    e->SetArguments(argument_expr_list);
    return e;
}

bool SelectQuery::AddCastFunctionExpression(
    const std::vector<BQType>& cast_info,
    const std::vector<BQType>& type_list,
    std::vector<ShrExpr>* expr_list) {

    if (cast_info.size() != expr_list->size() ||
        expr_list->size() != type_list.size()) {
        m_err = "Function argument size unmatch, cast_info.size():"
            + toft::NumberToString(cast_info.size()) + "type_list.size():"
                + toft::NumberToString(type_list.size()) + "expr_list.size():"
                    + toft::NumberToString(expr_list->size());
        LOG(ERROR) << m_err;
        return false;
    }

    for (size_t i = 0; i < cast_info.size(); ++i) {
        if (cast_info[i] == type_list[i]) {
            continue;
        }

        // wrap expr_list[i] with cast function
        std::vector<ShrExpr> arg_list;
        arg_list.push_back((*expr_list)[i]);

        const CastFunctionInfo* cast_fn_info =
            toft::Singleton<FunctionResolver>::Instance()->ResolveCastFunction(
                type_list[i], cast_info[i]);
        if (cast_fn_info == NULL) {
            m_err = "Cast function does not exist, cast from:"
                + EnumToString(type_list[i]) + " to:"
                + EnumToString(cast_info[i]) + " failed";
            LOG(ERROR) << m_err;
            return false;
        }

        FunctionExpression* e = new FunctionExpression((*expr_list)[i]->IsDistinct());
        e->SetCastFunction(cast_fn_info);
        e->SetArguments(arg_list);
        (*expr_list)[i] = ShrExpr(e);
    }
    return true;
}

bool SelectQuery::AnalyzeFunctionArguments(const RawArguments& args,
                                           std::vector<ShrExpr>* argument_list) {
    if (args.arg_is_star() || !args.has_arg_list()) {

        RawArguments* new_args = const_cast<RawArguments*>(&args);
        std::vector<std::string> path_list;
        // find a required field path replace *
        if (m_table_entrys[0]->GetRequiredFieldPath(&path_list)) {
            new_args->set_arg_is_star(false);
            ColumnPath* colume_path = new_args->mutable_arg_list()
                ->add_expr_list()->mutable_atomic()->mutable_column();
            for (size_t i = 0; i < path_list.size(); i++) {
                colume_path->add_field_list()->set_char_string(path_list[i]);
            }
        } else {
            m_err = "Expression like COUNT(*) is not supported in this table";
            LOG(ERROR) << m_err;
            return false;
        }
    }

    const RawExpressionList& raw_expr_list = args.arg_list();

    for (int i = 0; i < raw_expr_list.expr_list_size(); ++i) {
        ShrExpr e = AnalyzeExpression(raw_expr_list.expr_list(i));

        if (e == NULL) {
            return false;
        }
        argument_list->push_back(e);
    }
    return true;
}

void SelectQuery::GetFieldListFromColumn(const ColumnPath& column,
                                         std::vector<std::string>* column_path,
                                         std::string* column_path_string) {
    column_path->clear();
    *column_path_string = "";
    for (int i = 0; i < column.field_list_size(); ++i) {
        column_path->push_back(column.field_list(i).char_string());
        if (i > 0) {
            *column_path_string += ".";
        }
        *column_path_string =
            *column_path_string + column.field_list(i).char_string();
    }
}

void SelectQuery::GetFieldListFromString(const std::string& column_str,
                                         std::vector<std::string>* column_path,
                                         std::string* column_path_string) {
    std::vector<std::string> result;
    column_path->clear();
    *column_path_string = "";
    toft::SplitString(column_str, ".", &result);
    for (size_t i = 0; i < result.size(); ++i) {
        column_path->push_back(result[i]);
        if (i > 0) {
            *column_path_string += ".";
        }
        *column_path_string =
            *column_path_string + result[i];
    }
}

bool SelectQuery::GetAffectColumnInfo(std::vector<std::string> column_path,
                                      ColumnInfo* info,
                                      ShrTableEntry* affected_table_entry,
                                      const std::string& column_path_string) {

    std::vector<ShrTableEntry>::iterator iter;
    std::string table_list;
    int match_number = 0;

    for (iter = m_table_entrys.begin();
         iter != m_table_entrys.end(); ++iter) {
        if ((*iter)->GetAffectColumnInfo(column_path, info) &&
            info->m_type != BigQueryType::MESSAGE) {
            *affected_table_entry = *iter;
            table_list = table_list + (*iter)->GetTableName() + ", ";
            match_number++;
        }
    }

    if (match_number == 0) {
        m_err = "No table entry has affect column: " + column_path_string;
        return false;
    }

    if (match_number > 1) {
        m_err = "Multiple table entry:" + table_list +
            " have affect columnpath:" + column_path_string;
        return false;
    }

    return true;
}

bool SelectQuery::GetColumnFromTable(std::vector<std::string> column_path,
                                     ColumnInfo* info,
                                     ShrTableEntry* affected_table_entry,
                                     const std::string& column_path_string) {
    std::vector<ShrTableEntry>::iterator iter;
    std::string table_list;
    int match_number = 0;
    for (iter = m_table_entrys.begin();
         iter != m_table_entrys.end(); ++iter) {
        if ((*iter)->GetColumnInfo(column_path, info) &&
            info->m_type != BigQueryType::MESSAGE) {
            *affected_table_entry = *iter;
            table_list = table_list + (*iter)->GetTableName() + ", ";
            match_number++;
        }
    }

    if (match_number == 0) {
        m_err = "No table entry has data field: " + column_path_string;
        return false;
    }

    if (match_number > 1) {
        m_err = "Multiple table entry:" + table_list +
            " have columnpath:" + column_path_string;
        return false;
    }
    return true;
}

std::shared_ptr<Expression> SelectQuery::AnalyzeColumnExpression(
    const ColumnPath& column) {

    std::vector<ShrTableEntry>::iterator iter;
    std::vector<std::string> column_path;
    std::string column_path_string;
    std::string table_list;
    bool has_distinct = false;
    ColumnInfo info;

    if (column.has_has_distinct()) {
        has_distinct = true;
    }

    GetFieldListFromColumn(column, &column_path, &column_path_string);

    ShrTableEntry affected_table_entry;

    if ((m_stage != kTargets) && has_distinct) {
        m_err = "distinct column is only support in Targets";
        LOG(ERROR) << m_err;
        return ShrExpr(static_cast<Expression*>(NULL));
    }

    // column_expr in having MUST be column in result schema
    // column_expr in Target, Where, Join must be in table entry
    if (m_stage == kHaving) {
        return AnalyzeColumnInHaving(column_path, column_path_string);
    }

    if (!GetColumnFromTable(column_path, &info,
                            &affected_table_entry, column_path_string)) {
        return ShrExpr(static_cast<Expression*>(NULL));
    }

    std::string table_name = affected_table_entry->GetTableName();


    // check the colume, when only required or optional is accept

    if (has_distinct) {
        if (info.m_repeat_level != 0) {
            m_err = std::string("Cannot distinct a repeated field ")
                + column_path_string;
            LOG(ERROR) << m_err;
            return ShrExpr(static_cast<Expression*>(NULL));
        }
    }

    if (m_has_join) {
        if (info.m_repeat_level != 0) {
            m_err = std::string("Cannot exist repeated field in join query: ")
                + column_path_string;
            LOG(ERROR) << m_err;
            return ShrExpr(static_cast<Expression*>(NULL));
        }
    }
    // We only read affected columns in tablet reader,
    // When we do evaluate in expression, we have to know the index of column in
    // retrieved tuple(containing only affect column),
    // so we keep affect_id in column expression
    size_t affect_id = this->AddAffectedColumnInfo(info, table_name, has_distinct);
    affected_table_entry->AddAffectedColumnInfo(info, affect_id, has_distinct);

    ColumnExpression* e = new ColumnExpression(info, table_name,
                                               affect_id, has_distinct);

    return ShrExpr(e);
}

std::shared_ptr<Expression> SelectQuery::AnalyzeColumnInHaving(
    const std::vector<std::string>& column_path,
    const std::string column_path_string) {
    ShrExpr having_expr = ResolveColumnExprInResultSchema(column_path);

    if (having_expr == NULL) {
        having_expr = ResolveColumnExprInTargetAlias(column_path);
    }

    if (having_expr == NULL) {
        m_err = std::string("Can't find column_path:") + column_path_string
            + " for having, you have to use the full path or target alias";
        LOG(ERROR) << m_err;
    }
    return having_expr;
}

std::shared_ptr<Expression> SelectQuery::ResolveColumnExprInTargetAlias(
    const std::vector<std::string>& column_path) {
    if (column_path.size() > 1U) {
        return ShrExpr(static_cast<Expression*>(NULL));
    }

    for (size_t i = 0; i < m_target_list.size(); ++i) {
        if (m_target_list[i]->GetAlias() == NULL) {
            continue;
        }

        const std::string& alias_name = *(m_target_list[i]->GetAlias());
        if (alias_name == column_path[0]) {
            const ColumnInfo& info = m_result_column_info[i];
            ColumnExpression* e = new ColumnExpression(
                info, SchemaBuilder::kQueryResultMessageName, i, false);
            return ShrExpr(e);
        }
    }
    return ShrExpr(static_cast<Expression*>(NULL));
}

std::shared_ptr<Expression> SelectQuery::ResolveColumnExprInResultSchema(
    const std::vector<std::string>& column_path) {

    size_t target_id;
    ColumnInfo info;

    if (!GetColumnInfoInResultSchema(column_path, &info, &target_id)) {
        return ShrExpr(static_cast<Expression*>(NULL));
    }

    ColumnExpression* e = new ColumnExpression(
        info, SchemaBuilder::kQueryResultMessageName, target_id, false);
    return ShrExpr(e);
}

bool SelectQuery::GetColumnInfoInResultSchema(
    const std::vector<std::string>& column_path,
    ColumnInfo* column_info,
    size_t* target_id) {
    size_t i = 0;

    for (i = 0; i < m_result_column_info.size(); ++i) {
        const ColumnInfo& info = m_result_column_info[i];

        // column length not match
        if (column_path.size() != info.m_column_path.size()) {
            continue;
        }

        // is i-th column match
        size_t j = 0;
        for (j = 0; j < info.m_column_path.size(); ++j) {
            if (column_path[j] != info.m_column_path[j]) {
                break;
            }
        }

        // find a match column
        if (j == info.m_column_path.size()) {
            *column_info = info;
            *target_id = i;
            return true;
        }
    }
    return false;
}

size_t SelectQuery::AddAffectedColumnInfo(const ColumnInfo& info,
                                          const std::string& table_name,
                                          bool has_distinct) {
    size_t i;
    for (i = 0; i < m_affected_column_info.size(); i++) {
        const std::string column_path_string
            = m_affected_column_info[i].m_column_info.m_column_path_string;

        if (column_path_string == info.m_column_path_string) {
            m_affected_column_info[i].m_affect_times++;
            return m_affected_column_info[i].m_affect_id;
        }
    }

    size_t affect_id = i;
    m_affected_column_info.push_back(AffectedColumnInfo(info, affect_id,
                                                        table_name, has_distinct));

    return affect_id;
}

std::vector<std::shared_ptr<AggregateFunctionExpression> >
SelectQuery::GetAggregateExpressionsInQuery() {
    std::vector<ShrAggExpr> aggs;
    for (size_t i = 0; i < m_target_list.size(); ++i) {
        const std::vector<ShrAggExpr> aggs_in_target =
            m_target_list[i]->GetAggregateFunctionExpression();
        aggs.insert(aggs.end(), aggs_in_target.begin(), aggs_in_target.end());
    }
    return aggs;
}

} // namespace compiler
} // namespace gunir

