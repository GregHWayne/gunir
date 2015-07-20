// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_SELECT_QUERY_H
#define  GUNIR_COMPILER_SELECT_QUERY_H

#include <map>
#include <set>
#include <string>
#include <vector>

#include "toft/base/shared_ptr.h"

#include "gunir/proto/table.pb.h"
#include "gunir/compiler/expression.h"
#include "gunir/compiler/parser/select_stmt.pb.h"
#include "gunir/compiler/parser/table_schema.h"
#include "gunir/compiler/query_convertor.h"
#include "gunir/compiler/schema_builder.h"
#include "gunir/compiler/table_entry.h"
#include "gunir/compiler/target.h"

namespace gunir {
namespace compiler {

class SelectQuery {
public:
    enum AggregateType {
        kWithinAgg,
        kGroupByAgg,
        kNoneAgg
    };

private:
    typedef ::google::protobuf::FileDescriptorProto PBFileDescriptorProto;
    typedef std::shared_ptr<TableEntry> ShrTableEntry;
    typedef std::shared_ptr<Target> ShrTarget;
    typedef std::shared_ptr<Expression> ShrExpr;
    typedef std::shared_ptr<AggregateFunctionExpression> ShrAggExpr;

    enum AnalyzeStage {
        kConvertQuery,
        kFromList,
        kTargets,
        kConstruct,
        kWhereClause,
        kJoin,
        kGroupBy,
        kHaving,
        kOrderBy,
        kDistinct,
        kLimit,
        kAll,
    };

public:
    explicit SelectQuery(const SelectStmt& stmt);

    std::vector<std::string> GetQueryTables() const;

    bool Init(const std::vector<TableInfo>& table_infos);

    bool Analyze();

    const std::vector<ShrTableEntry>& GetTableEntry() const {
        return m_table_entrys;
    }

    const std::vector<ShrTarget> GetTargets() const {
        return m_target_list;
    }

    const ShrExpr& GetWhere() const {
        return m_where;
    }

    const std::vector<ShrExpr>& GetJoinExpr() const {
        return m_join_exprs;
    }

    const std::vector<JoinOperator>& GetJoinOps() const {
        return m_join_ops;
    }

    const ShrExpr& GetHaving() const {
        return m_having;
    }

    bool GetLimit(int64_t* start, int64_t* number) const {
        if (m_limit_start == -1) {
            return false;
        }

        *start = m_limit_start;
        *number = m_limit_number;
        return true;
    }

    int GetJoinTableStart() const {
        return m_join_table_start;
    }

    const std::vector<ColumnInfo>& GetResultColumnInfo() const {
        return m_result_column_info;
    }
    std::string GetResultSchema() const {
        return m_final_schema;
    }

    std::string GetTransSchema() const {
        return m_trans_schema;
    }

    std::string GetReadableResultSchema() const {
        return SchemaBuilder::GetReadableSchema(m_final_schema);
    }

    std::string GetReadableTransSchema() const {
        return SchemaBuilder::GetReadableSchema(m_trans_schema);
    }

    const std::vector<ShrTarget>& GetTargets() { return m_target_list; }

    const std::vector<GroupByColumn>& GetGroupByColumns() const {
        return m_groupby_columns;
    }

    const std::vector<OrderByColumn>& GetOrderByColumns() const {
        return m_orderby_columns;
    }

    const std::vector<DistinctColumn>& GetDistinctColumns() const {
        return m_distinct_columns;
    }

    const std::vector<DistinctColumn>& GetDisAggColumns() const {
        return m_dis_agg_columns;
    }

    const std::vector<AffectedColumnInfo>& GetAffectedColumnInfo() const {
        return m_affected_column_info;
    }

    const std::string GetErrString() const {
        return m_err;
    }

    bool GetHasJoin() const {
        return m_has_join;
    }

    bool GetHasDistinct() const {
        return (m_distinct_columns.size() > 0);
    }

    AggregateType GetAggregateType() const { return m_aggregate_type; }
    std::vector<ShrAggExpr> GetAggregateExpressionsInQuery();

private:
    // supported
    bool AnalyzeTableEntry(const RawTable& raw_table);
    bool AnalyzeTableEntryList(const RawTableList& raw_tables);
    bool AnalyzeTargets(const RawTargetList& targets);
    bool AnalyzeWhereClause(const RawExpression& expr);
    bool AnalyzeLimit(const Limit& limit);
    bool AnalyzeWithin(const RawWithin& within, ShrTarget t);

    bool AnalyzeJoin(const RawJoin& join);
    bool AnalyzeGroupBy(const ColumnPathList& column_list);
    bool AnalyzeHaving(const RawExpression& expr);
    bool AnalyzeOrderBy(const OrderColumnPathList& order_column_list);
    bool AnalyzeDistinct();

    bool AnalyzeOrderByColumn(
        const OrderColumnPath& raw_orderby_column,
        OrderByColumn* orderby_column);

    bool AnalyzeOthers();

    bool ConstructResultSchema();
    bool ConstructFinalSchema(bool has_groupby);
    bool ConstructTransSchema();

    ShrTarget AnalyzeTarget(const RawTarget& raw_target, int i);
    bool CheckWithinAndGroupBy(const RawTarget& raw_target,
                               const ShrExpr& expr,
                               int target_id);
    ShrExpr AnalyzeExpression(const RawExpression& expr);

    ShrExpr AnalyzeOperatorExpression(const RawExpression& expr);
    ShrExpr AnalyzeAtomicExpression(const RawAtomicExpression& atomic);
    ShrExpr AnalyzeConstExpression(const RawAtomicExpression& atomic);

    ShrExpr AnalyzeFunction(const RawFunction& raw_function);
    ShrExpr AnalyzeColumnExpression(const ColumnPath& column);
    ShrExpr AnalyzeColumnInHaving(
        const std::vector<std::string>& column_path,
        const std::string column_path_string);

    bool AnalyzeFunctionArguments(const RawArguments& args,
                                  std::vector<ShrExpr>* argument_list);

    bool IsAllAffectedColumnInWithinPath(ShrTarget t,
                                         const ColumnInfo& within_column);

    void GetFieldListFromColumn(const ColumnPath& column,
                                std::vector<std::string>* column_path,
                                std::string* column_path_string);

    void GetFieldListFromString(const std::string& column_str,
                                std::vector<std::string>* column_path,
                                std::string* column_path_string);

    ShrExpr ResolveColumnExprInTargetAlias(
        const std::vector<std::string>& column_path);
    ShrExpr ResolveColumnExprInResultSchema(
        const std::vector<std::string>& column_path);
    bool GetColumnInfoInResultSchema(
        const std::vector<std::string>& column_path,
        ColumnInfo* column_info,
        size_t* target_id);

    bool AddCastFunctionExpression(
        const std::vector<BQType>& cast_info,
        const std::vector<BQType>& type_list,
        std::vector<ShrExpr >* expr_list);

    bool GetAffectColumnInfo(std::vector<std::string> column_path,
                             ColumnInfo* info,
                             ShrTableEntry* affected_table_entry,
                             const std::string& column_path_string);

    bool GetColumnFromTable(std::vector<std::string> column_path,
                            ColumnInfo* info,
                            ShrTableEntry* affected_table_entry,
                            const std::string& column_path_string);

    size_t AddAffectedColumnInfo(const ColumnInfo& info,
                                 const std::string& table_name,
                                 bool has_distinct);
    bool AffectAllColumns();

    inline bool HasGroupBy() {
        return (m_aggregate_type == kGroupByAgg);
    }

private:
    std::vector<ShrTableEntry> m_table_entrys;

    std::vector<ShrTarget> m_target_list;

    std::set<std::string> m_used_entry_name;

    ShrExpr m_where;

    std::vector<ShrExpr> m_join_exprs;

    std::vector<JoinOperator> m_join_ops;

    std::vector<GroupByColumn> m_groupby_columns;

    ShrExpr m_having;

    std::vector<OrderByColumn> m_orderby_columns;

    std::vector<DistinctColumn> m_distinct_columns;

    std::vector<DistinctColumn> m_dis_agg_columns;

    std::string m_final_schema;
    std::string m_trans_schema;

    TableManager m_table_manager;

    int m_limit_start;
    int m_limit_number;

    int m_join_table_start;

    AnalyzeStage m_stage;
    std::vector<AffectedColumnInfo> m_affected_column_info;

    // info of columns in result schema,
    // one-to-one relationship with targets in target list
    std::vector<ColumnInfo> m_result_column_info;

    std::vector<ShrAggExpr> m_agg_exprs;
    AggregateType m_aggregate_type;
    int m_agg_number;

    SelectStmt m_select_stmt;
    std::string m_err;

    bool m_has_join;

    bool m_select_star;

    std::map<std::string, std::string> m_alias_to_path;

    std::map<std::string, int> m_alias_to_target_id;

    std::map<std::string, std::string> m_path_to_alias;

    QueryConvertor m_query_convertor;
};

} // namespace compiler
} // namespace gunir

#endif  // GUNIR_COMPILER_SELECT_QUERY_H

