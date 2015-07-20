// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#ifndef  GUNIR_COMPILER_AGGREGATE_FUNCTION_EXPRESSION_H
#define  GUNIR_COMPILER_AGGREGATE_FUNCTION_EXPRESSION_H

#include <vector>

#include "toft/base/shared_ptr.h"
#include "thirdparty/glog/logging.h"

#include "gunir/compiler/function_expression.h"
#include "gunir/compiler/parser/big_query_types.h"
#include "gunir/compiler/parser/plan.pb.h"

namespace gunir {
namespace compiler {

class AggregateFunctionExpression : public FunctionExpression {
public:
    explicit AggregateFunctionExpression(size_t id, bool is_distinct)
        : FunctionExpression(is_distinct), m_agg_id(id),
          m_trans(NULL), m_final(NULL) {
    }

    AggregateFunctionExpression(const ExpressionProto& proto,
                                ExpressionInitializer* initializer)
        : FunctionExpression(proto, initializer),
          m_trans(NULL), m_final(NULL) {
        ParseFromProto(proto, initializer);
    }

    void Setup() {
        if (m_agg_fn->m_trans_type == BigQueryType::BYTES) {
            m_trans = new DatumBlock(m_agg_fn->m_trans_type,
                                     m_agg_fn->m_trans_type_size);
        } else {
            m_trans = new DatumBlock(m_agg_fn->m_trans_type);
        }

        m_final = new DatumBlock(m_agg_fn->m_final_type);
    }

    void TearDown() {
        delete m_trans;
        m_trans = NULL;
        delete m_final;
        m_final = NULL;
    }

    bool Evaluate(const std::vector<DatumBlock*>& tuple,
                  DatumBlock* return_value) const {
        *return_value = *m_final;
        return true;
    }

    void Transfer(const std::vector<DatumBlock*>& tuple) {
        CHECK_EQ(1U, m_expr_list.size());

        DatumBlock expr_result(m_expr_list[0]->GetReturnType());
        CHECK(m_expr_list[0]->Evaluate(tuple, &expr_result));

        DatumBlock* saved_trans = new DatumBlock(*m_trans);
        BQFunctionInfo info;
        info.return_datum = &(m_trans->m_datum);

        info.arg[0] = saved_trans->m_datum;
        info.is_arg_null[0] = saved_trans->m_is_null;
        info.arg_type[0] = saved_trans->GetType();

        info.arg[1] = expr_result.m_datum;
        info.is_arg_null[1] = expr_result.m_is_null;
        info.arg_type[1] = expr_result.GetType();

        (*m_agg_fn->m_trans_fn)(&info);

        m_trans->m_is_null = info.is_null;
        delete saved_trans;
    }

    void Merge(const DatumBlock* block) {
        DatumBlock* saved_trans = new DatumBlock(*m_trans);
        BQFunctionInfo info;
        info.return_datum = &(m_trans->m_datum);

        info.arg[0] = saved_trans->m_datum;
        info.is_arg_null[0] = saved_trans->m_is_null;
        info.arg_type[0] = saved_trans->GetType();

        info.arg[1] = block->m_datum;
        info.is_arg_null[1] = block->m_is_null;
        info.arg_type[1] = block->GetType();

        (*m_agg_fn->m_merge_fn)(&info);

        m_trans->m_is_null = info.is_null;
        delete saved_trans;
    }

    void Finalize() {
        if (m_agg_fn->m_final_fn == NULL) {
            *m_final = *m_trans;
            return;
        }

        BQFunctionInfo info;
        info.return_datum = &(m_final->m_datum);

        info.arg[0] = m_trans->m_datum;
        info.is_arg_null[0] = m_trans->m_is_null;
        info.arg_type[0] = m_trans->GetType();

        (*m_agg_fn->m_final_fn)(&info);

        m_final->m_is_null = info.is_null;
        return;
    }

    void CopyToProto(ExpressionProto* proto) const {
        FunctionExpressionProto* fn_proto = proto->mutable_function();
        fn_proto->set_agg_id(m_agg_id);
        FunctionExpression::CopyToProto(proto);
    }

    void ParseFromProto(const ExpressionProto& proto,
                        ExpressionInitializer* initializer) {
        const FunctionExpressionProto& fn_proto = proto.function();
        CHECK(fn_proto.has_agg_id());
        m_agg_id = fn_proto.agg_id();
    }

    BQType GetTransType() {
        return m_agg_fn->m_trans_type;
    }

    const DatumBlock& GetTransBlock() {
        return *m_trans;
    }

    size_t GetAggId() const { return m_agg_id; }

private:
    size_t m_agg_id;
    DatumBlock* m_trans;
    DatumBlock* m_final;
};

struct AggExprComparator {
    typedef std::shared_ptr<AggregateFunctionExpression> ShrAggExpr;
    bool operator()(const ShrAggExpr& a1, const ShrAggExpr& a2) const {
        return a1->GetAggId() < a2->GetAggId();
    }
};

} // compiler
} // gunir

#endif  // GUNIR_COMPILER_AGGREGATE_FUNCTION_EXPRESSION_H

