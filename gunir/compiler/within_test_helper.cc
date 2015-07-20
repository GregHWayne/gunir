// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
// 
//
// Description:

#include "gunir/compiler/within_test_helper.pb.h"
#include "gunir/compiler/compiler_test_helper.h"

DEFINE_bool(multi_level, false, "create multi_level_agg test data");

namespace gunir {
namespace compiler {

void AggLinks(const Document& doc, within::MultiLevelAggregate* agg);
void AggName(const Document& doc, within::MultiLevelAggregate* agg);

/*
 * CreateMultiLevelAggregate create result for the following query
        " SELECT docid,"
        " MAX(links.backward) WITHIN RECORD AS max_bwd,"
        " SUM(links.backward) WITHIN links AS sum_bwd,"
        " COUNT(name.language.code) WITHIN RECORD AS cnt_record_code,"
        " COUNT(name.language.code) WITHIN name AS cnt_name_code,"
        " COUNT(name.language.code) WITHIN name.language AS cnt_lang_code"
        " FROM Document;";
*/
::google::protobuf::Message* CreateMultiLevelAggregate(const PBMessage* input) {
    const Document* pdoc = dynamic_cast<const Document*>(input); // NOLINT
    const Document& doc = *pdoc;

    within::MultiLevelAggregate agg;

    agg.set_docid(doc.docid());
    AggLinks(doc, &agg);
    AggName(doc, &agg);

    return new within::MultiLevelAggregate(agg);
}

/*
 * CreateAggregateForward create result for the following query
        " SELECT docid, "
        " MAX(links.forward) WITHIN RECORD as max_fwd, "
        " MIN(links.forward) WITHIN RECORD as min_fwd, "
        " AVG(links.forward) WITHIN RECORD as avg_fwd, "
        " SUM(links.forward) WITHIN RECORD as sum_fwd, "
        " COUNT(links.forward) WITHIN RECORD as count_fwd "
        " FROM Document;";
*/
::google::protobuf::Message* CreateAggregateForward(const PBMessage* input) {
    const Document* pdoc = dynamic_cast<const Document*>(input); // NOLINT
    const Document& doc = *pdoc;

    double max = 0;
    double min = INT_MAX;
    double sum = 0;
    int64_t count = 0;

    const Links& link = doc.links();
    for (int i = 0; i < link.forward_size(); ++i) {
        if (link.forward(i) > max) {
            max = link.forward(i);
        }
        if (link.forward(i) < min) {
            min = link.forward(i);
        }
        sum += link.forward(i);
    }

    count = link.forward_size();

    within::AggregateForward record;
    record.set_docid(doc.docid());
    if (count == 0) {
        return new within::AggregateForward(record);
    }

    record.set_max_fwd(max);
    record.set_min_fwd(min);
    record.set_avg_fwd(sum / count);
    record.set_sum_fwd(sum);
    record.set_count_fwd(count);
    return new within::AggregateForward(record);
}

void AggLinks(const Document& doc, within::MultiLevelAggregate* agg) {
    if (!doc.has_links()) {
        return;
    }

    if (doc.links().backward_size() == 0) {
        within::Links* l = agg->mutable_links();
        l = NULL;
        return;
    }

    const Links& links = doc.links();
    int64_t max_bwd = links.backward(0);
    double sum_bwd = 0;

    for (int i = 0; i < links.backward_size(); ++i) {
        if (links.backward(i) > max_bwd) {
            max_bwd = links.backward(i);
        }
        sum_bwd += links.backward(i);
    }

    agg->set_max_bwd(max_bwd);
    (agg->mutable_links())->set_sum_bwd(sum_bwd);
}

void AggName(const Document& doc, within::MultiLevelAggregate* agg) {
    if (doc.name_size() == 0) {
        return;
    }

    int64_t cnt_record_code = 0;

    for (int i = 0; i < doc.name_size(); ++i) {
        within::Name* name = agg->add_name();

        int64_t cnt_name_code = 0;

        for (int j = 0; j < doc.name(i).language_size(); ++j) {
            within::Language* language = name->add_language();

            if (doc.name(i).language(j).has_code()) {
                cnt_name_code++;
                language->set_cnt_lang_code(1);
            }
        }
        if (cnt_name_code != 0) {
            name->set_cnt_name_code(cnt_name_code);
        }
        cnt_record_code += cnt_name_code;
    }

    if (cnt_record_code != 0) {
        agg->set_cnt_record_code(cnt_record_code);
    }
}

void CreateAggregateForwardTestData() {
    CreateTestData("aggregate_forward",
                   "./testdata/within_test/Document.proto",
                   "Document",
                   "./testdata/within_test/aggregate_forward.proto",
                   "AggregateForward",
                   RandDocument,
                   CreateAggregateForward);
}

void CreateMultiLevelAggregateTestData() {
    CreateTestData("multi_level_agg",
                   "./testdata/within_test/Document.proto",
                   "Document",
                   "./testdata/within_test/multi_level_agg.proto",
                   "MultiLevelAggregate",
                   RandDocument,
                   CreateMultiLevelAggregate);
}

} // namespace compiler
} // namespace gunir

int main(int argc, char** argv) {
    google::ParseCommandLineFlags(&argc, &argv, false);

    if (FLAGS_multi_level) {
        LOG(ERROR) << "CreateMultiLevelAggregateTestData";
        ::gunir::compiler::CreateMultiLevelAggregateTestData();
    } else {
        LOG(ERROR) << "CreateAggregateForwardTestData";
        ::gunir::compiler::CreateAggregateForwardTestData();
    }
    return 0;
}
