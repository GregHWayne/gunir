// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
#include "gunir/compiler/scan_plan.h"

#include "thirdparty/glog/logging.h"

namespace gunir {
namespace compiler {

ScanPlan::ScanPlan(const std::vector<ColumnInfo>& column_infos,
                   const std::vector<uint64_t>& affect_ids)
    : m_affect_ids(affect_ids),
      m_reader(new ScanReader()) {

    for (size_t i = 0; i < column_infos.size(); i++) {
        m_column_name_list.push_back(column_infos[i].m_column_path_string);
        m_column_types.push_back(column_infos[i].m_type);
    }
    m_tablet_info.set_name("");
}

ScanPlan::ScanPlan(const std::string& table_name,
                   const TabletInfo& tablet_info,
                   const std::vector<ColumnInfo>& column_infos,
                   const std::vector<uint64_t>& affect_ids)
    : m_table_name(table_name),
      m_tablet_info(tablet_info),
      m_affect_ids(affect_ids),
      m_reader(new ScanReader()) {

    for (size_t i = 0; i < column_infos.size(); i++) {
        m_column_name_list.push_back(column_infos[i].m_column_path_string);
        m_column_types.push_back(column_infos[i].m_type);
    }
}

ScanPlan::ScanPlan(const PlanProto& proto)
    : m_reader(new ScanReader()) {
    ParseFromProto(proto);
}

bool ScanPlan::GetNextTuple(const std::vector<DatumBlock*>& tuple,
                            uint32_t* select_level) {
    CHECK(tuple.size() == m_column_name_list.size()) << "tuple size not match";
    return m_reader->Read(m_column_types, tuple, select_level, m_affect_ids);
}

void ScanPlan::CopyToProto(PlanProto* proto) const {
    proto->set_type(PlanProto::kScan);
    ScanPlanProto* scan_proto = proto->mutable_scan();
    scan_proto->set_table_name(m_table_name);
    scan_proto->mutable_tablet()->CopyFrom(m_tablet_info);

    for (size_t i = 0; i < m_column_name_list.size(); ++i) {
        ColumnInfoProto* column_proto =
            (scan_proto->mutable_column_info_list())->Add();
        column_proto->set_name(m_column_name_list[i]);
        column_proto->set_type(
            BigQueryType::ConvertBQTypeToPBProtoType(m_column_types[i]));
    }

    // copy the affect_column_name to proto, but don't parse it
    // from proto cause it's not necessary in scan_plan
    for (size_t i = 0; i < m_affect_ids.size(); ++i) {
        uint64_t pos = m_affect_ids[i];
        ColumnInfoProto* column_proto =
            (scan_proto->mutable_affect_column_list())->Add();
        column_proto->set_name(m_column_name_list[pos]);
        column_proto->set_type(
            BigQueryType::ConvertBQTypeToPBProtoType(m_column_types[pos]));
    }

    for (size_t i = 0; i < m_affect_ids.size() ; ++i) {
        scan_proto->add_affect_ids(m_affect_ids[i]);
    }
}

void ScanPlan::ParseFromProto(const PlanProto& proto) {
    CHECK(proto.type() == PlanProto::kScan) << "ScanPlan parse from proto:"
        << proto.DebugString() << " failed";

    const ScanPlanProto& scan_proto = proto.scan();

    CHECK(scan_proto.has_tablet()) << "scan proto did not specify tablet name";
    m_table_name = scan_proto.table_name();
    m_tablet_info.CopyFrom(scan_proto.tablet());

    for (int i = 0; i < scan_proto.column_info_list_size(); ++i) {
        const ColumnInfoProto& column_proto = scan_proto.column_info_list(i);
        m_column_name_list.push_back(column_proto.name());
        m_column_types.push_back(
            BigQueryType::ConvertPBProtoTypeToBQType(column_proto.type()));
    }

    for (int i = 0; i < scan_proto.affect_ids_size(); ++i) {
        m_affect_ids.push_back(scan_proto.affect_ids(static_cast<uint64_t>(i)));
    }
}

} // namespace compiler
} // namespace gunir

