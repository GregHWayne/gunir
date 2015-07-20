// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/stemnode/task_container.h"

#include "toft/base/string/number.h"
#include "toft/storage/file/file.h"

#include "gunir/io/slice.h"
#include "gunir/io/slice_writer.h"
#include "gunir/io/tablet_schema.pb.h"
#include "gunir/utils/filename_tool.h"

DECLARE_int64(gunir_job_result_size_limit);

namespace gunir {
namespace stemnode {

TaskContainer::TaskContainer(io::SliceWriter * writer) {
    if (writer == NULL) {
        m_writer.reset(new io::SliceWriter());
    } else {
        m_writer.reset(writer);
    }
    m_output_files.clear();
    m_slice_number = 0;
    m_result_size = 0;
    m_result_too_large = false;
}

TaskContainer::~TaskContainer() {}

bool TaskContainer::Init(const InterTaskSpec& spec) {
    m_spec.CopyFrom(spec);

    std::string filename = GetCacheJobFileName(m_spec.task_info().job_id());
    io::SchemaDescriptor schema_descriptor;
    schema_descriptor.set_type("pb");
    schema_descriptor.set_description(
        m_spec.task_input().result_proto());
    schema_descriptor.set_record_name(
        m_spec.task_input().result_message());

    if (!m_writer->Open(m_spec.task_input().result_message(),
                        schema_descriptor, filename)) {
        LOG(ERROR) << "OpenResultTabletWriter BuildTabletSchema ERROR";
        return false;
    }

    return true;
}

bool TaskContainer::Insert(const std::vector<io::Slice>& slices) {
    if (m_result_too_large) {
        return true;
    }
    for (uint32_t i = 0; i < slices.size(); ++i) {
        m_result_size += slices[i].ByteSize();
    }
    if (m_result_size > FLAGS_gunir_job_result_size_limit) {
        LOG(WARNING) << "Job result size: "
            << m_result_size << " is greater than size limit: "
            << FLAGS_gunir_job_result_size_limit
            << ", the exceeded part is ignored.";
        m_result_too_large = true;
        return true;
    }

    m_slice_number += slices.size();
    return m_writer->Write(slices);
}

bool TaskContainer::Close() {
    LOG(INFO) << "Root task container close.";
    return m_writer->Close(&m_output_files);
}

void TaskContainer::FillReportTaskState(TaskState* task_state) {
    int64_t total_size = 0;
    for (uint32_t i = 0; i < m_output_files.size(); ++i) {
        task_state->add_result_tablet_files(m_output_files[i]);
        total_size += File::GetSize(m_output_files[i].c_str());
    }
    task_state->set_result_slice_number(m_slice_number);
    task_state->set_result_file_size(total_size);
    m_output_files.clear();
}

}  // namespace stemnode
}  // namespace gunir
