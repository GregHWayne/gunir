// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include <vector>

#include "gunir/leafnode/task_worker.h"

#include "gunir/io/tablet_scanner.h"
#include "gunir/io/tablet_schema.pb.h"
#include "gunir/leafnode/task_container.h"
#include "gunir/leafnode/task_container_pool.h"
#include "gunir/utils/filename_tool.h"


namespace gunir {
namespace leafnode {

TaskWorker::TaskWorker(TaskContainerPool* container_pool)
    : m_container_pool(container_pool), m_container(NULL), m_read_bytes(0) {}

TaskWorker::~TaskWorker() {
    if (m_container != NULL) {
        delete m_container;
    }
    for (size_t i = 0 ; i < m_tablet_readers.size(); i++) {
        delete m_scanners[i];
        delete m_tablet_readers[i];
    }
    m_tablet_readers.clear();
    m_scanners.clear();
}

bool TaskWorker::OpenResultContainer() {
    TaskContainer* task_container = m_container_pool->NewContainer();
    task_container->Init(m_spec->task_info(), m_spec->parent_task_info());
    if (m_container != NULL) {
        delete m_container;
    }
    m_container = task_container;
    return true;
}

bool TaskWorker::OpenInputTabletReader() {
    for (size_t i = 0; i < m_tablet_readers.size(); ++i) {
        const ScannerInput& scanner_input =
            (m_spec->task_input()).scanner_input(i);
        const std::string& filename = scanner_input.tablet().name();
        std::vector<std::string> column_names;
        std::string column_name_string;

        for (int j = 0; j < scanner_input.column_names_size(); ++j) {
            column_names.push_back(scanner_input.column_names(j));
            column_name_string = column_name_string + column_names[j];
        }

        LOG(INFO) << "OpenInputTabletReader filename: " <<filename
            << " Columns:" << column_name_string;
        // TODO(alaxwang) add test data  assume open is right
        if (!m_tablet_readers[i]->Init(filename, column_names)) {
            return false;
        }
    }
    return true;
}

bool TaskWorker::Reset(LeafTaskSpec* spec, MemPool* mempool) {
    m_spec = spec;
    int32_t input_size = (spec->task_input()).scanner_input_size();
    for (int32_t i = 0; i < input_size; ++i) {
        io::TabletReader* tablet_reader = new io::TabletReader(mempool);
        io::TabletScanner* scanner = new io::TabletScanner(tablet_reader);
        m_tablet_readers.push_back(tablet_reader);
        m_scanners.push_back(scanner);
    }

    m_executor.reset(new compiler::Executor());

    return m_executor->Init(
        m_spec->task_input().exec_plan(),
        m_spec->task_input().result_proto(),
        m_spec->task_input().result_message());
}

bool TaskWorker::Open() {
    if (!OpenResultContainer()) {
        return false;
    }

    if (!OpenInputTabletReader()) {
        return false;
    }

    return true;
}

bool TaskWorker::Run() {
    std::vector<io::Scanner*> scanners;
    for (size_t i = 0 ; i < m_scanners.size(); ++i) {
        scanners.push_back(m_scanners[i]);
    }

    m_executor->SetScanner(scanners);
    m_executor->SetContainer(m_container);
    return m_executor->Run();
}

bool TaskWorker::Close() {
    if (!m_container->Close()) {
        return false;
    }

    for (size_t i = 0 ; i < m_tablet_readers.size(); i++) {
        if (!m_tablet_readers[i]->Close()) {
            return false;
        }
        m_read_bytes += m_tablet_readers[i]->GetTabletReadBytes();
        delete m_scanners[i];
        delete m_tablet_readers[i];
    }
    m_tablet_readers.clear();
    m_scanners.clear();

    m_container_pool->AddContainer(m_container);
    m_container = NULL;
    return true;
}

int64_t TaskWorker::GetReadBytes() const {
    return m_read_bytes;
}

}  // namespace leafnode
}  // namespace gunir
