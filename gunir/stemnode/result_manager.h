// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_STEMNODE_RESULT_MANAGER_H
#define  GUNIR_STEMNODE_RESULT_MANAGER_H

#include <map>
#include <vector>

#include "toft/base/string/string_piece.h"
#include "toft/system/threading/rwlock.h"

#include "gunir/proto/stemnode_rpc.pb.h"
#include "gunir/proto/task.pb.h"

namespace gunir {
namespace stemnode {

class ResultInfo;

class ResultManager {
public:
    ResultManager();
    ~ResultManager();

    void Reset(const InterTaskSpec& spec);

    void ReportResultSize(const ReportResultSizeRequest* request,
                          ReportResultSizeResponse* response);

    bool AddAndJudgeIsFull(const ReportTaskResultRequest* request,
                           ReportTaskResultResponse* response,
                           bool* is_add);

    void GetAllResult(std::vector<toft::StringPiece>* vec);

    void Clear() {
        ClearResultInfo();
    }

private:
    ResultInfo* GetResultInfo(uint32_t id);
    void InitResultInfo(const InterTaskInput& input);
    void ClearResultInfo();
    bool IsFinished();

private:
    mutable toft::RwLock m_rwlock;
    uint32_t m_finished_child;
    uint32_t m_least_child_number;
    InterTaskSpec m_inter_task_spec;
    std::map<uint32_t, ResultInfo*> m_infos;
};

}  // namespace stemnode
}  // namespace gunir

#endif  // GUNIR_STEMNODE_RESULT_MANAGER_H
