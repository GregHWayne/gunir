// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_STEMNODE_RESULT_INFO_H
#define  GUNIR_STEMNODE_RESULT_INFO_H

#include <map>
#include <vector>

#include "toft/base/scoped_ptr.h"
#include "toft/base/string/string_piece.h"
#include "toft/system/threading/rwlock.h"

#include "gunir/io/data_holder.h"
#include "gunir/proto/stemnode_rpc.pb.h"

namespace gunir {
namespace stemnode {

class ResultInfo {
public:
    ResultInfo();
    ~ResultInfo();

    void ReportResultSize(const ReportResultSizeRequest* request,
                          ReportResultSizeResponse* response);

    bool AddResult(const ReportTaskResultRequest* request,
                   ReportTaskResultResponse* response,
                   bool* is_add);

    uint32_t GetSize();
    toft::StringPiece GetResult();

private:
    void ClearResult();
    bool IsFinished();

private:
    mutable toft::RwLock m_rwlock;
    uint32_t m_size;
    toft::scoped_ptr<DataHolder> m_data_holder;

    bool m_is_finished;
    uint32_t m_next_sequence_id;
};

}  // namespace stemnode
}  // namespace gunir

#endif  // GUNIR_STEMNODE_RESULT_INFO_H

