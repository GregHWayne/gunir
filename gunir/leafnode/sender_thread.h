// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:


#ifndef  GUNIR_LEAFNODE_SENDER_THREAD_H
#define  GUNIR_LEAFNODE_SENDER_THREAD_H

#include <string>
#include <vector>

#include "toft/base/scoped_ptr.h"
#include "toft/base/uncopyable.h"
#include "toft/system/threading/base_thread.h"
#include "toft/system/threading/mutex.h"

#include "gunir/proto/task.pb.h"
#include "gunir/proto/stemnode_rpc.pb.h"
#include "gunir/leafnode/server_thread.h"

namespace gunir {

class LeafTaskSpec;
class ReportTaskResultRequest;
class ReportTaskResultResponse;

namespace io {
class TabletWriter;
class TabletReader;
}  // namespace io

namespace leafnode {

class SenderManager;

class SenderThread : public ServerThread {
    DECLARE_UNCOPYABLE(SenderThread);

public:
    SenderThread(SenderManager* sender_manager,
                 int32_t thread_index);
    ~SenderThread();

protected:
    void Entry();

protected:
    SenderManager *m_sender_manager;
};

}  // namespace leafnode
}  // namespace gunir

#endif  // GUNIR_LEAFNODE_SENDER_THREAD_H

