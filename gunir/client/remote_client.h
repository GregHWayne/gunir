// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#ifndef GUNIR_CLIENT_REMOTE_CLIENT_H
#define GUNIR_CLIENT_REMOTE_CLIENT_H

#include "gunir/client/base_client.h"

#include "toft/system/threading/thread_pool.h"

#include "gunir/proto/master_rpc.pb.h"

namespace gunir {
namespace master {
class MasterClient;
}

namespace client {

class RemoteClient : public BaseClient {
public:
    explicit RemoteClient();
    ~RemoteClient();

protected:
    bool SubmitJob(const SubmitJobRequest* request,
                   SubmitJobResponse* response);

    bool GetJobResult(const GetJobResultRequest* request,
                      GetJobResultResponse* response);

    bool GetMetaInfo(const GetMetaInfoRequest* request,
                     GetMetaInfoResponse* response);

    bool AddTable(const AddTableRequest* request,
                  AddTableResponse* response);

private:
    toft::scoped_ptr<master::MasterClient> m_master_client;
    toft::scoped_ptr<toft::ThreadPool> m_thread_pool;
};

} // namespace client
} // namespace gunir


#endif // GUNIR_CLIENT_REMOTE_CLIENT_H
