// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//


#ifndef GUNIR_RPC_CLIENT_H
#define GUNIR_RPC_CLIENT_H

#include <string>

#include "toft/base/scoped_ptr.h"
// #include "toft/system/threading/this_thread.h"
#include "toft/system/threading/thread_pool.h"
#include "trident/pbrpc.h"

#include "gunir/proto/status_code.pb.h"
// #include "gunir/proto/proto_helper.h"
#include "gunir/utils/ip_address.h"

DECLARE_string(gunir_rpcclient_log_level);
DECLARE_bool(gunir_rpcclient_traffic_limit_enabled);
DECLARE_int32(gunir_rpcclient_traffic_limit_max_inflow);
DECLARE_int32(gunir_rpcclient_traffic_limit_max_outflow);
DECLARE_int32(gunir_rpcclient_max_pending_buffer_size);
DECLARE_int32(gunir_rpcclient_work_thread_num);

namespace gunir {

template <class Request, class Response, class Callback>
struct RpcCallbackParam {
    trident::RpcController* rpc_controller;
    const Request* request;
    Response* response;
    Callback* closure;
    std::string tips;
    toft::ThreadPool* thread_pool;

    RpcCallbackParam(trident::RpcController* ctrler, const Request* req,
                     Response* resp, Callback* cb, const std::string& str,
                     toft::ThreadPool* tpool)
        : rpc_controller(ctrler), request(req), response(resp),
          closure(cb), tips(str), thread_pool(tpool) {}
};

class RpcClientBase {
public:
    static void SetOption(int32_t max_inflow = FLAGS_gunir_rpcclient_traffic_limit_max_inflow,
                          int32_t max_outflow = FLAGS_gunir_rpcclient_traffic_limit_max_outflow,
                          int32_t pending_buffer_size = FLAGS_gunir_rpcclient_max_pending_buffer_size,
                          int32_t thread_num = FLAGS_gunir_rpcclient_work_thread_num,
                          std::string log_level = FLAGS_gunir_rpcclient_log_level) {
        if (log_level == "NOTICE") {
            TRIDENT_SET_LOG_LEVEL(NOTICE);
        } else {
            TRIDENT_SET_LOG_LEVEL(ERROR);
        }

        if (FLAGS_gunir_rpcclient_traffic_limit_enabled
            && -1 != max_inflow) {
            m_rpc_client_options.max_throughput_in = max_inflow;
        }
        if (FLAGS_gunir_rpcclient_traffic_limit_enabled
            && -1 != max_outflow) {
            m_rpc_client_options.max_throughput_out = max_outflow;
        }
        if (-1 != pending_buffer_size) {
            m_rpc_client_options.max_pending_buffer_size = pending_buffer_size;
        }
        if (-1 != thread_num) {
            m_rpc_client_options.work_thread_num = thread_num;
        }
        m_rpc_client.ResetOptions(m_rpc_client_options);

        trident::RpcClientOptions new_options = m_rpc_client.GetOptions();
        LOG(INFO) << "set rpc option: ("
            << "log_level: " << log_level
            << "max_inflow: " << new_options.max_throughput_in
            << " MB/s, max_outflow: " << new_options.max_throughput_out
            << " MB/s, max_pending_buffer_size: " << new_options.max_pending_buffer_size
            << " MB, work_thread_num: " << new_options.work_thread_num
            << ")";
    }

    RpcClientBase() : m_rpc_channel(NULL) {}
    virtual ~RpcClientBase() {}

protected:
    virtual void ResetClient(const std::string& server_addr) {
        std::map<std::string, trident::RpcChannel*>::iterator it;
        m_mutex.Lock();
        it = m_rpc_channel_list.find(server_addr);
        if (it != m_rpc_channel_list.end()) {
            m_rpc_channel = it->second;
        } else {
            m_rpc_channel = m_rpc_channel_list[server_addr]
                = new trident::RpcChannel(&m_rpc_client, server_addr,
                                              m_channel_options);
        }
        m_mutex.Unlock();
    }

protected:
    trident::RpcChannel* m_rpc_channel;

    static trident::RpcChannelOptions m_channel_options;
    static std::map<std::string, trident::RpcChannel*> m_rpc_channel_list;
    static trident::RpcClientOptions m_rpc_client_options;
    static trident::RpcClient m_rpc_client;
    static toft::Mutex m_mutex;
};

template<class ServerType>
class RpcClient : public RpcClientBase {
public:
    RpcClient(const std::string& addr) {
        ResetClient(addr);
    }
    virtual ~RpcClient() {}

    std::string GetConnectAddr() const {
        return m_server_addr;
    }

protected:
    virtual void ResetClient(const std::string& server_addr) {
        if (m_server_addr == server_addr) {
            // VLOG(5) << "address [" << server_addr << "] not be applied";
            return;
        }
        /*
        IpAddress ip_address(server_addr);
        if (!ip_address.IsValid()) {
            LOG(ERROR) << "invalid address: " << server_addr;
            return;
        }
        */
        RpcClientBase::ResetClient(server_addr);
        m_server_client.reset(new ServerType(m_rpc_channel));
        m_server_addr = server_addr;
        //VLOG(5) << "reset connected address to: " << server_addr;

    }

    template <class Request, class Response, class Callback>
    bool SendMessageWithRetry(void(ServerType::*func)(
                              google::protobuf::RpcController*, const Request*,
                              Response*, google::protobuf::Closure*),
                              const Request* request, Response* response,
                              Callback* closure, const std::string& tips,
                              int32_t rpc_timeout, toft::ThreadPool* thread_pool) {
        if (NULL == m_server_client.get()) {
            if (closure == NULL) {
                return false;
            }
            toft::Closure<void ()>* done = toft::NewClosure(
                &RpcClient::template UserCallback<Request, Response, Callback>,
                request, response, closure, true,
                (int)trident::RPC_ERROR_RESOLVE_ADDRESS);
            thread_pool->AddTask(done);
            return true;
        }
        trident::RpcController* rpc_controller =
            new trident::RpcController;
        rpc_controller->SetTimeout(rpc_timeout);

        google::protobuf::Closure* done = NULL;
        if (closure != NULL) {
            RpcCallbackParam<Request, Response, Callback>* param =
                new RpcCallbackParam<Request, Response, Callback>(rpc_controller,
                        request, response, closure, tips, thread_pool);
            done = google::protobuf::NewCallback(
                &RpcClient::template RpcCallback<Request, Response, Callback>,
                param);
        }
        (m_server_client.get()->*func)(rpc_controller, request, response, done);
        return true;
    }

    template <class Request, class Response, class Callback>
    static void RpcCallback(RpcCallbackParam<Request, Response, Callback>* param) {
        trident::RpcController* rpc_controller = param->rpc_controller;
        const Request* request = param->request;
        Response* response = param->response;
        Callback* closure = param->closure;
        toft::ThreadPool* thread_pool = param->thread_pool;

        bool failed = rpc_controller->Failed();
        int error = rpc_controller->ErrorCode();
        if (failed) {
            //LOG(ERROR) << "RpcRequest failed: " << param->tips
            //    << ". Reason: " << rpc_controller->ErrorText();
        }
        delete rpc_controller;
        delete param;

        toft::Closure<void ()>* done = toft::NewClosure(
            &RpcClient::template UserCallback<Request, Response, Callback>,
            request, response, closure, failed, error);
        thread_pool->AddTask(done);
    }

    template <class Request, class Response, class Callback>
    static void UserCallback(const Request* request, Response* response,
                             Callback* closure, bool failed, int error) {
        closure->Run((Request*)request, response, failed, error);
    }

    virtual bool PollAndResetServerAddr() {
        return true;
    }

    virtual bool IsRetryStatus(const StatusCode& status) {
        return false;
    }

private:
    toft::scoped_ptr<ServerType> m_server_client;
    std::string m_server_addr;
};

} // namespace gunir

#endif // GUNIR_RPC_CLIENT_H
