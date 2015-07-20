// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/rpc_server.h"

#include <stdint.h>

#include "toft/system/time/timestamp.h"
#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"

DEFINE_string(gunir_rpcserver_log_level, "ERROR", "");

DEFINE_bool(gunir_rpcserver_traffic_limit_enabled, false, "");
DEFINE_int32(gunir_rpcserver_traffic_limit_max_inflow, 10,
             "max in-traffic bandwidth (in MB/s)");
DEFINE_int32(gunir_rpcserver_traffic_limit_max_outflow, 10,
             "max out-traffic bandwidth (in MB/s)");

DEFINE_int32(gunir_rpcserver_retry_times, 3,
             "the retry times when server meets exception");
DEFINE_int32(gunir_rpcserver_retry_period, 1000,
             "the retry period (in ms) between both retry operation");
DEFINE_int32(gunir_rpcserver_error_notify_interval, 2000,
             "the interval (in ms) between two error notification");
DEFINE_int32(gunir_rpcserver_error_retry_times, 15,
             "the maximal retry times of same error");
DEFINE_int32(gunir_rpcserver_keep_alive_time, 1800,
             "the maximal interval (in sec) to keep idle connections");
DEFINE_int32(gunir_rpcserver_max_pending_buffer_size, 2,
             "max pending buffer size (in MB) for server");
DEFINE_int32(gunir_rpcserver_work_thread_num, 4, "");


namespace gunir {

RpcServer::RpcServer(const std::string& ip, uint32_t port) {
        m_server_impl.reset(new RpcServerTrident(ip, port));
}

RpcServer::~RpcServer() {
    StopServer();
}

void RpcServer::Entry() {
    while (!IsStopRequested()) {
        m_server_impl->Loop();
    }
}

bool RpcServer::StartServer() {
    Start();
    return true;
}

bool RpcServer::StopServer() {
    m_server_impl->LoopBreak();
    SendStopRequest();
    if (IsJoinable()) {
        Join();
    } else {
        VLOG(10) << "gunir server is not started";
    }
    return true;
}

void RpcServer::RegisterService(::google::protobuf::Service* service) {
    m_server_impl->RegisterService(service);
}

// Trident RPC implementation

RpcServerEventHandler::RpcServerEventHandler()
    : m_error_retry(0), m_error_previous_timestamp(0) {}

void RpcServerEventHandler::NotifyAcceptFailed(trident::RpcErrorCode error_code,
                                               const std::string& error_text) {
    LOG(ERROR) << "accept failed, reason: " << error_text;
    if (error_code == trident::RPC_ERROR_TOO_MANY_OPEN_FILES) {
        int64_t cur_timestamp = toft::GetTimestamp();
        if (m_error_retry == 0
            || cur_timestamp > m_error_previous_timestamp
                + FLAGS_gunir_rpcserver_error_notify_interval) {
            m_error_retry = 1;
        } else {
            m_error_retry++;
        }
        m_error_previous_timestamp = cur_timestamp;
        CHECK(m_error_retry < FLAGS_gunir_rpcserver_error_retry_times)
            << ", fail to tolerate error: " << error_text;
    }
}

RpcServerTrident::RpcServerTrident(const std::string& ip, uint16_t port)
    : m_address(ip, port), m_started(false),
      m_server(NULL) {
    if (FLAGS_gunir_rpcserver_log_level == "NOTICE") {
        TRIDENT_SET_LOG_LEVEL(NOTICE);
    } else {
        TRIDENT_SET_LOG_LEVEL(ERROR);
    }
    if (FLAGS_gunir_rpcserver_traffic_limit_enabled) {
        m_options.max_throughput_in =
            FLAGS_gunir_rpcserver_traffic_limit_max_inflow;
        m_options.max_throughput_out =
            FLAGS_gunir_rpcserver_traffic_limit_max_outflow;
    }
    m_options.max_pending_buffer_size =
        FLAGS_gunir_rpcserver_max_pending_buffer_size;
    m_options.work_thread_num = FLAGS_gunir_rpcserver_work_thread_num;
    m_options.keep_alive_time = FLAGS_gunir_rpcserver_keep_alive_time;
    m_event_handler = new RpcServerEventHandler;
    m_server.reset(new trident::RpcServer(m_options, m_event_handler));

    LOG(INFO) << "RPC server is activated (log level: "
        << FLAGS_gunir_rpcserver_log_level
        << ", max_inflow: " << m_options.max_throughput_in
        << " M/s, max_outflow: " << m_options.max_throughput_out
        << " M/s, keep_alive_time: " << m_options.keep_alive_time
        << " sec, max_pending_buffer_size: " << m_options.max_pending_buffer_size
        << " MB, work_thread_num: " << m_options.work_thread_num
        << ")";
}

RpcServerTrident::~RpcServerTrident() {}

void RpcServerTrident::RegisterService(::google::protobuf::Service* service) {
    m_server->RegisterService(service);
}

void RpcServerTrident::Loop() {
    uint32_t wait_time = FLAGS_gunir_rpcserver_retry_period;
    for (int32_t retry = 0; !m_started && retry < FLAGS_gunir_rpcserver_retry_times;
         ++retry) {
        m_started = m_server->Start(m_address.ToString());
        if (!m_started) {
            toft::ThisThread::Sleep(wait_time);
            wait_time *= 2;
            LOG(WARNING) << "server fail to start, retry = " << retry;
        }
    }
    CHECK(m_started) << ", fail to start RPC server";
    m_event.Wait();
}

void RpcServerTrident::LoopBreak() {
    m_server->Stop();
    m_event.Set();
}

} // namespace gunir
