// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#include "gunir/rpc_client.h"

#include "thirdparty/gflags/gflags.h"

DEFINE_string(gunir_rpcclient_log_level, "ERROR", "");

DEFINE_bool(gunir_rpcclient_traffic_limit_enabled, false, "");
DEFINE_int32(gunir_rpcclient_traffic_limit_max_inflow, 10,
             "max in-traffic bandwidth (in MB/s)");
DEFINE_int32(gunir_rpcclient_traffic_limit_max_outflow, 10,
             "max out-traffic bandwidth (in MB/s)");

DEFINE_int32(gunir_rpcclient_max_pending_buffer_size, 2,
             "max pending buffer size (in MB) for server");
DEFINE_int32(gunir_rpcclient_work_thread_num, 4, "");


namespace gunir {

trident::RpcChannelOptions RpcClientBase::m_channel_options;
std::map<std::string, trident::RpcChannel*> RpcClientBase::m_rpc_channel_list;
trident::RpcClientOptions RpcClientBase::m_rpc_client_options;
trident::RpcClient RpcClientBase::m_rpc_client;
toft::Mutex RpcClientBase::m_mutex;

} // namespace gunir
