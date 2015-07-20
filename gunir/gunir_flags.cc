// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#include <stdint.h>
#include "thirdparty/gflags/gflags.h"

/////////// common //////////
DEFINE_string(gunir_user_identity, "", "the identity for user to be executed");

DEFINE_string(gunir_role, "", "the role for binary to be executed");
DEFINE_int64(gunir_heartbeat_period, 3000, "");
DEFINE_int64(gunir_heartbeat_retry_period_factor, 1, "");
DEFINE_int32(gunir_heartbeat_retry_times, 5, "");

DEFINE_int32(gunir_heartbeat_timeout_period_factor, 10, "");
DEFINE_int64(gunir_job_result_size_limit, 64*1024*1024, "");

DEFINE_int64(gunir_blocking_queue_time, 1000, "");

////////// comipler /////////

DEFINE_string(gunir_planner_mode, "parallel",
              "the planner modle: simple|parallel");
DEFINE_int32(gunir_compiler_inter_plan_limit, 10,
             "limit of inter tasks that report to one inter");
DEFINE_int32(gunir_compiler_leaf_plan_limit, 1000,
             "limit of leaf tasks that report to one inter");


/////////  io  /////////

DEFINE_string(gunir_default_charset_encoding, "UTF-8",
              "tablet data storage using charset encoding");
DEFINE_string(gunir_job_cache_dir, "./job_cache/",
              "inter server's job cache dir ");
DEFINE_string(gunir_query_output_dir, "./output/",
              "user recieve the query output in this dir");
DEFINE_uint64(gunir_tablet_file_size, 64 * 1024 * 1024, "used for mempool and dump logic.");
DEFINE_uint64(gunir_max_record_size, 32 * 1024 * 1024, "for block serialization.");
DEFINE_string(gunir_csv_delim, ",", "CSV separated char, default Comma");

//////////  master //////////
DEFINE_string(gunir_master_addr, "127.0.0.1", "");
DEFINE_string(gunir_master_port, "10000", "");

DEFINE_int32(gunir_master_connect_retry_period, 1000, "");
DEFINE_int32(gunir_master_rpc_timeout_period, 5000, "");
DEFINE_int32(gunir_master_thread_min_num, 1, "");
DEFINE_int32(gunir_master_thread_max_num, 10, "");

DEFINE_int64(gunir_remove_job_waiting_time, 1000, "");

DEFINE_string(gunir_master_meta_dir, "./gunir_meta", "the location of persistent storage");
DEFINE_string(gunir_master_meta_table, "table_meta", "the location name of table meta");

DEFINE_int32(gunir_table_thread_min_num, 1, "");
DEFINE_int32(gunir_table_thread_max_num, 10, "");

DEFINE_int32(gunir_job_thread_min_num, 1, "");
DEFINE_int32(gunir_job_thread_max_num, 10, "");

DEFINE_int32(gunir_master_emit_retry_times, 3, "");
DEFINE_int32(gunir_master_emit_thread_min_num, 1, "");
DEFINE_int32(gunir_master_emit_thread_max_num, 6, "");


//////////  leaf node //////////
DEFINE_string(gunir_leafnode_addr, "127.0.0.1", "");
DEFINE_string(gunir_leafnode_port, "10000", "");

DEFINE_int32(gunir_leafnode_connect_retry_period, 1000, "");
DEFINE_int32(gunir_leafnode_rpc_timeout_period, 5000, "");
DEFINE_int32(gunir_leafnode_thread_min_num, 1, "");
DEFINE_int32(gunir_leafnode_thread_max_num, 10, "");

DEFINE_int32(gunir_leafnode_send_package_size, 1024 * 1024, "limit for pb message");
DEFINE_int64(gunir_leafnode_result_memory_limit, 64 * 1024 * 1024, "");

DEFINE_int64(gunir_leafnode_container_memory_limit, 500 * 1024 * 1024, "");

DEFINE_int32(gunir_leafnode_worker_thread_num, 5, "");
DEFINE_int32(gunir_leafnode_sender_thread_num, 5, "");

//////////  stem node //////////
DEFINE_string(gunir_stemnode_addr, "127.0.0.1", "");
DEFINE_string(gunir_stemnode_port, "10000", "");

DEFINE_int32(gunir_stemnode_connect_retry_period, 1000, "");
DEFINE_int32(gunir_stemnode_rpc_timeout_period, 5000, "");
DEFINE_int32(gunir_stemnode_thread_min_num, 1, "");
DEFINE_int32(gunir_stemnode_thread_max_num, 10, "");

DEFINE_int64(gunir_stemnode_result_memory_limit, 1024 * 1024 * 1024, "");
DEFINE_int32(gunir_stemnode_worker_thread_num, 5, "");

//////////  client  /////////////
DEFINE_double(gunir_job_precision, 1.0, "");
DEFINE_string(gunir_user_defined_table_conf, "./user_defined.conf", "");
