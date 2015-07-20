// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_UTILS_FILENAME_TOOL_H
#define  GUNIR_UTILS_FILENAME_TOOL_H

#include <stdint.h>
#include <string>
#include <vector>

// #include "gunir/common/proto/task.pb.h"
#include "gunir/proto/tablet.pb.h"

namespace gunir {
bool CreateDir(const std::string& dir);
std::string EnsurePathEndsWithSlash(const std::string& path);
bool RemoveFile(const std::string& file_name, bool is_cur);
size_t FindMostLeftWildcard(const std::string& path);
bool GetFilesByPattern(const std::string& input_paths,
                       std::vector<std::string> *input_files,
                       int64_t* total_size);
uint32_t GetFileChunkIp(const std::string& file_name);
// std::string GetCacheTaskFileName(const TaskInfo& task_info);
std::string GetCacheJobFileName(uint64_t job_id);
std::string GetUserJobResultFileName(uint64_t job_id,
                                     const std::string& tail);
std::string CovertByteToString(const uint64_t size);
bool GetFileChunkIp(const std::string& file_name, TabletInfo* ips);
bool IsXfsPath(const std::string path);
std::string MoveXfsFileToLocalTmp(const std::string& xfs_path);
void GetMachineNumberByFileSize(int64_t file_size, uint32_t* machine_number);
void GetWorkerDiskSpaceByFileSize(int64_t file_size,
                                  uint32_t machine_number,
                                  uint32_t* disk_space);
}  // namespace gunir

#endif  // GUNIR_UTILS_FILENAME_TOOL_H
