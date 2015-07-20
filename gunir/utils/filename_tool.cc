// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/utils/filename_tool.h"

#include <glob.h>
#include <math.h>
#include <algorithm>

#include "toft/base/scoped_ptr.h"
#include "toft/base/string/algorithm.h"
#include "toft/base/string/format.h"
#include "toft/base/string/number.h"
#include "toft/storage/file/file.h"
#include "toft/storage/path/path_ext.h"
#include "toft/system/threading/thread.h"
// #include "toft/system/net/ip_address.h"
#include "toft/crypto/uuid/uuid.h"
#include "thirdparty/glog/logging.h"
#include "toft/base/string/string_format.h"


// DECLARE_string(gunir_task_cache_dir);
DECLARE_string(gunir_job_cache_dir);
DECLARE_string(gunir_query_output_dir);

DEFINE_int64(gunir_create_dir_retry_wait_time, 1000, "");


using namespace toft;

namespace gunir {

static const int32_t kMaxCreateDirRetryTimes = 3;
static const std::string kLocalIPAddress = "127.0.0.1";

#if 0
bool CreateDirWithRetry(const std::string& dir) {
    if (File::CheckExist(dir.c_str())) {
        return true;
    }

//     for (int i = 0; i < kMaxCreateDirRetryTimes; ++i) {
//         uint32_t error_code;
//         if (File::AddDir(dir.c_str(), &error_code) == 0) {
//             return true;
//         }
//         LOG(ERROR) << "Add dir failed, reason: "
//             << GetFileErrorCodeStr(error_code);
//         ThisThread::Sleep(FLAGS_gunir_create_dir_retry_wait_time);
//     }

    return false;
}
#endif

bool CreateRecursiveDir(const std::string& dir) {
    if (dir.empty()) {
        LOG(WARNING) << "dir is empty";
        return false;
    }

    std::string::size_type pos = 1;

    std::string xfs_prefix = "/hdfs/";
    if (xfs_prefix.compare(dir.substr(0, 6)) == 0) {
        pos = dir.find('/', 6);
        if (pos == std::string::npos) {
            LOG(ERROR) << "HDFS path format error, must like \"/hdfs/cluster/\"";
            return false;
        }
        pos = pos + 1;
    }

    while (true) {
        pos = dir.find('/', pos);

        if (!CreateDirWithRetry(dir.substr(0, pos))) {
            return false;
        }

        if (pos == std::string::npos) {
            return true;
        }
        ++pos;
    }
    return true;
}

bool CreateDir(const std::string& dir) {
    if (IsExist(dir)) {
        return true;
    }
    return CreateRecursiveDir(dir);
}

std::string EnsurePathEndsWithSlash(const std::string& path) {
    if (path.length() < 1) {
        LOG(ERROR) << "invalid empty path, reset to '/'";
        return "./";
    }

    std::string path_with_slash = path;
    if (path.at(path.length() - 1) != '/') {
        path_with_slash.append("/");
    }

    return path_with_slash;
}

bool RemoveFile(const std::string& file_name, bool is_cur) {
    if (!File::Exists(file_name)) {
        DLOG(INFO) << "File : " << file_name << " not exist";
        return true;
    }

    if (0 != File::Delete(file_name)) {
        LOG(ERROR) << "remove file " << file_name << " failed";
        return false;
    }
    return true;
}

size_t FindMostLeftWildcard(const std::string& path) {
    size_t star_pos = path.find("*");
    size_t first_wildcard_pos = star_pos;

    size_t question_pos = path.find("?");
    first_wildcard_pos = first_wildcard_pos < question_pos ? first_wildcard_pos : question_pos;

    size_t bracket_pos = path.find("[");
    first_wildcard_pos = first_wildcard_pos < bracket_pos ? first_wildcard_pos : bracket_pos;
    return first_wildcard_pos;
}

bool GetFilesByPattern(const std::string& input_paths,
                       std::vector<std::string> *input_files,
                       int64_t* total_size) {
    input_files->clear();
    std::vector<std::string> paths;
    SplitString(input_paths, ",", &paths);
    for (size_t i = 0; i < paths.size(); ++i) {
        size_t first_wildcard_pos = FindMostLeftWildcard(paths[i]);
//         if (first_wildcard_pos == std::string::npos) {
//             input_files->push_back(paths[i]);
//             continue;
//         }
        size_t pos = paths[i].substr(0, first_wildcard_pos).rfind("/");
        std::string prefix = paths[i].substr(0, pos + 1);
        scoped_ptr<FileIterator> itr(File::Iterate(prefix, "*", FileType_Regular));
        FileEntry entry;
        while (itr->GetNext(&entry)) {
            input_files->push_back(prefix + "/" + entry.name);
        }
//         size_t first_wildcard_pos = FindMostLeftWildcard(paths[i]);
//         AttrsMask mask;
//         mask.file_size = 1;
//         mask.file_type = 1;
//         mask.filter_subdirs = 0;
//         std::vector<AttrsInfo> attrs_infos;
//         int32_t ret = File::List(paths[i].c_str(), &mask, &attrs_infos);
//         if (ret != 0) {
//             DLOG(INFO) << "No input files match Pattern:" << paths[i];
//             continue;
//         }
//         size_t pos = paths[i].substr(0, first_wildcard_pos).rfind("/");
//         std::string prefix = paths[i].substr(0, pos + 1);
//         for (size_t i = 0; i < attrs_infos.size(); ++i) {
//             if (attrs_infos[i].file_type == FILE_TYPE_DIR) {
//                 continue;
//             }
//             input_files->push_back(prefix + attrs_infos[i].file_name);
//             *total_size += attrs_infos[i].file_size;
//         }
    }
    if (input_files->size() == 0) {
        DLOG(INFO) << "No input files match Pattern:" << input_paths;
        return false;
    }

    for (uint32_t i = 0; i < input_files->size(); ++i) {
        *total_size += toft::File::GetSize(input_files->at(i));
    }
    return true;
}

bool GetFileChunkIp(const std::string& file_name, TabletInfo* ips) {
//     if (!IsXfsPath(file_name)) {
//         ips->add_ip(IPAddress(kLocalIPAddress).ToInt());
//         return true;
//     }

//     scoped_ptr<File> file(File::Open(file_name.c_str(),
//                                      File::ENUM_FILE_OPEN_MODE_R));
//     if (NULL == file.get()) {
//         return false;
//     }

//     std::vector<DataLocation> locate;
//     if (file->LocateData(0, -1, &locate) < 0) {
//         return false;
//     }

//     if (locate.size() == 0) {
//         return false;
//     }

//     for (std::vector<DataLocation>::iterator i = locate.begin();
//          i != locate.end(); i++) {
//         ips->add_ip(i->net_order_ip);
//     }
    return true;
}

// std::string GetCacheTaskFileName(const TaskInfo& task_info) {
//     std::string filename = FLAGS_gunir_task_cache_dir;
//     if (task_info.type() == kInterTask) {
//         filename += "kInterTask";
//     } else if (task_info.type() == kLeafTask) {
//         filename += "kLeafTask";
//     } else {
//         filename += "kUnknownTask";
//     }
//     filename += "_" + NumberToString(task_info.job_id());
//     filename += "_" + NumberToString(task_info.task_id());
//     filename += "_" + NumberToString(task_info.attempt_id());
//     filename += ".result";
//     return filename;
// }

std::string GetCacheJobFileName(uint64_t job_id) {
    std::string filename = FLAGS_gunir_job_cache_dir;
    filename += "job_" + NumberToString(job_id) + ".result";
    return filename;
}

std::string GetUserJobResultFileName(uint64_t job_id,
                                     const std::string& tail) {
    std::string filename = FLAGS_gunir_query_output_dir;
    filename += "job_" + NumberToString(job_id) + "_" + tail;
    return filename;
}

std::string CovertByteToString(const uint64_t size) {
    std::string hight_unit;
    double min_size;
    const uint64_t kKB = 1024;
    const uint64_t kMB = kKB * 1024;
    const uint64_t kGB = kMB * 1024;
    const uint64_t kTB = kGB * 1024;
    const uint64_t kPB = kTB * 1024;

    if (size > kPB) {
        min_size = (1.0 * size) / kPB;
        hight_unit = "PB";
    } else if (size > kTB) {
        min_size = (1.0 * size) / kTB;
        hight_unit = "TB";
    } else if (size > kGB) {
        min_size = (1.0 * size) / kGB;
        hight_unit = "GB";
    } else if (size > kMB) {
        min_size = (1.0 * size) / kMB;
        hight_unit = "MB";
    } else if (size > kKB) {
        min_size = (1.0 * size) / kKB;
        hight_unit = "KB";
    } else {
        min_size = size;
        hight_unit = "Bytes";
    }

    return StringPrint("%.2f %s", min_size, hight_unit.c_str());
}

bool IsXfsPath(const std::string path) {
    std::string xfs_prefix = "/xfs/";
    if (xfs_prefix.compare(path.substr(0, 5)) == 0) {
        if (path.find('/', 5) != std::string::npos) {
            return true;
        }
    }
    return false;
}

std::string MoveXfsFileToLocalTmp(const std::string& xfs_path) {
    if (!IsXfsPath(xfs_path)) {
        return xfs_path;
    }

//     Uuid uuid;
//     uuid.Generate();
    std::string tmp_file_name = CreateCanonicalUUIDString() + ".proto";
//     if (0 == File::Copy(xfs_path.c_str(), tmp_file_name.c_str())) {
//         return tmp_file_name;
//     }
    return "";
}

// The relationship between machine number and file size is as follows:
// 1. if size <= 2G, number = 1;
// 2. if 2G < size <= 1024G, number = log2(size);
// 3. if size > 1024G, number = 10;
void GetMachineNumberByFileSize(int64_t file_size, uint32_t* machine_number) {
    int64_t base_size = 2;
    int64_t size_in_giga
        = std::max(base_size, file_size / (1024 * 1024 * 1024));
    *machine_number = std::min(10U, static_cast<uint32_t>(
            ceil(log(size_in_giga * 1.0) / log(2))));
}

// Suppose the output of map tasks are uniformly shuffled to reducer, the inter-
// mediate data size of each worker is map_output_size * 2 / machine_number;
// Because the intermediate-data is compressed in mapreduce framework, so we use
// size * 2 / machine_number as worker space even if map output may not shuffled
// uniformly.
void GetWorkerDiskSpaceByFileSize(int64_t file_size,
                                  uint32_t machine_number,
                                  uint32_t* disk_space) {
    CHECK_NE(0U, machine_number) << "Machine number should not equals to 0";
    int64_t size_in_mega = file_size / (1024 * 1024);
    *disk_space = size_in_mega * 2 / machine_number;
}

}  // namespace gunir
