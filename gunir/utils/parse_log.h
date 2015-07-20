// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  GUNIR_UTILS_PARSE_LOG_H
#define  GUNIR_UTILS_PARSE_LOG_H

#include <string>

#include "toft/base/scoped_ptr.h"
#include "toft/base/string/algorithm.h"
#include "toft/storage/file/file.h"

#include "gunir/utils/parse_log.pb.h"

class ParseLog {
public:
    ParseLog();

    bool Open(const std::string& file_name);

    bool IsFinished() {
        return m_is_done;
    }

    bool GetNextMessage(LogMessage* message);

private:
    bool FillSeverityAndMmdd(const std::string& word, LogMessage* message);
    bool FillTime(const std::string& word, LogMessage* message);
    bool FillThreadId(const std::string& word, LogMessage* message);
    bool FillFileAndFileLine(const std::string& word, LogMessage* message);
    bool IsLineMessageStart(const std::string& line, LogMessage* message);
    int ParseLine(toft::File* file, LogMessage* this_message,
                  LogMessage* new_message);
    bool ParseFileName(const std::string& file_name, LogMessage* message);

private:
    toft::scoped_ptr<toft::File> m_file;
    std::string m_file_name;

    LogMessage m_file_name_message;
    LogMessage m_next_message;
    bool m_is_done;
};

#endif  // GUNIR_UTILS_PARSE_LOG_H
