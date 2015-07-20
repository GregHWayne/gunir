// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/utils/parse_log.h"

#include <vector>

#include "toft/base/string/algorithm.h"
#include "toft/base/string/number.h"

ParseLog::ParseLog() : m_is_done(true) {}

bool ParseLog::FillSeverityAndMmdd(const std::string& word,
                                   LogMessage* message) {
    if (word.size() != 5) {
        return false;
    }

    const char& begin = word[0];
    switch (begin) {
    case 'W' :
        message->set_severity("WARNING");
        break;
    case 'E' :
        message->set_severity("ERROR");
        break;
    case 'I' :
        message->set_severity("INFO");
        break;
    case 'F' :
        message->set_severity("FATAL");
        break;
    default :
        return false;
    }

    message->set_mmdd(word.substr(1));
    return true;
}

bool ParseLog::FillTime(const std::string& word, LogMessage* message) {
    std::vector<std::string> split_string;
    toft::SplitString(word, ":", &split_string);
    if (split_string.size() != 3) {
        return false;
    }

    if (split_string[2].find('.') == std::string::npos) {
        return false;
    }

    message->set_time(word);
    return true;
}

bool ParseLog::FillThreadId(const std::string& word,
                            LogMessage* message) {
    int threadid = 0;
    if (toft::StringToNumber(word, &threadid)) {
        message->set_thread_id(threadid);
        return true;
    }
    return false;
}

bool ParseLog::FillFileAndFileLine(const std::string& word,
                                   LogMessage* message) {
    std::vector<std::string> split_string;
    toft::SplitString(word, ":", &split_string);
    if (split_string.size() != 2) {
        return false;
    }

    int file_line = 0;
    if (toft::StringToNumber(split_string[1], &file_line)) {
        message->set_file(split_string[0]);
        message->set_file_line(file_line);
        return true;
    }
    return false;
}

bool ParseLog::IsLineMessageStart(const std::string& line,
                                  LogMessage* message) {
    size_t pos = line.find_first_of(']');
    if (pos == std::string::npos) {
        return false;
    }

    std::vector<std::string> split_string;
    toft::SplitString(line.substr(0, pos), " ", &split_string);
    if (split_string.size() != 4) {
        return false;
    }

    if (!FillSeverityAndMmdd(split_string[0], message)) {
        return false;
    }

    if (!FillTime(split_string[1], message)) {
        return false;
    }

    if (!FillThreadId(split_string[2], message)) {
        return false;
    }

    if (!FillFileAndFileLine(split_string[3], message)) {
        return false;
    }

    message->set_msg(line.substr(pos + 2));
    return true;
}

int ParseLog::ParseLine(toft::File* file,
                        LogMessage* this_message,
                        LogMessage* new_message) {
    std::string line;
    int32_t ret = 0;
    while ((ret = file->ReadLine(&line)) > 0) {
        if (IsLineMessageStart(line, new_message)) {
            return 1;
        }
        this_message->set_msg(this_message->msg() + line);
    }

    return ret;
}

bool ParseLog::ParseFileName(const std::string& file_name,
                             LogMessage* message) {
    size_t pos = file_name.find_last_of('/');
    std::string name = file_name;
    if (pos != std::string::npos) {
        name = file_name.substr(pos + 1);
    }
    std::vector<std::string> split_string;
    toft::SplitString(name, ".", &split_string);
    if (split_string.size() < 7) {
        return false;
    }

    int pid = 0;
    size_t last_pos = split_string.size() - 1;
    if (!toft::StringToNumber(split_string[last_pos], &pid)) {
        return false;
    }
    message->set_program_name(split_string[0]);
    message->set_hostname(split_string[1]);
    message->set_username(split_string[last_pos - 4]);
    message->set_pid(pid);

    return true;
}

bool ParseLog::Open(const std::string& file_name) {
    m_file_name = file_name;
    m_file_name_message.Clear();
    if (!ParseFileName(file_name, &m_file_name_message)) {
        LOG(ERROR) << "Parse log file name error : " << file_name;
        return false;
    }

    m_file.reset(toft::File::Open(file_name, "r"));
    if (NULL == m_file.get()) {
        LOG(ERROR) << "Open file error : " << file_name;
        return false;
    }

    // get first message's head
    LogMessage message;
    m_next_message.Clear();
    m_next_message = m_file_name_message;
    int ret = ParseLine(m_file.get(), &message, &m_next_message);
    if (ret < 0) {
        LOG(ERROR) << "ParseMessage from file error : " << file_name;
        return false;
    }

    m_is_done = (ret == 0);
    return true;
}

bool ParseLog::GetNextMessage(LogMessage* message) {
    message->Clear();
    message->CopyFrom(m_next_message);
    m_next_message.Clear();
    m_next_message = m_file_name_message;
    int ret = ParseLine(m_file.get(), message, &m_next_message);
    if (ret < 0) {
        LOG(ERROR) << "ParseMessage from file error : " << m_file_name;
        return false;
    }

    m_is_done = (ret == 0);
    return true;
}
