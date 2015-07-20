// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/client/shell.h"

#include <stdio.h>
#include <stdlib.h>

#include <iostream>
#include <map>

#include "thirdparty/readline/history.h"
#include "thirdparty/readline/readline.h"

#include "gunir/client/base_client.h"
#include "gunir/client/sql_command_info.h"

namespace gunir {

static  key_color_info key_color_table[] =
{
    {"gunir", LIGHT_GREEN, 6},
    {"create", LIGHT_GREEN, 6},
    {"define", LIGHT_GREEN, 6},
    {"drop", LIGHT_GREEN, 4},
    {"select", LIGHT_GREEN, 6},
    {"as", LIGHT_GREEN, 2},
    {"from", LIGHT_GREEN, 4},
    {"where", LIGHT_GREEN, 5},
    {"within", LIGHT_GREEN, 6},
    {"record", LIGHT_GREEN, 6},
    {"groupby", LIGHT_GREEN, 7},
    {"having", LIGHT_GREEN, 6},
    {"orderby", LIGHT_GREEN, 7},
    {"distinct", LIGHT_GREEN, 8},
    {"join", LIGHT_GREEN, 4},
    {"on", LIGHT_GREEN, 2},
    {"inner", LIGHT_GREEN, 5},
    {"outer", LIGHT_GREEN, 5},
    {"left", LIGHT_GREEN, 4},
    {"limit", LIGHT_GREEN, 5},
    {"show", LIGHT_GREEN, 4},
    {"history", LIGHT_GREEN, 7},
    {"help", LIGHT_GREEN, 4},
    {"quit", LIGHT_GREEN, 4},
    {"exit", LIGHT_GREEN, 4},

    {"or", LIGHT_YELLOW, 2},
    {"and", LIGHT_YELLOW, 3},
    {"not", LIGHT_YELLOW, 3},
    {"true", LIGHT_YELLOW, 4},
    {"false", LIGHT_YELLOW, 5},
    {"div", LIGHT_YELLOW, 3},

    {"<<", LIGHT_YELLOW, 2},
    {"<=", LIGHT_YELLOW, 2},
    {"<>", LIGHT_YELLOW, 2},
    {"<", LIGHT_YELLOW, 1},
    {">>", LIGHT_YELLOW, 2},
    {">=", LIGHT_YELLOW, 2},
    {">", LIGHT_YELLOW, 1},
    {"==", LIGHT_YELLOW, 2},
    {"=", LIGHT_YELLOW, 1},
    {"!=", LIGHT_YELLOW, 2},
    {"!", LIGHT_YELLOW, 1},
    {"&&", LIGHT_YELLOW, 2},
    {"&", LIGHT_YELLOW, 1},
    {"~", LIGHT_YELLOW, 1},
    {"||", LIGHT_YELLOW, 2},
    {"|", LIGHT_YELLOW, 1},
    {"^", LIGHT_YELLOW, 1},
    {"/", LIGHT_YELLOW, 1},
    {"*", LIGHT_YELLOW, 1},
    {"+", LIGHT_YELLOW, 1},
    {"-", LIGHT_YELLOW, 1},
    {"%", LIGHT_YELLOW, 1},
    {".", LIGHT_YELLOW, 1},
    {":", LIGHT_YELLOW, 1},
    {",", LIGHT_YELLOW, 1},
    {";", LIGHT_YELLOW, 1},
    {"(", LIGHT_YELLOW, 1},
    {")", LIGHT_YELLOW, 1},
    {"[", LIGHT_YELLOW, 1},
    {"]", LIGHT_YELLOW, 1},
    {"{", LIGHT_YELLOW, 1},
    {"}", LIGHT_YELLOW, 1},
    {"contains", LIGHT_YELLOW, 8},

    {"concat", LIGHT_BLUE, 6},
    {"substr", LIGHT_BLUE, 6},
    {"length", LIGHT_BLUE, 6},

    {"sum", LIGHT_BLUE, 3},
    {"max", LIGHT_BLUE, 3},
    {"min", LIGHT_BLUE, 3},
    {"avg", LIGHT_BLUE, 3},
    {"count", LIGHT_BLUE, 5},
    {NULL, NULL, 0}
};

char *dupstr(const char *s)
{
    char *r;
    r = reinterpret_cast<char *>(malloc(strlen(s) + 1));
    strcpy(r, s); // NOLINT
    return (r);
}

char *GeneratorCmd(const char* text, int state) {
    static int list_index, len;
    const char *name;
    if (!state)
    {
        list_index = 0;
        len = strlen(text);
    }

    while (NULL != (name = g_command_lists[list_index].command))
    {
        list_index++;
        if (strncmp (name, text, len) == 0)
            return (dupstr(name));
    }
    return (reinterpret_cast<char *>(NULL));
}

char** CompletionCmd(const char *text, int start, int end) {
    char **matches = reinterpret_cast<char **>(NULL);
    matches = rl_completion_matches(text, GeneratorCmd);
    return matches;
}

QueryShell::QueryShell() {
    InitializeReadLine();
}

QueryShell::~QueryShell() {
}

void QueryShell::Init(client::BaseClient* client) {
    m_client = client;
}

void QueryShell::InitializeReadLine() {
    m_head_name="gunir> ";
    rl_attempted_completion_function = CompletionCmd;
    rl_set_key_color_table(key_color_table);
}

size_t QueryShell::GetHyphenIndex(const std::string& cur_line) {
    std::string back_slash = "\\";
    std::string comma = ",";
    size_t back_slash_index = cur_line.find_last_of(back_slash);
    size_t comma_index = cur_line.find_last_of(comma);
    size_t hyphen_index = std::string::npos;

    if (back_slash_index != std::string::npos) {
        hyphen_index = back_slash_index;
    } else if (comma_index != std::string::npos) {
        hyphen_index = comma_index;
    } else {
        return hyphen_index;
    }

    for (size_t index = hyphen_index + 1; index < cur_line.length(); index++) {
        char elem = cur_line.at(index);
        if (elem != '\t' && elem != ' '&&elem != 10) {
            hyphen_index = std::string::npos;
            break;
        }
    }
    return hyphen_index;
}

int QueryShell::Run() {
    const char* line_read = readline(m_head_name.c_str());
    return Run(line_read);
}

int QueryShell::Run(const char* line_read) {
    std::string part_header = "      > ";
    std::string full_header = "gunir> ";

    if (line_read) {
        const char* p = line_read;
        while (*p != '\0') {
            if (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') {
                p++;
            } else {
                break;
            }
        }
        if (*p == '\0')
            return 0;
        line_read = p;

        std::string cur_line = (std::string) line_read;
        size_t hyphen_index = GetHyphenIndex(cur_line);

        if (hyphen_index != std::string::npos) {
            if (cur_line[hyphen_index] == ',') ++hyphen_index;
            m_read_line += cur_line.substr(0, hyphen_index);
            m_head_name = part_header;
            return 0;
        } else {
            m_read_line  += cur_line;
            add_history(m_read_line.c_str());
            int ret = (m_client->Run(m_read_line) == false);
            if (ret) {

            }
            m_head_name = full_header;
            m_read_line.clear();
            return 0;
        }
    }
    return -1;
}

} // namespace gunir

