// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description

#include <errno.h>
#include <stdexcept>

#include "gunir/utils/encoding/charset_converter.h"


namespace gunir {

CharsetConverter::CharsetConverter(const StringPiece& from, const StringPiece& to) {
    m_cd = iconv_open(to.data(), from.data());
    if (m_cd == iconv_t(-1)) {
        int error = errno;
        std::string msg = "CharsetConverter ctor error: ";
        msg += strerror(error);
        throw std::runtime_error(msg);
    }
}

CharsetConverter::CharsetConverter() : m_cd(iconv_t(-1)) {}

CharsetConverter::~CharsetConverter() {
    if (m_cd != iconv_t(-1)) {
        iconv_close(m_cd);
        m_cd = NULL;
    }
}

bool CharsetConverter::Create(const StringPiece& from, const StringPiece& to) {
    assert(m_cd == iconv_t(-1));
    m_cd = iconv_open(to.data(), from.data());
    return m_cd != iconv_t(-1);
}

bool CharsetConverter::ConvertAppend(
    const StringPiece& in,
    std::string* out,
    size_t* converted_size) {
    iconv(m_cd, NULL, NULL, NULL, NULL); // reset iconv

    char* in_ptr = const_cast<char*>(in.data());
    size_t in_left = in.size();

    for (;;) {
        size_t org_out_size = out->size();
        size_t out_left = 2 * in_left + 1; // extra 1 make out not empty
        out->resize(out->size() + out_left);
//         char* out_ptr = string_as_array(out) + org_out_size;
        char* out_ptr = const_cast<char*>(out->c_str());
        out_ptr += org_out_size;
        size_t converted = iconv(m_cd, &in_ptr, &in_left, &out_ptr, &out_left);
        if (converted != size_t(-1)) {
            out->resize(out->size() - out_left);
            if (converted_size)
                *converted_size = in.size();
            return true;
        } else {
            switch (errno) {
            case E2BIG:
                continue;
            default:
                out->resize(out->size() - out_left);
                if (converted_size)
                    *converted_size = in.size() - in_left;
                return false;
            }
        }
    }
    return false;
}

bool CharsetConverter::Convert(
    const StringPiece& in,
    std::string* out,
    size_t* converted_size) {
    out->clear();
//     *out = in;
    return false;
//     return ConvertAppend(in, out, converted_size);
}

// CharsetConverter& Utf8ToGbk()
// {
//     static CharsetConverter converter("UTF-8", "GBK");
//     return converter.Value();
// }

// CharsetConverter& GbkToUtf8()
// {
//     static CharsetConverter converter("GBK", "UTF-8");
//     return converter.Value();
// }

// CharsetConverter& Big5ToUtf8()
// {
//     static CharsetConverter converter("BIG-5", "UTF-8");
//     return converter.Value();
// }

// CharsetConverter& Big5ToGbk()
// {
//     static CharsetConverter converter("BIG-5", "GBK");
//     return converter.Value();
// }

} // namespace gunir
