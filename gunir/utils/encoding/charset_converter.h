// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef GUNIR_UTILS_ENCODING_CHARSET_CONVERTER_H
#define GUNIR_UTILS_ENCODING_CHARSET_CONVERTER_H

// disallow define iconv to libiconv if libiconv is installed
#define LIBICONV_PLUG
#include <iconv.h>

#include <stddef.h>
#include <string>
#include "toft/base/string/string_piece.h"
#include "toft/base/uncopyable.h"


using namespace toft;

namespace gunir {

/// i18n charset converter
/// @note CharsetConverter is not thread safe,
/// so it can not be used in multiple threads simultaneously
class CharsetConverter
{
    DECLARE_UNCOPYABLE(CharsetConverter);
public:
    /// Create a immediate usable converter, if fail, fatal error will be
    /// raised.
    /// @param from encoding name to be converter from
    /// @param to encoding name to be converter to
    CharsetConverter(const StringPiece& from, const StringPiece& to);

    /// Create an empty and unusable converter, explicit Create should be
    /// called before use it
    CharsetConverter();
    ~CharsetConverter();

    /// Create a converter
    /// @param from encoding name to be converter from
    /// @param to encoding name to be converter to
    bool Create(const StringPiece& from, const StringPiece& to);

    /// convert string to another charset, append the result to *out
    /// @param in input string
    /// @param out converted result will be appended to this
    /// @param converted_size pointer to a size_t to receive converted byte size
    /// @returns whether all bytes converted successfully
    bool ConvertAppend(
        const StringPiece& in,
        std::string* out,
        size_t* converted_size = NULL);

    /// convert string to another charset
    /// @param in input string
    /// @param out to receive converted result
    /// @param converted_size pointer to a size_t to receive converted byte size
    /// @returns whether all bytes converted successfully
    bool Convert(
        const StringPiece& in,
        std::string* out,
        size_t* converted_size = NULL);
private:
    iconv_t m_cd;
};

/////////////////////////////////////////////////////////////////////////////
// some handy thread local singletons, the result should not be shared
// between threads
// CharsetConverter& Utf8ToGbk();
// CharsetConverter& GbkToUtf8();
// CharsetConverter& Big5ToUtf8();
// CharsetConverter& Big5ToGbk();

/// convert GBK string to UTF-8 string
// inline bool ConvertGbkToUtf8(const StringPiece& in, std::string* out)
// {
//     return GbkToUtf8().Convert(in, out);
// }

// /// convert UTF-8 string to GBK string
// inline bool ConvertUtf8ToGbk(const StringPiece& in, std::string* out)
// {
//     return Utf8ToGbk().Convert(in, out);
// }

} // namespace gunir

#endif // GUNIR_UTILS_ENCODING_CHARSET_CONVERTER_H
