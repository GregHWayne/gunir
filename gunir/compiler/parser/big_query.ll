digit [0-9]
id [_a-zA-Z]

%{
// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin <qinan@baidu.com>
//

#include <cstdlib>
#include <cstring>
#include <iostream>

#include "gunir/compiler/parser/big_query_parser.h"
#include "gunir/compiler/parser/big_query.yy.hh"

#define YY_USER_ACTION \
        gunir_lloc.first_column = yycolumn; \
        gunir_lloc.last_column = yycolumn + yyleng - 1; \
        yycolumn += yyleng;

#define YY_USER_INIT gunir_lloc.first_line = 0; \
                     gunir_lloc.last_line = 0; \
                     gunir_lloc.first_column = 0; \
                     gunir_lloc.last_column = 0

#define YY_DECL int yylex(yyscan_t yyscanner, YaccMemoryPool* memory_pool, std::ostream* output)
static void print_message(const char* msg, std::ostream* output);

namespace dc = ::gunir::compiler;
static std::string char_string_content;
enum QuoteType {
    kNone = 0,
    kSingle = 1,
    kDouble = 2
};

static QuoteType quote_type = kNone;
%}

%option yylineno
%option case-insensitive
%x STR

%%
"<="          { return LESS_THAN_OR_EQUAL; }
"<"           { return LESS_THAN; }
">="          { return GREATER_THAN_OR_EQUAL; }
">"           { return GREATER_THAN; }
"=="          { return EQUAL; }
"="           { return EQUAL; }
"<>"          { return NOT_EQUAL; }
"!="          { return NOT_EQUAL; }

"&"           { return BITWISE_AND; }
"~"           { return BITWISE_NOT; }
"|"           { return BITWISE_OR; }
"\^"          { return BITWISE_XOR; }
"<<"          { return BITWISE_LEFT_SHIFT; }
">>"          { return BITWISE_RIGHT_SHIFT; }
"&&"          { return LOGICAL_AND; }
"||"          { return LOGICAL_OR; }
"!"           { return LOGICAL_NOT; }

"/"           { return SLASH; }
"*"           { return STAR; }
"+"           { return PLUS; }
"-"           { return MINUS; }
"%"           { return REMAINDER; }

"."           { return DOT; }
":"           { return COLON; }
","           { return COMMA; }
";"           { return SEMICOLON; }
"("           { return LPAREN; }
")"           { return RPAREN; }
"["           { return LSQUARE; }
"]"           { return RSQUARE; }
"{"           { return LCURLY; }
"}"           { return RCURLY; }

HISTORY       { return HISTORY; }
SHOW          { return SHOW; }
HELP          { return HELP; }
QUIT          { return QUIT; }
EXIT          { return QUIT; }
DEFINE        { return DEFINE; }
CREATE        { return CREATE; }
DROP          { return DROP; }
SELECT        { return SELECT; }
WITHIN        { return WITHIN; }
RECORD        { return RECORD; }
AS            { return AS; }
FROM          { return FROM; }
WHERE         { return WHERE; }
JOIN          { return JOIN; }
INNER         { return INNER; }
LEFT          { return LEFT; }
OUTER         { return OUTER; }
ON            { return ON; }
GROUPBY       { return GROUPBY; }
HAVING        { return HAVING; }
ORDERBY       { return ORDERBY; }
DISTINCT      { return DISTINCT; }
DESC          { return DESC; }
ASC           { return ASC; }
LIMIT         { return LIMIT; }
OR            { return LOGICAL_OR; }
AND           { return LOGICAL_AND; }
NOT           { return LOGICAL_NOT; }
CONTAINS      { return CONTAINS; }
DIV           { return DIV; }

<<EOF>>       { return ENDOF; }

TRUE {
    gunir_lval._bool = true;
    return BOOLEAN;
}

FALSE {
    gunir_lval._bool = false;
    return BOOLEAN;
}

({id}+{digit}*)+ {
    dc::StringMessage* m = NewMessage<dc::StringMessage>(memory_pool);
    *(m->mutable_char_string()) = std::string(yytext);
    gunir_lval._string = m;
    return ID;
}

{digit}+ {
    gunir_lval._int = atol(yytext);
    return INT;
}

{digit}+\.{digit}+ {
    gunir_lval._float = atof(yytext);
    return FLOAT;
}

\" {
    /* start match char string when first find quote:" */
    char_string_content.clear();
    quote_type = kDouble;
    BEGIN(STR);
}

\' {
    char_string_content.clear();
    quote_type = kSingle;
    BEGIN(STR);
}

<STR>[\'\"] {
    if ((quote_type == kSingle && yytext[0] == '\"') ||
        (quote_type == kDouble && yytext[0] == '\'')) {
        char_string_content.push_back(yytext[0]); 
    } else {
        /* saw closing quote" - all done */
        /* return string constant token type and value to parser */

        BEGIN(INITIAL);
        dc::StringMessage* m = NewMessage<dc::StringMessage>(memory_pool);

        *(m->mutable_char_string()) = char_string_content;
        gunir_lval._string = m;
        return STRING;
    }
}

<STR>\n {
    /* error - unterminated string constant */
    /* generate error message */

    print_message("lex parse error, unexpected \\n", output);
    return LEX_ERROR;
}

<STR>\\\\ {
    char_string_content.push_back(yytext[1]); 
}

<STR>\\\' {
    /* it is a escaped quote:' */
    char_string_content.push_back(yytext[1]); 
}

<STR>\\\" {
    /* it is a escaped quote:" */
    char_string_content.push_back(yytext[1]); 
}

<STR>\\[0-7]{1,3} {
    /* octal escape sequence */
    int result;
    sscanf(yytext + 1, "%o", &result );

    /* error, constant is out-of-bounds */
    if (result > 0xff) {
        print_message("lex parse error, constant is out-of-bounds", output);
        return LEX_ERROR;
    } else {
        char_string_content.push_back(static_cast<char>(result)); 
    }
}

<STR>\\[0-9]+ {
    /* generate error - bad escape sequence; something
     * like '\48' or '\0777777'
     */

    print_message("lex parse error, constant is out-of-bounds", output);
    return LEX_ERROR;
}

<STR>\\n {
    char_string_content.push_back('\n'); 
}

<STR>\\t {
    char_string_content.push_back('\t'); 
}

<STR>\\ {
    char_string_content.push_back('\\'); 
}

<STR>[^\"\'\n\\]+ {
    /* regex match all content in quote:" */
    char *yptr = yytext;

    while (*yptr) {
        char_string_content.push_back(*yptr); 
        yptr++;
    }
}

[\ \t\r\n\0]+ {
    // return WS;
}

. {
    print_message("lex parse error", output);
    return LEX_ERROR;
}

%%

void print_message(const char* msg, std::ostream* output) {
    *output << msg << std::endl;
}
