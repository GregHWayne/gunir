/* A Bison parser, made by GNU Bison 3.0.2.  */

/* Bison implementation for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2013 Free Software Foundation, Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Identify Bison output.  */
#define YYBISON 1

/* Bison version.  */
#define YYBISON_VERSION "3.0.2"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 0

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1


/* Substitute the variable and function names.  */
#define yyparse          gunir_parse
#define yylex            gunir_lex
#define yyerror          gunir_error
#define yydebug          gunir_debug
#define yynerrs          gunir_nerrs

#define yylval           gunir_lval
#define yychar           gunir_char
#define yylloc           gunir_lloc

/* Copy the first part of user declarations.  */
#line 1 "gunir/compiler/parser/big_query.yy" /* yacc.c:339  */

// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin <qinan@baiducom>
//
#include "gunir/compiler/parser/big_query_parser.h"

#include <iostream>

using namespace gunir;
using namespace compiler;

RawExpression* make_expression(RawExpression* left, Operators op,
    RawExpression* right, YaccMemoryPool* memory_pool);

OrderColumnPath* add_order_column_path(const ColumnPath& column_path,
    OrderType type, YaccMemoryPool* memory_pool);

#define make_query_stmt(TYPE, Value1, Value)  \
            QueryStmt* query_stmt = NewMessage<QueryStmt>(memory_pool);\
            (query_stmt->mutable_##TYPE())->CopyFrom(*Value1);\
            Value = query_stmt;\
            (gunir_get_extra(yyscanner))->query_stmt = Value;\
            return 0;


#line 101 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:339  */

# ifndef YY_NULLPTR
#  if defined __cplusplus && 201103L <= __cplusplus
#   define YY_NULLPTR nullptr
#  else
#   define YY_NULLPTR 0
#  endif
# endif

/* Enabling verbose error messages.  */
#ifdef YYERROR_VERBOSE
# undef YYERROR_VERBOSE
# define YYERROR_VERBOSE 1
#else
# define YYERROR_VERBOSE 1
#endif

/* In a future release of Bison, this section will be replaced
   by #include "big_query.yy.hh".  */
#ifndef YY__GUNIR_BUILD64_DEBUG_GUNIR_PARSER_BIG_QUERY_YY_HH_INCLUDED
# define YY__GUNIR_BUILD64_DEBUG_GUNIR_PARSER_BIG_QUERY_YY_HH_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int  gunir_debug;
#endif

/* Token type.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    SHOW = 258,
    HISTORY = 259,
    HELP = 260,
    QUIT = 261,
    DEFINE = 262,
    CREATE = 263,
    DROP = 264,
    SELECT = 265,
    WITHIN = 266,
    RECORD = 267,
    AS = 268,
    FROM = 269,
    WHERE = 270,
    JOIN = 271,
    INNER = 272,
    LEFT = 273,
    OUTER = 274,
    ON = 275,
    GROUPBY = 276,
    HAVING = 277,
    ORDERBY = 278,
    DISTINCT = 279,
    DESC = 280,
    ASC = 281,
    LIMIT = 282,
    LOGICAL_OR = 283,
    LOGICAL_AND = 284,
    LOGICAL_NOT = 285,
    BITWISE_AND = 286,
    BITWISE_NOT = 287,
    BITWISE_OR = 288,
    BITWISE_XOR = 289,
    BITWISE_LEFT_SHIFT = 290,
    BITWISE_RIGHT_SHIFT = 291,
    EQUAL = 292,
    NOT_EQUAL = 293,
    LESS_THAN_OR_EQUAL = 294,
    LESS_THAN = 295,
    GREATER_THAN_OR_EQUAL = 296,
    GREATER_THAN = 297,
    SLASH = 298,
    DIV = 299,
    STAR = 300,
    PLUS = 301,
    MINUS = 302,
    REMAINDER = 303,
    CONTAINS = 304,
    DOT = 305,
    COLON = 306,
    COMMA = 307,
    SEMICOLON = 308,
    LPAREN = 309,
    RPAREN = 310,
    LSQUARE = 311,
    RSQUARE = 312,
    LCURLY = 313,
    RCURLY = 314,
    ENDOF = 315,
    LEX_ERROR = 316,
    WS = 317,
    INT = 318,
    FLOAT = 319,
    BOOLEAN = 320,
    ID = 321,
    STRING = 322
  };
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE YYSTYPE;
union YYSTYPE
{
#line 34 "gunir/compiler/parser/big_query.yy" /* yacc.c:355  */

    int64_t _int;
    bool _bool;
    double _float;
    gunir::compiler::StringMessage* _string;

    gunir::compiler::ColumnPath* _column_path;
    gunir::compiler::ColumnPathList* _column_path_list;
    gunir::compiler::CreateTableStmt* _create_stmt;

    gunir::compiler::DropTableStmt* _drop_stmt;
    gunir::compiler::HelpStmt* _help_stmt;
    gunir::compiler::ShowStmt* _show_stmt;
    gunir::compiler::HistoryStmt* _history_stmt;
    gunir::compiler::QuitStmt* _quit_stmt;
    gunir::compiler::DefineTableStmt* _define_stmt;

    gunir::compiler::JoinOperator _join_op;
    gunir::compiler::Limit* _limit;
    gunir::compiler::Operators  _operator;
    gunir::compiler::OrderColumnPathList* _order_column_path_list;
    gunir::compiler::OrderColumnPath* _order_column_path;
    gunir::compiler::OrderType _order_type;

    gunir::compiler::RawArguments* _raw_arguments;
    gunir::compiler::RawAtomicExpression* _raw_atomic_expression;
    gunir::compiler::RawExpressionList* _raw_expression_list;
    gunir::compiler::RawExpression* _raw_expression;
    gunir::compiler::RawFunction* _raw_function;
    gunir::compiler::RawJoin* _raw_join;
    gunir::compiler::RawTableList* _raw_table_list;
    gunir::compiler::RawTable* _raw_table;
    gunir::compiler::RawTargetList* _raw_target_list;
    gunir::compiler::RawTarget* _raw_target;
    gunir::compiler::RawWithin* _raw_within;

    gunir::compiler::QueryStmt* _query_stmt;
    gunir::compiler::SelectStmt* _select_stmt;

#line 249 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:355  */
};
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif

/* Location type.  */
#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE YYLTYPE;
struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
};
# define YYLTYPE_IS_DECLARED 1
# define YYLTYPE_IS_TRIVIAL 1
#endif


extern YYSTYPE  gunir_lval;
extern YYLTYPE  gunir_lloc;
int  gunir_parse (yyscan_t yyscanner, YaccMemoryPool* memory_pool, std::ostream* output);

#endif /* !YY__GUNIR_BUILD64_DEBUG_GUNIR_PARSER_BIG_QUERY_YY_HH_INCLUDED  */

/* Copy the second part of user declarations.  */

#line 278 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:358  */

#ifdef short
# undef short
#endif

#ifdef YYTYPE_UINT8
typedef YYTYPE_UINT8 yytype_uint8;
#else
typedef unsigned char yytype_uint8;
#endif

#ifdef YYTYPE_INT8
typedef YYTYPE_INT8 yytype_int8;
#else
typedef signed char yytype_int8;
#endif

#ifdef YYTYPE_UINT16
typedef YYTYPE_UINT16 yytype_uint16;
#else
typedef unsigned short int yytype_uint16;
#endif

#ifdef YYTYPE_INT16
typedef YYTYPE_INT16 yytype_int16;
#else
typedef short int yytype_int16;
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif ! defined YYSIZE_T
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned int
# endif
#endif

#define YYSIZE_MAXIMUM ((YYSIZE_T) -1)

#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(Msgid) dgettext ("bison-runtime", Msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(Msgid) Msgid
# endif
#endif

#ifndef YY_ATTRIBUTE
# if (defined __GNUC__                                               \
      && (2 < __GNUC__ || (__GNUC__ == 2 && 96 <= __GNUC_MINOR__)))  \
     || defined __SUNPRO_C && 0x5110 <= __SUNPRO_C
#  define YY_ATTRIBUTE(Spec) __attribute__(Spec)
# else
#  define YY_ATTRIBUTE(Spec) /* empty */
# endif
#endif

#ifndef YY_ATTRIBUTE_PURE
# define YY_ATTRIBUTE_PURE   YY_ATTRIBUTE ((__pure__))
#endif

#ifndef YY_ATTRIBUTE_UNUSED
# define YY_ATTRIBUTE_UNUSED YY_ATTRIBUTE ((__unused__))
#endif

#if !defined _Noreturn \
     && (!defined __STDC_VERSION__ || __STDC_VERSION__ < 201112)
# if defined _MSC_VER && 1200 <= _MSC_VER
#  define _Noreturn __declspec (noreturn)
# else
#  define _Noreturn YY_ATTRIBUTE ((__noreturn__))
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YYUSE(E) ((void) (E))
#else
# define YYUSE(E) /* empty */
#endif

#if defined __GNUC__ && 407 <= __GNUC__ * 100 + __GNUC_MINOR__
/* Suppress an incorrect diagnostic about yylval being uninitialized.  */
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN \
    _Pragma ("GCC diagnostic push") \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")\
    _Pragma ("GCC diagnostic ignored \"-Wmaybe-uninitialized\"")
# define YY_IGNORE_MAYBE_UNINITIALIZED_END \
    _Pragma ("GCC diagnostic pop")
#else
# define YY_INITIAL_VALUE(Value) Value
#endif
#ifndef YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_END
#endif
#ifndef YY_INITIAL_VALUE
# define YY_INITIAL_VALUE(Value) /* Nothing. */
#endif


#if ! defined yyoverflow || YYERROR_VERBOSE

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined EXIT_SUCCESS
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
      /* Use EXIT_SUCCESS as a witness for stdlib.h.  */
#     ifndef EXIT_SUCCESS
#      define EXIT_SUCCESS 0
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's 'empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (0)
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined EXIT_SUCCESS \
       && ! ((defined YYMALLOC || defined malloc) \
             && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef EXIT_SUCCESS
#    define EXIT_SUCCESS 0
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined EXIT_SUCCESS
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined EXIT_SUCCESS
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* ! defined yyoverflow || YYERROR_VERBOSE */


#if (! defined yyoverflow \
     && (! defined __cplusplus \
         || (defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL \
             && defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yytype_int16 yyss_alloc;
  YYSTYPE yyvs_alloc;
  YYLTYPE yyls_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (sizeof (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (sizeof (yytype_int16) + sizeof (YYSTYPE) + sizeof (YYLTYPE)) \
      + 2 * YYSTACK_GAP_MAXIMUM)

# define YYCOPY_NEEDED 1

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)                           \
    do                                                                  \
      {                                                                 \
        YYSIZE_T yynewbytes;                                            \
        YYCOPY (&yyptr->Stack_alloc, Stack, yysize);                    \
        Stack = &yyptr->Stack_alloc;                                    \
        yynewbytes = yystacksize * sizeof (*Stack) + YYSTACK_GAP_MAXIMUM; \
        yyptr += yynewbytes / sizeof (*yyptr);                          \
      }                                                                 \
    while (0)

#endif

#if defined YYCOPY_NEEDED && YYCOPY_NEEDED
/* Copy COUNT objects from SRC to DST.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(Dst, Src, Count) \
      __builtin_memcpy (Dst, Src, (Count) * sizeof (*(Src)))
#  else
#   define YYCOPY(Dst, Src, Count)              \
      do                                        \
        {                                       \
          YYSIZE_T yyi;                         \
          for (yyi = 0; yyi < (Count); yyi++)   \
            (Dst)[yyi] = (Src)[yyi];            \
        }                                       \
      while (0)
#  endif
# endif
#endif /* !YYCOPY_NEEDED */

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  65
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   215

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  68
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  66
/* YYNRULES -- Number of rules.  */
#define YYNRULES  144
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  219

/* YYTRANSLATE[YYX] -- Symbol number corresponding to YYX as returned
   by yylex, with out-of-bounds checking.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   322

#define YYTRANSLATE(YYX)                                                \
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[TOKEN-NUM] -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex, without out-of-bounds checking.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44,
      45,    46,    47,    48,    49,    50,    51,    52,    53,    54,
      55,    56,    57,    58,    59,    60,    61,    62,    63,    64,
      65,    66,    67
};

#if YYDEBUG
  /* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_uint16 yyrline[] =
{
       0,   201,   201,   205,   209,   213,   217,   221,   225,   229,
     233,   237,   241,   245,   249,   253,   257,   261,   265,   272,
     282,   295,   303,   308,   316,   321,   329,   334,   340,   349,
     356,   365,   405,   411,   417,   423,   432,   440,   451,   465,
     471,   479,   483,   489,   497,   505,   511,   519,   529,   533,
     537,   546,   550,   554,   558,   562,   566,   570,   574,   578,
     582,   586,   600,   614,   618,   627,   636,   640,   644,   650,
     658,   667,   675,   681,   688,   692,   696,   700,   704,   710,
     714,   720,   724,   730,   734,   740,   744,   750,   754,   760,
     764,   770,   774,   780,   784,   790,   794,   800,   804,   810,
     816,   826,   833,   840,   847,   854,   861,   868,   877,   887,
     895,   903,   912,   922,   928,   934,   940,   946,   952,   956,
     962,   966,   970,   974,   980,   984,   990,   994,  1000,  1004,
    1008,  1015,  1021,  1025,  1031,  1035,  1041,  1047,  1053,  1059,
    1063,  1067,  1073,  1077,  1081
};
#endif

#if YYDEBUG || YYERROR_VERBOSE || 1
/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "$end", "error", "$undefined", "SHOW", "HISTORY", "HELP", "QUIT",
  "DEFINE", "CREATE", "DROP", "SELECT", "WITHIN", "RECORD", "AS", "FROM",
  "WHERE", "JOIN", "INNER", "LEFT", "OUTER", "ON", "GROUPBY", "HAVING",
  "ORDERBY", "DISTINCT", "DESC", "ASC", "LIMIT", "LOGICAL_OR",
  "LOGICAL_AND", "LOGICAL_NOT", "BITWISE_AND", "BITWISE_NOT", "BITWISE_OR",
  "BITWISE_XOR", "BITWISE_LEFT_SHIFT", "BITWISE_RIGHT_SHIFT", "EQUAL",
  "NOT_EQUAL", "LESS_THAN_OR_EQUAL", "LESS_THAN", "GREATER_THAN_OR_EQUAL",
  "GREATER_THAN", "SLASH", "DIV", "STAR", "PLUS", "MINUS", "REMAINDER",
  "CONTAINS", "DOT", "COLON", "COMMA", "SEMICOLON", "LPAREN", "RPAREN",
  "LSQUARE", "RSQUARE", "LCURLY", "RCURLY", "ENDOF", "LEX_ERROR", "WS",
  "INT", "FLOAT", "BOOLEAN", "ID", "STRING", "$accept", "query_stmt",
  "createStatement", "dropStatement", "showStatement", "helpStatement",
  "historyStatement", "quitStatement", "defineStatement",
  "selectStatement", "selectClause", "targetClause", "targetList",
  "target", "withinClause", "fromClause", "tableList", "table",
  "whereClause", "joinClause", "joinOperator", "groupbyClause",
  "havingClause", "orderbyClause", "limitClause", "orderbyColumnPathList",
  "orderbyColumnPath", "columnPathList", "columnPath", "fieldName",
  "tableName", "alias", "expression", "binary10thPrcdexpr",
  "binary9thPrcdexpr", "binary8thPrcdexpr", "binary7thPrcdexpr",
  "binary6thPrcdexpr", "binary5thPrcdexpr", "binary4thPrcdexpr",
  "binary3rdPrcdexpr", "binary2ndPrcdexpr", "binary1stPrcdexpr",
  "unaryPrefixExpr", "atomExpr", "function", "args", "expressionList",
  "binary11thPrcdOp", "binary10thPrcdOp", "binary9thPrcdOp",
  "binary8thPrcdOp", "binary7thPrcdOp", "binary6thPrcdOp",
  "binary5thPrcdOp", "binary4thPrcdOp", "binary3rdPrcdOp",
  "binary2ndPrcdOp", "binary1stPrcdOp", "unaryPrefixOp", "divideOp",
  "multiplyOp", "id", "char_string", "integer", "float", YY_NULLPTR
};
#endif

# ifdef YYPRINT
/* YYTOKNUM[NUM] -- (External) token number corresponding to the
   (internal) symbol number NUM (which must be that of a token).  */
static const yytype_uint16 yytoknum[] =
{
       0,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   288,   289,   290,   291,   292,   293,   294,
     295,   296,   297,   298,   299,   300,   301,   302,   303,   304,
     305,   306,   307,   308,   309,   310,   311,   312,   313,   314,
     315,   316,   317,   318,   319,   320,   321,   322
};
# endif

#define YYPACT_NINF -155

#define yypact_value_is_default(Yystate) \
  (!!((Yystate) == (-155)))

#define YYTABLE_NINF -55

#define yytable_value_is_error(Yytable_value) \
  0

  /* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
     STATE-NUM.  */
static const yytype_int16 yypact[] =
{
       7,   -46,   -12,   -36,  -155,   -46,   -46,   -46,    -5,  -155,
      73,   -14,     3,    11,    16,    41,    46,    49,    51,    79,
    -155,  -155,  -155,    40,    42,  -155,   -12,  -155,  -155,   100,
     109,  -155,     4,  -155,  -155,  -155,   -27,    14,    68,  -155,
    -155,  -155,    77,  -155,    80,  -155,    15,   107,   104,   105,
     110,    43,    86,    55,    61,    76,    89,  -155,  -155,  -155,
      68,    88,  -155,  -155,  -155,  -155,    83,  -155,    84,  -155,
      85,  -155,    90,  -155,    91,  -155,    92,  -155,    93,  -155,
      95,  -155,    -9,   125,  -155,  -155,  -155,   -36,   -36,    80,
    -155,  -155,  -155,   -26,    68,   -46,     9,  -155,   133,    68,
    -155,    68,  -155,    68,  -155,    68,  -155,    68,  -155,  -155,
      68,  -155,  -155,  -155,  -155,    68,  -155,  -155,    68,  -155,
    -155,    68,  -155,  -155,  -155,  -155,    68,  -155,  -155,  -155,
      68,  -155,    20,  -155,  -155,  -155,  -155,  -155,  -155,  -155,
    -155,   138,   106,  -155,   143,    68,    37,  -155,   -36,  -155,
    -155,  -155,  -155,    80,   -46,   107,   104,   105,   110,    43,
      86,    55,    61,    76,    89,  -155,  -155,   129,   112,   113,
     115,    -9,   -46,   129,  -155,   140,   141,   145,   -36,  -155,
    -155,  -155,    68,   151,  -155,  -155,  -155,     4,   144,    -9,
     -36,   129,   -46,   116,    80,    68,   150,   154,  -155,  -155,
       4,   129,     4,   148,    68,    80,   128,  -155,    -3,   -12,
    -155,   129,     4,  -155,  -155,   130,  -155,   -12,  -155
};

  /* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
     Performed when YYTABLE does not specify something else to do.  Zero
     means the default is an error.  */
static const yytype_uint8 yydefact[] =
{
       0,    22,    26,    24,    29,     0,     0,     0,     0,    18,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     137,    23,    75,     0,     0,   139,    27,   138,    25,     0,
       0,    21,     0,   133,   132,    33,     0,     0,     0,   142,
     103,    32,    34,    35,   106,    71,    41,    77,    79,    81,
      83,    85,    87,    89,    91,    93,    95,    97,    99,   105,
       0,    74,   104,   101,   102,     1,     0,     5,     0,     7,
       0,     9,     0,    13,     0,    11,     0,    15,     0,    17,
       0,     3,     0,    49,   140,   141,    28,     0,     0,    73,
      74,   143,   144,     0,     0,     0,     0,   113,    37,     0,
     114,     0,   115,     0,   116,     0,   117,     0,   118,   119,
       0,   121,   120,   123,   122,     0,   124,   125,     0,   126,
     127,     0,   134,   135,   136,   130,     0,   129,   128,   131,
       0,   100,     0,     4,     6,     8,    12,    10,    14,    16,
       2,     0,    42,    43,    45,     0,    51,    30,     0,   107,
      36,    72,    39,    40,     0,    78,    80,    82,    84,    86,
      88,    90,    92,    94,    96,    98,   110,   111,     0,   109,
       0,     0,     0,    48,    53,     0,    56,     0,     0,    38,
      76,   108,     0,     0,    44,    46,    52,     0,    58,     0,
      19,   112,     0,    55,    69,     0,    60,     0,    20,    47,
       0,    57,     0,    63,     0,    70,    59,    64,    66,     0,
      31,    50,     0,    68,    67,    61,    65,     0,    62
};

  /* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -155,  -155,  -155,  -155,  -155,  -155,  -155,  -155,  -155,    36,
    -155,  -155,  -155,    87,  -155,  -155,  -155,  -141,  -155,  -155,
    -155,  -155,  -155,  -155,  -155,  -155,   -29,  -155,   -24,    98,
     111,  -154,   -35,    96,    97,    81,    94,    78,    99,    71,
      72,    75,    82,   -51,  -155,  -155,  -155,  -155,  -155,  -155,
    -155,  -155,  -155,  -155,  -155,  -155,  -155,  -155,  -155,  -155,
    -155,  -155,     0,     1,    -2,  -155
};

  /* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,    10,    11,    12,    13,    14,    15,    16,    17,    18,
      19,    41,    42,    43,    98,    83,   142,   143,   146,   176,
     177,   188,   196,   203,   210,   206,   207,   193,    44,    45,
     144,   179,    46,    47,    48,    49,    50,    51,    52,    53,
      54,    55,    56,    57,    58,    59,   168,   169,    99,   101,
     103,   105,   107,   110,   115,   118,   121,   126,   130,    60,
     127,   128,    61,    62,    63,    64
};

  /* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
     positive, shift that token.  If negative, reduce the rule whose
     number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int16 yytable[] =
{
      26,    22,    97,    93,    28,    22,    22,    22,    89,   131,
       1,     2,     3,     4,     5,     6,     7,     8,   185,    32,
      20,   152,   213,   214,    86,    33,    96,    34,    32,   149,
     184,    27,    90,    32,    23,    24,    84,    91,   199,    66,
      35,    36,    37,    97,    32,   141,    67,    95,   197,    38,
      33,    25,    34,   -54,   174,   175,    68,    20,    25,    39,
      40,    20,    27,    69,    70,   166,    36,    37,     9,    72,
      20,    71,   153,    65,    38,    20,    73,    85,    92,   165,
     108,   109,    22,    25,    39,    40,    20,    27,   147,   148,
     116,   117,    32,    82,    74,    90,    90,   167,    33,    76,
      34,    75,    78,    84,    80,    85,    77,   119,   120,    79,
     173,    81,    21,    87,    36,    37,    29,    30,    31,   122,
     123,   124,    38,    88,   125,   111,   112,   113,   114,    94,
      95,    25,    39,    40,    20,    27,   100,   102,   129,   104,
     145,   106,   132,   133,   134,   135,   154,   191,     8,   178,
     136,   137,   138,   139,   180,   140,   172,    97,   171,   186,
     201,   189,   187,   194,   192,   182,   195,   181,   200,   211,
     183,    22,   180,   202,   204,   209,   205,   170,   208,   190,
     212,   150,   217,   216,   157,   159,   161,    90,   208,    22,
     162,   198,   180,   151,     0,   155,   163,     0,   156,   158,
      90,     0,    90,     0,     0,     0,     0,   215,   164,   160,
       0,     0,    90,     0,     0,   218
};

static const yytype_int16 yycheck[] =
{
       2,     1,    28,    38,     3,     5,     6,     7,    32,    60,
       3,     4,     5,     6,     7,     8,     9,    10,   172,    24,
      66,    12,    25,    26,    26,    30,    11,    32,    24,    55,
     171,    67,    32,    24,    46,    47,    63,    64,   192,    53,
      45,    46,    47,    28,    24,    54,    60,    50,   189,    54,
      30,    63,    32,    16,    17,    18,    53,    66,    63,    64,
      65,    66,    67,    60,    53,    45,    46,    47,    61,    53,
      66,    60,    96,     0,    54,    66,    60,    63,    64,   130,
      37,    38,    82,    63,    64,    65,    66,    67,    87,    88,
      35,    36,    24,    14,    53,    95,    96,   132,    30,    53,
      32,    60,    53,    63,    53,    63,    60,    46,    47,    60,
     145,    60,     1,    13,    46,    47,     5,     6,     7,    43,
      44,    45,    54,    14,    48,    39,    40,    41,    42,    52,
      50,    63,    64,    65,    66,    67,    29,    33,    49,    34,
      15,    31,    54,    60,    60,    60,    13,   182,    10,   148,
      60,    60,    60,    60,   154,    60,    13,    28,    52,    19,
     195,    16,    21,   187,    13,    52,    22,    55,    52,   204,
      55,   171,   172,    23,    20,    27,   200,   141,   202,   178,
      52,    94,    52,   212,   103,   107,   115,   187,   212,   189,
     118,   190,   192,    95,    -1,    99,   121,    -1,   101,   105,
     200,    -1,   202,    -1,    -1,    -1,    -1,   209,   126,   110,
      -1,    -1,   212,    -1,    -1,   217
};

  /* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
     symbol of state STATE-NUM.  */
static const yytype_uint8 yystos[] =
{
       0,     3,     4,     5,     6,     7,     8,     9,    10,    61,
      69,    70,    71,    72,    73,    74,    75,    76,    77,    78,
      66,    98,   130,    46,    47,    63,   132,    67,   131,    98,
      98,    98,    24,    30,    32,    45,    46,    47,    54,    64,
      65,    79,    80,    81,    96,    97,   100,   101,   102,   103,
     104,   105,   106,   107,   108,   109,   110,   111,   112,   113,
     127,   130,   131,   132,   133,     0,    53,    60,    53,    60,
      53,    60,    53,    60,    53,    60,    53,    60,    53,    60,
      53,    60,    14,    83,    63,    63,   132,    13,    14,    96,
     130,    64,    64,   100,    52,    50,    11,    28,    82,   116,
      29,   117,    33,   118,    34,   119,    31,   120,    37,    38,
     121,    39,    40,    41,    42,   122,    35,    36,   123,    46,
      47,   124,    43,    44,    45,    48,   125,   128,   129,    49,
     126,   111,    54,    60,    60,    60,    60,    60,    60,    60,
      60,    54,    84,    85,    98,    15,    86,   131,   131,    55,
      81,    97,    12,    96,    13,   101,   102,   103,   104,   105,
     106,   107,   108,   109,   110,   111,    45,   100,   114,   115,
      77,    52,    13,   100,    17,    18,    87,    88,   131,    99,
     130,    55,    52,    55,    85,    99,    19,    21,    89,    16,
     131,   100,    13,    95,    96,    22,    90,    85,   131,    99,
      52,   100,    23,    91,    20,    96,    93,    94,    96,    27,
      92,   100,    52,    25,    26,   132,    94,    52,   132
};

  /* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint8 yyr1[] =
{
       0,    68,    69,    69,    69,    69,    69,    69,    69,    69,
      69,    69,    69,    69,    69,    69,    69,    69,    69,    70,
      70,    71,    72,    72,    73,    73,    74,    74,    74,    75,
      76,    77,    78,    79,    79,    80,    80,    81,    81,    82,
      82,    82,    83,    84,    84,    85,    85,    85,    86,    86,
      87,    87,    88,    88,    88,    89,    89,    90,    90,    91,
      91,    92,    92,    92,    93,    93,    94,    94,    94,    95,
      95,    96,    96,    96,    97,    98,    99,   100,   100,   101,
     101,   102,   102,   103,   103,   104,   104,   105,   105,   106,
     106,   107,   107,   108,   108,   109,   109,   110,   110,   111,
     111,   112,   112,   112,   112,   112,   112,   112,   113,   114,
     114,   115,   115,   116,   117,   118,   119,   120,   121,   121,
     122,   122,   122,   122,   123,   123,   124,   124,   125,   125,
     125,   126,   127,   127,   128,   128,   129,   130,   131,   132,
     132,   132,   133,   133,   133
};

  /* YYR2[YYN] -- Number of symbols on the right hand side of rule YYN.  */
static const yytype_uint8 yyr2[] =
{
       0,     2,     3,     2,     3,     2,     3,     2,     3,     2,
       3,     2,     3,     2,     3,     2,     3,     2,     1,     6,
       7,     2,     1,     2,     1,     2,     1,     2,     3,     1,
       4,     8,     2,     1,     1,     1,     3,     2,     4,     2,
       2,     0,     2,     1,     3,     1,     3,     5,     2,     0,
       5,     0,     2,     1,     0,     2,     0,     2,     0,     2,
       0,     2,     4,     0,     1,     3,     1,     2,     2,     1,
       3,     1,     3,     2,     1,     1,     1,     1,     3,     1,
       3,     1,     3,     1,     3,     1,     3,     1,     3,     1,
       3,     1,     3,     1,     3,     1,     3,     1,     3,     1,
       2,     1,     1,     1,     1,     1,     1,     3,     4,     1,
       1,     1,     3,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       2,     2,     1,     2,     2
};


#define yyerrok         (yyerrstatus = 0)
#define yyclearin       (yychar = YYEMPTY)
#define YYEMPTY         (-2)
#define YYEOF           0

#define YYACCEPT        goto yyacceptlab
#define YYABORT         goto yyabortlab
#define YYERROR         goto yyerrorlab


#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)                                  \
do                                                              \
  if (yychar == YYEMPTY)                                        \
    {                                                           \
      yychar = (Token);                                         \
      yylval = (Value);                                         \
      YYPOPSTACK (yylen);                                       \
      yystate = *yyssp;                                         \
      goto yybackup;                                            \
    }                                                           \
  else                                                          \
    {                                                           \
      yyerror (yyscanner, memory_pool, output, YY_("syntax error: cannot back up")); \
      YYERROR;                                                  \
    }                                                           \
while (0)

/* Error token number */
#define YYTERROR        1
#define YYERRCODE       256


/* YYLLOC_DEFAULT -- Set CURRENT to span from RHS[1] to RHS[N].
   If N is 0, then set CURRENT to the empty location which ends
   the previous symbol: RHS[0] (always defined).  */

#ifndef YYLLOC_DEFAULT
# define YYLLOC_DEFAULT(Current, Rhs, N)                                \
    do                                                                  \
      if (N)                                                            \
        {                                                               \
          (Current).first_line   = YYRHSLOC (Rhs, 1).first_line;        \
          (Current).first_column = YYRHSLOC (Rhs, 1).first_column;      \
          (Current).last_line    = YYRHSLOC (Rhs, N).last_line;         \
          (Current).last_column  = YYRHSLOC (Rhs, N).last_column;       \
        }                                                               \
      else                                                              \
        {                                                               \
          (Current).first_line   = (Current).last_line   =              \
            YYRHSLOC (Rhs, 0).last_line;                                \
          (Current).first_column = (Current).last_column =              \
            YYRHSLOC (Rhs, 0).last_column;                              \
        }                                                               \
    while (0)
#endif

#define YYRHSLOC(Rhs, K) ((Rhs)[K])


/* Enable debugging if requested.  */
#if YYDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)                        \
do {                                            \
  if (yydebug)                                  \
    YYFPRINTF Args;                             \
} while (0)


/* YY_LOCATION_PRINT -- Print the location on the stream.
   This macro was not mandated originally: define only if we know
   we won't break user code: when these are the locations we know.  */

#ifndef YY_LOCATION_PRINT
# if defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL

/* Print *YYLOCP on YYO.  Private, do not rely on its existence. */

YY_ATTRIBUTE_UNUSED
static unsigned
yy_location_print_ (FILE *yyo, YYLTYPE const * const yylocp)
{
  unsigned res = 0;
  int end_col = 0 != yylocp->last_column ? yylocp->last_column - 1 : 0;
  if (0 <= yylocp->first_line)
    {
      res += YYFPRINTF (yyo, "%d", yylocp->first_line);
      if (0 <= yylocp->first_column)
        res += YYFPRINTF (yyo, ".%d", yylocp->first_column);
    }
  if (0 <= yylocp->last_line)
    {
      if (yylocp->first_line < yylocp->last_line)
        {
          res += YYFPRINTF (yyo, "-%d", yylocp->last_line);
          if (0 <= end_col)
            res += YYFPRINTF (yyo, ".%d", end_col);
        }
      else if (0 <= end_col && yylocp->first_column < end_col)
        res += YYFPRINTF (yyo, "-%d", end_col);
    }
  return res;
 }

#  define YY_LOCATION_PRINT(File, Loc)          \
  yy_location_print_ (File, &(Loc))

# else
#  define YY_LOCATION_PRINT(File, Loc) ((void) 0)
# endif
#endif


# define YY_SYMBOL_PRINT(Title, Type, Value, Location)                    \
do {                                                                      \
  if (yydebug)                                                            \
    {                                                                     \
      YYFPRINTF (stderr, "%s ", Title);                                   \
      yy_symbol_print (stderr,                                            \
                  Type, Value, Location, yyscanner, memory_pool, output); \
      YYFPRINTF (stderr, "\n");                                           \
    }                                                                     \
} while (0)


/*----------------------------------------.
| Print this symbol's value on YYOUTPUT.  |
`----------------------------------------*/

static void
yy_symbol_value_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp, yyscan_t yyscanner, YaccMemoryPool* memory_pool, std::ostream* output)
{
  FILE *yyo = yyoutput;
  YYUSE (yyo);
  YYUSE (yylocationp);
  YYUSE (yyscanner);
  YYUSE (memory_pool);
  YYUSE (output);
  if (!yyvaluep)
    return;
# ifdef YYPRINT
  if (yytype < YYNTOKENS)
    YYPRINT (yyoutput, yytoknum[yytype], *yyvaluep);
# endif
  YYUSE (yytype);
}


/*--------------------------------.
| Print this symbol on YYOUTPUT.  |
`--------------------------------*/

static void
yy_symbol_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp, yyscan_t yyscanner, YaccMemoryPool* memory_pool, std::ostream* output)
{
  YYFPRINTF (yyoutput, "%s %s (",
             yytype < YYNTOKENS ? "token" : "nterm", yytname[yytype]);

  YY_LOCATION_PRINT (yyoutput, *yylocationp);
  YYFPRINTF (yyoutput, ": ");
  yy_symbol_value_print (yyoutput, yytype, yyvaluep, yylocationp, yyscanner, memory_pool, output);
  YYFPRINTF (yyoutput, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

static void
yy_stack_print (yytype_int16 *yybottom, yytype_int16 *yytop)
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)                            \
do {                                                            \
  if (yydebug)                                                  \
    yy_stack_print ((Bottom), (Top));                           \
} while (0)


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

static void
yy_reduce_print (yytype_int16 *yyssp, YYSTYPE *yyvsp, YYLTYPE *yylsp, int yyrule, yyscan_t yyscanner, YaccMemoryPool* memory_pool, std::ostream* output)
{
  unsigned long int yylno = yyrline[yyrule];
  int yynrhs = yyr2[yyrule];
  int yyi;
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %lu):\n",
             yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr,
                       yystos[yyssp[yyi + 1 - yynrhs]],
                       &(yyvsp[(yyi + 1) - (yynrhs)])
                       , &(yylsp[(yyi + 1) - (yynrhs)])                       , yyscanner, memory_pool, output);
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)          \
do {                                    \
  if (yydebug)                          \
    yy_reduce_print (yyssp, yyvsp, yylsp, Rule, yyscanner, memory_pool, output); \
} while (0)

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args)
# define YY_SYMBOL_PRINT(Title, Type, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !YYDEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif


#if YYERROR_VERBOSE

# ifndef yystrlen
#  if defined __GLIBC__ && defined _STRING_H
#   define yystrlen strlen
#  else
/* Return the length of YYSTR.  */
static YYSIZE_T
yystrlen (const char *yystr)
{
  YYSIZE_T yylen;
  for (yylen = 0; yystr[yylen]; yylen++)
    continue;
  return yylen;
}
#  endif
# endif

# ifndef yystpcpy
#  if defined __GLIBC__ && defined _STRING_H && defined _GNU_SOURCE
#   define yystpcpy stpcpy
#  else
/* Copy YYSRC to YYDEST, returning the address of the terminating '\0' in
   YYDEST.  */
static char *
yystpcpy (char *yydest, const char *yysrc)
{
  char *yyd = yydest;
  const char *yys = yysrc;

  while ((*yyd++ = *yys++) != '\0')
    continue;

  return yyd - 1;
}
#  endif
# endif

# ifndef yytnamerr
/* Copy to YYRES the contents of YYSTR after stripping away unnecessary
   quotes and backslashes, so that it's suitable for yyerror.  The
   heuristic is that double-quoting is unnecessary unless the string
   contains an apostrophe, a comma, or backslash (other than
   backslash-backslash).  YYSTR is taken from yytname.  If YYRES is
   null, do not copy; instead, return the length of what the result
   would have been.  */
static YYSIZE_T
yytnamerr (char *yyres, const char *yystr)
{
  if (*yystr == '"')
    {
      YYSIZE_T yyn = 0;
      char const *yyp = yystr;

      for (;;)
        switch (*++yyp)
          {
          case '\'':
          case ',':
            goto do_not_strip_quotes;

          case '\\':
            if (*++yyp != '\\')
              goto do_not_strip_quotes;
            /* Fall through.  */
          default:
            if (yyres)
              yyres[yyn] = *yyp;
            yyn++;
            break;

          case '"':
            if (yyres)
              yyres[yyn] = '\0';
            return yyn;
          }
    do_not_strip_quotes: ;
    }

  if (! yyres)
    return yystrlen (yystr);

  return yystpcpy (yyres, yystr) - yyres;
}
# endif

/* Copy into *YYMSG, which is of size *YYMSG_ALLOC, an error message
   about the unexpected token YYTOKEN for the state stack whose top is
   YYSSP.

   Return 0 if *YYMSG was successfully written.  Return 1 if *YYMSG is
   not large enough to hold the message.  In that case, also set
   *YYMSG_ALLOC to the required number of bytes.  Return 2 if the
   required number of bytes is too large to store.  */
static int
yysyntax_error (YYSIZE_T *yymsg_alloc, char **yymsg,
                yytype_int16 *yyssp, int yytoken)
{
  YYSIZE_T yysize0 = yytnamerr (YY_NULLPTR, yytname[yytoken]);
  YYSIZE_T yysize = yysize0;
  enum { YYERROR_VERBOSE_ARGS_MAXIMUM = 5 };
  /* Internationalized format string. */
  const char *yyformat = YY_NULLPTR;
  /* Arguments of yyformat. */
  char const *yyarg[YYERROR_VERBOSE_ARGS_MAXIMUM];
  /* Number of reported tokens (one for the "unexpected", one per
     "expected"). */
  int yycount = 0;

  /* There are many possibilities here to consider:
     - If this state is a consistent state with a default action, then
       the only way this function was invoked is if the default action
       is an error action.  In that case, don't check for expected
       tokens because there are none.
     - The only way there can be no lookahead present (in yychar) is if
       this state is a consistent state with a default action.  Thus,
       detecting the absence of a lookahead is sufficient to determine
       that there is no unexpected or expected token to report.  In that
       case, just report a simple "syntax error".
     - Don't assume there isn't a lookahead just because this state is a
       consistent state with a default action.  There might have been a
       previous inconsistent state, consistent state with a non-default
       action, or user semantic action that manipulated yychar.
     - Of course, the expected token list depends on states to have
       correct lookahead information, and it depends on the parser not
       to perform extra reductions after fetching a lookahead from the
       scanner and before detecting a syntax error.  Thus, state merging
       (from LALR or IELR) and default reductions corrupt the expected
       token list.  However, the list is correct for canonical LR with
       one exception: it will still contain any token that will not be
       accepted due to an error action in a later state.
  */
  if (yytoken != YYEMPTY)
    {
      int yyn = yypact[*yyssp];
      yyarg[yycount++] = yytname[yytoken];
      if (!yypact_value_is_default (yyn))
        {
          /* Start YYX at -YYN if negative to avoid negative indexes in
             YYCHECK.  In other words, skip the first -YYN actions for
             this state because they are default actions.  */
          int yyxbegin = yyn < 0 ? -yyn : 0;
          /* Stay within bounds of both yycheck and yytname.  */
          int yychecklim = YYLAST - yyn + 1;
          int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
          int yyx;

          for (yyx = yyxbegin; yyx < yyxend; ++yyx)
            if (yycheck[yyx + yyn] == yyx && yyx != YYTERROR
                && !yytable_value_is_error (yytable[yyx + yyn]))
              {
                if (yycount == YYERROR_VERBOSE_ARGS_MAXIMUM)
                  {
                    yycount = 1;
                    yysize = yysize0;
                    break;
                  }
                yyarg[yycount++] = yytname[yyx];
                {
                  YYSIZE_T yysize1 = yysize + yytnamerr (YY_NULLPTR, yytname[yyx]);
                  if (! (yysize <= yysize1
                         && yysize1 <= YYSTACK_ALLOC_MAXIMUM))
                    return 2;
                  yysize = yysize1;
                }
              }
        }
    }

  switch (yycount)
    {
# define YYCASE_(N, S)                      \
      case N:                               \
        yyformat = S;                       \
      break
      YYCASE_(0, YY_("syntax error"));
      YYCASE_(1, YY_("syntax error, unexpected %s"));
      YYCASE_(2, YY_("syntax error, unexpected %s, expecting %s"));
      YYCASE_(3, YY_("syntax error, unexpected %s, expecting %s or %s"));
      YYCASE_(4, YY_("syntax error, unexpected %s, expecting %s or %s or %s"));
      YYCASE_(5, YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s"));
# undef YYCASE_
    }

  {
    YYSIZE_T yysize1 = yysize + yystrlen (yyformat);
    if (! (yysize <= yysize1 && yysize1 <= YYSTACK_ALLOC_MAXIMUM))
      return 2;
    yysize = yysize1;
  }

  if (*yymsg_alloc < yysize)
    {
      *yymsg_alloc = 2 * yysize;
      if (! (yysize <= *yymsg_alloc
             && *yymsg_alloc <= YYSTACK_ALLOC_MAXIMUM))
        *yymsg_alloc = YYSTACK_ALLOC_MAXIMUM;
      return 1;
    }

  /* Avoid sprintf, as that infringes on the user's name space.
     Don't have undefined behavior even if the translation
     produced a string with the wrong number of "%s"s.  */
  {
    char *yyp = *yymsg;
    int yyi = 0;
    while ((*yyp = *yyformat) != '\0')
      if (*yyp == '%' && yyformat[1] == 's' && yyi < yycount)
        {
          yyp += yytnamerr (yyp, yyarg[yyi++]);
          yyformat += 2;
        }
      else
        {
          yyp++;
          yyformat++;
        }
  }
  return 0;
}
#endif /* YYERROR_VERBOSE */

/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

static void
yydestruct (const char *yymsg, int yytype, YYSTYPE *yyvaluep, YYLTYPE *yylocationp, yyscan_t yyscanner, YaccMemoryPool* memory_pool, std::ostream* output)
{
  YYUSE (yyvaluep);
  YYUSE (yylocationp);
  YYUSE (yyscanner);
  YYUSE (memory_pool);
  YYUSE (output);
  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yytype, yyvaluep, yylocationp);

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YYUSE (yytype);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}




/* The lookahead symbol.  */
int yychar;

/* The semantic value of the lookahead symbol.  */
YYSTYPE yylval;
/* Location data for the lookahead symbol.  */
YYLTYPE yylloc
# if defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL
  = { 1, 1, 1, 1 }
# endif
;
/* Number of syntax errors so far.  */
int yynerrs;


/*----------.
| yyparse.  |
`----------*/

int
yyparse (yyscan_t yyscanner, YaccMemoryPool* memory_pool, std::ostream* output)
{
    int yystate;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus;

    /* The stacks and their tools:
       'yyss': related to states.
       'yyvs': related to semantic values.
       'yyls': related to locations.

       Refer to the stacks through separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* The state stack.  */
    yytype_int16 yyssa[YYINITDEPTH];
    yytype_int16 *yyss;
    yytype_int16 *yyssp;

    /* The semantic value stack.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs;
    YYSTYPE *yyvsp;

    /* The location stack.  */
    YYLTYPE yylsa[YYINITDEPTH];
    YYLTYPE *yyls;
    YYLTYPE *yylsp;

    /* The locations where the error started and ended.  */
    YYLTYPE yyerror_range[3];

    YYSIZE_T yystacksize;

  int yyn;
  int yyresult;
  /* Lookahead token as an internal (translated) token number.  */
  int yytoken = 0;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;
  YYLTYPE yyloc;

#if YYERROR_VERBOSE
  /* Buffer for error messages, and its allocated size.  */
  char yymsgbuf[128];
  char *yymsg = yymsgbuf;
  YYSIZE_T yymsg_alloc = sizeof yymsgbuf;
#endif

#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N), yylsp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  yyssp = yyss = yyssa;
  yyvsp = yyvs = yyvsa;
  yylsp = yyls = yylsa;
  yystacksize = YYINITDEPTH;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yystate = 0;
  yyerrstatus = 0;
  yynerrs = 0;
  yychar = YYEMPTY; /* Cause a token to be read.  */
  yylsp[0] = yylloc;
  goto yysetstate;

/*------------------------------------------------------------.
| yynewstate -- Push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
 yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;

 yysetstate:
  *yyssp = yystate;

  if (yyss + yystacksize - 1 <= yyssp)
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYSIZE_T yysize = yyssp - yyss + 1;

#ifdef yyoverflow
      {
        /* Give user a chance to reallocate the stack.  Use copies of
           these so that the &'s don't force the real ones into
           memory.  */
        YYSTYPE *yyvs1 = yyvs;
        yytype_int16 *yyss1 = yyss;
        YYLTYPE *yyls1 = yyls;

        /* Each stack pointer address is followed by the size of the
           data in use in that stack, in bytes.  This used to be a
           conditional around just the two extra args, but that might
           be undefined if yyoverflow is a macro.  */
        yyoverflow (YY_("memory exhausted"),
                    &yyss1, yysize * sizeof (*yyssp),
                    &yyvs1, yysize * sizeof (*yyvsp),
                    &yyls1, yysize * sizeof (*yylsp),
                    &yystacksize);

        yyls = yyls1;
        yyss = yyss1;
        yyvs = yyvs1;
      }
#else /* no yyoverflow */
# ifndef YYSTACK_RELOCATE
      goto yyexhaustedlab;
# else
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
        goto yyexhaustedlab;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
        yystacksize = YYMAXDEPTH;

      {
        yytype_int16 *yyss1 = yyss;
        union yyalloc *yyptr =
          (union yyalloc *) YYSTACK_ALLOC (YYSTACK_BYTES (yystacksize));
        if (! yyptr)
          goto yyexhaustedlab;
        YYSTACK_RELOCATE (yyss_alloc, yyss);
        YYSTACK_RELOCATE (yyvs_alloc, yyvs);
        YYSTACK_RELOCATE (yyls_alloc, yyls);
#  undef YYSTACK_RELOCATE
        if (yyss1 != yyssa)
          YYSTACK_FREE (yyss1);
      }
# endif
#endif /* no yyoverflow */

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;
      yylsp = yyls + yysize - 1;

      YYDPRINTF ((stderr, "Stack size increased to %lu\n",
                  (unsigned long int) yystacksize));

      if (yyss + yystacksize - 1 <= yyssp)
        YYABORT;
    }

  YYDPRINTF ((stderr, "Entering state %d\n", yystate));

  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;

/*-----------.
| yybackup.  |
`-----------*/
yybackup:

  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yypact_value_is_default (yyn))
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either YYEMPTY or YYEOF or a valid lookahead symbol.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token: "));
      yychar = yylex (yyscanner, memory_pool, output);
    }

  if (yychar <= YYEOF)
    {
      yychar = yytoken = YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yytable_value_is_error (yyn))
        goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);

  /* Discard the shifted token.  */
  yychar = YYEMPTY;

  yystate = yyn;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END
  *++yylsp = yylloc;
  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- Do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     '$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];

  /* Default location.  */
  YYLLOC_DEFAULT (yyloc, (yylsp - yylen), yylen);
  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
        case 2:
#line 201 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            make_query_stmt(select, (yyvsp[-2]._select_stmt), (yyval._query_stmt))
        }
#line 1644 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 3:
#line 205 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            make_query_stmt(select, (yyvsp[-1]._select_stmt), (yyval._query_stmt))
        }
#line 1652 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 4:
#line 209 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            make_query_stmt(create, (yyvsp[-2]._create_stmt), (yyval._query_stmt))
        }
#line 1660 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 5:
#line 213 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            make_query_stmt(create, (yyvsp[-1]._create_stmt), (yyval._query_stmt))
        }
#line 1668 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 6:
#line 217 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            make_query_stmt(drop, (yyvsp[-2]._drop_stmt), (yyval._query_stmt))
        }
#line 1676 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 7:
#line 221 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            make_query_stmt(drop, (yyvsp[-1]._drop_stmt), (yyval._query_stmt))
        }
#line 1684 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 8:
#line 225 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            make_query_stmt(show, (yyvsp[-2]._show_stmt), (yyval._query_stmt))
        }
#line 1692 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 9:
#line 229 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            make_query_stmt(show, (yyvsp[-1]._show_stmt), (yyval._query_stmt))
        }
#line 1700 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 10:
#line 233 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            make_query_stmt(history, (yyvsp[-2]._history_stmt), (yyval._query_stmt))
        }
#line 1708 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 11:
#line 237 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            make_query_stmt(history, (yyvsp[-1]._history_stmt), (yyval._query_stmt))
        }
#line 1716 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 12:
#line 241 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            make_query_stmt(help, (yyvsp[-2]._help_stmt), (yyval._query_stmt))
        }
#line 1724 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 13:
#line 245 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            make_query_stmt(help, (yyvsp[-1]._help_stmt), (yyval._query_stmt))
        }
#line 1732 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 14:
#line 249 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            make_query_stmt(quit, (yyvsp[-2]._quit_stmt), (yyval._query_stmt))
        }
#line 1740 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 15:
#line 253 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            make_query_stmt(quit, (yyvsp[-1]._quit_stmt), (yyval._query_stmt))
        }
#line 1748 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 16:
#line 257 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            make_query_stmt(define, (yyvsp[-2]._define_stmt), (yyval._query_stmt))
        }
#line 1756 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 17:
#line 261 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            make_query_stmt(define, (yyvsp[-1]._define_stmt), (yyval._query_stmt))
        }
#line 1764 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 18:
#line 265 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            return -1;
        }
#line 1772 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 19:
#line 272 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            CreateTableStmt* stmt = NewMessage<CreateTableStmt>(memory_pool);

            (stmt->mutable_table_name())->CopyFrom(*(yyvsp[-4]._string));
            (stmt->mutable_input_path())->CopyFrom(*(yyvsp[-2]._string));
            (stmt->mutable_table_schema())->CopyFrom(*(yyvsp[-1]._string));
            (stmt->mutable_message_name())->CopyFrom(*(yyvsp[0]._string));
            (yyval._create_stmt) = stmt;
        }
#line 1786 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 20:
#line 282 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            CreateTableStmt* stmt = NewMessage<CreateTableStmt>(memory_pool);

            (stmt->mutable_table_name())->CopyFrom(*(yyvsp[-5]._string));
            (stmt->mutable_input_path())->CopyFrom(*(yyvsp[-3]._string));
            (stmt->mutable_table_schema())->CopyFrom(*(yyvsp[-2]._string));
            (stmt->mutable_message_name())->CopyFrom(*(yyvsp[-1]._string));
            (stmt->mutable_charset_encoding())->CopyFrom(*(yyvsp[0]._string));
            (yyval._create_stmt) = stmt;
        }
#line 1801 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 21:
#line 295 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            DropTableStmt* stmt = NewMessage<DropTableStmt>(memory_pool);
            (stmt->mutable_table_name())->CopyFrom(*(yyvsp[0]._string));
            (yyval._drop_stmt) = stmt;
        }
#line 1811 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 22:
#line 303 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            ShowStmt* stmt = NewMessage<ShowStmt>(memory_pool);
            (yyval._show_stmt) = stmt;
        }
#line 1820 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 23:
#line 308 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            ShowStmt* stmt = NewMessage<ShowStmt>(memory_pool);
            (stmt->mutable_table_name())->CopyFrom(*(yyvsp[0]._string));
            (yyval._show_stmt) = stmt;
        }
#line 1830 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 24:
#line 316 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            HelpStmt* stmt = NewMessage<HelpStmt>(memory_pool);
            (yyval._help_stmt) = stmt;
        }
#line 1839 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 25:
#line 321 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            HelpStmt* stmt = NewMessage<HelpStmt>(memory_pool);
            (stmt->mutable_cmd_name())->CopyFrom(*(yyvsp[0]._string));
            (yyval._help_stmt) = stmt;
        }
#line 1849 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 26:
#line 329 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            HistoryStmt* stmt = NewMessage<HistoryStmt>(memory_pool);
            (yyval._history_stmt) = stmt;
        }
#line 1858 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 27:
#line 334 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            HistoryStmt* stmt = NewMessage<HistoryStmt>(memory_pool);
            stmt->set_start((yyvsp[0]._int));
            (yyval._history_stmt) = stmt;
        }
#line 1868 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 28:
#line 340 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            HistoryStmt* stmt = NewMessage<HistoryStmt>(memory_pool);
            stmt->set_start((yyvsp[-1]._int));
            stmt->set_size((yyvsp[0]._int));
            (yyval._history_stmt) = stmt;
        }
#line 1879 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 29:
#line 349 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            QuitStmt* stmt = NewMessage<QuitStmt>(memory_pool);
            (yyval._quit_stmt) = stmt;
        }
#line 1888 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 30:
#line 356 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            DefineTableStmt* stmt = NewMessage<DefineTableStmt>(memory_pool);
            (stmt->mutable_table_name())->CopyFrom(*(yyvsp[-2]._string));
            (stmt->mutable_input_path())->CopyFrom(*(yyvsp[0]._string));
            (yyval._define_stmt) = stmt;
        }
#line 1899 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 31:
#line 366 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            SelectStmt* stmt = NewMessage<SelectStmt>(memory_pool);

            if ((yyvsp[-7]._raw_target_list) != NULL) {
                (stmt->mutable_target_list())->CopyFrom(*(yyvsp[-7]._raw_target_list));
            }

            if ((yyvsp[-6]._raw_table_list) != NULL) {
                (stmt->mutable_from_list())->CopyFrom(*(yyvsp[-6]._raw_table_list));
            }

            if ((yyvsp[-5]._raw_expression) != NULL) {
                (stmt->mutable_where_clause())->CopyFrom(*(yyvsp[-5]._raw_expression));
            }

            if ((yyvsp[-4]._raw_join) != NULL) {
                (stmt->mutable_join())->CopyFrom(*(yyvsp[-4]._raw_join));
            }

            if ((yyvsp[-3]._column_path_list) != NULL) {
                (stmt->mutable_groupby())->CopyFrom(*(yyvsp[-3]._column_path_list));
            }

            if ((yyvsp[-2]._raw_expression) != NULL) {
                (stmt->mutable_having())->CopyFrom(*(yyvsp[-2]._raw_expression));
            }

            if ((yyvsp[-1]._order_column_path_list) != NULL) {
                (stmt->mutable_orderby())->CopyFrom(*(yyvsp[-1]._order_column_path_list));
            }

            if ((yyvsp[0]._limit) != NULL) {
                (stmt->mutable_limit())->CopyFrom(*(yyvsp[0]._limit));
            }
            (yyval._select_stmt) = stmt;
        }
#line 1940 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 32:
#line 405 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._raw_target_list) = (yyvsp[0]._raw_target_list);
        }
#line 1948 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 33:
#line 411 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            RawTargetList* target_list = NewMessage<RawTargetList>(memory_pool);
            target_list->set_target_is_star(true);
            (yyval._raw_target_list) = target_list;
        }
#line 1958 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 34:
#line 417 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._raw_target_list) = (yyvsp[0]._raw_target_list);
        }
#line 1966 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 35:
#line 423 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            RawTargetList* target_list = NewMessage<RawTargetList>(memory_pool);
            RawTarget* t = (target_list->mutable_target_list())->Add();

            t->CopyFrom(*(yyvsp[0]._raw_target));
            target_list->set_target_is_star(false);
            (yyval._raw_target_list) = target_list;
        }
#line 1979 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 36:
#line 432 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            RawTarget* t = ((yyvsp[-2]._raw_target_list)->mutable_target_list())->Add();
            t->CopyFrom(*(yyvsp[0]._raw_target));
            (yyval._raw_target_list) = (yyvsp[-2]._raw_target_list);
        }
#line 1989 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 37:
#line 440 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            RawTarget* target = NewMessage<RawTarget>(memory_pool);

            (target->mutable_expression())->CopyFrom(*(yyvsp[-1]._raw_expression));

            if ((yyvsp[0]._raw_within) != NULL) {
                (target->mutable_within())->CopyFrom(*(yyvsp[0]._raw_within));
            }
            (yyval._raw_target) = target;
        }
#line 2004 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 38:
#line 451 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            RawTarget* target = NewMessage<RawTarget>(memory_pool);

            (target->mutable_expression())->CopyFrom(*(yyvsp[-3]._raw_expression));

            if ((yyvsp[-2]._raw_within) != NULL) {
                (target->mutable_within())->CopyFrom(*(yyvsp[-2]._raw_within));
            }
            (target->mutable_alias())->CopyFrom(*(yyvsp[0]._string));
            (yyval._raw_target) = target;
        }
#line 2020 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 39:
#line 465 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            RawWithin* within = NewMessage<RawWithin>(memory_pool);
            within->set_is_record(true);
            (yyval._raw_within) = within;
        }
#line 2030 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 40:
#line 471 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            RawWithin* within = NewMessage<RawWithin>(memory_pool);

            within->set_is_record(false);
            (within->mutable_column())->CopyFrom(*(yyvsp[0]._column_path));
            (yyval._raw_within) = within;
        }
#line 2042 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 41:
#line 479 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    { (yyval._raw_within) = NULL; }
#line 2048 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 42:
#line 483 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._raw_table_list) = (yyvsp[0]._raw_table_list);
        }
#line 2056 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 43:
#line 489 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            RawTableList* list = NewMessage<RawTableList>(memory_pool);

            RawTable* t =  (list->mutable_table_list())->Add();
            t->CopyFrom(*(yyvsp[0]._raw_table));
            (yyval._raw_table_list) = list;
        }
#line 2068 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 44:
#line 497 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            RawTable* t = ((yyvsp[-2]._raw_table_list)->mutable_table_list())->Add();
            t->CopyFrom(*(yyvsp[0]._raw_table));
            (yyval._raw_table_list) = (yyvsp[-2]._raw_table_list);
        }
#line 2078 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 45:
#line 505 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            RawTable* table = NewMessage<RawTable>(memory_pool);
            (table->mutable_table_name())->CopyFrom(*(yyvsp[0]._string));
            (yyval._raw_table) = table;
        }
#line 2088 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 46:
#line 511 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            RawTable* table = NewMessage<RawTable>(memory_pool);

            (table->mutable_table_name())->CopyFrom(*(yyvsp[-2]._string));
            (table->mutable_alias())->CopyFrom(*(yyvsp[0]._string));
            (yyval._raw_table) = table;
        }
#line 2100 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 47:
#line 519 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            RawTable* table = NewMessage<RawTable>(memory_pool);

            (table->mutable_select_stmt())->CopyFrom(*(yyvsp[-3]._select_stmt));
            (table->mutable_alias())->CopyFrom(*(yyvsp[0]._string));
            (yyval._raw_table) = table;
        }
#line 2112 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 48:
#line 529 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._raw_expression) = (yyvsp[0]._raw_expression);
        }
#line 2120 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 49:
#line 533 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    { (yyval._raw_expression) = NULL; }
#line 2126 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 50:
#line 537 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            RawJoin* join = NewMessage<RawJoin>(memory_pool);

            join->set_op((yyvsp[-4]._join_op));
            (join->mutable_partner())->CopyFrom(*(yyvsp[-2]._raw_table));
            (join->mutable_expression())->CopyFrom(*(yyvsp[0]._raw_expression));
            (yyval._raw_join) = join;
        }
#line 2139 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 51:
#line 546 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    { (yyval._raw_join) = NULL; }
#line 2145 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 52:
#line 550 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._join_op) = kLeftOuter;
        }
#line 2153 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 53:
#line 554 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._join_op) = kInner;
        }
#line 2161 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 54:
#line 558 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    { (yyval._join_op) = kInner; }
#line 2167 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 55:
#line 562 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._column_path_list) = (yyvsp[0]._column_path_list);
        }
#line 2175 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 56:
#line 566 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    { (yyval._column_path_list) = NULL; }
#line 2181 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 57:
#line 570 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._raw_expression) = (yyvsp[0]._raw_expression);
        }
#line 2189 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 58:
#line 574 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    { (yyval._raw_expression) = NULL; }
#line 2195 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 59:
#line 578 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._order_column_path_list) = (yyvsp[0]._order_column_path_list);
        }
#line 2203 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 60:
#line 582 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    { (yyval._order_column_path_list) = NULL; }
#line 2209 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 61:
#line 586 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            Limit* limit = NewMessage<Limit>(memory_pool);

            if ((yyvsp[0]._int) < 0) {
                yyerror(yyscanner, memory_pool, output,
                    "error, limit number is negative");
                return -1;
            }

            limit->set_start(0);
            limit->set_number((yyvsp[0]._int));
            (yyval._limit) = limit;
        }
#line 2227 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 62:
#line 600 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            Limit* limit = NewMessage<Limit>(memory_pool);

            if ((yyvsp[-2]._int) < 0 || (yyvsp[0]._int) < 0) {
                yyerror(yyscanner, memory_pool, output,
                    "error, limit number is negative");
                return -1;
            }

            limit->set_start((yyvsp[-2]._int));
            limit->set_number((yyvsp[0]._int));
            (yyval._limit) = limit;
        }
#line 2245 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 63:
#line 614 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    { (yyval._limit) = NULL; }
#line 2251 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 64:
#line 618 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            OrderColumnPathList* list =
                NewMessage<OrderColumnPathList>(memory_pool);

            OrderColumnPath* path = (list->mutable_path_list())->Add();
            path->CopyFrom(*(yyvsp[0]._order_column_path));
            (yyval._order_column_path_list) = list;
        }
#line 2264 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 65:
#line 627 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            OrderColumnPath* path = ((yyvsp[-2]._order_column_path_list)->mutable_path_list())->Add();
            path->CopyFrom(*(yyvsp[0]._order_column_path));
            (yyval._order_column_path_list) = (yyvsp[-2]._order_column_path_list);
        }
#line 2274 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 66:
#line 636 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._order_column_path) = add_order_column_path(*(yyvsp[0]._column_path), kAsc, memory_pool);
        }
#line 2282 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 67:
#line 640 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._order_column_path) = add_order_column_path(*(yyvsp[-1]._column_path), kAsc, memory_pool);
        }
#line 2290 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 68:
#line 644 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._order_column_path) = add_order_column_path(*(yyvsp[-1]._column_path), kDesc, memory_pool);
        }
#line 2298 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 69:
#line 650 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            ColumnPathList * list = NewMessage<ColumnPathList>(memory_pool);

            ColumnPath* path = (list->mutable_path_list())->Add();
            path->CopyFrom(*(yyvsp[0]._column_path));
            (yyval._column_path_list) = list;
        }
#line 2310 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 70:
#line 658 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            ColumnPath* path = ((yyvsp[-2]._column_path_list)->mutable_path_list())->Add();
            path->CopyFrom(*(yyvsp[0]._column_path));
            (yyval._column_path_list) = (yyvsp[-2]._column_path_list);
        }
#line 2320 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 71:
#line 667 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            ColumnPath* path = NewMessage<ColumnPath>(memory_pool);

            StringMessage* s = (path->mutable_field_list())->Add();
            s->CopyFrom(*(yyvsp[0]._string));
            (yyval._column_path) = path;
        }
#line 2332 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 72:
#line 675 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            StringMessage* s = ((yyvsp[-2]._column_path)->mutable_field_list())->Add();
            s->CopyFrom(*(yyvsp[0]._string));
            (yyval._column_path) = (yyvsp[-2]._column_path);
        }
#line 2342 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 73:
#line 681 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyvsp[0]._column_path)->set_has_distinct(true);
            (yyval._column_path) = (yyvsp[0]._column_path);
        }
#line 2351 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 74:
#line 688 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    { (yyval._string) = (yyvsp[0]._string); }
#line 2357 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 75:
#line 692 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    { (yyval._string) = (yyvsp[0]._string); }
#line 2363 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 76:
#line 696 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    { (yyval._string) = (yyvsp[0]._string); }
#line 2369 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 77:
#line 700 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._raw_expression) = (yyvsp[0]._raw_expression);
        }
#line 2377 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 78:
#line 704 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._raw_expression) = make_expression((yyvsp[-2]._raw_expression), (yyvsp[-1]._operator), (yyvsp[0]._raw_expression), memory_pool);
        }
#line 2385 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 79:
#line 710 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._raw_expression) = (yyvsp[0]._raw_expression);
        }
#line 2393 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 80:
#line 714 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._raw_expression) = make_expression((yyvsp[-2]._raw_expression), (yyvsp[-1]._operator), (yyvsp[0]._raw_expression), memory_pool);
        }
#line 2401 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 81:
#line 720 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._raw_expression) = (yyvsp[0]._raw_expression);
        }
#line 2409 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 82:
#line 724 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._raw_expression) = make_expression((yyvsp[-2]._raw_expression), (yyvsp[-1]._operator), (yyvsp[0]._raw_expression), memory_pool);
        }
#line 2417 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 83:
#line 730 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._raw_expression) = (yyvsp[0]._raw_expression);
        }
#line 2425 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 84:
#line 734 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._raw_expression) = make_expression((yyvsp[-2]._raw_expression), (yyvsp[-1]._operator), (yyvsp[0]._raw_expression), memory_pool);
        }
#line 2433 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 85:
#line 740 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._raw_expression) = (yyvsp[0]._raw_expression);
        }
#line 2441 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 86:
#line 744 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._raw_expression) = make_expression((yyvsp[-2]._raw_expression), (yyvsp[-1]._operator), (yyvsp[0]._raw_expression), memory_pool);
        }
#line 2449 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 87:
#line 750 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._raw_expression) = (yyvsp[0]._raw_expression);
        }
#line 2457 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 88:
#line 754 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._raw_expression) = make_expression((yyvsp[-2]._raw_expression), (yyvsp[-1]._operator), (yyvsp[0]._raw_expression), memory_pool);
        }
#line 2465 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 89:
#line 760 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._raw_expression) = (yyvsp[0]._raw_expression);
        }
#line 2473 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 90:
#line 764 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._raw_expression) = make_expression((yyvsp[-2]._raw_expression), (yyvsp[-1]._operator), (yyvsp[0]._raw_expression), memory_pool);
        }
#line 2481 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 91:
#line 770 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._raw_expression) = (yyvsp[0]._raw_expression);
        }
#line 2489 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 92:
#line 774 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._raw_expression) = make_expression((yyvsp[-2]._raw_expression), (yyvsp[-1]._operator), (yyvsp[0]._raw_expression), memory_pool);
        }
#line 2497 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 93:
#line 780 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._raw_expression) = (yyvsp[0]._raw_expression);
        }
#line 2505 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 94:
#line 784 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._raw_expression) = make_expression((yyvsp[-2]._raw_expression), (yyvsp[-1]._operator), (yyvsp[0]._raw_expression), memory_pool);
        }
#line 2513 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 95:
#line 790 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._raw_expression) = (yyvsp[0]._raw_expression);
        }
#line 2521 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 96:
#line 794 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._raw_expression) = make_expression((yyvsp[-2]._raw_expression), (yyvsp[-1]._operator), (yyvsp[0]._raw_expression), memory_pool);
        }
#line 2529 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 97:
#line 800 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._raw_expression) = (yyvsp[0]._raw_expression);
        }
#line 2537 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 98:
#line 804 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._raw_expression) = make_expression((yyvsp[-2]._raw_expression), (yyvsp[-1]._operator), (yyvsp[0]._raw_expression), memory_pool);
        }
#line 2545 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 99:
#line 810 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            RawExpression* expr = NewMessage<RawExpression>(memory_pool);
            (expr->mutable_atomic())->CopyFrom(*(yyvsp[0]._raw_atomic_expression));
            (yyval._raw_expression) = expr;
        }
#line 2555 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 100:
#line 816 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            RawExpression* expr = NewMessage<RawExpression>(memory_pool);

            expr->set_op((yyvsp[-1]._operator));
            (expr->mutable_left())->CopyFrom(*(yyvsp[0]._raw_expression));
            (yyval._raw_expression) = expr;
        }
#line 2567 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 101:
#line 826 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            RawAtomicExpression* expr =
                NewMessage<RawAtomicExpression>(memory_pool);
            expr->set_integer((yyvsp[0]._int));
            (yyval._raw_atomic_expression) = expr;
        }
#line 2578 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 102:
#line 833 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            RawAtomicExpression* expr =
                NewMessage<RawAtomicExpression>(memory_pool);
            expr->set_floating((yyvsp[0]._float));
            (yyval._raw_atomic_expression) = expr;
        }
#line 2589 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 103:
#line 840 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            RawAtomicExpression* expr =
                NewMessage<RawAtomicExpression>(memory_pool);
            expr->set_boolean((yyvsp[0]._bool));
            (yyval._raw_atomic_expression) = expr;
        }
#line 2600 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 104:
#line 847 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            RawAtomicExpression* expr =
                NewMessage<RawAtomicExpression>(memory_pool);
            (expr->mutable_char_string())->CopyFrom(*(yyvsp[0]._string));
            (yyval._raw_atomic_expression) = expr;
        }
#line 2611 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 105:
#line 854 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            RawAtomicExpression* expr =
                NewMessage<RawAtomicExpression>(memory_pool);
            (expr->mutable_function())->CopyFrom(*(yyvsp[0]._raw_function));
            (yyval._raw_atomic_expression) = expr;
        }
#line 2622 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 106:
#line 861 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            RawAtomicExpression* expr =
                NewMessage<RawAtomicExpression>(memory_pool);
            (expr->mutable_column())->CopyFrom(*(yyvsp[0]._column_path));
            (yyval._raw_atomic_expression) = expr;
        }
#line 2633 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 107:
#line 868 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            RawAtomicExpression* expr =
                NewMessage<RawAtomicExpression>(memory_pool);
            (expr->mutable_expression())->CopyFrom(*(yyvsp[-1]._raw_expression));
            (yyval._raw_atomic_expression) = expr;
        }
#line 2644 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 108:
#line 877 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            RawFunction* function = NewMessage<RawFunction>(memory_pool);

            (function->mutable_function_name())->CopyFrom(*(yyvsp[-3]._string));
            (function->mutable_args())->CopyFrom(*(yyvsp[-1]._raw_arguments));
            (yyval._raw_function) = function;
        }
#line 2656 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 109:
#line 887 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            RawArguments* args = NewMessage<RawArguments>(memory_pool);

            (args->mutable_arg_list())->CopyFrom(*(yyvsp[0]._raw_expression_list));
            args->set_arg_is_star(false);
            (yyval._raw_arguments) = args;
        }
#line 2668 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 110:
#line 895 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            RawArguments* args = NewMessage<RawArguments>(memory_pool);
            args->set_arg_is_star(true);
            (yyval._raw_arguments) = args;
        }
#line 2678 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 111:
#line 903 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            RawExpressionList* list =
                NewMessage<RawExpressionList>(memory_pool);
            RawExpression* e = (list->mutable_expr_list())->Add();

            e->CopyFrom(*(yyvsp[0]._raw_expression));
            (yyval._raw_expression_list) = list;
        }
#line 2691 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 112:
#line 912 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            RawExpression* e = ((yyvsp[-2]._raw_expression_list)->mutable_expr_list())->Add();
            e->CopyFrom(*(yyvsp[0]._raw_expression));
            (yyval._raw_expression_list) = (yyvsp[-2]._raw_expression_list);
        }
#line 2701 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 113:
#line 922 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._operator) = kLogicalOr;
        }
#line 2709 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 114:
#line 928 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._operator) = kLogicalAnd;
        }
#line 2717 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 115:
#line 934 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._operator) = kBitwiseOr;
        }
#line 2725 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 116:
#line 940 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._operator) = kBitwiseXor;
        }
#line 2733 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 117:
#line 946 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._operator) = kBitwiseAnd;
        }
#line 2741 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 118:
#line 952 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._operator) = kEqual;
        }
#line 2749 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 119:
#line 956 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._operator) = kNotEqual;
        }
#line 2757 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 120:
#line 962 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._operator) = kLess;
        }
#line 2765 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 121:
#line 966 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._operator) = kLessEqual;
        }
#line 2773 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 122:
#line 970 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._operator) = kGreater;
        }
#line 2781 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 123:
#line 974 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._operator) = kGreaterEqual;
        }
#line 2789 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 124:
#line 980 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._operator) = kBitwiseLeftShift;
        }
#line 2797 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 125:
#line 984 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._operator) = kBitwiseRightShift;
        }
#line 2805 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 126:
#line 990 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._operator) = kAdd;
        }
#line 2813 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 127:
#line 994 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._operator) = kSub;
        }
#line 2821 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 128:
#line 1000 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._operator) = (yyvsp[0]._operator);
        }
#line 2829 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 129:
#line 1004 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._operator) = (yyvsp[0]._operator);
        }
#line 2837 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 130:
#line 1008 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._operator) = kRemainder;
        }
#line 2845 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 131:
#line 1015 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._operator) = kContains;
        }
#line 2853 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 132:
#line 1021 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._operator) = kBitwiseNot;
        }
#line 2861 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 133:
#line 1025 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._operator) = kLogicalNot;
        }
#line 2869 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 134:
#line 1031 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._operator) = kDiv;
        }
#line 2877 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 135:
#line 1035 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._operator) = kDiv;
        }
#line 2885 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 136:
#line 1041 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._operator) = kMul;
        }
#line 2893 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 137:
#line 1047 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._string) = (yyvsp[0]._string);
        }
#line 2901 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 138:
#line 1053 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._string) = (yyvsp[0]._string);
        }
#line 2909 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 139:
#line 1059 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._int) = (yyvsp[0]._int);
        }
#line 2917 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 140:
#line 1063 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._int) = (yyvsp[0]._int);
        }
#line 2925 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 141:
#line 1067 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._int) = (yyvsp[0]._int) * -1;
        }
#line 2933 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 142:
#line 1073 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._float) = (yyvsp[0]._float);
        }
#line 2941 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 143:
#line 1077 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._float) = (yyvsp[0]._float);
        }
#line 2949 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;

  case 144:
#line 1081 "gunir/compiler/parser/big_query.yy" /* yacc.c:1646  */
    {
            (yyval._float) = (yyvsp[0]._float) * -1;
        }
#line 2957 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
    break;


#line 2961 "build64_debug/gunir/compiler/parser/big_query.yy.cc" /* yacc.c:1646  */
      default: break;
    }
  /* User semantic actions sometimes alter yychar, and that requires
     that yytoken be updated with the new translation.  We take the
     approach of translating immediately before every use of yytoken.
     One alternative is translating here after every semantic action,
     but that translation would be missed if the semantic action invokes
     YYABORT, YYACCEPT, or YYERROR immediately after altering yychar or
     if it invokes YYBACKUP.  In the case of YYABORT or YYACCEPT, an
     incorrect destructor might then be invoked immediately.  In the
     case of YYERROR or YYBACKUP, subsequent parser actions might lead
     to an incorrect destructor call or verbose syntax error message
     before the lookahead is translated.  */
  YY_SYMBOL_PRINT ("-> $$ =", yyr1[yyn], &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);

  *++yyvsp = yyval;
  *++yylsp = yyloc;

  /* Now 'shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */

  yyn = yyr1[yyn];

  yystate = yypgoto[yyn - YYNTOKENS] + *yyssp;
  if (0 <= yystate && yystate <= YYLAST && yycheck[yystate] == *yyssp)
    yystate = yytable[yystate];
  else
    yystate = yydefgoto[yyn - YYNTOKENS];

  goto yynewstate;


/*--------------------------------------.
| yyerrlab -- here on detecting error.  |
`--------------------------------------*/
yyerrlab:
  /* Make sure we have latest lookahead translation.  See comments at
     user semantic actions for why this is necessary.  */
  yytoken = yychar == YYEMPTY ? YYEMPTY : YYTRANSLATE (yychar);

  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
#if ! YYERROR_VERBOSE
      yyerror (yyscanner, memory_pool, output, YY_("syntax error"));
#else
# define YYSYNTAX_ERROR yysyntax_error (&yymsg_alloc, &yymsg, \
                                        yyssp, yytoken)
      {
        char const *yymsgp = YY_("syntax error");
        int yysyntax_error_status;
        yysyntax_error_status = YYSYNTAX_ERROR;
        if (yysyntax_error_status == 0)
          yymsgp = yymsg;
        else if (yysyntax_error_status == 1)
          {
            if (yymsg != yymsgbuf)
              YYSTACK_FREE (yymsg);
            yymsg = (char *) YYSTACK_ALLOC (yymsg_alloc);
            if (!yymsg)
              {
                yymsg = yymsgbuf;
                yymsg_alloc = sizeof yymsgbuf;
                yysyntax_error_status = 2;
              }
            else
              {
                yysyntax_error_status = YYSYNTAX_ERROR;
                yymsgp = yymsg;
              }
          }
        yyerror (yyscanner, memory_pool, output, yymsgp);
        if (yysyntax_error_status == 2)
          goto yyexhaustedlab;
      }
# undef YYSYNTAX_ERROR
#endif
    }

  yyerror_range[1] = yylloc;

  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
         error, discard it.  */

      if (yychar <= YYEOF)
        {
          /* Return failure if at end of input.  */
          if (yychar == YYEOF)
            YYABORT;
        }
      else
        {
          yydestruct ("Error: discarding",
                      yytoken, &yylval, &yylloc, yyscanner, memory_pool, output);
          yychar = YYEMPTY;
        }
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:

  /* Pacify compilers like GCC when the user code never invokes
     YYERROR and the label yyerrorlab therefore never appears in user
     code.  */
  if (/*CONSTCOND*/ 0)
     goto yyerrorlab;

  yyerror_range[1] = yylsp[1-yylen];
  /* Do not reclaim the symbols of the rule whose action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;      /* Each real token shifted decrements this.  */

  for (;;)
    {
      yyn = yypact[yystate];
      if (!yypact_value_is_default (yyn))
        {
          yyn += YYTERROR;
          if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYTERROR)
            {
              yyn = yytable[yyn];
              if (0 < yyn)
                break;
            }
        }

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
        YYABORT;

      yyerror_range[1] = *yylsp;
      yydestruct ("Error: popping",
                  yystos[yystate], yyvsp, yylsp, yyscanner, memory_pool, output);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END

  yyerror_range[2] = yylloc;
  /* Using YYLLOC is tempting, but would change the location of
     the lookahead.  YYLOC is available though.  */
  YYLLOC_DEFAULT (yyloc, yyerror_range, 2);
  *++yylsp = yyloc;

  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", yystos[yyn], yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturn;

/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturn;

#if !defined yyoverflow || YYERROR_VERBOSE
/*-------------------------------------------------.
| yyexhaustedlab -- memory exhaustion comes here.  |
`-------------------------------------------------*/
yyexhaustedlab:
  yyerror (yyscanner, memory_pool, output, YY_("memory exhausted"));
  yyresult = 2;
  /* Fall through.  */
#endif

yyreturn:
  if (yychar != YYEMPTY)
    {
      /* Make sure we have latest lookahead translation.  See comments at
         user semantic actions for why this is necessary.  */
      yytoken = YYTRANSLATE (yychar);
      yydestruct ("Cleanup: discarding lookahead",
                  yytoken, &yylval, &yylloc, yyscanner, memory_pool, output);
    }
  /* Do not reclaim the symbols of the rule whose action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
                  yystos[*yyssp], yyvsp, yylsp, yyscanner, memory_pool, output);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif
#if YYERROR_VERBOSE
  if (yymsg != yymsgbuf)
    YYSTACK_FREE (yymsg);
#endif
  return yyresult;
}
#line 1086 "gunir/compiler/parser/big_query.yy" /* yacc.c:1906  */


void yyerror(yyscan_t yyscanner, YaccMemoryPool* pool,
std::ostream* output, const char *err_msg) {
    const char* query = (gunir_get_extra(yyscanner))->query;

    if (query == NULL) {
        query = "<query is stdin>";
    }

    yylloc.first_column -= 1;
    yylloc.last_column -= 1;

    *output << err_msg << " near column " << yylloc.first_column
        << " to " << yylloc.last_column << " : " << std::endl;
    *output << query << std::endl;

    for (int i = 0; i < yylloc.first_column; ++i)
        *output << " ";
    *output << "^" << std::endl;
}

int gunir_wrap(void* p) {
    return 1;
}

std::string trim_query(const char* query) {
    const char* q = query;
    std::string trimed_query;

    while (*q != '\0') {
        if (*q == '\t' || *q == '\n') {
            trimed_query.push_back(' ');
        }
        trimed_query.push_back(*q);
        q++;
    }
    return trimed_query;
}

int parse_line(const char* query, gunir::compiler::QueryStmt** query_stmt,
               std::ostream* output) {
    std::string trimed_query = trim_query(query);
    yyscan_t yyscanner = NULL;
    BigQueryExtra extra_record;
    YaccMemoryPool memory_pool;

    extra_record.query = trimed_query.c_str();;

    gunir_lex_init(&yyscanner);
    gunir_set_extra(&extra_record, yyscanner);

    YY_BUFFER_STATE string_buffer = gunir__scan_string(query, yyscanner);

    gunir__switch_to_buffer(string_buffer, yyscanner);
    gunir_set_column(0, yyscanner);

    int ret = yyparse(yyscanner, &memory_pool, output);

    gunir__delete_buffer(string_buffer, yyscanner);

    gunir_lex_destroy(yyscanner);

    if (ret == 0) {
        *query_stmt = new QueryStmt(*extra_record.query_stmt);
    } else {
        *query_stmt = NULL;
    }

    memory_pool.clearMessages();
    return ret;
}

RawExpression* make_expression(RawExpression* left, Operators op,
    RawExpression* right, YaccMemoryPool* memory_pool) {
    RawExpression* expr = NewMessage<RawExpression>(memory_pool);

    if (left != NULL) {
        (expr->mutable_left())->CopyFrom(*left);
    }
    if (right != NULL) {
        (expr->mutable_right())->CopyFrom(*right);
    }

    expr->set_op(op);
    return expr;
}

OrderColumnPath*
add_order_column_path(const ColumnPath& column_path,
    OrderType type, YaccMemoryPool* memory_pool) {
    OrderColumnPath* path = NewMessage<OrderColumnPath>(memory_pool);

    (path->mutable_path())->CopyFrom(column_path);
    path->set_type(type);
    return path;
}
