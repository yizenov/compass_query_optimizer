/*
 * Copyright 2017 MapD Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef PARSER_RESERVEDKEYWORDS_H
#define PARSER_RESERVEDKEYWORDS_H

#include <set>
#include <string>

// Keywords from https://calcite.apache.org/docs/reference.html#keywords
static std::set<std::string> reserved_keywords{
    "ROWID",  // MapD internal
    "AMMSC",  // MapD legacy
    "ASC",
    "CHAR_LENGTH",
    "CONTINUE",
    "COPY",
    "DATABASE",
    "DATETIME",
    "DATE_TRUNC",
    "DESC",
    "FIRST",
    "FOUND",
    "IF",
    "ILIKE",
    "LAST",
    "LENGTH",
    "NOW",
    "NULLX",
    "OPTION",
    "PRIVILEGES",
    "PUBLIC",
    "RENAME",
    "SCHEMA",
    "SHOW",
    "TEXT",
    "VIEW",
    "WORK",
    "ABS",  // Calcite reserved keywords
    "ALL",
    "ALLOCATE",
    "ALLOW",
    "ALTER",
    "AND",
    "ANY",
    "ARE",
    "ARRAY",
    "AS",
    "ASENSITIVE",
    "ASYMMETRIC",
    "AT",
    "ATOMIC",
    "AUTHORIZATION",
    "AVG",
    "BEGIN",
    "BETWEEN",
    "BIGINT",
    "BINARY",
    "BIT",
    "BLOB",
    "BOOLEAN",
    "BOTH",
    "BY",
    "CALL",
    "CALLED",
    "CARDINALITY",
    "CASCADED",
    "CASE",
    "CAST",
    "CEIL",
    "CEILING",
    "CHAR",
    "CHARACTER",
    "CHARACTER_LENGTH",
    "CHAR_LENGTH",
    "CHECK",
    "CLOB",
    "CLOSE",
    "COALESCE",
    "COLLATE",
    "COLLECT",
    "COLUMN",
    "COMMIT",
    "CONDITION",
    "CONNECT",
    "CONSTRAINT",
    "CONVERT",
    "CORR",
    "CORRESPONDING",
    "COUNT",
    "COVAR_POP",
    "COVAR_SAMP",
    "CREATE",
    "CROSS",
    "CUBE",
    "CUME_DIST",
    "CURRENT",
    "CURRENT_CATALOG",
    "CURRENT_DATE",
    "CURRENT_DEFAULT_TRANSFORM_GROUP",
    "CURRENT_PATH",
    "CURRENT_ROLE",
    "CURRENT_SCHEMA",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "CURRENT_TRANSFORM_GROUP_FOR_TYPE",
    "CURRENT_USER",
    "CURSOR",
    "CYCLE",
    "DATE",
    "DAY",
    "DEALLOCATE",
    "DEC",
    "DECIMAL",
    "DECLARE",
    "DEFAULT",
    "DELETE",
    "DENSE_RANK",
    "DEREF",
    "DESCRIBE",
    "DETERMINISTIC",
    "DISALLOW",
    "DISCONNECT",
    "DISTINCT",
    "DOUBLE",
    "DROP",
    "DYNAMIC",
    "EACH",
    "ELEMENT",
    "ELSE",
    "END",
    "END-EXEC",
    "ESCAPE",
    "EVERY",
    "EXCEPT",
    "EXEC",
    "EXECUTE",
    "EXISTS",
    "EXP",
    "EXPLAIN",
    "EXTEND",
    "EXTERNAL",
    "EXTRACT",
    "FALSE",
    "FETCH",
    "FILTER",
    "FIRST_VALUE",
    "FLOAT",
    "FLOOR",
    "FOR",
    "FOREIGN",
    "FREE",
    "FROM",
    "FULL",
    "FUNCTION",
    "FUSION",
    "GET",
    "GLOBAL",
    "GRANT",
    "GROUP",
    "GROUPING",
    "HAVING",
    "HOLD",
    "HOUR",
    "IDENTITY",
    "IMPORT",
    "IN",
    "INDICATOR",
    "INNER",
    "INOUT",
    "INSENSITIVE",
    "INSERT",
    "INT",
    "INTEGER",
    "INTERSECT",
    "INTERSECTION",
    "INTERVAL",
    "INTO",
    "IS",
    "JOIN",
    "LANGUAGE",
    "LARGE",
    "LAST_VALUE",
    "LATERAL",
    "LEADING",
    "LEFT",
    "LIKE",
    "LIMIT",
    "LN",
    "LOCAL",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "LOWER",
    "MATCH",
    "MAX",
    "MEMBER",
    "MERGE",
    "METHOD",
    "MIN",
    "MINUTE",
    "MOD",
    "MODIFIES",
    "MODULE",
    "MONTH",
    "MULTISET",
    "NATIONAL",
    "NATURAL",
    "NCHAR",
    "NCLOB",
    "NEW",
    "NEXT",
    "NO",
    "NONE",
    "NORMALIZE",
    "NOT",
    "NULL",
    "NULLIF",
    "NUMERIC",
    "OCTET_LENGTH",
    "OF",
    "OFFSET",
    "OLD",
    "ON",
    "ONLY",
    "OPEN",
    "OR",
    "ORDER",
    "OUT",
    "OUTER",
    "OVER",
    "OVERLAPS",
    "OVERLAY",
    "PARAMETER",
    "PARTITION",
    "PERCENTILE_CONT",
    "PERCENTILE_DISC",
    "PERCENT_RANK",
    "POSITION",
    "POWER",
    "PRECISION",
    "PREPARE",
    "PRIMARY",
    "PROCEDURE",
    "RANGE",
    "RANK",
    "READS",
    "REAL",
    "RECURSIVE",
    "REF",
    "REFERENCES",
    "REFERENCING",
    "REGR_AVGX",
    "REGR_AVGY",
    "REGR_COUNT",
    "REGR_INTERCEPT",
    "REGR_R2",
    "REGR_SLOPE",
    "REGR_SXX",
    "REGR_SXY",
    "REGR_SYY",
    "RELEASE",
    "RESET",
    "RESULT",
    "RETURN",
    "RETURNS",
    "REVOKE",
    "RIGHT",
    "ROLE",
    "ROLLBACK",
    "ROLLUP",
    "ROW",
    "ROWS",
    "ROW_NUMBER",
    "SAVEPOINT",
    "SCOPE",
    "SCROLL",
    "SEARCH",
    "SECOND",
    "SELECT",
    "SENSITIVE",
    "SESSION_USER",
    "SET",
    "SIMILAR",
    "SMALLINT",
    "SOME",
    "SPECIFIC",
    "SPECIFICTYPE",
    "SQL",
    "SQLEXCEPTION",
    "SQLSTATE",
    "SQLWARNING",
    "SQRT",
    "START",
    "STATIC",
    "STDDEV_POP",
    "STDDEV_SAMP",
    "STREAM",
    "SUBMULTISET",
    "SUBSTRING",
    "SUM",
    "SYMMETRIC",
    "SYSTEM",
    "SYSTEM_USER",
    "TABLE",
    "TABLESAMPLE",
    "TEMPORARY",
    "THEN",
    "TIME",
    "TIMESTAMP",
    "TIMEZONE_HOUR",
    "TIMEZONE_MINUTE",
    "TINYINT",
    "TO",
    "TRAILING",
    "TRANSLATE",
    "TRANSLATION",
    "TREAT",
    "TRIGGER",
    "TRIM",
    "TRUE",
    "TRUNCATE",
    "UESCAPE",
    "UNION",
    "UNIQUE",
    "UNKNOWN",
    "UNNEST",
    "UPDATE",
    "UPPER",
    "UPSERT",
    "USER",
    "USING",
    "VALUE",
    "VALUES",
    "VARBINARY",
    "VARCHAR",
    "VARYING",
    "VAR_POP",
    "VAR_SAMP",
    "WHEN",
    "WHENEVER",
    "WHERE",
    "WIDTH_BUCKET",
    "WINDOW",
    "WITH",
    "WITHIN",
    "WITHOUT",
    "YEAR",
};

#endif  // PARSER_RESERVEDKEYWORDS_H