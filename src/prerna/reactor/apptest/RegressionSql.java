/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.apptest;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/** A complete-consumption grammar for the SQL surface of a disposable H2 fixture. */
final class RegressionSql {
    private static final Pattern TOKENS = Pattern.compile(
            "\\s+|--[^\\r\\n]*|/\\*.*?\\*/|'(?:[^']|'')*'|\"[A-Za-z][A-Za-z0-9_ ]{0,62}\"|[A-Za-z][A-Za-z0-9_]{0,62}|[0-9]+(?:\\.[0-9]+)?|<=|>=|<>|!=|[(),.*=<>+;-]", Pattern.DOTALL);
    private static final Set<String> AGGREGATES = Set.of("COUNT", "SUM", "AVG", "MIN", "MAX");
    private static final Set<String> KEYWORDS = Set.of("SELECT", "DISTINCT", "FROM", "AS", "WHERE", "AND", "OR", "NOT", "IS", "NULL", "TRUE", "FALSE", "IN", "LIKE", "BETWEEN", "ORDER", "BY", "ASC", "DESC", "NULLS", "FIRST", "LAST", "LIMIT", "OFFSET", "ROWS", "ROW", "FETCH", "NEXT", "ONLY", "GROUP", "HAVING", "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE", "COUNT", "SUM", "AVG", "MIN", "MAX");
    private static final Set<String> DIAGNOSTIC_KEYWORDS = Set.of("JOIN", "UNION", "WITH", "OVER", "FILTER", "CALL", "CREATE", "DROP", "ALTER", "FILE_READ", "FILE_WRITE", "CSVREAD", "CSVWRITE", "CURRENT_USER", "CURRENT_DATE");
    private static final Set<String> CONTEXT = Set.of("CURRENT_USER", "USER", "SESSION_USER", "SYSTEM_USER", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_SCHEMA", "CURRENT_CATALOG", "LOCALTIME", "LOCALTIMESTAMP");
    private final List<String> tokens;
    private int at;
    private int depth;

    private RegressionSql(String sql) {
        tokens = tokenize(sql);
        if (tokens.size() > 1024) throw unsupported();
    }

    static boolean isRead(String sql) {
        if (sql == null || sql.isBlank() || sql.length() > 4096) throw unsupported();
        var parser = new RegressionSql(sql);
        boolean read = parser.take("SELECT");
        if (read) parser.select();
        else if (parser.take("INSERT")) parser.insert();
        else if (parser.take("UPDATE")) parser.update();
        else if (parser.take("DELETE")) parser.delete();
        else throw unsupported();
        parser.take(";");
        if (parser.at != parser.tokens.size()) throw unsupported();
        return read;
    }

    /** Only keywords and punctuation survive; identifiers, numbers and string values are redacted. */
    static String shape(String sql) {
        try {
            var result = new ArrayList<String>();
            for (String token : tokenize(sql == null ? "" : sql.substring(0, Math.min(sql.length(), 8192)))) {
                String upper = token.toUpperCase(Locale.ROOT);
                result.add(KEYWORDS.contains(upper) || DIAGNOSTIC_KEYWORDS.contains(upper) ? upper : token.matches("[(),.*=<>!;+\\-]+") ? token : "?");
                if (String.join(" ", result).length() > 240) { result.add("…"); break; }
            }
            return String.join(" ", result);
        } catch (IllegalArgumentException failure) { return "[unrecognized SQL; values omitted]"; }
    }

    private static List<String> tokenize(String sql) {
        var values = new ArrayList<String>();
        var matcher = TOKENS.matcher(sql);
        int end = 0;
        while (matcher.find()) {
            if (matcher.start() != end) throw unsupported();
            String token = matcher.group();
            end = matcher.end();
            if (!token.isBlank() && !token.startsWith("--") && !token.startsWith("/*")) values.add(token);
        }
        if (end != sql.length()) throw unsupported();
        return values;
    }

    private void select() {
        take("DISTINCT");
        do { projection(); alias(); } while (take(","));
        require("FROM"); table(); alias();
        if (take("WHERE")) condition();
        if (take("GROUP")) { require("BY"); do { column(); } while (take(",")); }
        if (take("HAVING")) condition();
        if (take("ORDER")) {
            require("BY");
            do {
                scalar();
                if (!take("ASC")) take("DESC");
                if (take("NULLS") && !take("FIRST")) require("LAST");
            } while (take(","));
        }
        if (take("LIMIT")) {
            integer();
            if (take("OFFSET")) integer();
        } else {
            if (take("OFFSET")) { integer(); if (!take("ROWS")) take("ROW"); }
            if (take("FETCH")) {
                if (!take("FIRST")) require("NEXT");
                integer(); if (!take("ROWS")) require("ROW"); require("ONLY");
            }
        }
    }

    private void insert() {
        require("INTO"); table();
        if (take("(")) { do { identifier(); } while (take(",")); require(")"); }
        require("VALUES");
        do { require("("); do { literal(); } while (take(",")); require(")"); } while (take(","));
    }

    private void update() {
        table(); alias(); require("SET");
        do { column(); require("="); literal(); } while (take(","));
        require("WHERE"); condition();
    }

    private void delete() { require("FROM"); table(); alias(); require("WHERE"); condition(); }

    private void condition() {
        conjunction();
        while (take("OR")) conjunction();
    }

    private void conjunction() {
        predicate();
        while (take("AND")) predicate();
    }

    private void predicate() {
        if (++depth > 16) throw unsupported();
        if (take("NOT")) predicate();
        else if (take("(")) { condition(); require(")"); }
        else {
            scalar();
            if (take("IS")) { take("NOT"); require("NULL"); }
            else {
                boolean negated = take("NOT");
                if (take("IN")) { require("("); do { literal(); } while (take(",")); require(")"); }
                else if (take("LIKE")) literal();
                else if (take("BETWEEN")) { literal(); require("AND"); literal(); }
                else {
                    if (negated || !(take("=") || take("!=") || take("<>") || take("<=") || take(">=") || take("<") || take(">"))) throw unsupported();
                    literal();
                }
            }
        }
        depth--;
    }

    private void projection() { if (!take("*")) scalar(); }

    private void scalar() {
        String name = peek().toUpperCase(Locale.ROOT);
        if (AGGREGATES.contains(name) && at + 1 < tokens.size() && tokens.get(at + 1).equals("(")) {
            at += 2;
            take("DISTINCT");
            if (!take("*")) column();
            else if (!name.equals("COUNT")) throw unsupported();
            require(")");
        } else column();
    }

    private void column() { identifier(); if (take(".")) identifier(); }

    private void table() {
        String first = peek(); identifier();
        if (take(".")) {
            if (!first.equalsIgnoreCase("PUBLIC") && !first.equals("\"PUBLIC\"")) throw unsupported();
            identifier();
        }
    }

    private void alias() {
        if (take("AS")) identifier();
        else if (at < tokens.size() && isIdentifier(peek()) && !KEYWORDS.contains(peek().toUpperCase(Locale.ROOT))) identifier();
    }

    private void identifier() {
        String token = peek();
        if (!isIdentifier(token) || !token.startsWith("\"") && (KEYWORDS.contains(token.toUpperCase(Locale.ROOT)) || CONTEXT.contains(token.toUpperCase(Locale.ROOT)))) throw unsupported();
        at++;
    }

    private static boolean isIdentifier(String value) {
        return value.matches("[A-Za-z][A-Za-z0-9_]{0,62}|\"[A-Za-z][A-Za-z0-9_ ]{0,62}\"");
    }

    private void literal() {
        boolean signed = take("-") || take("+");
        String value = peek();
        if (value.matches("[0-9]+(?:\\.[0-9]+)?")) at++;
        else if (!signed && (value.startsWith("'") || take("NULL") || take("TRUE") || take("FALSE"))) {
            if (value.startsWith("'")) at++;
        } else throw unsupported();
    }

    private void integer() {
        String value = peek();
        if (!value.matches("[0-9]{1,7}")) throw unsupported();
        at++;
    }

    private String peek() { if (at == tokens.size()) throw unsupported(); return tokens.get(at); }
    private boolean take(String token) { if (at < tokens.size() && tokens.get(at).equalsIgnoreCase(token)) { at++; return true; } return false; }
    private void require(String token) { if (!take(token)) throw unsupported(); }
    private static IllegalArgumentException unsupported() {
        return new IllegalArgumentException("UNSUPPORTED_TEST_DATABASE_OPERATION: Supported SQL uses one fixture table, columns/aliases, COUNT/SUM/AVG/MIN/MAX, literal comparisons/IN/LIKE/NULL, grouping, ordering and LIMIT/OFFSET. INSERT requires literal values; UPDATE/DELETE require WHERE. Joins, subqueries and other functions are unavailable. The real database was not accessed.");
    }
}
