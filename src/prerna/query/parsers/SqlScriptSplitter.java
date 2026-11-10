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
package prerna.query.parsers;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Splits a SQL script into its statements, keeping each statement's text
 * exactly as submitted. A statement ends at a semicolon outside quotes,
 * comments and parentheses, as in psql, and its text runs through that
 * semicolon. Surrounding whitespace is trimmed, and a piece holding nothing but
 * comments is not a statement.
 *
 * <p>
 * The split knows the quoting SQL databases share: single-quoted strings (with
 * backslash escapes in {@code E'...'} strings), double-quoted and backquoted
 * identifiers, dollar-quoted strings such as function bodies, line comments and
 * block comments, which may nest. A statement split in the wrong place, such as
 * a procedure body that is not dollar-quoted, fails when it runs.
 * </p>
 *
 * <p>
 * A SQL-standard function body ({@code BEGIN ATOMIC ... END}) holds semicolons
 * that do not end its statement, so a script containing one is rejected rather
 * than split in the wrong place.
 * </p>
 */
public final class SqlScriptSplitter {

	private static final int LEADING_WORD_COUNT = 5;
	/**
	 * The first words of the statements databases treat as data definition: the
	 * ones that change a database's structure or permissions
	 */
	private static final Set<String> DATA_DEFINITION = Set.of("CREATE", "ALTER", "DROP", "TRUNCATE", "RENAME",
			"COMMENT", "GRANT", "REVOKE", "ANALYZE", "AUDIT", "NOAUDIT", "PURGE", "FLASHBACK");
	/**
	 * The first words of the statements that only read or change rows: queries, and
	 * inserts, updates, deletes and merges
	 */
	private static final Set<String> ROW_STATEMENTS = Set.of("SELECT", "WITH", "VALUES", "TABLE", "INSERT", "UPDATE",
			"DELETE", "MERGE", "UPSERT", "REPLACE", "EXPLAIN", "SHOW", "DESCRIBE", "DESC");

	/**
	 * One statement of a script.
	 *
	 * @param text         the statement as submitted, trimmed, through its
	 *                     terminating semicolon when it has one
	 * @param leadingWords the first words of the statement outside quotes and
	 *                     comments, in upper case, at most five
	 */
	public record Statement(String text, List<String> leadingWords) {

		/**
		 * The statement without its terminating semicolon, for parsers that read one
		 * statement at a time.
		 *
		 * @return the text, less a final semicolon and the whitespace before it
		 */
		public String body() {
			return this.text.endsWith(";") ? this.text.substring(0, this.text.length() - 1).stripTrailing() : this.text;
		}

		/**
		 * Whether the statement is data definition, read from its first word: it
		 * changes the database's structure or permissions, as CREATE, ALTER, DROP,
		 * TRUNCATE and GRANT do. A temporary table counts too, though some databases
		 * treat it differently.
		 *
		 * @return true for a data definition statement
		 */
		public boolean isDataDefinition() {
			return !this.leadingWords.isEmpty() && DATA_DEFINITION.contains(this.leadingWords.get(0));
		}

		/**
		 * Whether the statement only reads or changes rows, read from its first word: a
		 * query, or an insert, update, delete or merge. Anything else, such as LOCK
		 * TABLES, CALL or a database setting, is not.
		 *
		 * @return true for a statement that only reads or changes rows
		 */
		public boolean isRowStatement() {
			return !this.leadingWords.isEmpty() && ROW_STATEMENTS.contains(this.leadingWords.get(0));
		}
	}

	private SqlScriptSplitter() {
	}

	/**
	 * Splits a script into its statements.
	 *
	 * @param script the SQL script
	 * @return the statements, in script order
	 * @throws IllegalArgumentException when a string, identifier, dollar-quoted
	 *                                  string or block comment is not closed, or
	 *                                  the script holds a {@code BEGIN ATOMIC} body
	 */
	public static List<Statement> split(String script) {
		List<Statement> statements = new ArrayList<>();
		if (script == null) {
			return statements;
		}
		int length = script.length();
		int start = 0;
		boolean hasContent = false;
		int parenthesisDepth = 0;
		List<String> leadingWords = new ArrayList<>();
		String lastWord = null;
		int index = 0;
		while (index < length) {
			char c = script.charAt(index);
			char next = index + 1 < length ? script.charAt(index + 1) : '\0';
			int tagEnd = c == '$' ? dollarTagEnd(script, index) : -1;
			if (c == '-' && next == '-') {
				index = lineEnd(script, index);
			} else if (c == '/' && next == '*') {
				index = skipBlockComment(script, index);
			} else if (c == '\'') {
				boolean backslashEscapes = "E".equals(lastWordEndingAt(script, index));
				index = skipQuoted(script, index, '\'', backslashEscapes);
				hasContent = true;
			} else if (c == '"' || c == '`') {
				index = skipQuoted(script, index, c, false);
				hasContent = true;
			} else if (tagEnd > 0) {
				String delimiter = script.substring(index, tagEnd + 1);
				int close = script.indexOf(delimiter, index + delimiter.length());
				if (close < 0) {
					throw new IllegalArgumentException(
							"The dollar-quoted string opened with " + delimiter + " is not closed");
				}
				index = close + delimiter.length();
				hasContent = true;
			} else if (c == ';' && parenthesisDepth == 0) {
				addStatement(statements, script.substring(start, index + 1), hasContent, leadingWords);
				start = index + 1;
				hasContent = false;
				leadingWords = new ArrayList<>();
				lastWord = null;
				index++;
			} else if (isWordStart(c)) {
				int wordEnd = index + 1;
				while (wordEnd < length && isWordPart(script.charAt(wordEnd))) {
					wordEnd++;
				}
				String word = script.substring(index, wordEnd).toUpperCase(Locale.ROOT);
				if ("ATOMIC".equals(word) && "BEGIN".equals(lastWord)) {
					throw new IllegalArgumentException("SQL-standard function bodies (BEGIN ATOMIC ... END) are not "
							+ "supported; write the body as a dollar-quoted string instead");
				}
				if (leadingWords.size() < LEADING_WORD_COUNT) {
					leadingWords.add(word);
				}
				lastWord = word;
				hasContent = true;
				index = wordEnd;
			} else {
				if (c == '(') {
					parenthesisDepth++;
				} else if (c == ')' && parenthesisDepth > 0) {
					parenthesisDepth--;
				}
				if (!Character.isWhitespace(c)) {
					hasContent = true;
				}
				index++;
			}
		}
		if (start < length) {
			addStatement(statements, script.substring(start), hasContent, leadingWords);
		}
		return statements;
	}

	private static void addStatement(List<Statement> statements, String text, boolean hasContent,
			List<String> leadingWords) {
		if (hasContent) {
			statements.add(new Statement(text.strip(), List.copyOf(leadingWords)));
		}
	}

	/**
	 * Finds the end of a line comment. A line ends at a line feed or a carriage
	 * return, so a script saved with either line ending splits the same way.
	 *
	 * @return the index after the comment's line break, or the script's length
	 */
	private static int lineEnd(String script, int start) {
		for (int index = start; index < script.length(); index++) {
			char c = script.charAt(index);
			if (c == '\n' || c == '\r') {
				return index + 1;
			}
		}
		return script.length();
	}

	/**
	 * Skips a block comment, counting nested ones.
	 *
	 * @return the index after the comment
	 */
	private static int skipBlockComment(String script, int open) {
		int depth = 0;
		int index = open;
		while (index < script.length()) {
			if (script.startsWith("/*", index)) {
				depth++;
				index += 2;
			} else if (script.startsWith("*/", index)) {
				depth--;
				index += 2;
				if (depth == 0) {
					return index;
				}
			} else {
				index++;
			}
		}
		throw new IllegalArgumentException("A block comment is not closed");
	}

	/**
	 * Skips a quoted string or identifier. A doubled quote character stands for one
	 * quote inside it.
	 *
	 * @return the index after the closing quote
	 */
	private static int skipQuoted(String script, int open, char quote, boolean backslashEscapes) {
		int index = open + 1;
		while (index < script.length()) {
			char c = script.charAt(index);
			if (backslashEscapes && c == '\\') {
				index += 2;
			} else if (c == quote) {
				if (index + 1 < script.length() && script.charAt(index + 1) == quote) {
					index += 2;
				} else {
					return index + 1;
				}
			} else {
				index++;
			}
		}
		if (quote == '\'') {
			throw new IllegalArgumentException("A quoted string is not closed");
		}
		throw new IllegalArgumentException(
				"A " + (quote == '"' ? "double-quoted" : "backquoted") + " identifier is not closed");
	}

	/**
	 * Finds the end of a dollar-quote delimiter such as {@code $$} or
	 * {@code $body$} that opens at the given index. A {@code $} inside an
	 * identifier or before a digit, as in a parameter like {@code $1}, does not
	 * open one.
	 *
	 * @return the index of the delimiter's closing {@code $}, or -1 when no
	 *         delimiter opens here
	 */
	private static int dollarTagEnd(String script, int open) {
		if (open > 0 && isWordPart(script.charAt(open - 1))) {
			return -1;
		}
		int index = open + 1;
		if (index < script.length() && script.charAt(index) == '$') {
			return index;
		}
		if (index >= script.length() || !isWordStart(script.charAt(index))) {
			return -1;
		}
		while (index < script.length()) {
			char c = script.charAt(index);
			if (c == '$') {
				return index;
			}
			if (!Character.isLetterOrDigit(c) && c != '_') {
				return -1;
			}
			index++;
		}
		return -1;
	}

	/**
	 * The word that ends right before the given index, in upper case.
	 *
	 * @return the word, or null when none ends there
	 */
	private static String lastWordEndingAt(String script, int end) {
		int index = end;
		while (index > 0 && isWordPart(script.charAt(index - 1))) {
			index--;
		}
		if (index == end || !isWordStart(script.charAt(index))) {
			return null;
		}
		return script.substring(index, end).toUpperCase(Locale.ROOT);
	}

	private static boolean isWordStart(char c) {
		return Character.isLetter(c) || c == '_';
	}

	private static boolean isWordPart(char c) {
		return Character.isLetterOrDigit(c) || c == '_' || c == '$';
	}
}
