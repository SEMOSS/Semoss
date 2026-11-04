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

/**
 * Reads a mail search the way a person types it in Outlook or Gmail. A word
 * matches anywhere a mailbox searches by default; a word or a
 * {@code "quoted phrase"} after {@code from:}, {@code to:}, {@code cc:},
 * {@code subject:} or {@code body:} matches only that part of an email; and a
 * field followed by a space, such as {@code from: ada}, takes what follows.
 *
 * <p>
 * The result is the search's terms rather than any one mailbox's syntax, so
 * each mailbox writes them out its own way and quotes what it has to. A word
 * such as {@code has:attachment}, whose name before the colon is not one of the
 * fields, stays one word, for the mailbox to read as its own operator.
 * </p>
 */
public final class MailSearchParser {

	/** The part of an email a term has to match. */
	public enum Field {

		/**
		 * Wherever the mailbox searches by default: the sender, the subject and the
		 * body.
		 */
		ANY(null), FROM("from"), TO("to"), CC("cc"), SUBJECT("subject"), BODY("body");

		private final String prefix;

		Field(String prefix) {
			this.prefix = prefix;
		}

		/**
		 * The name a search gives the field before its colon.
		 *
		 * @return the name, such as {@code from}, or null for {@link #ANY}
		 */
		public String getPrefix() {
			return this.prefix;
		}

		/**
		 * The field a search names.
		 *
		 * @param name what came before a colon
		 * @return the field, or null when the name is not one
		 */
		private static Field named(String name) {
			for (Field field : values()) {
				if (field.prefix != null && field.prefix.equalsIgnoreCase(name)) {
					return field;
				}
			}
			return null;
		}
	}

	/**
	 * One thing a search looks for.
	 *
	 * @param field  the part of the email it has to match
	 * @param value  the word, or the words of a phrase, without quotes
	 * @param phrase whether the words have to appear together, as a phrase typed in
	 *               quotes does
	 */
	public record Term(Field field, String value, boolean phrase) {
	}

	private MailSearchParser() {
	}

	/**
	 * Read a search as a person typed it.
	 *
	 * @param search the search, or null
	 * @return its terms, in the order typed, empty when there is nothing to look
	 *         for
	 */
	public static List<Term> parse(String search) {
		return read(search, Field.ANY, true);
	}

	/**
	 * Read text that has to match one part of an email, such as the words a subject
	 * has to contain. Each word is a term of its own and a quoted phrase stays one;
	 * a field typed in the text is read as part of a word.
	 *
	 * @param field the part of the email
	 * @param text  the text, or null
	 * @return its terms, empty when there is nothing to look for
	 */
	public static List<Term> parse(Field field, String text) {
		return read(text, field, false);
	}

	/**
	 * Split text into terms.
	 *
	 * @param text        the text, or null
	 * @param fallback    the field of a word no field is named for
	 * @param readsFields whether {@code from:} and the like name a field
	 * @return the terms
	 */
	private static List<Term> read(String text, Field fallback, boolean readsFields) {
		List<Term> terms = new ArrayList<>();
		if (text == null) {
			return terms;
		}
		int length = text.length();
		int at = 0;
		// a field typed with a space after its colon takes the word or phrase that
		// follows
		Field waiting = null;
		while (at < length) {
			if (Character.isWhitespace(text.charAt(at))) {
				at++;
				continue;
			}
			Field field = waiting == null ? fallback : waiting;
			waiting = null;
			if (text.charAt(at) == '"') {
				at = readPhrase(text, at, field, terms);
				continue;
			}
			int end = at;
			while (end < length && !Character.isWhitespace(text.charAt(end))) {
				end++;
			}
			int colon = text.indexOf(':', at);
			Field named = readsFields && colon > at && colon < end ? Field.named(text.substring(at, colon)) : null;
			if (named == null) {
				add(terms, field, text.substring(at, end), false);
				at = end;
			} else if (colon + 1 == end) {
				waiting = named;
				at = end;
			} else if (text.charAt(colon + 1) == '"') {
				at = readPhrase(text, colon + 1, named, terms);
			} else {
				add(terms, named, text.substring(colon + 1, end), false);
				at = end;
			}
		}
		return terms;
	}

	/**
	 * Read a phrase in quotes, which runs to the end of the text when its closing
	 * quote was never typed.
	 *
	 * @param text  the text
	 * @param quote where the opening quote is
	 * @param field the part of the email the phrase has to match
	 * @param terms the terms read so far, added to
	 * @return where reading goes on
	 */
	private static int readPhrase(String text, int quote, Field field, List<Term> terms) {
		int close = text.indexOf('"', quote + 1);
		add(terms, field, text.substring(quote + 1, close < 0 ? text.length() : close), true);
		return close < 0 ? text.length() : close + 1;
	}

	/**
	 * Add a term, unless it holds nothing to look for.
	 *
	 * @param terms  the terms
	 * @param field  the part of the email
	 * @param value  the word or phrase
	 * @param phrase whether it was typed in quotes
	 */
	private static void add(List<Term> terms, Field field, String value, boolean phrase) {
		String trimmed = value.trim();
		if (trimmed.isEmpty()) {
			return;
		}
		// a phrase of one word is that word
		boolean manyWords = trimmed.chars().anyMatch(Character::isWhitespace);
		terms.add(new Term(field, trimmed, phrase && manyWords));
	}
}
