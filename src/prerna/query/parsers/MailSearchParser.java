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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads a mail search the way a person types it in Outlook or Gmail. A word
 * matches anywhere a mailbox searches by default; a word or a
 * {@code "quoted phrase"} after {@code from:}, {@code to:}, {@code cc:},
 * {@code subject:} or {@code body:} matches only that part of an email; a field
 * followed by a space, such as {@code from: ada}, takes what follows; and a
 * leading minus, as in {@code -draft} or {@code -from:ada}, asks for mail
 * without it.
 *
 * <p>
 * The result is the search's terms rather than any one mailbox's syntax, so
 * each mailbox writes them out its own way and quotes what it has to. Any other
 * name typed before a value, such as {@code received} in
 * {@code received>=2026-10-01} or {@code label} in {@code label:"Client X"}, is
 * kept as an {@link Operator} with its value whole, for each mailbox to write
 * as its own operator when it has one by that name, and as plain words when it
 * does not.
 * </p>
 */
public final class MailSearchParser {

	/**
	 * A name typed before a value: letters and underscores, then a colon or a
	 * comparison.
	 */
	private static final Pattern NAMED = Pattern.compile("([A-Za-z_]+)(:|>=|<=|<>|>|<|=)");

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
	 * A name typed before a value that is not one of the fields, which a mailbox
	 * may read as an operator of its own.
	 *
	 * @param name     the name as typed, such as {@code received} or {@code label}
	 * @param relation how the value relates to it: {@code :}, {@code =},
	 *                 {@code <>}, {@code <}, {@code >}, {@code <=} or {@code >=}
	 */
	public record Operator(String name, String relation) {
	}

	/**
	 * One thing a search looks for.
	 *
	 * @param field    the part of the email it has to match, {@link Field#ANY} for
	 *                 a term with an operator
	 * @param operator the name typed before the value when it is not one of the
	 *                 fields, or null
	 * @param value    the word, or the words of a phrase, without quotes
	 * @param phrase   whether the words have to appear together, as a phrase typed
	 *                 in quotes does
	 * @param excluded whether the mail found has to be without it, as a leading
	 *                 minus asks
	 */
	public record Term(Field field, Operator operator, String value, boolean phrase, boolean excluded) {

		/**
		 * A term that has to be in the mail found, with no operator.
		 *
		 * @param field  the part of the email it has to match
		 * @param value  the word, or the words of a phrase, without quotes
		 * @param phrase whether the words have to appear together
		 */
		public Term(Field field, String value, boolean phrase) {
			this(field, null, value, phrase, false);
		}
	}

	/**
	 * What the next word or phrase belongs to, when a field or an operator was
	 * typed with a space after it.
	 *
	 * @param field    the part of the email
	 * @param operator the operator, or null
	 * @param excluded whether a minus came before it
	 */
	private record Waiting(Field field, Operator operator, boolean excluded) {
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
	 * a field, an operator or a minus typed in the text is read as part of a word.
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
	 * @param readsSyntax whether fields, operators and a leading minus are read as
	 *                    such
	 * @return the terms
	 */
	private static List<Term> read(String text, Field fallback, boolean readsSyntax) {
		List<Term> terms = new ArrayList<>();
		if (text == null) {
			return terms;
		}
		int length = text.length();
		int at = 0;
		Waiting waiting = null;
		while (at < length) {
			if (Character.isWhitespace(text.charAt(at))) {
				at++;
				continue;
			}
			Waiting owner = waiting;
			waiting = null;
			boolean excluded = owner != null && owner.excluded();
			// a minus on its own, as in "Q3 - budget", is a word rather than an exclusion
			if (owner == null && readsSyntax && text.charAt(at) == '-' && at + 1 < length
					&& !Character.isWhitespace(text.charAt(at + 1))) {
				excluded = true;
				at++;
			}
			Field field = owner == null ? fallback : owner.field();
			Operator operator = owner == null ? null : owner.operator();
			if (text.charAt(at) == '"') {
				at = readPhrase(text, at, new Waiting(field, operator, excluded), terms);
				continue;
			}
			int end = at;
			while (end < length && !Character.isWhitespace(text.charAt(end))) {
				end++;
			}
			Matcher named = NAMED.matcher(text).region(at, end);
			if (owner != null || !readsSyntax || !named.lookingAt()) {
				add(terms, new Waiting(field, operator, excluded), text.substring(at, end), false);
				at = end;
				continue;
			}
			Field namedField = ":".equals(named.group(2)) ? Field.named(named.group(1)) : null;
			Waiting typed = namedField == null
					? new Waiting(Field.ANY, new Operator(named.group(1), named.group(2)), excluded)
					: new Waiting(namedField, null, excluded);
			int valueStart = named.end();
			if (valueStart == end) {
				waiting = typed;
				at = end;
			} else if (text.charAt(valueStart) == '"') {
				at = readPhrase(text, valueStart, typed, terms);
			} else {
				add(terms, typed, text.substring(valueStart, end), false);
				at = end;
			}
		}
		// an operator typed last with nothing after it, such as "Re:", is a word
		if (waiting != null && waiting.operator() != null) {
			add(terms, new Waiting(fallback, null, waiting.excluded()), waiting.operator().name(), false);
		}
		return terms;
	}

	/**
	 * Read a phrase in quotes, which runs to the end of the text when its closing
	 * quote was never typed.
	 *
	 * @param text  the text
	 * @param quote where the opening quote is
	 * @param owner the field or operator the phrase belongs to, and whether it is
	 *              excluded
	 * @param terms the terms read so far, added to
	 * @return where reading goes on
	 */
	private static int readPhrase(String text, int quote, Waiting owner, List<Term> terms) {
		int close = text.indexOf('"', quote + 1);
		add(terms, owner, text.substring(quote + 1, close < 0 ? text.length() : close), true);
		return close < 0 ? text.length() : close + 1;
	}

	/**
	 * Add a term, unless it holds nothing to look for.
	 *
	 * @param terms  the terms
	 * @param owner  the field or operator it belongs to, and whether it is excluded
	 * @param value  the word or phrase
	 * @param phrase whether it was typed in quotes
	 */
	private static void add(List<Term> terms, Waiting owner, String value, boolean phrase) {
		String trimmed = value.trim();
		if (trimmed.isEmpty()) {
			return;
		}
		// a phrase of one word is that word
		boolean manyWords = trimmed.chars().anyMatch(Character::isWhitespace);
		terms.add(new Term(owner.field(), owner.operator(), trimmed, phrase && manyWords, owner.excluded()));
	}
}
