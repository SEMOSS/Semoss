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
package prerna.reactor.automation;

/**
 * Safe, dependency-free evaluator for automation conditional / while-loop expressions
 * and set-variable arithmetic.
 *
 * <p>Replaces the previous {@code javax.script} (JavaScript) evaluation. That path
 * ran attacker-influenceable data (prior node outputs, HTTP/LLM responses substituted
 * into the expression by {@link AutomationExecutionUtils#resolve}) through a scripting
 * engine, which is a remote-code-execution vector once any JS engine is on the
 * classpath; and on a JDK with no JS engine it silently degraded to an always-true
 * check, so conditions never actually evaluated. This evaluator does neither: it
 * parses a small, fixed grammar and can only ever return a value - it cannot reach
 * Java classes, the filesystem, or the network.
 *
 * <p>Supported grammar (after {@code ${var}} substitution):
 * <ul>
 *   <li>Logical: {@code || && !}</li>
 *   <li>Equality: {@code == === != !==}</li>
 *   <li>Relational: {@code < <= > >=}</li>
 *   <li>Arithmetic: {@code + - * / %} and unary {@code -}, with parentheses</li>
 *   <li>Literals: numbers, {@code "..."} / {@code '...'} strings, {@code true false null}</li>
 *   <li>A bare word (no operators) is treated as a string operand</li>
 * </ul>
 *
 * <p>Numeric comparisons are used when both operands parse as numbers; otherwise
 * string comparison is used. Truthiness follows the automation convention: a string is
 * falsy when empty or equal (ignoring case) to {@code "false"} / {@code "null"} /
 * {@code "0"}, truthy otherwise.
 */
public final class AutomationConditionEvaluator {

	private AutomationConditionEvaluator() {
		// utility class
	}

	/**
	 * Evaluates {@code expression} and returns its truthiness. If the expression is not
	 * a parseable expression (e.g. plain free text), falls back to treating the whole
	 * trimmed string by the truthiness convention rather than failing the node.
	 *
	 * @param expression the fully resolved expression (no {@code ${var}} tokens remaining)
	 * @return the boolean result
	 */
	public static boolean toBoolean(String expression) {
		if (expression == null) {
			return false;
		}
		try {
			Object value = new Parser(expression).parse();
			return truthy(value);
		} catch (ParseException e) {
			// Not an expression we understand - preserve the legacy "truthy string" behavior.
			return truthyString(expression.trim());
		}
	}

	/**
	 * Evaluates {@code expression} as arithmetic and returns the numeric result, or
	 * {@code null} if it is not a pure numeric expression.
	 *
	 * @param expression the fully resolved expression
	 * @return the numeric result, or {@code null} if non-numeric / unparseable
	 */
	public static Double toNumber(String expression) {
		if (expression == null) {
			return null;
		}
		try {
			Object value = new Parser(expression).parse();
			return asNumber(value);
		} catch (ParseException e) {
			return null;
		}
	}

	// -- Truthiness / coercion -------------------------------------------------------

	private static boolean truthy(Object v) {
		if (v == null) {
			return false;
		}
		if (v instanceof Boolean) {
			return (Boolean) v;
		}
		if (v instanceof Double) {
			double d = (Double) v;
			return d != 0.0 && !Double.isNaN(d);
		}
		return truthyString(v.toString().trim());
	}

	private static boolean truthyString(String s) {
		return !s.isEmpty() && !"false".equalsIgnoreCase(s)
				&& !"null".equalsIgnoreCase(s) && !"0".equals(s);
	}

	private static Double asNumber(Object v) {
		if (v instanceof Double) {
			return (Double) v;
		}
		if (v instanceof Boolean) {
			return ((Boolean) v) ? 1.0 : 0.0;
		}
		if (v instanceof String) {
			String s = ((String) v).trim();
			if (s.isEmpty()) {
				return null;
			}
			try {
				return Double.parseDouble(s);
			} catch (NumberFormatException e) {
				return null;
			}
		}
		return null;
	}

	private static String asString(Object v) {
		return v == null ? "null" : v.toString();
	}

	// -- Recursive-descent parser ----------------------------------------------------

	/** Thrown internally when the input is not a valid expression. */
	private static final class ParseException extends RuntimeException {
		private static final long serialVersionUID = 1L;

		ParseException(String message) {
			super(message);
		}
	}

	private static final class Parser {

		private final String src;
		private int pos;

		Parser(String src) {
			this.src = src;
		}

		Object parse() {
			Object result = parseOr();
			skipWhitespace();
			if (this.pos < this.src.length()) {
				throw new ParseException("Unexpected trailing input at position " + this.pos);
			}
			return result;
		}

		private Object parseOr() {
			Object left = parseAnd();
			while (match("||")) {
				boolean l = truthy(left);
				Object right = parseAnd();
				left = l || truthy(right);
			}
			return left;
		}

		private Object parseAnd() {
			Object left = parseEquality();
			while (match("&&")) {
				boolean l = truthy(left);
				Object right = parseEquality();
				left = l && truthy(right);
			}
			return left;
		}

		private Object parseEquality() {
			Object left = parseRelational();
			while (true) {
				if (match("===")) {
					left = strictEquals(left, parseRelational());
				} else if (match("!==")) {
					left = !strictEquals(left, parseRelational());
				} else if (match("==")) {
					left = looseEquals(left, parseRelational());
				} else if (match("!=")) {
					left = !looseEquals(left, parseRelational());
				} else {
					break;
				}
			}
			return left;
		}

		private Object parseRelational() {
			Object left = parseAdditive();
			while (true) {
				String op = matchAny("<=", ">=", "<", ">");
				if (op == null) {
					break;
				}
				Object right = parseAdditive();
				left = compare(left, right, op);
			}
			return left;
		}

		private Object parseAdditive() {
			Object left = parseMultiplicative();
			while (true) {
				String op = matchAny("+", "-");
				if (op == null) {
					break;
				}
				Object right = parseMultiplicative();
				Double ln = asNumber(left);
				Double rn = asNumber(right);
				if ("+".equals(op)) {
					// numeric add when both numeric, else string concatenation
					left = (ln != null && rn != null) ? (Object) (ln + rn)
							: (Object) (asString(left) + asString(right));
				} else {
					left = requireNumber(ln, op) - requireNumber(rn, op);
				}
			}
			return left;
		}

		private Object parseMultiplicative() {
			Object left = parseUnary();
			while (true) {
				String op = matchAny("*", "/", "%");
				if (op == null) {
					break;
				}
				double l = requireNumber(asNumber(left), op);
				double r = requireNumber(asNumber(parseUnary()), op);
				switch (op) {
					case "*": left = l * r; break;
					case "/": left = l / r; break;
					default:  left = l % r; break;
				}
			}
			return left;
		}

		private Object parseUnary() {
			if (match("!")) {
				return !truthy(parseUnary());
			}
			if (match("-")) {
				return -requireNumber(asNumber(parseUnary()), "-");
			}
			return parsePrimary();
		}

		private Object parsePrimary() {
			skipWhitespace();
			if (this.pos >= this.src.length()) {
				throw new ParseException("Unexpected end of expression");
			}
			char c = this.src.charAt(this.pos);
			if (c == '(') {
				this.pos++;
				Object inner = parseOr();
				skipWhitespace();
				if (!match(")")) {
					throw new ParseException("Expected ')'");
				}
				return inner;
			}
			if (c == '"' || c == '\'') {
				return readString(c);
			}
			if (Character.isDigit(c) || (c == '.' && peekDigit(1))) {
				return readNumber();
			}
			if (Character.isLetter(c) || c == '_' || c == '$') {
				return readWord();
			}
			throw new ParseException("Unexpected character '" + c + "' at position " + this.pos);
		}

		private Object readString(char quote) {
			this.pos++; // opening quote
			StringBuilder sb = new StringBuilder();
			while (this.pos < this.src.length()) {
				char c = this.src.charAt(this.pos++);
				if (c == '\\' && this.pos < this.src.length()) {
					char n = this.src.charAt(this.pos++);
					switch (n) {
						case 'n': sb.append('\n'); break;
						case 't': sb.append('\t'); break;
						case 'r': sb.append('\r'); break;
						default:  sb.append(n); break;
					}
				} else if (c == quote) {
					return sb.toString();
				} else {
					sb.append(c);
				}
			}
			throw new ParseException("Unterminated string literal");
		}

		private Object readNumber() {
			int start = this.pos;
			while (this.pos < this.src.length()) {
				char c = this.src.charAt(this.pos);
				if (Character.isDigit(c) || c == '.' || c == 'e' || c == 'E'
						|| ((c == '+' || c == '-') && this.pos > start
								&& (this.src.charAt(this.pos - 1) == 'e' || this.src.charAt(this.pos - 1) == 'E'))) {
					this.pos++;
				} else {
					break;
				}
			}
			try {
				return Double.parseDouble(this.src.substring(start, this.pos));
			} catch (NumberFormatException e) {
				throw new ParseException("Invalid number literal");
			}
		}

		private Object readWord() {
			int start = this.pos;
			while (this.pos < this.src.length()) {
				char c = this.src.charAt(this.pos);
				if (Character.isLetterOrDigit(c) || c == '_' || c == '$') {
					this.pos++;
				} else {
					break;
				}
			}
			String word = this.src.substring(start, this.pos);
			if ("true".equals(word)) {
				return Boolean.TRUE;
			}
			if ("false".equals(word)) {
				return Boolean.FALSE;
			}
			if ("null".equals(word)) {
				return null;
			}
			return word; // bare word treated as a string operand
		}

		// -- token helpers -----------------------------------------------------------

		private boolean match(String token) {
			skipWhitespace();
			if (this.src.startsWith(token, this.pos)) {
				this.pos += token.length();
				return true;
			}
			return false;
		}

		private String matchAny(String... tokens) {
			for (String t : tokens) {
				if (match(t)) {
					return t;
				}
			}
			return null;
		}

		private void skipWhitespace() {
			while (this.pos < this.src.length() && Character.isWhitespace(this.src.charAt(this.pos))) {
				this.pos++;
			}
		}

		private boolean peekDigit(int ahead) {
			int i = this.pos + ahead;
			return i < this.src.length() && Character.isDigit(this.src.charAt(i));
		}
	}

	// -- comparison helpers ----------------------------------------------------------

	private static double requireNumber(Double d, String op) {
		if (d == null) {
			throw new ParseException("Operator '" + op + "' requires a numeric operand");
		}
		return d;
	}

	private static boolean compare(Object left, Object right, String op) {
		Double ln = asNumber(left);
		Double rn = asNumber(right);
		int cmp;
		if (ln != null && rn != null) {
			cmp = Double.compare(ln, rn);
		} else {
			cmp = asString(left).compareTo(asString(right));
		}
		switch (op) {
			case "<":  return cmp < 0;
			case "<=": return cmp <= 0;
			case ">":  return cmp > 0;
			default:   return cmp >= 0; // ">="
		}
	}

	private static boolean looseEquals(Object a, Object b) {
		Double an = asNumber(a);
		Double bn = asNumber(b);
		if (an != null && bn != null) {
			return an.doubleValue() == bn.doubleValue();
		}
		return asString(a).equals(asString(b));
	}

	private static boolean strictEquals(Object a, Object b) {
		if (a == null || b == null) {
			return a == b;
		}
		if (a.getClass() != b.getClass()) {
			return false;
		}
		return a.equals(b);
	}
}
