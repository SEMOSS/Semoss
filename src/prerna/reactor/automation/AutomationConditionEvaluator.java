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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.automation;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

/**
 * Parses and evaluates the restricted expression language used by Automation
 * branching nodes.
 *
 * <p>This deliberately is not a general scripting engine. Expressions may use
 * scope references, scalar JSON literals, comparisons, boolean operators, and
 * parentheses. Evaluation never invokes Python, Pixel, reflection, or user code.
 */
final class AutomationConditionEvaluator {

	private static final int MAX_EXPRESSION_LENGTH = 4_096;
	private static final int MAX_TOKENS = 256;
	private static final int MAX_NESTING_DEPTH = 32;
	private static final int MAX_PATH_SEGMENTS = 32;

	private AutomationConditionEvaluator() {
	}

	/** Validates an expression without reading runtime scope values. */
	static void validate(String expression) {
		parse(expression);
	}

	/** Evaluates an expression against the current run's read-only scope. */
	static boolean evaluate(String expression, Map<String, Object> scope) {
		Object result = parse(expression).evaluate(scope != null ? scope : Map.of());
		if (result instanceof Boolean value) {
			return value;
		}
		throw conditionError("expression must evaluate to a boolean");
	}

	private static Expression parse(String expression) {
		if (expression == null || expression.isBlank()) {
			throw conditionError("expression must be nonblank");
		}
		if (expression.length() > MAX_EXPRESSION_LENGTH) {
			throw conditionError("expression exceeds " + MAX_EXPRESSION_LENGTH + " characters");
		}
		return new Parser(new Lexer(expression).tokens()).parse();
	}

	private static IllegalArgumentException conditionError(String message) {
		return new IllegalArgumentException("Invalid automation condition: " + message + ".");
	}

	private interface Expression {
		Object evaluate(Map<String, Object> scope);
	}

	private record Literal(Object value) implements Expression {
		@Override
		public Object evaluate(Map<String, Object> scope) {
			return value;
		}
	}

	private record Reference(String source, String root, List<PathSegment> path) implements Expression {
		@Override
		public Object evaluate(Map<String, Object> scope) {
			if (!scope.containsKey(root)) {
				throw conditionError("reference '" + source + "' is not present in scope");
			}
			Object current = scope.get(root);
			for (PathSegment segment : path) {
				current = segment.resolve(current, source);
			}
			return current;
		}
	}

	private interface PathSegment {
		Object resolve(Object value, String source);
	}

	private record KeySegment(String key) implements PathSegment {
		@Override
		public Object resolve(Object value, String source) {
			if (!(value instanceof Map<?, ?> map) || !map.containsKey(key)) {
				throw conditionError("reference '" + source + "' cannot resolve key '" + key + "'");
			}
			return map.get(key);
		}
	}

	private record IndexSegment(int index) implements PathSegment {
		@Override
		public Object resolve(Object value, String source) {
			if (!(value instanceof List<?> list) || index >= list.size()) {
				throw conditionError("reference '" + source + "' cannot resolve index " + index);
			}
			return list.get(index);
		}
	}

	private record Unary(Expression operand) implements Expression {
		@Override
		public Object evaluate(Map<String, Object> scope) {
			Object value = operand.evaluate(scope);
			if (value instanceof Boolean booleanValue) {
				return !booleanValue;
			}
			throw conditionError("operator '!' requires a boolean operand");
		}
	}

	private record Binary(Expression left, TokenType operator, Expression right) implements Expression {
		@Override
		public Object evaluate(Map<String, Object> scope) {
			if (operator == TokenType.AND) {
				return booleanValue(left.evaluate(scope), "&&")
						&& booleanValue(right.evaluate(scope), "&&");
			}
			if (operator == TokenType.OR) {
				return booleanValue(left.evaluate(scope), "||")
						|| booleanValue(right.evaluate(scope), "||");
			}

			Object leftValue = left.evaluate(scope);
			Object rightValue = right.evaluate(scope);
			return switch (operator) {
				case EQUAL -> valuesEqual(leftValue, rightValue);
				case NOT_EQUAL -> !valuesEqual(leftValue, rightValue);
				case GREATER -> compareNumbers(leftValue, rightValue, ">") > 0;
				case GREATER_EQUAL -> compareNumbers(leftValue, rightValue, ">=") >= 0;
				case LESS -> compareNumbers(leftValue, rightValue, "<") < 0;
				case LESS_EQUAL -> compareNumbers(leftValue, rightValue, "<=") <= 0;
				default -> throw conditionError("unsupported operator '" + operator + "'");
			};
		}

		private static boolean booleanValue(Object value, String operator) {
			if (value instanceof Boolean booleanValue) {
				return booleanValue;
			}
			throw conditionError("operator '" + operator + "' requires boolean operands");
		}

		private static boolean valuesEqual(Object left, Object right) {
			if (left instanceof Number && right instanceof Number) {
				return numberValue(left, "==").compareTo(numberValue(right, "==")) == 0;
			}
			return Objects.equals(left, right);
		}

		private static int compareNumbers(Object left, Object right, String operator) {
			if (!(left instanceof Number) || !(right instanceof Number)) {
				throw conditionError("operator '" + operator + "' requires numeric operands");
			}
			return numberValue(left, operator).compareTo(numberValue(right, operator));
		}

		private static BigDecimal numberValue(Object value, String operator) {
			try {
				return new BigDecimal(value.toString());
			} catch (NumberFormatException e) {
				throw conditionError("operator '" + operator + "' received a non-finite number");
			}
		}
	}

	private enum TokenType {
		REFERENCE,
		STRING,
		NUMBER,
		TRUE,
		FALSE,
		NULL,
		AND,
		OR,
		NOT,
		EQUAL,
		NOT_EQUAL,
		GREATER,
		GREATER_EQUAL,
		LESS,
		LESS_EQUAL,
		LEFT_PAREN,
		RIGHT_PAREN,
		END
	}

	private record Token(TokenType type, String text, int position) {
	}

	private static final class Lexer {

		private final String source;
		private final List<Token> tokens = new ArrayList<>();
		private int index;

		private Lexer(String source) {
			this.source = source;
		}

		private List<Token> tokens() {
			while (index < source.length()) {
				char current = source.charAt(index);
				if (Character.isWhitespace(current)) {
					index++;
				} else if (current == '$' && peek('{', 1)) {
					reference();
				} else if (current == '"') {
					string();
				} else if (current == '-' || isDigit(current)) {
					number();
				} else if (Character.isLetter(current)) {
					keyword();
				} else {
					operator();
				}
				if (tokens.size() > MAX_TOKENS) {
					throw conditionError("expression exceeds " + MAX_TOKENS + " tokens");
				}
			}
			tokens.add(new Token(TokenType.END, "", index));
			return List.copyOf(tokens);
		}

		private void reference() {
			int start = index;
			int close = source.indexOf('}', index + 2);
			if (close < 0) {
				throw errorAt(start, "unterminated scope reference");
			}
			index = close + 1;
			add(TokenType.REFERENCE, source.substring(start, index), start);
		}

		private void string() {
			int start = index++;
			boolean escaped = false;
			while (index < source.length()) {
				char current = source.charAt(index++);
				if (current == '"' && !escaped) {
					add(TokenType.STRING, source.substring(start, index), start);
					return;
				}
				escaped = current == '\\' && !escaped;
				if (current != '\\') {
					escaped = false;
				}
			}
			throw errorAt(start, "unterminated string literal");
		}

		private void number() {
			int start = index;
			if (source.charAt(index) == '-') {
				index++;
			}
			if (index < source.length() && source.charAt(index) == '0') {
				index++;
				if (index < source.length() && isDigit(source.charAt(index))) {
					throw errorAt(start, "number literals cannot contain leading zeroes");
				}
			} else {
				digits(start);
			}
			if (index < source.length() && source.charAt(index) == '.') {
				index++;
				digits(start);
			}
			if (index < source.length() && (source.charAt(index) == 'e' || source.charAt(index) == 'E')) {
				index++;
				if (index < source.length() && (source.charAt(index) == '+' || source.charAt(index) == '-')) {
					index++;
				}
				digits(start);
			}
			add(TokenType.NUMBER, source.substring(start, index), start);
		}

		private void digits(int start) {
			int digitStart = index;
			while (index < source.length() && isDigit(source.charAt(index))) {
				index++;
			}
			if (digitStart == index) {
				throw errorAt(start, "invalid number literal");
			}
		}

		private void keyword() {
			int start = index;
			while (index < source.length() && Character.isLetter(source.charAt(index))) {
				index++;
			}
			String keyword = source.substring(start, index);
			TokenType type = switch (keyword) {
				case "true" -> TokenType.TRUE;
				case "false" -> TokenType.FALSE;
				case "null" -> TokenType.NULL;
				default -> throw errorAt(start, "unsupported token '" + keyword + "'");
			};
			add(type, keyword, start);
		}

		private void operator() {
			int start = index;
			if (match("&&")) add(TokenType.AND, "&&", start);
			else if (match("||")) add(TokenType.OR, "||", start);
			else if (match("==")) add(TokenType.EQUAL, "==", start);
			else if (match("!=")) add(TokenType.NOT_EQUAL, "!=", start);
			else if (match(">=")) add(TokenType.GREATER_EQUAL, ">=", start);
			else if (match("<=")) add(TokenType.LESS_EQUAL, "<=", start);
			else if (match("!")) add(TokenType.NOT, "!", start);
			else if (match(">")) add(TokenType.GREATER, ">", start);
			else if (match("<")) add(TokenType.LESS, "<", start);
			else if (match("(")) add(TokenType.LEFT_PAREN, "(", start);
			else if (match(")")) add(TokenType.RIGHT_PAREN, ")", start);
			else throw errorAt(start, "unsupported character '" + source.charAt(index) + "'");
		}

		private boolean peek(char expected, int offset) {
			return index + offset < source.length() && source.charAt(index + offset) == expected;
		}

		private boolean isDigit(char value) {
			return value >= '0' && value <= '9';
		}

		private boolean match(String value) {
			if (!source.startsWith(value, index)) {
				return false;
			}
			index += value.length();
			return true;
		}

		private void add(TokenType type, String text, int position) {
			tokens.add(new Token(type, text, position));
		}

		private IllegalArgumentException errorAt(int position, String message) {
			return conditionError(message + " at character " + position);
		}
	}

	private static final class Parser {

		private final List<Token> tokens;
		private int index;
		private int depth;

		private Parser(List<Token> tokens) {
			this.tokens = tokens;
		}

		private Expression parse() {
			Expression expression = or();
			consume(TokenType.END, "unexpected trailing token");
			return expression;
		}

		private Expression or() {
			Expression expression = and();
			while (match(TokenType.OR)) {
				expression = new Binary(expression, previous().type(), and());
			}
			return expression;
		}

		private Expression and() {
			Expression expression = equality();
			while (match(TokenType.AND)) {
				expression = new Binary(expression, previous().type(), equality());
			}
			return expression;
		}

		private Expression equality() {
			Expression expression = comparison();
			if (match(TokenType.EQUAL, TokenType.NOT_EQUAL)) {
				expression = new Binary(expression, previous().type(), comparison());
			}
			return expression;
		}

		private Expression comparison() {
			Expression expression = unary();
			if (match(TokenType.GREATER, TokenType.GREATER_EQUAL, TokenType.LESS, TokenType.LESS_EQUAL)) {
				expression = new Binary(expression, previous().type(), unary());
			}
			return expression;
		}

		private Expression unary() {
			if (match(TokenType.NOT)) {
				enterDepth();
				try {
					return new Unary(unary());
				} finally {
					depth--;
				}
			}
			return primary();
		}

		private Expression primary() {
			if (match(TokenType.TRUE)) return new Literal(Boolean.TRUE);
			if (match(TokenType.FALSE)) return new Literal(Boolean.FALSE);
			if (match(TokenType.NULL)) return new Literal(null);
			if (match(TokenType.NUMBER)) return numberLiteral(previous());
			if (match(TokenType.STRING)) return stringLiteral(previous());
			if (match(TokenType.REFERENCE)) return reference(previous());
			if (match(TokenType.LEFT_PAREN)) {
				enterDepth();
				try {
					Expression expression = or();
					consume(TokenType.RIGHT_PAREN, "expected ')'");
					return expression;
				} finally {
					depth--;
				}
			}
			throw error(current(), "expected a literal, scope reference, or '('");
		}

		private Expression numberLiteral(Token token) {
			try {
				return new Literal(new BigDecimal(token.text()));
			} catch (NumberFormatException e) {
				throw error(token, "invalid number literal");
			}
		}

		private Expression stringLiteral(Token token) {
			try {
				return new Literal(JsonParser.parseString(token.text()).getAsString());
			} catch (JsonParseException | IllegalStateException e) {
				throw error(token, "invalid JSON string literal");
			}
		}

		private Expression reference(Token token) {
			String body = token.text().substring(2, token.text().length() - 1);
			int position = 0;
			String root = identifier(body, position, token);
			position += root.length();
			List<PathSegment> path = new ArrayList<>();
			while (position < body.length()) {
				if (path.size() >= MAX_PATH_SEGMENTS) {
					throw error(token, "scope reference exceeds " + MAX_PATH_SEGMENTS + " path segments");
				}
				char current = body.charAt(position);
				if (current == '.') {
					position++;
					String key = identifier(body, position, token);
					position += key.length();
					path.add(new KeySegment(key));
				} else if (current == '[') {
					int close = body.indexOf(']', position + 1);
					if (close < 0) {
						throw error(token, "unterminated reference index");
					}
					String indexText = body.substring(position + 1, close);
					if (indexText.isEmpty() || !indexText.chars().allMatch(Character::isDigit)) {
						throw error(token, "reference indexes must be non-negative integers");
					}
					try {
						path.add(new IndexSegment(Integer.parseInt(indexText)));
					} catch (NumberFormatException e) {
						throw error(token, "reference index is too large");
					}
					position = close + 1;
				} else {
					throw error(token, "invalid scope reference path");
				}
			}
			return new Reference(token.text(), root, List.copyOf(path));
		}

		private String identifier(String value, int start, Token token) {
			if (start >= value.length() || !isIdentifierStart(value.charAt(start))) {
				throw error(token, "scope references require an identifier");
			}
			int end = start + 1;
			while (end < value.length() && isIdentifierPart(value.charAt(end))) {
				end++;
			}
			return value.substring(start, end);
		}

		private boolean isIdentifierStart(char value) {
			return Character.isLetter(value) || value == '_';
		}

		private boolean isIdentifierPart(char value) {
			return Character.isLetterOrDigit(value) || value == '_';
		}

		private void enterDepth() {
			depth++;
			if (depth > MAX_NESTING_DEPTH) {
				throw conditionError("expression exceeds nesting depth " + MAX_NESTING_DEPTH);
			}
		}

		private boolean match(TokenType... types) {
			for (TokenType type : types) {
				if (current().type() == type) {
					index++;
					return true;
				}
			}
			return false;
		}

		private Token consume(TokenType type, String message) {
			if (current().type() == type) {
				return tokens.get(index++);
			}
			throw error(current(), message);
		}

		private Token current() {
			return tokens.get(index);
		}

		private Token previous() {
			return tokens.get(index - 1);
		}

		private IllegalArgumentException error(Token token, String message) {
			return conditionError(message + " at character " + token.position());
		}
	}
}
