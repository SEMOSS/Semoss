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
package prerna.sablecc2;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.algorithm.api.ITableDataFrame;
import prerna.auth.User;
import prerna.om.ColorByValueRule;
import prerna.om.Insight;
import prerna.om.InsightPanel;
import prerna.om.InsightSheet;
import prerna.om.Pixel;
import prerna.om.PixelList;
import prerna.query.parsers.ParamStruct;
import prerna.query.parsers.ParamStructDetails;
import prerna.query.parsers.ParamStructDetails.QUOTE;
import prerna.query.parsers.ParamStructToJsonGenerator;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.transform.QsToPixelConverter;
import prerna.reactor.export.IFormatter;
import prerna.reactor.insights.SetInsightConfigReactor;
import prerna.sablecc2.analysis.DepthFirstAdapter;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;
import prerna.sablecc2.pipeline.PipelineTranslation;
import prerna.sablecc2.translations.DatasourceTranslation;
import prerna.sablecc2.translations.ParamStructSaveRecipeTranslation;
import prerna.sablecc2.translations.ParameterizeSaveRecipeTranslation;
import prerna.sablecc2.translations.ReplaceDatasourceTranslation;
import prerna.util.insight.InsightUtility;

public class PixelUtility {

	private static final Logger classLogger = LogManager.getLogger(PixelUtility.class);
	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

	/**
	 * Validates a pixel expression without executing it.
	 *
	 * @param pixelExpression pixel expression to validate
	 * @return validation output from the parser
	 * @throws ParserException when parsing fails
	 * @throws LexerException  when tokenization fails
	 * @throws IOException     when stream reading fails
	 */
	public static Set<String> validatePixel(String pixelExpression)
			throws ParserException, LexerException, IOException {
		return PixelRunner.validatePixel(pixelExpression);
	}

	/**
	 * Parses a pixel expression into individual recipe steps and restores any
	 * encoded text back to the original form.
	 *
	 * @param pixelExpression pixel expression to parse
	 * @return list of parsed pixel steps
	 * @throws ParserException when parsing fails
	 * @throws LexerException  when tokenization fails
	 * @throws IOException     when stream reading fails
	 */
	public static List<String> parsePixel(String pixelExpression) throws ParserException, LexerException, IOException {
		List<String> parsed = new ArrayList<>();
		List<String> encodingList = new ArrayList<>();
		Map<String, String> encodedTextToOriginal = new HashMap<>();
		String processedPixel = PixelPreProcessor.preProcessPixel(pixelExpression, encodingList, encodedTextToOriginal);
		List<String> parsedResults = PixelRunner.parsePixel(processedPixel);
		for (int i = 0; i < parsedResults.size(); i++) {
			String origExpression = PixelUtility.recreateOriginalPixelExpression(parsedResults.get(i), encodingList,
					encodedTextToOriginal);
			parsed.add(origExpression);
		}
		return parsed;
	}

	/**
	 * Builds a noun from a scalar value.
	 *
	 * @param value number, literal string, or column-like string
	 * @return noun metadata inferred from the input value
	 */
	public static NounMetadata getNoun(Object value) {
		NounMetadata noun = null;
		if (value instanceof Number) {
			noun = new NounMetadata(((Number) value).doubleValue(), PixelDataType.CONST_DECIMAL);
		} else if (value instanceof String) {
			if (isLiteral((String) value)) {
				// we have a literal
				String literal = decodePixelStringLiteral((String) value);
				noun = new NounMetadata(literal, PixelDataType.CONST_STRING);
			} else {
				// try to convert to a number
				try {
					double doubleValue = Double.parseDouble(value.toString().trim());
					noun = new NounMetadata(doubleValue, PixelDataType.CONST_DECIMAL);
				} catch (NumberFormatException e) {
					// confirmed that it is not a double
					// and that we have a column
					noun = new NounMetadata(value.toString().trim(), PixelDataType.COLUMN);
				}
			}
		} else {
			throw new IllegalArgumentException("Value must be a number or string!");
		}

		return noun;
	}

	public static String generatePixelString(String assignment, String value) {
		return assignment + " = " + value + ";";
	}

	public static String generatePixelString(String assignment, Object value) {
		return generatePixelString(assignment, value.toString());
	}

	/**
	 * Parses and applies a pixel string to a translation object.
	 *
	 * @param translation translation target
	 * @param pixelString pixel expression
	 */
	public static void addPixelToTranslation(DepthFirstAdapter translation, String pixelString) {
		try {
			Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(
					new ByteArrayInputStream(pixelString.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8))));
			Start tree = p.parse();
			tree.apply(translation);
		} catch (ParserException | LexerException | IOException ioe) {
			classLogger.error("Failed to parse pixel expression for translation: {}", pixelString, ioe);
		} catch (Exception e) {
			classLogger.error("Unexpected error while applying translation for pixel expression: {}", pixelString, e);
		}
	}

	/**
	 * Removes a single leading and trailing quote character, if present.
	 *
	 * @param literal input string
	 * @return string without outer quotes
	 */
	public static String removeSurroundingQuotes(String literal) {
		literal = literal.trim();
		if (literal.startsWith("\"") || literal.startsWith("'")) {
			// remove the first quote
			literal = literal.substring(1);
		}

		if (literal.endsWith("\"") || literal.endsWith("'")) {
			if (literal.length() == 1) {
				literal = "";
			} else {
				// remove the end quote
				literal = literal.substring(0, literal.length() - 1);
			}
		}

		return literal;
	}

	/**
	 * Decodes a Pixel string literal token by removing surrounding quotes and
	 * resolving supported escape sequences.
	 *
	 * @param literalToken token text as produced by the parser
	 * @return decoded string content, or {@code null} when input is null
	 */
	public static String decodePixelStringLiteral(String literalToken) {
		if (literalToken == null) {
			return null;
		}
		String unquoted = removeSurroundingQuotes(literalToken.trim());
		return decodeEscapedString(unquoted);
	}

	/**
	 * Decodes common escaped character sequences in a string.
	 * <p>
	 * Supported escapes: {@code \\}, {@code \"}, {@code \'}, {@code \n},
	 * {@code \r}, {@code \t}, {@code \b}, {@code \f}, {@code \/}, and
	 * {@code \\uXXXX}. Unknown escapes are preserved as-is.
	 *
	 * @param input string to decode
	 * @return decoded string, or {@code null} when input is null
	 */
	public static String decodeEscapedString(String input) {
		if (input == null || input.indexOf('\\') < 0) {
			return input;
		}

		StringBuilder decoded = new StringBuilder(input.length());
		int length = input.length();
		for (int i = 0; i < length; i++) {
			char current = input.charAt(i);
			if (current != '\\' || i == length - 1) {
				decoded.append(current);
				continue;
			}

			char next = input.charAt(++i);
			switch (next) {
			case '\\':
				decoded.append('\\');
				break;
			case '"':
				decoded.append('"');
				break;
			case '\'':
				decoded.append('\'');
				break;
			case 'n':
				decoded.append('\n');
				break;
			case 'r':
				decoded.append('\r');
				break;
			case 't':
				decoded.append('\t');
				break;
			case 'b':
				decoded.append('\b');
				break;
			case 'f':
				decoded.append('\f');
				break;
			case '/':
				decoded.append('/');
				break;
			case 'u':
				if (i + 4 < length) {
					String unicodeHex = input.substring(i + 1, i + 5);
					if (isHexSequence(unicodeHex)) {
						decoded.append((char) Integer.parseInt(unicodeHex, 16));
						i += 4;
						break;
					}
				}
				decoded.append('\\').append(next);
				break;
			default:
				decoded.append('\\').append(next);
				break;
			}
		}

		return decoded.toString();
	}

	private static boolean isHexSequence(String value) {
		for (int i = 0; i < value.length(); i++) {
			if (Character.digit(value.charAt(i), 16) < 0) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Determines whether the input is surrounded by matching quote delimiters.
	 *
	 * @param literal input string
	 * @return true when the value starts and ends with a quote character
	 */
	public static boolean isLiteral(String literal) {
		literal = literal.trim();
		return ((literal.startsWith("\"") || literal.startsWith("'"))
				&& (literal.endsWith("\"") || literal.endsWith("'")));
	}

	/**
	 * Checks whether a pixel recipe should be treated as non-cacheable.
	 *
	 * @param pixels pixel steps
	 * @return true when the recipe is not cacheable
	 */
	public static boolean isNotCacheable(String[] pixels) {
		String recipe = String.join("", pixels);
		return PixelUtility.isNotCacheable(recipe);
	}

	/**
	 * Checks whether a pixel recipe should be treated as non-cacheable.
	 *
	 * @param pixels pixel steps
	 * @return true when the recipe is not cacheable
	 */
	public static boolean isNotCacheable(List<String> pixels) {
		String recipe = String.join("", pixels);
		return PixelUtility.isNotCacheable(recipe);
	}

	/**
	 * Checks whether a pixel recipe should be treated as non-cacheable.
	 *
	 * @param pixel full pixel expression
	 * @return true when the recipe is not cacheable
	 */
	public static boolean isNotCacheable(String pixel) {
		pixel = PixelPreProcessor.preProcessPixel(pixel, new ArrayList<String>(), new HashMap<String, String>());
		try {
			Parser p = new Parser(new Lexer(new PushbackReader(
					new InputStreamReader(new ByteArrayInputStream(pixel.getBytes(StandardCharsets.UTF_8)),
							StandardCharsets.UTF_8),
					pixel.length())));
			InsightParamTranslation translation = new InsightParamTranslation();
			// parsing the pixel - this process also determines if expression is
			// syntactically correct
			Start tree = p.parse();
			// apply the translation.
			tree.apply(translation);
			return translation.notCacheable();
		} catch (ParserException | LexerException | IOException e) {
			classLogger.error("Unable to evaluate cacheability for the provided pixel expression", e);
			String eMessage = e.getMessage();
			if (eMessage.startsWith("[")) {
				Pattern pattern = Pattern.compile("\\[\\d+,\\d+\\]");
				Matcher matcher = pattern.matcher(eMessage);
				if (matcher.find()) {
					String location = matcher.group(0);
					location = location.substring(1, location.length() - 1);
					int findIndex = Integer.parseInt(location.split(",")[1]);
					eMessage += ". Error in syntax around " + pixel
							.substring(Math.max(findIndex - 10, 0), Math.min(findIndex + 10, pixel.length())).trim();
				}
			}
			classLogger.info(eMessage);
		}
		return false;
	}

	/**
	 * Retrieves the parameter panel JSON configuration from a recipe, when present.
	 *
	 * @param pixelList recipe steps
	 * @return parsed parameter view JSON, or {@code null} if unavailable
	 */
	public static Map<String, Object> getInsightParameterJson(List<String> pixelList) {
		String pixel = String.join("", pixelList);
		pixel = PixelPreProcessor.preProcessPixel(pixel, new ArrayList<String>(), new HashMap<String, String>());
		try {
			Parser p = new Parser(new Lexer(new PushbackReader(
					new InputStreamReader(new ByteArrayInputStream(pixel.getBytes(StandardCharsets.UTF_8)),
							StandardCharsets.UTF_8),
					pixel.length())));
			InsightParamTranslation translation = new InsightParamTranslation();
			// parsing the pixel - this process also determines if expression is
			// syntactically correct
			Start tree = p.parse();
			// apply the translation.
			tree.apply(translation);
			return translation.getPanelViewJson();
		} catch (ParserException | LexerException | IOException e) {
			classLogger.error("Unable to parse insight parameter JSON from recipe", e);
			String eMessage = e.getMessage();
			if (eMessage.startsWith("[")) {
				Pattern pattern = Pattern.compile("\\[\\d+,\\d+\\]");
				Matcher matcher = pattern.matcher(eMessage);
				if (matcher.find()) {
					String location = matcher.group(0);
					location = location.substring(1, location.length() - 1);
					int findIndex = Integer.parseInt(location.split(",")[1]);
					eMessage += ". Error in syntax around " + pixel
							.substring(Math.max(findIndex - 10, 0), Math.min(findIndex + 10, pixel.length())).trim();
				}
			}
			classLogger.info(eMessage);
		}

		return null;
	}

	/**
	 * Checks whether a recipe is a dashboard recipe.
	 *
	 * @param pixels recipe steps
	 * @return true when the recipe is a dashboard recipe
	 */
	public static boolean isDashboard(String[] pixels) {
		String recipe = String.join("", pixels);
		return PixelUtility.isDashboard(recipe);
	}

	/**
	 * Checks whether a recipe is a dashboard recipe.
	 *
	 * @param pixels recipe steps
	 * @return true when the recipe is a dashboard recipe
	 */
	@Deprecated
	public static boolean isDashboard(List<String> pixels) {
		String recipe = String.join("", pixels);
		return PixelUtility.isDashboard(recipe);
	}

	/**
	 * Checks whether a recipe is a dashboard recipe.
	 *
	 * @param pixel full recipe expression
	 * @return true when the recipe is a dashboard recipe
	 */
	@Deprecated
	public static boolean isDashboard(String pixel) {
		pixel = PixelPreProcessor.preProcessPixel(pixel, new ArrayList<String>(), new HashMap<String, String>());
		try {
			Parser p = new Parser(new Lexer(new PushbackReader(
					new InputStreamReader(new ByteArrayInputStream(pixel.getBytes(StandardCharsets.UTF_8)),
							StandardCharsets.UTF_8),
					pixel.length())));
			DashboardRecipeTranslation translation = new DashboardRecipeTranslation();
			// parsing the pixel - this process also determines if expression is
			// syntactically correct
			Start tree = p.parse();
			// apply the translation.
			tree.apply(translation);
			return translation.isDashboard();
		} catch (ParserException | LexerException | IOException e) {
			classLogger.error("Unable to evaluate dashboard recipe from pixel expression", e);
			String eMessage = e.getMessage();
			if (eMessage.startsWith("[")) {
				Pattern pattern = Pattern.compile("\\[\\d+,\\d+\\]");
				Matcher matcher = pattern.matcher(eMessage);
				if (matcher.find()) {
					String location = matcher.group(0);
					location = location.substring(1, location.length() - 1);
					int findIndex = Integer.parseInt(location.split(",")[1]);
					eMessage += ". Error in syntax around " + pixel
							.substring(Math.max(findIndex - 10, 0), Math.min(findIndex + 10, pixel.length())).trim();
				}
			}
			classLogger.info(eMessage);
		}
		return false;
	}

	/**
	 * Extracts `into` and `values` inputs from a form-widget pixel expression.
	 *
	 * @param pixel pixel expression
	 * @return object array where index 0 is `into` and index 1 is `values`
	 */
	public static Object[] getFormWidgetInputs(String pixel) {
		pixel = PixelPreProcessor.preProcessPixel(pixel, new ArrayList<String>(), new HashMap<String, String>());
		Object[] ret = new Object[2];
		try {
			Parser p = new Parser(new Lexer(new PushbackReader(
					new InputStreamReader(new ByteArrayInputStream(pixel.getBytes(StandardCharsets.UTF_8)),
							StandardCharsets.UTF_8),
					pixel.length())));
			FormWidgetTranslation translation = new FormWidgetTranslation();
			// parsing the pixel - this process also determines if expression is
			// syntactically correct
			Start tree = p.parse();
			// apply the translation.
			tree.apply(translation);
			ret[0] = translation.getInto();
			ret[1] = translation.getValues();
			return ret;
		} catch (ParserException | LexerException | IOException e) {
			classLogger.error("Unable to parse form widget inputs from pixel expression", e);
		}
		return null;
	}

	/**
	 * Replaces encoded placeholders in a parsed expression back to their original
	 * values.
	 *
	 * @param expression            original parsed expression
	 * @param encodingList          ordered list of encoded tokens
	 * @param encodedTextToOriginal mapping from encoded token to original value
	 * @return expression with original text restored
	 */
	public static String recreateOriginalPixelExpression(String expression, List<String> encodingList,
			Map<String, String> encodedTextToOriginal) {
		if (encodedTextToOriginal == null || encodedTextToOriginal.isEmpty()) {
			return expression;
		}
		// loop through and see if any encodedText portions have been modified
		// if they have, try and replace them back so it looks pretty for the FE
		Collections.sort(encodingList, new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				int l1 = o1.length();
				int l2 = o2.length();

				if (l1 > l2) {
					return -1;
				} else if (l1 < l2) {
					return 1;
				}

				return 0;
			}
		});

		Iterator<String> iterator = encodingList.iterator();
		while (iterator.hasNext()) {
			String encodedText = iterator.next();
			String original = encodedTextToOriginal.get(encodedText);
			if (original == null) {
				iterator.remove();
				continue;
			}
			// URI-encoding was a no-op (inner content had no special chars), so the
			// encoded token is byte-identical to the value the user wrote. Restoring
			// <encode>...</encode> wrapping serves no purpose here and a positionally
			// naive replace would corrupt the expression by matching the same chars
			// elsewhere - e.g. "sea" inside the param name "search". Drop the block
			// entirely; the expression already reads correctly without the wrapper.
			if (original.contains(encodedText)) {
				iterator.remove();
				continue;
			}
			// Otherwise the encoded form contains %XX escapes (which can't appear
			// in identifiers or grammar tokens, and never inside a quoted string
			// boundary since URI-encoding maps " -> %22). Anchor the match to a
			// string-literal position so an unrelated coincidental occurrence of
			// the same escape sequence elsewhere can't be picked up. Every encode
			// block in this codebase is emitted inside "..." so anchoring is safe.
			String quotedEncoded = "\"" + encodedText + "\"";
			int idx = expression.indexOf(quotedEncoded);
			if (idx >= 0) {
				expression = expression.substring(0, idx + 1) + original
						+ expression.substring(idx + 1 + encodedText.length());
				iterator.remove();
			}
		}
		return expression;
	}

	/**
	 * Parses an expression and returns datasource metadata detected in its recipe
	 * steps.
	 *
	 * @param user       execution user context
	 * @param expression full recipe expression
	 * @return list of datasource metadata maps
	 */
	public static List<Map<String, Object>> getDatasourcesMetadata(User user, String expression) {
		/*
		 * Using a translation object to go through and figure out all the datasources
		 * and how we would want to manipulate and change them as people swap the data
		 * but want to use the same routine / analysis
		 */

		Insight in = new Insight();
		in.setUser(user);
		DatasourceTranslation translation = new DatasourceTranslation(in);
		try {
			expression = PixelPreProcessor.preProcessPixel(expression, new ArrayList<String>(),
					new HashMap<String, String>());
			Parser p = new Parser(new Lexer(new PushbackReader(
					new InputStreamReader(new ByteArrayInputStream(expression.getBytes(StandardCharsets.UTF_8)),
							StandardCharsets.UTF_8),
					expression.length())));
			Start tree = p.parse();
			// apply the translation.
			tree.apply(translation);
		} catch (ParserException | LexerException | IOException e) {
			classLogger.error("Unable to parse datasource metadata from expression", e);
		}

		return translation.getDatasourcePixels();
	}

	/**
	 * Extracts datasource engine ids from a list of recipe steps.
	 *
	 * @param user       execution user context
	 * @param expression recipe steps
	 * @return set of datasource engine ids referenced by the recipe
	 */
	public static Set<String> getDatabaseIds(User user, List<String> expression) {
		StringBuilder finalExpression = new StringBuilder();
		expression.forEach(s -> finalExpression.append(s));
		return getDatabaseIds(user, finalExpression.toString());
	}

	/**
	 * Extracts datasource engine ids from a full recipe expression.
	 *
	 * @param user       execution user context
	 * @param expression full recipe expression
	 * @return set of datasource engine ids referenced by the recipe
	 */
	public static Set<String> getDatabaseIds(User user, String expression) {
		/*
		 * Using a translation object to go through and figure out all the datasources
		 * and how we would want to manipulate and change them as people swap the data
		 * but want to use the same routine / analysis
		 */

		Insight in = new Insight();
		in.setUser(user);
		DatasourceTranslation translation = new DatasourceTranslation(in);
		try {
			expression = PixelPreProcessor.preProcessPixel(expression, new ArrayList<String>(),
					new HashMap<String, String>());
			Parser p = new Parser(new Lexer(new PushbackReader(
					new InputStreamReader(new ByteArrayInputStream(expression.getBytes(StandardCharsets.UTF_8)),
							StandardCharsets.UTF_8),
					expression.length())));
			Start tree = p.parse();
			// apply the translation.
			tree.apply(translation);
		} catch (ParserException | LexerException | IOException e) {
			classLogger.error("Unable to extract datasource ids from expression", e);
		}

		Set<String> databaseIds = new HashSet<>();
		List<Map<String, Object>> datasourcePixels = translation.getDatasourcePixels();
		for (Map<String, Object> datasourceMetaMap : datasourcePixels) {
			Map<String, Object> paramMap = (Map<String, Object>) datasourceMetaMap.get("params");
			List<SelectQueryStruct> qsList = null;
			Object obj = paramMap.get("qs");
			if (obj instanceof List) {
				qsList = (List<SelectQueryStruct>) obj;
			} else {
				qsList = new ArrayList<>();
				qsList.add((SelectQueryStruct) obj);
			}
			if (qsList != null && !qsList.isEmpty()) {
				for (SelectQueryStruct qs : qsList) {
					if (qs.getQsType() == AbstractQueryStruct.QUERY_STRUCT_TYPE.ENGINE
							|| qs.getQsType() == AbstractQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY) {
						databaseIds.add(qs.getEngineId());
					}
				}
			}
		}

		return databaseIds;
	}

	/**
	 * Applies datasource replacement options to an existing insight recipe.
	 *
	 * @param in                 insight context
	 * @param fullRecipe         original recipe text
	 * @param replacementOptions replacement maps containing step index and
	 *                           replacement pixel text
	 * @return updated recipe steps
	 */
	public static List<String> modifyInsightDatasource(Insight in, String fullRecipe,
			List<Map<String, Object>> replacementOptions) {
		ReplaceDatasourceTranslation translation = new ReplaceDatasourceTranslation(in);
		translation.setReplacements(replacementOptions);
		try {
			fullRecipe = PixelPreProcessor.preProcessPixel(fullRecipe, translation.encodingList,
					translation.encodedToOriginal);
			Parser p = new Parser(new Lexer(new PushbackReader(
					new InputStreamReader(new ByteArrayInputStream(fullRecipe.getBytes(StandardCharsets.UTF_8)),
							StandardCharsets.UTF_8),
					fullRecipe.length())));
			Start tree = p.parse();
			// apply the translation.
			tree.apply(translation);
		} catch (ParserException | LexerException | IOException e) {
			classLogger.error("Unable to modify datasource recipe for insight", e);
		}

		return translation.getPixels();
	}

	/**
	 * Determines whether execution should auto-resume after user input.
	 *
	 * @param opTypes operation types from execution output
	 * @return true when the operation indicates a login-required flow
	 */
	public static boolean autoExecuteAfterUserInput(List<PixelOperationType> opTypes) {
		if (opTypes.contains(PixelOperationType.LOGGIN_REQUIRED_ERROR)) {
			return true;
		}
		return false;
	}

	/**
	 * Parameterizes recipe steps and builds a parameter-panel recipe for save flow.
	 *
	 * @param currentInsight source insight
	 * @param recipe         recipe steps
	 * @param recipeIds      ids for each recipe step
	 * @param params         parameter definitions to apply
	 * @param insightName    display name used in generated parameter JSON
	 * @return recipe containing parameter setup and panel view steps
	 */
	public static List<String> parameterizeRecipe(Insight currentInsight, List<String> recipe, List<String> recipeIds,
			List<ParamStruct> params, String insightName) {
		Insight in = new Insight();
		ParamStructSaveRecipeTranslation translation = new ParamStructSaveRecipeTranslation(in);
		translation.setInputsToParameterize(params);

		// loop through recipe
		int recipeSize = recipe.size();
		for (int i = 0; i < recipeSize; i++) {
			String expression = recipe.get(i);
			String pixelId = recipeIds.get(i);
			try {
				expression = PixelPreProcessor.preProcessPixel(expression.trim(), translation.encodingList,
						translation.encodedToOriginal);
				Parser p = new Parser(new Lexer(new PushbackReader(
						new InputStreamReader(new ByteArrayInputStream(expression.getBytes(StandardCharsets.UTF_8))),
						expression.length())));
				// parsing the pixel - this process also determines if expression is
				// syntactically correct
				Start tree = p.parse();
				// apply the translation.
				translation.setCurrentPixelId(pixelId);
				tree.apply(translation);
			} catch (ParserException | LexerException | IOException e) {
				classLogger.error("Unable to parameterize recipe step for pixel id {}", pixelId, e);
			}
		}

		// combine the pixels together into a string
		List<String> paramedPixels = translation.getPixels();
		StringBuilder fullRecipe = new StringBuilder();
		for (String s : paramedPixels) {
			fullRecipe.append(s.trim());
		}
		List<String> setParameters = PixelUtility.getSetParamValuePixels(currentInsight);
		for (String s : setParameters) {
			fullRecipe.append(s);
		}
		List<Map<String, Object>> insightJsonObject = ParamStructToJsonGenerator
				.generateInsightJsonForParameters(insightName, fullRecipe.toString(), params);

		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		List<String> paramedRecipe = new ArrayList<>(currentInsight.getVarStore().getInsightParameterKeys().size() + 2);
		// if i add parameters here
		appendAddInsightParameter(currentInsight, paramedRecipe);
		paramedRecipe.add("META | AddPanel(0);");
		paramedRecipe.add("META | Panel (0) | SetPanelView(\"param\", \"<encode> {\"beFill\":true, \"json\":"
				+ gson.toJson(insightJsonObject) + "}</encode>\");");
		return paramedRecipe;
	}

	/**
	 * Builds supplemental metadata recipe steps used during insight save.
	 *
	 * @param in        insight context
	 * @param pixelList source pixel list used for recipe-position metadata
	 * @return additional metadata steps
	 */
	public static List<String> getMetaInsightRecipeSteps(Insight in, PixelList pixelList) {
		List<String> additionalSteps = new ArrayList<>();
		// add the pipeline positions
		appendPositionInsightRecipeStep(pixelList, additionalSteps);
//		// add the semoss parameters
//		appendAddInsightParameter(in, additionalSteps);
		// add the insight config
		appendSetInsightConfig(in, additionalSteps);
		// add read insight theme
		appendReadInsightTheme(additionalSteps);
		return additionalSteps;
	}

	/**
	 * Append the position insight recipe step to the recipe list
	 * 
	 * @param pixelList
	 * @param additionalSteps
	 */
	public static void appendPositionInsightRecipeStep(PixelList pixelList, List<String> additionalSteps) {
		int size = pixelList.size();
		if (size > 0) {
			StringBuilder builder = new StringBuilder("META | PositionInsightRecipe(");
			for (int i = 0; i < size; i++) {
				Pixel pixel = pixelList.get(i);
				Map<String, Object> position = pixel.getPositionMap();
				if (position == null) {
					builder.append("null");
				} else {
					builder.append(GSON.toJson(position));
				}

				if (i + 1 != size) {
					builder.append(" , ");
				}
			}
			builder.append(");");
			additionalSteps.add(builder.toString());
		}
	}

	/**
	 * Appends `AddInsightParameter` metadata steps for all stored insight params.
	 *
	 * @param in              insight context
	 * @param additionalSteps destination recipe list
	 */
	public static void appendAddInsightParameter(Insight in, List<String> additionalSteps) {
		VarStore varStore = in.getVarStore();
		List<String> params = varStore.getInsightParameterKeys();
		// loop through the keys
		// and gson it
		for (String paramKey : params) {
			NounMetadata paramNoun = varStore.get(paramKey);
			ParamStruct param = (ParamStruct) paramNoun.getValue();
			// also adjust for the new optimized id if it is set
			param.swapOptimizedIds();
			additionalSteps.add("META | AddInsightParameter(" + GSON.toJson(param) + ");");
			// swap back for this instance of the insight
			// since we make a new pixel list object all together
			// during the save process
			param.swapOptimizedIds();
		}
	}

	/**
	 * Appends `AddPreDefinedParameter` metadata steps for predefined params.
	 *
	 * @param in              insight context
	 * @param additionalSteps destination recipe list
	 * @return the same destination list for chaining
	 */
	public static List<String> appendPreAppliedParameter(Insight in, List<String> additionalSteps) {
		VarStore varStore = in.getVarStore();
		List<String> params = varStore.getPreDefinedParametersKeys();
		// loop through the keys and gson it
		for (String paramKey : params) {
			NounMetadata paramNoun = varStore.get(paramKey);
			ParamStruct param = (ParamStruct) paramNoun.getValue();
			// also adjust for the new optimized id if it is set
			param.swapOptimizedIds();
			additionalSteps.add("META | AddPreDefinedParameter(" + GSON.toJson(param) + ");");
			// swap back for this instance of the insight
			// since we make a new pixel list object all together
			// during the save process
			param.swapOptimizedIds();
		}
		return additionalSteps;
	}

	/**
	 * Appends a `SetInsightConfig` step when the insight config noun is present.
	 *
	 * @param in              insight context
	 * @param additionalSteps destination recipe list
	 */
	public static void appendSetInsightConfig(Insight in, List<String> additionalSteps) {
		VarStore varStore = in.getVarStore();
		NounMetadata noun = varStore.get(SetInsightConfigReactor.INSIGHT_CONFIG);
		if (noun != null) {
			StringBuilder builder = new StringBuilder("META | SetInsightConfig(");
			builder.append(GSON.toJson(noun.getValue()));
			builder.append(");");
			additionalSteps.add(builder.toString());
		}
	}

	/**
	 * Appends a step to reload the current insight theme.
	 *
	 * @param additionalSteps destination recipe list
	 */
	public static void appendReadInsightTheme(List<String> additionalSteps) {
		additionalSteps.add("ReadInsightTheme();");
	}

	/**
	 * Builds `SetInsightParamValue` steps for existing insight parameters.
	 * <p>
	 * Returned steps include placeholders (for example `&lt;param&gt;`) and are
	 * intended to be resolved before execution.
	 *
	 * @param in insight context
	 * @return parameter-value recipe steps with placeholders
	 */
	public static List<String> getSetParamValuePixels(Insight in) {
		List<String> additionalSteps = new ArrayList<>();
		VarStore varStore = in.getVarStore();
		List<String> params = varStore.getInsightParameterKeys();
		// loop through the keys
		// and gson it
		for (String paramKey : params) {
			NounMetadata paramNoun = varStore.get(paramKey);
			ParamStruct param = (ParamStruct) paramNoun.getValue();
			String paramName = param.getParamName();
			if (ParamStruct.PARAM_FILL_USE_ARRAY_TYPES.contains(param.getModelDisplay())
					|| param.getDetailsList().get(0).getQuote() == QUOTE.NO) {
				additionalSteps.add("META | SetInsightParamValue(paramName=\"" + paramName + "\", paramValue=[<"
						+ paramName + ">]);");
			} else {
				PixelDataType importType = param.getDetailsList().get(0).getType();
				if (importType == PixelDataType.CONST_INT || importType == PixelDataType.CONST_DECIMAL) {
					additionalSteps.add("META | SetInsightParamValue(paramName=\"" + paramName + "\", paramValue=<"
							+ paramName + ">);");
				} else {
					additionalSteps.add("META | SetInsightParamValue(paramName=\"" + paramName + "\", paramValue=\"<"
							+ paramName + ">\");");
				}
			}
		}
		return additionalSteps;
	}

	/**
	 * Creates a cache-oriented recipe for reconstructing insight state.
	 *
	 * @param in insight context
	 * @return cache recipe steps
	 */
	public static List<String> getCachedInsightRecipe(Insight in) {
		List<String> cacheRecipe = new ArrayList<>();
		List<String> panelTasks = new ArrayList<>();

		// add sheets
		Map<String, InsightSheet> sheets = in.getInsightSheets();
		for (String sheetId : sheets.keySet()) {
			cacheRecipe.add("CachedSheet(\"" + sheetId + "\");");
		}

		// add panels
		Map<String, InsightPanel> panels = in.getInsightPanels();
		for (String panelId : panels.keySet()) {
			cacheRecipe.add("CachedPanel(\"" + panelId + "\");");
			InsightPanel panel = panels.get(panelId);

			boolean isVisualizaiton = panel.getPanelView() != null
					&& panel.getPanelView().equalsIgnoreCase("visualization");
			if (isVisualizaiton) {
				Map<String, SelectQueryStruct> qsMap = panel.getLayerQueryStruct();
				Map<String, TaskOptions> tOptionsMap = panel.getLayerTaskOption();
				Map<String, IFormatter> formatMap = panel.getLayerFormatter();

				if (qsMap != null) {
					for (String layer : qsMap.keySet()) {
						// grab the last pixel for each panel/layer combination
						Pixel viewPixel = in.getPixelList().findLastPixelViewNotRefresh(panelId, layer);
						if (viewPixel != null) {
							panelTasks.add(viewPixel.getPixelString());
						} else {
							// this is likely a clone of a viz
							// recreate the pixel that generated this chart
							SelectQueryStruct qs = qsMap.get(layer);
							TaskOptions tOptions = tOptionsMap.get(layer);
							IFormatter format = formatMap.get(layer);

							StringBuffer taskPixel = new StringBuffer(QsToPixelConverter.getPixel(qs, true))
									.append(" | Format(type=['").append(format.getFormatType()).append("']");
							if (format.getOptionsMap() != null && !format.getOptionsMap().isEmpty()) {
								taskPixel.append(", options=[").append(GSON.toJson(format.getOptionsMap()))
										.append("])");
							} else {
								taskPixel.append(")");
							}
							taskPixel.append(" | TaskOptions(").append(GSON.toJson(tOptions.getOptions()));
							if (tOptions.getLayout(panelId).equals("PivotTable")) {
								taskPixel.append(") | CollectPivot();");
							} else {
								taskPixel.append(") | Collect(").append(panel.getNumCollect()).append(");");
							}

							panelTasks.add(taskPixel.toString());
						}
					}
				} else {
					// TODO: THIS CURRENTLY HAPPENS FOR WHEN YOU HAVE A GRAPH
					// AND YOU WROTE A CUSTOM PIXEL RECIPE WHERE YOU NEVER CREATED A GRID
					// IT IS STORED IN THE VIEW PIXEL BUT NOT IN THE INSIGHT DEPENDENCY
					// try to find the task on layer 0
					Pixel viewPixel = in.getPixelList().findLastPixelViewNotRefresh(panelId, "0");
					if (viewPixel != null) {
						panelTasks.add(viewPixel.getPixelString());
					}
				}
			}
		}

		// add all the panel tasks to the cached recipe
		cacheRecipe.addAll(panelTasks);

		// add the color by values at the end of the recipe
		for (String panelId : panels.keySet()) {
			InsightPanel panel = panels.get(panelId);
			List<ColorByValueRule> cbvs = panel.getColorByValue();
			for (ColorByValueRule cbv : cbvs) {
				cacheRecipe.add("Panel(\"" + panelId + "\") | RetrievePanelColorByValue(name=[\"" + cbv.getId()
						+ "\"]) | Collect(2000);");
			}
		}

		// add the insight template
		cacheRecipe.add("ReadInsightTheme();");

		return cacheRecipe;
	}

	/**
	 * Builds an optimized pixel list for save/export scenarios.
	 *
	 * @param in insight context
	 * @return optimized pixel list
	 */
	public static PixelList getOptimizedPixelList(Insight in) {
		PixelList pList = new PixelList();

		PixelList insightPixelList = in.getPixelList();
		List<ParamStruct> paramStructs = in.getVarStore().pullParamStructs();
		List<ParamStructDetails> paramStructDetails = new ArrayList<>();
		for (ParamStruct pStruct : paramStructs) {
			List<ParamStructDetails> paramDetails = pStruct.getDetailsList();
			for (ParamStructDetails pDetail : paramDetails) {
				paramStructDetails.add(pDetail);
			}
		}

		int numSteps = insightPixelList.size();
		int newPixelId = numSteps + 1;

		// add sheets
		Map<String, InsightSheet> sheets = in.getInsightSheets();
		for (String sheetId : sheets.keySet()) {
			pList.addPixel(new Pixel((newPixelId++) + "", "AddSheet(\"" + sheetId + "\");"));
			String sheetState = (String) InsightUtility.getSheetState(sheets.get(sheetId), "string").getValue();
			pList.addPixel(new Pixel((newPixelId++) + "", "SetSheetState(" + sheetState + ");"));
		}

		// add panels
		Map<String, InsightPanel> panels = in.getInsightPanels();
		for (String panelId : panels.keySet()) {
			InsightPanel panel = panels.get(panelId);

			pList.addPixel(new Pixel((newPixelId++) + "",
					"AddPanel(panel=[\"" + panelId + "\"], sheet=[\"" + panel.getSheetId() + "\"]);"));
			String panelState = (String) InsightUtility.getPanelState(panel, "string").getValue();
			pList.addPixel(new Pixel((newPixelId++) + "", "SetPanelState(\"<encode>" + panelState + "</encode>\");"));
		}

		// add the main of the recipe - data loading / transformations / code blocks
		for (int i = 0; i < numSteps; i++) {
			Pixel pixelObject = insightPixelList.get(i);
			if (pixelObject.isFrameTransformation() || pixelObject.isCodeExecution() || pixelObject.isAssignment()
					|| pixelObject.isSaveDataTransformation()) {
				Pixel copy = pixelObject.copy();
				pList.addPixel(copy);

				// adjust parameters
				for (ParamStructDetails pDetail : paramStructDetails) {
					if (pDetail.getPixelId().equals(pixelObject.getId())) {
						// set to the new copy the optimized id recipe
						// which is the new index
						// recall its 0 based
						pDetail.setOptimizedPixelId((pList.size() - 1) + "");
					}
				}
			}
		}

		// add frame filters
		List<ITableDataFrame> refList = new ArrayList<>();
		VarStore vStore = in.getVarStore();
		List<String> frameNames = vStore.getFrameKeysCopy();
		for (String frameKey : frameNames) {
			ITableDataFrame frame = (ITableDataFrame) vStore.get(frameKey).getValue();
			if (!refList.contains(frame)) {
				GenRowFilters grf = frame.getFrameFilters();
				if (grf != null && !grf.isEmpty()) {
					StringBuffer filter = new StringBuffer();
					if (frameKey.equals(Insight.CUR_FRAME_KEY)) {
						filter.append(frame.getName());
					} else {
						filter.append(frameKey);
					}
					filter.append(" | AddFrameFilter(").append(QsToPixelConverter.convertGenRowFilters(grf))
							.append(");");
					pList.addPixel(new Pixel((newPixelId++) + "", filter.toString()));
				}

				refList.add(frame);
			}
		}

		// add panel tasks
		for (String panelId : panels.keySet()) {
			InsightPanel panel = panels.get(panelId);

			boolean isVisualizaiton = (panel.getPanelView() != null
					&& panel.getPanelView().equalsIgnoreCase("visualization"));
			if (isVisualizaiton) {
				Map<String, SelectQueryStruct> qsMap = panel.getLayerQueryStruct();
				Map<String, TaskOptions> tOptionsMap = panel.getLayerTaskOption();
				Map<String, IFormatter> formatMap = panel.getLayerFormatter();

				if (qsMap != null) {
					for (String layer : qsMap.keySet()) {
						// grab the last pixel for each panel/layer combination
						Pixel viewPixel = insightPixelList.findLastPixelViewNotRefresh(panelId, layer);
						if (viewPixel != null) {
							pList.addPixel(new Pixel((newPixelId++) + "", viewPixel.getPixelString()));
						} else {
							// this is likely a clone of a viz
							// recreate the pixel that generated this chart
							SelectQueryStruct qs = qsMap.get(layer);
							TaskOptions tOptions = tOptionsMap.get(layer);
							IFormatter format = formatMap.get(layer);

							StringBuffer taskPixel = new StringBuffer(QsToPixelConverter.getPixel(qs, true))
									.append(" | Format(type=['").append(format.getFormatType()).append("']");
							if (format.getOptionsMap() != null && !format.getOptionsMap().isEmpty()) {
								taskPixel.append(", options=[").append(GSON.toJson(format.getOptionsMap()))
										.append("])");
							} else {
								taskPixel.append(")");
							}
							taskPixel.append(" | TaskOptions(").append(GSON.toJson(tOptions.getOptions()));
							if (tOptions.getLayout(panelId).equals("PivotTable")) {
								taskPixel.append(") | CollectPivot();");
							} else {
								taskPixel.append(") | Collect(").append(panel.getNumCollect()).append(");");
							}

							pList.addPixel(new Pixel((newPixelId++) + "", taskPixel.toString()));
						}
					}
				} else {
					// TODO: THIS CURRENTLY HAPPENS FOR WHEN YOU HAVE A GRAPH
					// AND YOU WROTE A CUSTOM PIXEL RECIPE WHERE YOU NEVER CREATED A GRID
					// IT IS STORED IN THE VIEW PIXEL BUT NOT IN THE INSIGHT DEPENDENCY
					// try to find the task on layer 0
					Pixel viewPixel = insightPixelList.findLastPixelViewNotRefresh(panelId, "0");
					if (viewPixel != null) {
						pList.addPixel(new Pixel((newPixelId++) + "", viewPixel.getPixelString()));
					}
				}

				// add the color by values at the end of the recipe
				List<ColorByValueRule> cbvs = panel.getColorByValue();
				for (ColorByValueRule cbv : cbvs) {
					StringBuffer cbvTask = new StringBuffer("Panel(\"").append(panelId)
							.append("\") | RetrievePanelColorByValue(name=[\"").append(cbv.getId())
							.append("\"]) | Collect(2000);");

					pList.addPixel(new Pixel((newPixelId++) + "", cbvTask.toString()));
				}
			}
		}

		// add visualization steps and data exports at the end
		for (int i = 0; i < numSteps; i++) {
			Pixel pixelObject = insightPixelList.get(i);
			if (pixelObject.isSaveDataExport() || pixelObject.isSaveVisualization()) {
				pList.addPixel(new Pixel((newPixelId++) + "", pixelObject.getPixelString()));
			}
		}

		return pList;
	}

	/**
	 * Removes task pixels that are not needed to reconstruct visualization state.
	 *
	 * @param in insight context
	 */
	public static void removeUnnecessaryTaskPixels(Insight in) {
		PixelList insightPixelList = in.getPixelList();
		List<ParamStruct> paramStructs = in.getVarStore().pullParamStructs();
		Map<ParamStructDetails, Pixel> paramToPixelObj = new HashMap<>();
		for (ParamStruct pStruct : paramStructs) {
			List<ParamStructDetails> paramDetails = pStruct.getDetailsList();
			for (ParamStructDetails pDetail : paramDetails) {
				paramToPixelObj.put(pDetail, insightPixelList.getPixel(pDetail.getPixelId()));
			}
		}

		Map<String, InsightPanel> insightPanels = in.getInsightPanels();
		Map<String, Boolean> panelIsVisualization = new HashMap<>();
		for (String panelId : insightPanels.keySet()) {
			InsightPanel panel = insightPanels.get(panelId);
			boolean isVisualizaiton = panel.getPanelView().equalsIgnoreCase("visualization");
			panelIsVisualization.put(panelId, isVisualizaiton);
		}

		Map<String, Boolean> panelCloneConsidered = new HashMap<>();
		Set<String> panelIsNotClone = new HashSet<>();
		Set<String> panelLayer = new HashSet<>();
		LinkedList<String> idsToRemove = new LinkedList<>();
		// we need to store the last pixel executed for each task
		int numSteps = insightPixelList.size();
		for (int i = numSteps - 1; i >= 0; i--) {
			Pixel pixelObject = insightPixelList.get(i);

			boolean removeStep = true;
			Set<String> panelIds = null;
			List<TaskOptions> tOptions = pixelObject.getTaskOptions();
			if (tOptions != null && !tOptions.isEmpty()) {
				for (TaskOptions tOption : tOptions) {
					if (tOption.isOrnament()) {
						removeStep = false;
						continue;
					}
					panelIds = tOption.getPanelIds();
					for (String panelId : panelIds) {

						// if we are not visualization on this panel
						// this step is not needed
						InsightPanel panel = insightPanels.get(panelId);
						if (panel == null) {
							// this is a closed panel
							continue;
						}
						if (!panelIsVisualization.get(panelId)) {
							// panel is not visualization
							continue;
						}

						String layerId = tOption.getPanelLayerId(panelId);
						if (layerId == null) {
							layerId = "0";
						}
						String panelLayerId = panelId + "__" + layerId;
						if (!panelLayer.contains(panelLayerId)) {
							if (pixelObject.isRefreshPanel()) {
								// need to find the original
								Pixel correctPixel = insightPixelList.findLastPixelViewNotRefresh(panelId, layerId);
								if (correctPixel != null) {
									// set the original pixel value into this object
									pixelObject.setPixelString(correctPixel.getPixelString());
									pixelObject.setRefreshPanel(false);
								}
							}
							removeStep = false;
							panelLayer.add(panelLayerId);
							panelIsNotClone.add(panelId);
						}
						// if another panel depends on this query
						if (panelCloneConsidered.containsKey(panelId)) {
							if (!panelCloneConsidered.get(panelId)) {
								removeStep = false;
								panelCloneConsidered.put(panelId, true);
							}
						}
					}
				}

				if (removeStep) {
					idsToRemove.addFirst(pixelObject.getId());
				}
			}

			// store panels that are clones
			List<Map<String, String>> cloneMapList = pixelObject.getCloneMapList();
			if (cloneMapList != null && !cloneMapList.isEmpty()) {
				for (Map<String, String> simpleMap : cloneMapList) {
					String thisPanel = simpleMap.get("original");
					String clonePanel = simpleMap.get("clone");

					if (!panelIsNotClone.contains(clonePanel)) {
						// we are a clone and this is the view
						// we need to keep this panel
						panelCloneConsidered.put(thisPanel, false);
					}
				}
			}
		}

		// remove the ids from the pixel list
		if (!idsToRemove.isEmpty()) {
			insightPixelList.removeIds(idsToRemove, false);

			// update the references for the parameters
			for (ParamStructDetails pDetails : paramToPixelObj.keySet()) {
				pDetails.setPixelId(paramToPixelObj.get(pDetails).getId());
			}
		}
	}

	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////

	/**
	 * Generates pipeline metadata for the current insight recipe.
	 *
	 * @param in insight context
	 * @return map containing pixel id mapping and parsed pipeline structure
	 */
	public static Map<String, Object> generatePipeline(Insight in) {
		long start = System.currentTimeMillis();
		PipelineTranslation translation = new PipelineTranslation(in);
		List<String> encodingList = new ArrayList<>();
		Map<String, String> encodedTextToOriginal = new HashMap<>();

		PixelList pixelList = in.getPixelList();
		int size = pixelList.size();
		for (int i = 0; i < size; i++) {
			Pixel pixel = pixelList.get(i);
			String pixelString = pixel.getPixelString();
			try {
				pixelString = PixelPreProcessor.preProcessPixel(pixelString, encodingList, encodedTextToOriginal);
				Parser p = new Parser(new Lexer(new PushbackReader(
						new InputStreamReader(new ByteArrayInputStream(pixelString.getBytes(StandardCharsets.UTF_8)),
								StandardCharsets.UTF_8),
						pixelString.length())));

				// parsing the pixel - this process also determines if expression is
				// syntactically correct
				Start tree = p.parse();
				// apply the translation.
				tree.apply(translation);
			} catch (SemossPixelException e) {
				if (!e.isContinueThreadOfExecution()) {
					throw e;
				}
			} catch (ParserException | LexerException | IOException e) {
				// we only need to catch invalid syntax here
				// other exceptions are caught in lazy translation
				String eMessage = e.getMessage();
				if (eMessage.startsWith("[")) {
					Pattern pattern = Pattern.compile("\\[\\d+,\\d+\\]");
					Matcher matcher = pattern.matcher(eMessage);
					if (matcher.find()) {
						String location = matcher.group(0);
						location = location.substring(1, location.length() - 1);
						int findIndex = Integer.parseInt(location.split(",")[1]);
						eMessage += ". Error in syntax around " + pixelString
								.substring(Math.max(findIndex - 10, 0), Math.min(findIndex + 10, pixelString.length()))
								.trim();
					}
				}
				throw new IllegalArgumentException(eMessage, e);
			}
		}
		long end = System.currentTimeMillis();
		classLogger.debug("Total time to process = " + (end - start));

		Map<String, Object> retMap = new HashMap<>();
		retMap.put("idMapping", pixelList);
		retMap.put("pixelParsing", translation.getAllRoutines());
		return retMap;
	}

	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * OLD DEPRECATED METHODS - WILL DELETE AFTER A WHILE COMMENT DATED - 2021-01-11
	 */

	/**
	 * Legacy parameterization path used by older save flows.
	 *
	 * @param user        execution user context
	 * @param recipe      recipe steps
	 * @param paramsMap   parameter definitions
	 * @param insightName insight display name
	 * @return generated parameter-panel recipe
	 */
	@Deprecated
	public static List<String> getParameterizedRecipe(User user, List<String> recipe,
			List<Map<String, Object>> paramsMap, String insightName) {
		int numParams = paramsMap.size();
		List<String> params = new ArrayList<>(numParams);
		for (Map<String, Object> pMap : paramsMap) {
			String pName = (String) pMap.get("paramName");
			if (pName == null || pName.isEmpty()) {
				throw new IllegalArgumentException("Parameter list must all contain 'paramName'");
			}
			params.add(pName);
		}

		Insight in = new Insight();
		in.setUser(user);
		ParameterizeSaveRecipeTranslation translation = new ParameterizeSaveRecipeTranslation(in);
		translation.setInputsToParameterize(params);

		// loop through recipe
		for (String expression : recipe) {
			try {
				expression = PixelPreProcessor.preProcessPixel(expression.trim(), translation.encodingList,
						translation.encodedToOriginal);
				Parser p = new Parser(new Lexer(new PushbackReader(
						new InputStreamReader(new ByteArrayInputStream(expression.getBytes(StandardCharsets.UTF_8)),
								StandardCharsets.UTF_8),
						expression.length())));
				// parsing the pixel - this process also determines if expression is
				// syntactically correct
				Start tree = p.parse();
				// apply the translation.
				tree.apply(translation);
			} catch (ParserException | LexerException | IOException e) {
				classLogger.error("Unable to parameterize deprecated recipe expression: {}", expression, e);
			}
		}

		Map<String, Map<String, String>> processedParams = translation.getParamToSource();
		Map<String, List<String>> colToComparators = translation.getColToComparators();
		List<String> newRecipe = translation.getPixels();
		// since i am already adding a AddPanel(0) at the start of this recipe
		// if it is contained as the first index inside newRecipe
		// i will remove it
		String first = newRecipe.get(0);
		first = first.replace(" ", "").trim();
		if (first.equals("AddPanel(0);")) {
			newRecipe.remove(0);
		}

		StringBuilder fullRecipe = new StringBuilder();
		for (String s : newRecipe) {
			fullRecipe.append(s.trim());
		}

		List<Map<String, Object>> vec = new ArrayList<>();
		Map<String, Object> map = new LinkedHashMap<>();
		// recipe is the query
		map.put("query", fullRecipe.toString());
		map.put("label", insightName);
		map.put("description", "Please select paramters for the insight");
		// add params
		int infiniteScrollCounter = 0;
		List<Map<String, Object>> paramList = new ArrayList<>();
		for (int i = 0; i < numParams; i++) {
			Map<String, Object> pMap = paramsMap.get(i);
			String param = (String) pMap.get("paramName");
			// for now
			// we will add each comparator once
			// based on its occurrence
			Set<String> comparators = new HashSet<>(colToComparators.get(param));

			boolean keepSearch = keepSearchParameter(pMap);
			// we will run this
			// for every column-comparator combination
			for (String comparator : comparators) {
				boolean isNumeric = IQueryFilter.comparatorIsNumeric(comparator);
				String jsonParamName = param + "__" + IQueryFilter.getSimpleNameForComparator(comparator);
				String comparatorDisplay = IQueryFilter.getDisplayNameForComparator(comparator);
				// and now for the param itself
				Map<String, Object> paramMap = new LinkedHashMap<>();
				paramMap.put("paramName", jsonParamName);
				paramMap.put("required", true);
				paramMap.put("useSelectedValues", true);
				// nested map for view
				Map<String, Object> paramViewMap = new LinkedHashMap<>();
				if (isNumeric) {
					paramViewMap.put("label", "Enter value for : " + param + "  " + comparatorDisplay); // + " [user
																										// input]");
					paramViewMap.put("displayType", "number");
				} else {
					paramViewMap.put("label", "Select values for : " + param + "  " + comparatorDisplay); // + " [user
																											// input]");
					paramViewMap.put("displayType", "checklist");
				}

				// nested attributes map within nested view map
				Map<String, Boolean> paramViewAttrMap = new LinkedHashMap<>();
				// if numeric - no search and single valued
				paramViewAttrMap.put("searchable", !isNumeric);
				paramViewAttrMap.put("multiple", !isNumeric);
				paramViewAttrMap.put("quickselect", !isNumeric);
				paramViewMap.put("attributes", paramViewAttrMap);
				// add view
				paramMap.put("view", paramViewMap);
				// nested map for model
				Map<String, Object> modelMap = new LinkedHashMap<>();
				Map<String, String> processedParam = processedParams.get(param);
				String physicalQs = processedParam.get("qs");
				String infiniteVar = "infinite" + infiniteScrollCounter++;
				String paramQ = "(" + infiniteVar + " = " + processedParam.get("source") + " | Select(" + physicalQs
						+ ") | Filter(" + physicalQs + " ?like \"<" + jsonParamName + "_Search>\") | Sort(columns=["
						+ physicalQs + "], sort=[asc]) | Iterate()) | Collect(20);";
				modelMap.put("query", paramQ);
				paramMap.put("model", modelMap);

				if (keepSearch & !isNumeric) {
					// add to model map
					modelMap.put("infiniteQuery", infiniteVar + " | Collect(20)");
					modelMap.put("searchParam", jsonParamName + "_Search");
					modelMap.put("dependsOn", new String[] { jsonParamName + "_Search" });

					// create search as well
					Map<String, Object> paramSearchMap = new LinkedHashMap<>();
					paramSearchMap.put("paramName", jsonParamName + "_Search");
					paramSearchMap.put("view", false);
					Map<String, String> paramSearchModel = new LinkedHashMap<>();
					paramSearchModel.put("defaultValue", "");
					paramSearchMap.put("model", paramSearchModel);
					paramList.add(paramSearchMap);
				}

				// now merge the existing pMap into the default values
				recursivelyMergeMaps(paramMap, pMap);
				// add to the param list
				paramList.add(paramMap);
			}
		}
		// add param list
		map.put("params", paramList);
		// add execute
		map.put("execute", "button");
		vec.add(map);

		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		List<String> paramedRecipe = new ArrayList<>(2);
		paramedRecipe.add("AddPanel(0);");
		paramedRecipe.add("META | Panel (0) | SetPanelView(\"param\", \"<encode> {\"json\":" + gson.toJson(vec)
				+ "}</encode>\");");
		return paramedRecipe;
	}

	/**
	 * Recursively merges keys from {@code newMap} into {@code mainMap}.
	 *
	 * @param mainMap destination map
	 * @param newMap  source map
	 */
	private static void recursivelyMergeMaps(Map<String, Object> mainMap, Map<String, Object> newMap) {
		if (newMap != null) {
			for (String key : newMap.keySet()) {
				if (mainMap.containsKey(key)) {
					// we have an overlap
					// lets see if the children are both maps
					boolean newKeyIsMap = (newMap.get(key) instanceof Map);
					boolean existingKeyIsMap = (mainMap.get(key) instanceof Map);
					if (newKeyIsMap && existingKeyIsMap) {
						// recursively go through and try to add
						recursivelyMergeMaps((Map) mainMap.get(key), (Map) newMap.get(key));
					} else {
						// both are not maps
						// just override

						// with new changes where BE separates
						// the comparators out
						if (!key.equals("paramName")) {
							mainMap.put(key, newMap.get(key));
						}
					}
				} else {
					// brand new key
					// put all into the main map
					mainMap.put(key, newMap.get(key));
				}
			}
		}
	}

	/**
	 * Determines whether search scaffolding should be generated for a parameter.
	 *
	 * @param map parameter configuration map
	 * @return true when search support should be retained
	 */
	private static boolean keepSearchParameter(Map<String, Object> map) {
		final String MODEL_KEY = "model";
		final String DEFAULT_OPTIONS_KEY = "defaultOptions";

		if (map.containsKey(MODEL_KEY)) {
			Map<String, Object> innerMap = (Map<String, Object>) map.get(MODEL_KEY);
			if (innerMap.containsKey(DEFAULT_OPTIONS_KEY)) {
				return false;
			}
		}

		return true;
	}
}
