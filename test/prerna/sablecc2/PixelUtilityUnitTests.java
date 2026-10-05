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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import prerna.om.Insight;
import prerna.om.Pixel;
import prerna.om.PixelList;
import prerna.query.parsers.ParamStruct;
import prerna.query.parsers.ParamStructDetails;
import prerna.query.parsers.ParamStructDetails.QUOTE;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.reactor.insights.SetInsightConfigReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.parser.ParserException;
import prerna.sablecc2.pipeline.PipelineOperation;

public class PixelUtilityUnitTests {

	////////////////////////////////////////////////////////////////////////////////
	// string literal decoding
	////////////////////////////////////////////////////////////////////////////////

	@Test
	void decodePixelStringLiteral_decodesCommonEscapes() {
		String input = "\"line1\\nline2\\tindent\\rreturn\\bback\\fform\\\\slash\\\"dq\\'sq\\/\"";
		String expected = "line1\nline2\tindent\rreturn\bback\fform\\slash\"dq'sq/";
		assertEquals(expected, PixelUtility.decodePixelStringLiteral(input));
	}

	@Test
	void decodePixelStringLiteral_decodesUnicodeEscapes() {
		String input = "\"hello " + "\\" + "u263A\"";
		assertEquals("hello " + (char) 0x263A, PixelUtility.decodePixelStringLiteral(input));
	}

	@Test
	void decodePixelStringLiteral_handlesNullEmptyAndUnquotedTokens() {
		assertNull(PixelUtility.decodePixelStringLiteral(null));
		assertEquals("", PixelUtility.decodePixelStringLiteral("\"\""));
		assertEquals("", PixelUtility.decodePixelStringLiteral("''"));
		assertEquals("no-quotes", PixelUtility.decodePixelStringLiteral("no-quotes"));
		assertEquals("padded", PixelUtility.decodePixelStringLiteral("   \"padded\"   "));
	}

	@Test
	void decodeEscapedString_preservesUnknownEscapes() {
		assertEquals("path\\q\\u12G4", PixelUtility.decodeEscapedString("path\\q\\u12G4"));
	}

	@Test
	void decodeEscapedString_handlesNullAndPlainText() {
		assertNull(PixelUtility.decodeEscapedString(null));
		assertEquals("plain-text", PixelUtility.decodeEscapedString("plain-text"));
		assertEquals("", PixelUtility.decodeEscapedString(""));
	}

	@Test
	void decodeEscapedString_keepsDanglingBackslash() {
		assertEquals("ends-with\\", PixelUtility.decodeEscapedString("ends-with\\"));
	}

	@Test
	void decodeEscapedString_keepsTruncatedUnicodeEscape() {
		// only 3 hex characters remain so the sequence cannot be decoded
		assertEquals("a\\u004", PixelUtility.decodeEscapedString("a\\u004"));
		// exactly 4 hex characters is decodable even at the very end of the input
		assertEquals("aA", PixelUtility.decodeEscapedString("a\\u0041"));
	}

	////////////////////////////////////////////////////////////////////////////////
	// quote handling
	////////////////////////////////////////////////////////////////////////////////

	@Test
	void removeSurroundingQuotes_stripsMatchingQuotes() {
		assertEquals("abc", PixelUtility.removeSurroundingQuotes("\"abc\""));
		assertEquals("abc", PixelUtility.removeSurroundingQuotes("'abc'"));
		assertEquals("abc", PixelUtility.removeSurroundingQuotes("   \"abc\"   "));
	}

	@Test
	void removeSurroundingQuotes_stripsUnbalancedQuotes() {
		assertEquals("abc", PixelUtility.removeSurroundingQuotes("\"abc"));
		assertEquals("abc", PixelUtility.removeSurroundingQuotes("abc\""));
		assertEquals("abc", PixelUtility.removeSurroundingQuotes("\"abc'"));
	}

	@Test
	void removeSurroundingQuotes_handlesEmptyAndQuoteOnlyInput() {
		assertEquals("", PixelUtility.removeSurroundingQuotes(""));
		assertEquals("", PixelUtility.removeSurroundingQuotes("\""));
		assertEquals("", PixelUtility.removeSurroundingQuotes("\"\""));
		assertEquals("unquoted", PixelUtility.removeSurroundingQuotes("unquoted"));
	}

	@Test
	void isLiteral_identifiesQuotedValues() {
		assertTrue(PixelUtility.isLiteral("\"abc\""));
		assertTrue(PixelUtility.isLiteral("'abc'"));
		assertTrue(PixelUtility.isLiteral("  \"abc\"  "));
		assertFalse(PixelUtility.isLiteral("abc"));
		assertFalse(PixelUtility.isLiteral("\"abc"));
		assertFalse(PixelUtility.isLiteral("abc\""));
	}

	////////////////////////////////////////////////////////////////////////////////
	// noun + pixel string generation
	////////////////////////////////////////////////////////////////////////////////

	@Test
	void getNoun_convertsNumbersToDecimalConstants() {
		NounMetadata noun = PixelUtility.getNoun(Integer.valueOf(5));
		assertEquals(PixelDataType.CONST_DECIMAL, noun.getNounType());
		assertEquals(Double.valueOf(5), noun.getValue());
	}

	@Test
	void getNoun_convertsQuotedValuesToStringConstants() {
		NounMetadata noun = PixelUtility.getNoun("\"hello\\nworld\"");
		assertEquals(PixelDataType.CONST_STRING, noun.getNounType());
		assertEquals("hello\nworld", noun.getValue());
	}

	@Test
	void getNoun_convertsNumericStringsToDecimalConstants() {
		NounMetadata noun = PixelUtility.getNoun("  12.5  ");
		assertEquals(PixelDataType.CONST_DECIMAL, noun.getNounType());
		assertEquals(Double.valueOf(12.5), noun.getValue());
	}

	@Test
	void getNoun_treatsNonNumericUnquotedValuesAsColumns() {
		NounMetadata noun = PixelUtility.getNoun("  TABLE__COLUMN  ");
		assertEquals(PixelDataType.COLUMN, noun.getNounType());
		assertEquals("TABLE__COLUMN", noun.getValue());
	}

	@Test
	void getNoun_rejectsUnsupportedValueTypes() {
		assertThrows(IllegalArgumentException.class, () -> PixelUtility.getNoun(Boolean.TRUE));
	}

	@Test
	void generatePixelString_buildsAssignmentStatements() {
		assertEquals("a = b;", PixelUtility.generatePixelString("a", "b"));
		assertEquals("a = 5;", PixelUtility.generatePixelString("a", Integer.valueOf(5)));
	}

	////////////////////////////////////////////////////////////////////////////////
	// encoded text restoration
	////////////////////////////////////////////////////////////////////////////////

	@Test
	void recreateOriginalPixelExpression_returnsExpressionWhenNothingWasEncoded() {
		String expression = "Select ( a ) ;";
		assertEquals(expression, PixelUtility.recreateOriginalPixelExpression(expression, new ArrayList<>(), null));
		assertEquals(expression,
				PixelUtility.recreateOriginalPixelExpression(expression, new ArrayList<>(), new HashMap<>()));
	}

	@Test
	void recreateOriginalPixelExpression_restoresEncodedBlocksInsideStringLiterals() {
		List<String> encodingList = new ArrayList<>(Collections.singletonList("a%20b"));
		Map<String, String> encodedTextToOriginal = new HashMap<>();
		encodedTextToOriginal.put("a%20b", "<encode>a b</encode>");

		String restored = PixelUtility.recreateOriginalPixelExpression(" Query ( \"a%20b\" ) ; ", encodingList,
				encodedTextToOriginal);

		assertEquals(" Query ( \"<encode>a b</encode>\" ) ; ", restored);
		assertTrue(encodingList.isEmpty());
	}

	@Test
	void recreateOriginalPixelExpression_dropsEncodeBlocksThatWereNeverEscaped() {
		// URI-encoding was a no-op, so restoring the wrapper would risk corrupting
		// unrelated occurrences of the same characters - "sea" inside "search"
		List<String> encodingList = new ArrayList<>(Collections.singletonList("sea"));
		Map<String, String> encodedTextToOriginal = new HashMap<>();
		encodedTextToOriginal.put("sea", "<encode>sea</encode>");

		String expression = " Search ( search = [ \"sea\" ] ) ; ";
		assertEquals(expression,
				PixelUtility.recreateOriginalPixelExpression(expression, encodingList, encodedTextToOriginal));
		assertTrue(encodingList.isEmpty());
	}

	@Test
	void recreateOriginalPixelExpression_ignoresEncodingsWithoutAnOriginal() {
		List<String> encodingList = new ArrayList<>(Collections.singletonList("orphan%20token"));
		Map<String, String> encodedTextToOriginal = new HashMap<>();
		encodedTextToOriginal.put("other%20token", "<encode>other token</encode>");

		String expression = " Query ( \"orphan%20token\" ) ; ";
		assertEquals(expression,
				PixelUtility.recreateOriginalPixelExpression(expression, encodingList, encodedTextToOriginal));
		assertTrue(encodingList.isEmpty());
	}

	////////////////////////////////////////////////////////////////////////////////
	// parsing
	////////////////////////////////////////////////////////////////////////////////

	@Test
	void parsePixel_splitsStepsAndRestoresEncodedText() throws Exception {
		List<String> parsed = PixelUtility.parsePixel("AddPanel(0); Query(\"<encode>a b</encode>\");");

		assertEquals(2, parsed.size());
		assertTrue(parsed.get(0).contains("AddPanel"));
		assertTrue(parsed.get(1).contains("<encode>a b</encode>"));
	}

	@Test
	void validatePixel_throwsOnInvalidSyntax() {
		assertThrows(ParserException.class, () -> PixelUtility.validatePixel("Select("));
	}

	@Test
	void addPixelToTranslation_appliesParsedExpression() {
		DashboardRecipeTranslation translation = new DashboardRecipeTranslation();
		PixelUtility.addPixelToTranslation(translation, "DashboardInsightConfig();");
		assertTrue(translation.isDashboard());
	}

	@Test
	void addPixelToTranslation_swallowsInvalidSyntax() {
		DashboardRecipeTranslation translation = new DashboardRecipeTranslation();
		PixelUtility.addPixelToTranslation(translation, "DashboardInsightConfig(");
		assertFalse(translation.isDashboard());
	}

	////////////////////////////////////////////////////////////////////////////////
	// recipe inspection
	////////////////////////////////////////////////////////////////////////////////

	@Test
	void isNotCacheable_detectsSinglePanelNonCacheableViews() {
		assertTrue(PixelUtility.isNotCacheable("AddPanel(0); Panel(0) | SetPanelView(\"default-handle\");"));
		assertTrue(PixelUtility.isNotCacheable("AddPanel(0); Panel(0) | SetPanelView(\"grid-delta\");"));
	}

	@Test
	void isNotCacheable_isFalseForOrdinaryAndMultiPanelRecipes() {
		assertFalse(PixelUtility.isNotCacheable("AddPanel(0);"));
		assertFalse(
				PixelUtility.isNotCacheable("AddPanel(0); AddPanel(1); Panel(0) | SetPanelView(\"default-handle\");"));
	}

	@Test
	void isNotCacheable_isFalseForUnparseableRecipes() {
		assertFalse(PixelUtility.isNotCacheable("Select("));
	}

	@Test
	void isNotCacheable_arrayAndListOverloadsJoinTheRecipe() {
		String[] steps = new String[] { "AddPanel(0);", "Panel(0) | SetPanelView(\"default-handle\");" };
		assertTrue(PixelUtility.isNotCacheable(steps));
		assertTrue(PixelUtility.isNotCacheable(Arrays.asList(steps)));
	}

	@Test
	void getInsightParameterJson_returnsDecodedPanelView() {
		List<String> recipe = Arrays.asList("AddPanel(0);",
				"Panel(0) | SetPanelView(\"param\", \"<encode>{\"json\":\"abc\"}</encode>\");");

		Map<String, Object> panelView = PixelUtility.getInsightParameterJson(recipe);

		assertNotNull(panelView);
		assertEquals("abc", panelView.get("json"));
	}

	@Test
	void getInsightParameterJson_returnsNullWhenNoParamViewExists() {
		assertNull(PixelUtility.getInsightParameterJson(Collections.singletonList("AddPanel(0);")));
	}

	@Test
	@SuppressWarnings("deprecation")
	void isDashboard_detectsDashboardConfigStep() {
		assertTrue(PixelUtility.isDashboard("AddPanel(0); DashboardInsightConfig();"));
		assertFalse(PixelUtility.isDashboard("AddPanel(0);"));
		assertFalse(PixelUtility.isDashboard("Select("));
	}

	@Test
	void isDashboard_arrayOverloadJoinsTheRecipe() {
		assertTrue(PixelUtility.isDashboard(new String[] { "AddPanel(0);", "DashboardInsightConfig();" }));
	}

	@Test
	void getFormWidgetInputs_extractsIntoAndValues() {
		Object[] inputs = PixelUtility.getFormWidgetInputs("Insert(into=[myColumn], values=[\"<MyParam>\"]);");

		assertNotNull(inputs);
		assertEquals(Collections.singletonList("myColumn"), inputs[0]);
		assertEquals(Collections.singletonList("MyParam"), inputs[1]);
	}

	@Test
	void getFormWidgetInputs_returnsNullForUnparseableExpressions() {
		assertNull(PixelUtility.getFormWidgetInputs("Insert(into=["));
	}

	@Test
	void getDatabaseIds_isEmptyWhenRecipeHasNoDatasource() {
		Set<String> ids = PixelUtility.getDatabaseIds(null, Collections.singletonList("AddPanel(0);"));
		assertNotNull(ids);
		assertTrue(ids.isEmpty());
	}

	@Test
	void getDatasourcesMetadata_isEmptyWhenRecipeHasNoDatasource() {
		List<Map<String, Object>> metadata = PixelUtility.getDatasourcesMetadata(null, "AddPanel(0);");
		assertNotNull(metadata);
		assertTrue(metadata.isEmpty());
	}

	@Test
	void autoExecuteAfterUserInput_onlyTrueForLoginRequired() {
		assertTrue(PixelUtility
				.autoExecuteAfterUserInput(Collections.singletonList(PixelOperationType.LOGGIN_REQUIRED_ERROR)));
		assertFalse(PixelUtility.autoExecuteAfterUserInput(Collections.singletonList(PixelOperationType.ERROR)));
		assertFalse(PixelUtility.autoExecuteAfterUserInput(Collections.<PixelOperationType>emptyList()));
	}

	////////////////////////////////////////////////////////////////////////////////
	// recipe generation helpers
	////////////////////////////////////////////////////////////////////////////////

	@Test
	void appendPositionInsightRecipeStep_serializesEachPixelPosition() {
		PixelList pixelList = new PixelList();
		Pixel positioned = new Pixel("1", "AddPanel(0);");
		Map<String, Object> position = new LinkedHashMap<>();
		position.put("top", "1");
		positioned.setPositionMap(position);
		pixelList.addPixel(positioned);
		// pixels default to an empty position map
		pixelList.addPixel(new Pixel("2", "AddPanel(1);"));
		Pixel unpositioned = new Pixel("3", "AddPanel(2);");
		unpositioned.setPositionMap(null);
		pixelList.addPixel(unpositioned);

		List<String> steps = new ArrayList<>();
		PixelUtility.appendPositionInsightRecipeStep(pixelList, steps);

		assertEquals(Collections.singletonList("META | PositionInsightRecipe({\"top\":\"1\"} , {} , null);"), steps);
	}

	@Test
	void appendPositionInsightRecipeStep_addsNothingForEmptyPixelList() {
		List<String> steps = new ArrayList<>();
		PixelUtility.appendPositionInsightRecipeStep(new PixelList(), steps);
		assertTrue(steps.isEmpty());
	}

	@Test
	void appendReadInsightTheme_addsThemeStep() {
		List<String> steps = new ArrayList<>();
		PixelUtility.appendReadInsightTheme(steps);
		assertEquals(Collections.singletonList("ReadInsightTheme();"), steps);
	}

	@Test
	void appendSetInsightConfig_addsStepOnlyWhenConfigExists() {
		Insight insight = new Insight();

		List<String> steps = new ArrayList<>();
		PixelUtility.appendSetInsightConfig(insight, steps);
		assertTrue(steps.isEmpty());

		Map<String, Object> config = new LinkedHashMap<>();
		config.put("a", "b");
		insight.getVarStore().put(SetInsightConfigReactor.INSIGHT_CONFIG, new NounMetadata(config, PixelDataType.MAP));

		PixelUtility.appendSetInsightConfig(insight, steps);
		assertEquals(Collections.singletonList("META | SetInsightConfig({\"a\":\"b\"});"), steps);
	}

	@Test
	void appendAddInsightParameter_addsOneStepPerStoredParameter() {
		Insight insight = new Insight();
		insight.getVarStore().put("myParam",
				new NounMetadata(buildParamStruct("myParam", null, QUOTE.DOUBLE, PixelDataType.CONST_STRING),
						PixelDataType.PARAM_STRUCT));

		List<String> steps = new ArrayList<>();
		PixelUtility.appendAddInsightParameter(insight, steps);

		assertEquals(1, steps.size());
		assertTrue(steps.get(0).startsWith("META | AddInsightParameter("));
		assertTrue(steps.get(0).endsWith(");"));
		assertTrue(steps.get(0).contains("\"paramName\":\"myParam\""));
	}

	@Test
	void appendPreAppliedParameter_addsOneStepPerPreDefinedParameter() {
		Insight insight = new Insight();
		insight.getVarStore().put("preParam",
				new NounMetadata(buildParamStruct("preParam", null, QUOTE.DOUBLE, PixelDataType.CONST_STRING),
						PixelDataType.PREAPPLIED_PARAM_STRUCT));

		List<String> steps = PixelUtility.appendPreAppliedParameter(insight, new ArrayList<>());

		assertEquals(1, steps.size());
		assertTrue(steps.get(0).startsWith("META | AddPreDefinedParameter("));
		assertTrue(steps.get(0).contains("\"paramName\":\"preParam\""));
	}

	@Test
	void getSetParamValuePixels_usesArraySyntaxForFillTypesAndUnquotedParams() {
		Insight arrayFill = new Insight();
		arrayFill.getVarStore().put("listParam",
				new NounMetadata(buildParamStruct("listParam", "checklist", QUOTE.DOUBLE, PixelDataType.CONST_STRING),
						PixelDataType.PARAM_STRUCT));
		assertEquals(
				Collections.singletonList(
						"META | SetInsightParamValue(paramName=\"listParam\", " + "paramValue=[<listParam>]);"),
				PixelUtility.getSetParamValuePixels(arrayFill));

		Insight unquoted = new Insight();
		unquoted.getVarStore().put("rawParam",
				new NounMetadata(buildParamStruct("rawParam", "dropdown", QUOTE.NO, PixelDataType.CONST_STRING),
						PixelDataType.PARAM_STRUCT));
		assertEquals(
				Collections.singletonList(
						"META | SetInsightParamValue(paramName=\"rawParam\", " + "paramValue=[<rawParam>]);"),
				PixelUtility.getSetParamValuePixels(unquoted));
	}

	@Test
	void getSetParamValuePixels_omitsQuotesForNumericParams() {
		Insight insight = new Insight();
		insight.getVarStore().put("numParam",
				new NounMetadata(buildParamStruct("numParam", "dropdown", QUOTE.DOUBLE, PixelDataType.CONST_INT),
						PixelDataType.PARAM_STRUCT));

		assertEquals(
				Collections.singletonList(
						"META | SetInsightParamValue(paramName=\"numParam\", " + "paramValue=<numParam>);"),
				PixelUtility.getSetParamValuePixels(insight));
	}

	@Test
	void getSetParamValuePixels_quotesStringParams() {
		Insight insight = new Insight();
		insight.getVarStore().put("strParam",
				new NounMetadata(buildParamStruct("strParam", "dropdown", QUOTE.DOUBLE, PixelDataType.CONST_STRING),
						PixelDataType.PARAM_STRUCT));

		assertEquals(
				Collections.singletonList(
						"META | SetInsightParamValue(paramName=\"strParam\", " + "paramValue=\"<strParam>\");"),
				PixelUtility.getSetParamValuePixels(insight));
	}

	@Test
	void getCachedInsightRecipe_cachesSheetsAndTheInsightTheme() {
		List<String> cacheRecipe = PixelUtility.getCachedInsightRecipe(new Insight());

		assertEquals(Arrays.asList("CachedSheet(\"" + Insight.DEFAULT_SHEET_ID + "\");", "ReadInsightTheme();"),
				cacheRecipe);
	}

	@Test
	void getMetaInsightRecipeSteps_alwaysEndsWithTheInsightTheme() {
		List<String> steps = PixelUtility.getMetaInsightRecipeSteps(new Insight(), new PixelList());

		assertEquals(Collections.singletonList("ReadInsightTheme();"), steps);
	}

	////////////////////////////////////////////////////////////////////////////////
	// pipeline generation
	////////////////////////////////////////////////////////////////////////////////

	@Test
	void generatePipeline_decodesEncodedQueryText() {
		String query = "SELECT first_name FROM people WHERE note = 'hello world' AND code LIKE '%20%'";
		String pixelExpression = "Database(database=[\"test-database\"]) | Query(\"<encode>" + query
				+ "</encode>\") | Import(frame=[CreateFrame(frameType=[GRID], override=[true]).as([\"Frame_1\"])]);";
		Insight insight = new Insight();
		insight.getPixelList().addPixel(new Pixel("pixel-1", pixelExpression));

		Map<String, Object> pipeline = PixelUtility.generatePipeline(insight);
		@SuppressWarnings("unchecked")
		List<List<PipelineOperation>> routines = (List<List<PipelineOperation>>) pipeline.get("pixelParsing");
		HardSelectQueryStruct queryStruct = null;
		for (List<PipelineOperation> routine : routines) {
			for (PipelineOperation operation : routine) {
				List<Map> queryStructInputs = operation.getNounInputs().get("qs");
				if (queryStructInputs != null && !queryStructInputs.isEmpty()) {
					Object value = queryStructInputs.get(0).get("value");
					if (value instanceof HardSelectQueryStruct) {
						queryStruct = (HardSelectQueryStruct) value;
					}
				}
			}
		}

		assertNotNull(queryStruct);
		assertEquals(query, queryStruct.getQuery());
	}

	@Test
	void generatePipeline_returnsTheIdMappingAlongsideTheParsing() {
		Insight insight = new Insight();
		insight.getPixelList().addPixel(new Pixel("pixel-1", "AddPanel(0);"));

		Map<String, Object> pipeline = PixelUtility.generatePipeline(insight);

		assertNotNull(pipeline.get("pixelParsing"));
		assertEquals(insight.getPixelList(), pipeline.get("idMapping"));
	}

	@Test
	void generatePipeline_isEmptyForAnEmptyRecipe() {
		Map<String, Object> pipeline = PixelUtility.generatePipeline(new Insight());

		@SuppressWarnings("unchecked")
		List<List<PipelineOperation>> routines = (List<List<PipelineOperation>>) pipeline.get("pixelParsing");
		assertNotNull(routines);
		assertTrue(routines.isEmpty());
	}

	@Test
	void generatePipeline_throwsWithSyntaxContextForInvalidSteps() {
		Insight insight = new Insight();
		insight.getPixelList().addPixel(new Pixel("pixel-1", "Select("));

		IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
				() -> PixelUtility.generatePipeline(insight));
		assertNotNull(thrown.getMessage());
		assertNotNull(thrown.getCause());
	}

	////////////////////////////////////////////////////////////////////////////////

	private static ParamStruct buildParamStruct(String paramName, String modelDisplay, QUOTE quote,
			PixelDataType type) {
		ParamStruct param = new ParamStruct();
		param.setParamName(paramName);
		param.setModelDisplay(modelDisplay);

		ParamStructDetails details = new ParamStructDetails();
		details.setQuote(quote);
		details.setType(type);
		param.addParamStructDetails(details);

		return param;
	}
}
