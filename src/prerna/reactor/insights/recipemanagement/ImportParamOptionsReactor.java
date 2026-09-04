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
package prerna.reactor.insights.recipemanagement;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.om.Insight;
import prerna.om.Pixel;
import prerna.om.PixelList;
import prerna.query.parsers.GenExpressionWrapper;
import prerna.query.parsers.ParamStruct;
import prerna.query.parsers.ParamStructDetails;
import prerna.query.parsers.ParamStructDetails.BASE_QS_TYPE;
import prerna.query.parsers.ParamStructDetails.QUOTE;
import prerna.query.parsers.SqlParser;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.transform.QSParseParamStruct;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.PixelPreProcessor;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;
import prerna.sablecc2.translations.ImportQueryTranslation;

public class ImportParamOptionsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ImportParamOptionsReactor.class);

	public static final String PARAM_OPTIONS = "PARAM_OPTIONS";

	@Override
	public NounMetadata execute() {
		PixelList pixelList = this.insight.getPixelList();

		Insight tempInsight = new Insight();
		ImportQueryTranslation translation = new ImportQueryTranslation(tempInsight);
		// loop through recipe
		for (Pixel pixel : pixelList) {
			try {
				String expression = pixel.getPixelString();
				translation.setPixelObj(pixel);
				expression = PixelPreProcessor.preProcessPixel(expression.trim(), new ArrayList<String>(),
						new HashMap<String, String>());
				Parser p = new Parser(new Lexer(new PushbackReader(
						new InputStreamReader(new ByteArrayInputStream(expression.getBytes("UTF-8"))),
						expression.length())));
				// parsing the pixel - this process also determines if expression is
				// syntactically correct
				Start tree = p.parse();
				// apply the translation.
				tree.apply(translation);
			} catch (ParserException | LexerException | IOException e) {
				classLogger.error("Failed to parse pixel {} with expression {}", pixel.getId(), pixel.getPixelString(),
						e);
			}
		}

		Map<Pixel, SelectQueryStruct> imports = translation.getImportQsMap();
		// for each import
		// we need to get the proper param struct
		List<Map<String, Object>> params = new ArrayList<>();
		for (Pixel pixelStep : imports.keySet()) {
			SelectQueryStruct qs = imports.get(pixelStep);
			List<ParamStruct> paramList = getParamsForImport(imports.get(pixelStep), pixelStep);
			Map<String, Map<String, Map<String, Map<String, List<ParamStruct>>>>> paramOutput = organizeStruct(
					paramList);

			Map<String, Object> output = new HashMap<>();
			if (qs instanceof HardSelectQueryStruct) {
				output.put("baseQsType", "hqs");
			} else {
				output.put("baseQsType", "sqs");
			}
			output.put("qsType", qs.getQsType());
			// legacy to remove
			output.put("appId", qs.getEngineId());
			output.put("databaseId", qs.getEngineId());
			output.put("pixelId", pixelStep.getId());
			output.put("pixelString", pixelStep.getPixelString());
			output.put("params", paramOutput);
			params.add(output);
		}

		NounMetadata retMap = new NounMetadata(params, PixelDataType.VECTOR);
		return retMap;
	}

	private List<ParamStruct> getParamsForImport(SelectQueryStruct qs, Pixel pixelObj) {
		List<ParamStruct> paramList = new ArrayList<>();
		if (qs instanceof HardSelectQueryStruct || qs.getCustomFrom() != null) {
			// do the logic of getting the params. The only issue here is
			// we assume the latest level which may not be true
			// but let us see
			String query = qs.getCustomFrom();
			if (query == null && qs instanceof HardSelectQueryStruct) {
				query = ((HardSelectQueryStruct) qs).getQuery();
			}

			SqlParser sqp2 = new SqlParser();
			sqp2.parameterize = false;
			try {
				GenExpressionWrapper wrapper = sqp2.processQuery(query);
				Iterator<ParamStructDetails> structIterator = wrapper.paramToExpressionMap.keySet().iterator();
				while (structIterator.hasNext()) {
					ParamStructDetails nextStructDetails = structIterator.next();
					nextStructDetails.setBaseQsType(BASE_QS_TYPE.HQS);
					nextStructDetails.setDatabaseId(qs.getEngineId());
					nextStructDetails.setPixelId(pixelObj.getId());
					nextStructDetails.setPixelString(pixelObj.getPixelString());
					ParamStruct nextStruct = new ParamStruct();
					nextStruct.addParamStructDetails(nextStructDetails);
					if (nextStructDetails.getOperator().equalsIgnoreCase("in")) {
						nextStruct.setMultiple(true);
					}
					paramList.add(nextStruct);
				}
			} catch (Exception e) {
				classLogger.error("Failed to parse the SQL query {}", query, e);
			}
		} else {
			// get the filters first
			GenRowFilters importFilters = qs.getExplicitFilters();
			Set<String> filteredColumns = importFilters.getAllQsFilteredColumns();

			QSParseParamStruct parser = new QSParseParamStruct(qs, pixelObj);
			for (IQueryFilter filter : importFilters) {
				parser.parseFilter(filter, paramList);
			}

			// the above should be the filtered options
			// lets go through the selectors
			// and what is not filtered will be added as well
			List<String> addedQs = new ArrayList<>();
			List<IQuerySelector> selectors = qs.getSelectors();
			for (IQuerySelector select : selectors) {
				List<QueryColumnSelector> allColumnSelectors = select.getAllQueryColumns();
				for (QueryColumnSelector colS : allColumnSelectors) {

					String colQS = colS.getQueryStructName();
					if (filteredColumns.contains(colQS)) {
						// already have a filter on it
						continue;
					}
					if (addedQs.contains(colQS)) {
						// we have already added this
						continue;
					}

					String frameOutput = pixelObj.getFrameOutputs().iterator().next();
					Map<String, Map<String, Object>> endingHeaders = pixelObj.getEndingFrameHeaders();
					Map<String, String> aliasToType = Pixel.getFrameHeadersToDataType(endingHeaders, frameOutput);

					ParamStructDetails detailsStruct = new ParamStructDetails();
					detailsStruct.setBaseQsType(BASE_QS_TYPE.SQS);
					detailsStruct.setDatabaseId(qs.getEngineId());
					detailsStruct.setPixelId(pixelObj.getId());
					detailsStruct.setPixelString(pixelObj.getPixelString());
					detailsStruct.setTableName(colS.getTable());
					detailsStruct.setColumnName(colS.getColumn());
					detailsStruct.setOperator("==");
					SemossDataType dataType = SemossDataType.convertStringToDataType(aliasToType.get(colS.getAlias()));
					detailsStruct.setType(PixelDataType.convertFromSemossDataType(dataType));
					if (dataType == SemossDataType.INT || dataType == SemossDataType.DOUBLE) {
						detailsStruct.setQuote(QUOTE.NO);
					}
					ParamStruct pStruct = new ParamStruct();
					pStruct.setMultiple(true);
					pStruct.setSearchable(true);
					pStruct.addParamStructDetails(detailsStruct);
					paramList.add(pStruct);
					// store that this qs has been added
					addedQs.add(colQS);
				}
			}
		}

		return paramList;
	}

	public Map<String, Map<String, Map<String, Map<String, List<ParamStruct>>>>> organizeStruct(
			List<ParamStruct> structs) {
		Map<String, Map<String, Map<String, Map<String, List<ParamStruct>>>>> columnMap = new TreeMap<>();
		// level 1 - column name
		// column (key) -- List of tables (Value)
		// level 2 - column + table
		// column + table (key) - operator (value)
		// level 3 - column + table + operator
		// column + table + operator(key) - Param Struct(value)
		// level 4 - frames - dont know how to get to this but..

		for (int paramIndex = 0; paramIndex < structs.size(); paramIndex++) {
			ParamStruct thisStruct = structs.get(paramIndex);
			// these structs will always only have 1 struct
			ParamStructDetails thisStructDetails = thisStruct.getDetailsList().get(0);
			String columnName = thisStructDetails.getColumnName();
			String tableName = thisStructDetails.getTableName();
			String opName = thisStructDetails.getOperator();
			String opuName = thisStructDetails.getuOperator();
			if (opuName == null) {
				opuName = opName;
			}

			// get the table
			Map<String, Map<String, Map<String, List<ParamStruct>>>> tableMap = null;
			if (columnMap.containsKey(columnName)) {
				tableMap = columnMap.get(columnName);
			} else {
				tableMap = new TreeMap<>();
			}

			// get the operator from the table
			Map<String, Map<String, List<ParamStruct>>> opMap = null;
			if (tableMap.containsKey(tableName)) {
				opMap = tableMap.get(tableName);
			} else {
				opMap = new TreeMap<>();
			}

			// get the table unique operator
			Map<String, List<ParamStruct>> opuMap = null;
			if (opMap.containsKey(opName)) {
				opuMap = opMap.get(opName);
			} else {
				opuMap = new TreeMap<>();
			}

			// get the actual paramstruct
			List<ParamStruct> curList = null;
			if (opuMap.containsKey(opuName)) {
				curList = opuMap.get(opuName);
			} else {
				curList = new ArrayList<>();
			}

			// add the paramstruct
			curList.add(thisStruct);
			// add the opumap
			opuMap.put(opuName, curList);
			// add it to the operator
			opMap.put(opName, opuMap);
			// put the table
			tableMap.put(tableName, opMap);
			// put the column
			columnMap.put(columnName, tableMap);
		}

		return columnMap;
	}

}
