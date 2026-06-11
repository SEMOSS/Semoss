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
package prerna.util.gson;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
import com.google.gson.Strictness;
import com.google.gson.stream.JsonReader;

import prerna.date.SemossDate;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.ColorByValueRule;
import prerna.om.HeadersDataRow;
import prerna.om.Pixel;
import prerna.om.PixelList;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.ParquetQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.TemporalEngineHardQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.FunctionQueryFilter;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryCustomOrderBy;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.selectors.QueryIfSelector;
import prerna.query.querystruct.selectors.QueryOpaqueSelector;
import prerna.query.querystruct.selectors.QueryTypedColumnSelector;
import prerna.query.querystruct.update.UpdateQueryStruct;
import prerna.reactor.export.ClustergramFormatter;
import prerna.reactor.export.GraphFormatter;
import prerna.reactor.export.HierarchyFormatter;
import prerna.reactor.export.IFormatter;
import prerna.reactor.export.JsonFormatter;
import prerna.reactor.export.KeyValueFormatter;
import prerna.reactor.export.TableFormatter;
import prerna.reactor.qs.SubQueryExpression;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.util.Constants;

public class GsonUtility {

	private static final Logger classLogger = LogManager.getLogger(GsonUtility.class);

	private static boolean testing = false;

	private GsonUtility() {

	}

	public static Gson getDefaultGson(boolean pretty) {
		GsonBuilder gsonBuilder = new GsonBuilder().disableHtmlEscaping()
				.excludeFieldsWithModifiers(Modifier.STATIC, Modifier.TRANSIENT)
				.registerTypeAdapter(Double.class, new NumberAdapter())
				.registerTypeAdapter(SemossDate.class, new SemossDateAdapter())

				// qs
				.registerTypeAdapter(TemporalEngineHardQueryStruct.class,
						new TemporalEngineHardSelectQueryStructAdapter())
				.registerTypeAdapter(HardSelectQueryStruct.class, new HardSelectQueryStructAdapter())
				.registerTypeAdapter(SelectQueryStruct.class, new SelectQueryStructAdapter())
				.registerTypeAdapter(CsvQueryStruct.class, new CsvQueryStructAdapter())
				.registerTypeAdapter(ExcelQueryStruct.class, new ExcelQueryStructAdapter())
				.registerTypeAdapter(ParquetQueryStruct.class, new ParquetQueryStructAdapter())
				.registerTypeAdapter(UpdateQueryStruct.class, new UpdateQueryStructAdapter())
				.registerTypeAdapter(ColorByValueRule.class, new ColorByValueRuleAdapter())

				// selectors
				.registerTypeAdapter(IQuerySelector.class, new IQuerySelectorAdapter())
				.registerTypeAdapter(QueryColumnSelector.class, new QueryColumnSelectorAdapter())
				.registerTypeAdapter(QueryTypedColumnSelector.class, new QueryTypedColumnSelectorAdapter())
				.registerTypeAdapter(QueryFunctionSelector.class, new QueryFunctionSelectorAdapter())
				.registerTypeAdapter(QueryArithmeticSelector.class, new QueryArithmeticSelectorAdapter())
				.registerTypeAdapter(QueryOpaqueSelector.class, new QueryOpaqueSelectorAdapter())
				.registerTypeAdapter(QueryIfSelector.class, new QueryIfSelectorAdapter())
				.registerTypeAdapter(QueryConstantSelector.class, new QueryConstantSelectorAdapter())
				// part of query constants
				.registerTypeAdapter(SubQueryExpression.class, new SubQueryExpressionAdapter())

				// filters
				.registerTypeAdapter(GenRowFilters.class, new GenRowFiltersAdapter())
				.registerTypeAdapter(IQueryFilter.class, new IQueryFilterAdapter())
				.registerTypeAdapter(SimpleQueryFilter.class, new SimpleQueryFilterAdapter())
				.registerTypeAdapter(OrQueryFilter.class, new OrQueryFilterAdapter())
				.registerTypeAdapter(AndQueryFilter.class, new AndQueryFilterAdapter())
				.registerTypeAdapter(FunctionQueryFilter.class, new FunctionQueryFilterAdapter())

				// sorts
				.registerTypeAdapter(IQuerySort.class, new IQuerySortAdapter())
				.registerTypeAdapter(QueryColumnOrderBySelector.class, new QueryColumnOrderBySelectorAdapter())
				.registerTypeAdapter(QueryCustomOrderBy.class, new QueryCustomOrderByAdapter())

				// object models
				.registerTypeAdapter(NounMetadata.class, new NounMetadataAdapter())
				.registerTypeAdapter(VarStore.class, new VarStoreAdapter())

				// iterators
				.registerTypeAdapter(BasicIteratorTask.class, new BasicIteratorTaskAdapter())
				.registerTypeAdapter(TaskOptions.class, new TaskOptionsAdapter())
				// formatters
				.registerTypeAdapter(IFormatter.class, new IFormatterAdapter())
				.registerTypeAdapter(TableFormatter.class, new IFormatterAdapter())
				.registerTypeAdapter(GraphFormatter.class, new IFormatterAdapter())
				.registerTypeAdapter(JsonFormatter.class, new IFormatterAdapter())
				.registerTypeAdapter(KeyValueFormatter.class, new IFormatterAdapter())
				.registerTypeAdapter(ClustergramFormatter.class, new IFormatterAdapter())
				.registerTypeAdapter(HierarchyFormatter.class, new IFormatterAdapter())

				// OLD LEGACY STUFF
				.registerTypeAdapter(SEMOSSVertex.class, new SEMOSSVertexAdapter())
				.registerTypeAdapter(SEMOSSEdge.class, new SEMOSSEdgeAdapter())

				// cluster
				.registerTypeAdapter(IHeadersDataRow.class, new IHeadersDataRowAdapter())
				.registerTypeAdapter(HeadersDataRow.class, new HeadersDataRowAdapter())

				// pixel objects
				.registerTypeAdapter(Pixel.class, new PixelAdapter())
				.registerTypeAdapter(PixelList.class, new PixelListAdapter())

				// dates
				.registerTypeAdapter(LocalDateTime.class, new LocalDateTimeAdapter())
				.registerTypeAdapter(ZonedDateTime.class, new ZonedDateTimeAdapter());

		if (pretty) {
			gsonBuilder.setPrettyPrinting();
		}

		return gsonBuilder.create();
	}

	public static Gson getDefaultGson() {
		return getDefaultGson(testing);
	}

	/**
	 * 
	 * @param filePath
	 * @param typeToken
	 * @throws IOException
	 */
	public static Object readJsonFileToObject(String filePath, java.lang.reflect.Type type) throws IOException {
		return readJsonFileToObject(new File(filePath), type);
	}

	/**
	 * 
	 * @param file
	 * @param typeToken
	 * @throws IOException
	 */
	public static Object readJsonFileToObject(File file, java.lang.reflect.Type type) throws IOException {
		JsonReader jReader = null;
		BufferedReader fReader = null;
		try {
			Gson gson = new GsonBuilder().registerTypeAdapter(ZonedDateTime.class, new ZonedDateTimeAdapter()).create();
			fReader = Files.newBufferedReader(file.toPath(), StandardCharsets.UTF_8);
			jReader = new JsonReader(fReader);
			return gson.fromJson(jReader, type);
		} finally {
			if (fReader != null) {
				try {
					fReader.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			if (jReader != null) {
				try {
					jReader.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	/**
	 * 
	 * @param file
	 * @param gson
	 * @param objToWrite
	 * @throws IOException
	 */
	public static void writeObjectToJsonFile(File file, Gson gson, Object objToWrite) throws IOException {
		FileWriter writer = null;
		try {
			writer = new FileWriter(file);
			gson.toJson(objToWrite, writer);
		} finally {
			if (writer != null) {
				try {
					writer.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	/**
	 * Validates that the given string is valid JSON format using strict parsing.
	 *
	 * @param jsonString the JSON string to validate
	 * @throws IllegalArgumentException if the string is not valid JSON
	 */
	public static void validateJsonString(String jsonString) {
		if (jsonString == null || jsonString.trim().isEmpty()) {
			throw new IllegalArgumentException("JSON string cannot be null or empty");
		}
		try {
			JsonReader reader = new JsonReader(new StringReader(jsonString));
			reader.setStrictness(Strictness.STRICT);
			JsonParser.parseReader(reader);
			// Ensure there's no extra content after the JSON
			reader.peek();
		} catch (Exception e) {
			throw new IllegalArgumentException("Invalid JSON format: " + e.getMessage());
		}
	}

	/**
	 * Checks if the given string is valid JSON format using strict parsing.
	 *
	 * @param jsonString the JSON string to check
	 * @return true if the string is valid JSON, false otherwise
	 */
	public static boolean isValidJson(String jsonString) {
		if (jsonString == null || jsonString.trim().isEmpty()) {
			return false;
		}
		try {
			JsonReader reader = new JsonReader(new StringReader(jsonString));
			reader.setStrictness(Strictness.STRICT);
			JsonParser.parseReader(reader);
			reader.peek();
			return true;
		} catch (Exception e) {
			return false;
		}
	}

}
