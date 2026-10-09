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
/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * Licensed under the Apache License, Version 2.0 or GPLv2, as applicable.
 *******************************************************************************/
package prerna.ds.py;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import javax.crypto.Cipher;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.SemossDataType;
import prerna.cache.CachePropFileFrameObject;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.shared.AbstractTableDataFrame;
import prerna.ds.shared.RawCachedWrapper;
import prerna.ds.util.flatfile.CsvFileIterator;
import prerna.ds.util.flatfile.ParquetFileIterator;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.HeadersException;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.PolarsQueryInterpreter;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.ParquetQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.reactor.imports.ImportUtility;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;

/**
 * Owns one eager Polars DataFrame inside an Insight's Python runtime.
 *
 * <p>
 * The generated Python identifier is independent of the user-visible frame
 * alias, which keeps aliases out of executable Python. Queries cross the bridge
 * as structural JSON plans and execute as Polars lazy plans. Mutations build a
 * replacement DataFrame before rebinding it, so a failed mutation leaves the
 * original data unchanged.
 *
 * <p>
 * The frame borrows the Insight's {@link PyTranslator}; closing the frame
 * removes only its generated Python variable and does not close the translator.
 */
public class PolarsFrame extends AbstractTableDataFrame {

	public static final String DATA_MAKER_NAME = "PolarsFrame";

	private static final Gson GSON = new GsonBuilder().serializeNulls().disableHtmlEscaping().create();
	private static final Pattern PYTHON_IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

	private final PyTranslator pyTranslator;
	private final String runtimeName;
	private boolean cache = true;

	/**
	 * Creates an empty Polars frame with a generated alias.
	 *
	 * @param pyTranslator translator owned by the containing Insight
	 */
	public PolarsFrame(PyTranslator pyTranslator) {
		this(null, pyTranslator);
	}

	/**
	 * Creates an empty Polars frame.
	 *
	 * @param tableName    user-visible frame alias, or {@code null} to generate one
	 * @param pyTranslator translator owned by the containing Insight
	 */
	public PolarsFrame(String tableName, PyTranslator pyTranslator) {
		this.pyTranslator = pyTranslator;
		this.frameName = tableName == null || tableName.trim().isEmpty()
				? "POLARS_" + UUID.randomUUID().toString().replace("-", "_")
				: tableName;
		this.originalName = this.frameName;
		this.runtimeName = "_smss_polars_" + UUID.randomUUID().toString().replace("-", "_");
		initializeRuntime();
	}

	private void initializeRuntime() {
		try {
			this.pyTranslator.runEmptyPy("from semoss_polars import SemossPolarsFrame",
					this.runtimeName + " = SemossPolarsFrame()");
		} catch (RuntimeException e) {
			throw new IllegalStateException(
					"Unable to initialize a Polars frame. Verify py/install_config/.venv "
							+ "was synchronized with uv and contains polars.",
					e);
		}
	}

	/**
	 * @return generated Python identifier used only inside the Insight runtime
	 */
	public String getRuntimeName() {
		return this.runtimeName;
	}

	/**
	 * Replaces this frame's empty data with a clone of an existing
	 * {@code polars.DataFrame} variable.
	 *
	 * @param variableName valid Python identifier in the same Insight runtime
	 * @throws IllegalArgumentException if the name is not a Python identifier
	 */
	public void registerExistingVariable(String variableName) {
		if (variableName == null || !PYTHON_IDENTIFIER.matcher(variableName).matches()) {
			throw new IllegalArgumentException("Invalid Python variable name");
		}
		String script = "from semoss_polars import SemossPolarsFrame\n" + this.runtimeName
				+ " = SemossPolarsFrame(" + variableName + ".clone())";
		this.pyTranslator.runEmptyPy(script);
		recreateMeta();
	}

	/**
	 * Imports all remaining rows. CSV and parquet iterators use native readers;
	 * all other iterators are materialized through the typed row boundary.
	 *
	 * @param iterator source rows
	 */
	public void addRowsViaIterator(Iterator<IHeadersDataRow> iterator) {
		if (iterator instanceof CsvFileIterator) {
			importCsv((CsvFileIterator) iterator);
			return;
		}
		if (iterator instanceof ParquetFileIterator) {
			importParquet((ParquetFileIterator) iterator);
			return;
		}
		List<String> headers = this.metaData.getOrderedAliasOrUniqueNames();
		List<List<Object>> rows = new ArrayList<>();
		while (iterator.hasNext()) {
			Object[] values = iterator.next().getValues();
			List<Object> row = new ArrayList<>(values.length);
			for (Object value : values) {
				row.add(value);
			}
			rows.add(row);
		}

		Map<String, String> schema = new LinkedHashMap<>();
		Map<String, SemossDataType> typeMap = this.metaData.getHeaderToTypeMap();
		for (String header : headers) {
			String uniqueName = this.metaData.getUniqueNameFromAlias(header);
			SemossDataType type = typeMap.get(uniqueName == null ? header : uniqueName);
			schema.put(cleanColumn(header), SemossDataType.convertDataTypeToString(type));
		}
		List<String> cleanHeaders = new ArrayList<>(headers.size());
		for (String header : headers) {
			cleanHeaders.add(cleanColumn(header));
		}
		String args = pythonArgs(cleanHeaders, rows, schema);
		this.pyTranslator.runEmptyPy("from semoss_polars import SemossPolarsFrame\n" + this.runtimeName
				+ " = SemossPolarsFrame.from_rows(*__import__('json').loads(" + GSON.toJson(args) + "))");
		recreateMeta();
	}

	private void importCsv(CsvFileIterator iterator) {
		CsvQueryStruct queryStruct = iterator.getQs();
		String[] headers = iterator.getHeaders();
		List<String> inputColumns = new ArrayList<>();
		List<String> outputColumns = new ArrayList<>();
		Map<String, String> newHeaders = queryStruct.getNewHeaderNames();
		for (String header : headers) {
			outputColumns.add(header);
			String source = newHeaders == null ? null : newHeaders.get(header);
			inputColumns.add(source == null ? header : source);
		}
		Map<String, String> schema = new LinkedHashMap<>();
		if (queryStruct.getColumnTypes() != null) {
			for (Map.Entry<String, String> entry : queryStruct.getColumnTypes().entrySet()) {
				schema.put(entry.getKey(), entry.getValue());
			}
		}
		String args = pythonArgs(iterator.getFileLocation(), String.valueOf(queryStruct.getDelimiter()), inputColumns,
				outputColumns, queryStruct.getLimit(), schema);
		this.pyTranslator.runEmptyPy("from semoss_polars import SemossPolarsFrame\n" + this.runtimeName
				+ " = SemossPolarsFrame.import_csv(*__import__('json').loads(" + GSON.toJson(args) + "))");
		recreateMeta();
	}

	private void importParquet(ParquetFileIterator iterator) {
		ParquetQueryStruct queryStruct = iterator.getQs();
		String[] headers = iterator.getHeaders();
		List<String> inputColumns = new ArrayList<>();
		List<String> outputColumns = new ArrayList<>();
		Map<String, String> newHeaders = queryStruct.getNewHeaderNames();
		for (String header : headers) {
			outputColumns.add(header);
			String source = newHeaders == null ? null : newHeaders.get(header);
			inputColumns.add(source == null ? header : source);
		}
		String args = pythonArgs(iterator.getFileLocation(), inputColumns, outputColumns, queryStruct.getLimit());
		this.pyTranslator.runEmptyPy("from semoss_polars import SemossPolarsFrame\n" + this.runtimeName
				+ " = SemossPolarsFrame.import_parquet(*__import__('json').loads(" + GSON.toJson(args) + "))");
		recreateMeta();
	}

	@Override
	public IRawSelectWrapper query(SelectQueryStruct queryStruct) {
		queryStruct.getRelations().clear();
		SelectQueryStruct physicalQs = QSAliasToPhysicalConverter.getPhysicalQs(queryStruct, this.metaData);
		if (physicalQs.getPragmap() != null && physicalQs.getPragmap().containsKey("xCache")) {
			this.cache = Boolean.parseBoolean(String.valueOf(physicalQs.getPragmap().get("xCache")));
		}
		if (physicalQs.getPragmap() != null && physicalQs.getPragmap().containsKey("format")
				&& !"grid".equalsIgnoreCase(String.valueOf(physicalQs.getPragmap().get("format")))) {
			throw new IllegalArgumentException(
					"Polars currently supports grid query output only; requested format "
							+ physicalQs.getPragmap().get("format"));
		}

		PolarsQueryInterpreter interpreter = new PolarsQueryInterpreter();
		interpreter.setQueryStruct(physicalQs);
		interpreter.setFrameFilters(this.grf.copy());
		String plan = interpreter.composeQuery();

		if (this.cache && this.queryCache.containsKey(plan)) {
			RawCachedWrapper wrapper = new RawCachedWrapper();
			wrapper.setIterator(this.queryCache.get(plan));
			return wrapper;
		}

		Object output = this.pyTranslator.runDirectPy(
				this.runtimeName + ".query(__import__('json').loads(" + GSON.toJson(plan) + "))");
		if (!(output instanceof Map)) {
			throw new IllegalStateException("Polars query returned an unexpected result shape");
		}
		return createWrapper((Map<?, ?>) output, plan);
	}

	@Override
	public IRawSelectWrapper query(String query) {
		throw new IllegalArgumentException(
				"Raw query strings are unsupported for Polars frames; use SelectQueryStruct");
	}

	private IRawSelectWrapper createWrapper(Map<?, ?> output, String query) {
		List<?> columnList = (List<?>) output.get("columns");
		List<?> typeList = (List<?>) output.get("types");
		List<Object> data = (List<Object>) output.get("data");
		if (columnList == null || typeList == null || data == null || columnList.size() != typeList.size()) {
			throw new IllegalStateException("Polars query result is missing columns, types, or data");
		}

		String[] headers = new String[columnList.size()];
		SemossDataType[] types = new SemossDataType[typeList.size()];
		for (int i = 0; i < headers.length; i++) {
			headers[i] = String.valueOf(columnList.get(i));
			types[i] = SemossDataType.convertStringToDataType(String.valueOf(typeList.get(i)));
		}
		PolarsIterator iterator = new PolarsIterator(headers, data, types);
		iterator.setQuery(query);
		RawPolarsWrapper wrapper = new RawPolarsWrapper();
		wrapper.setIterator(iterator);
		if (!this.cache) {
			clearQueryCache();
		}
		return wrapper;
	}

	@Override
	public IQueryInterpreter getQueryInterpreter() {
		PolarsQueryInterpreter interpreter = new PolarsQueryInterpreter();
		interpreter.setFrameFilters(this.grf.copy());
		return interpreter;
	}

	@Override
	public long size(String tableName) {
		Number size = this.pyTranslator.getLong(this.runtimeName + ".data.height");
		return size.longValue();
	}

	@Override
	public boolean isEmpty() {
		return size(this.frameName) == 0;
	}

	@Override
	public CachePropFileFrameObject save(String folderDir, Cipher cipher) throws IOException {
		CachePropFileFrameObject cacheObject = new CachePropFileFrameObject();
		String framePath = folderDir + DIR_SEPARATOR + this.frameName + ".arrow";
		cacheObject.setFrameCacheLocation(framePath);
		runMethod("write_ipc", framePath);
		saveMeta(cacheObject, folderDir, this.frameName, cipher);
		return cacheObject;
	}

	@Override
	public void open(CachePropFileFrameObject cacheObject, Cipher cipher) {
		openCacheMeta(cacheObject, cipher);
		String path = cacheObject.getFrameCacheLocation().replace("\\", "/");
		this.pyTranslator.runEmptyPy("from semoss_polars import SemossPolarsFrame\n" + this.runtimeName
				+ " = SemossPolarsFrame.from_ipc(*__import__('json').loads("
				+ GSON.toJson(pythonArgs(path)) + "))");
		recreateMeta();
	}

	@Override
	public void close() {
		if (isClosed()) {
			return;
		}
		super.close();
		this.pyTranslator.runEmptyPy("if '" + this.runtimeName + "' in globals():\n    del " + this.runtimeName);
	}

	@Override
	public DataFrameTypeEnum getFrameType() {
		return DataFrameTypeEnum.POLARS;
	}

	@Override
	public String getDataMakerName() {
		return DATA_MAKER_NAME;
	}

	/**
	 * Rebuilds SEMOSS metadata from the authoritative Polars schema, cleaning and
	 * rebinding invalid headers when required. Query and metric caches are cleared.
	 */
	public void recreateMeta() {
		Object schemaOutput = this.pyTranslator.runDirectPy(this.runtimeName + ".schema()");
		if (!(schemaOutput instanceof Map)) {
			throw new IllegalStateException("Unable to read Polars frame schema");
		}
		Map<?, ?> schema = (Map<?, ?>) schemaOutput;
		List<?> rawColumns = (List<?>) schema.get("columns");
		List<?> rawTypes = (List<?>) schema.get("types");
		String[] columns = new String[rawColumns.size()];
		String[] types = new String[rawTypes.size()];
		for (int i = 0; i < columns.length; i++) {
			columns[i] = String.valueOf(rawColumns.get(i));
			types[i] = String.valueOf(rawTypes.get(i));
		}
		String[] cleanedColumns = HeadersException.getInstance().getCleanHeaders(columns);
		boolean columnsChanged = false;
		for (int i = 0; i < columns.length; i++) {
			if (!columns[i].equals(cleanedColumns[i])) {
				columnsChanged = true;
				break;
			}
		}
		if (columnsChanged) {
			runMethod("clean_columns", List.of(cleanedColumns));
			columns = cleanedColumns;
		}

		Map<String, String> additionalTypes = this.metaData.getHeaderToAdtlTypeMap();
		Map<String, List<String>> sources = this.metaData.getHeaderToSources();
		Map<String, String[]> complexSelectors = this.metaData.getComplexSelectorsMap();
		this.metaData.close();
		this.metaData = new OwlTemporalEngineMeta();
		ImportUtility.parseTableColumnsAndTypesToFlatTable(this.metaData, columns, types, this.frameName,
				additionalTypes, sources, complexSelectors);
		syncHeaders();
		clearCachedMetrics();
		clearQueryCache();
	}

	/**
	 * Renames one physical column and refreshes frame metadata.
	 *
	 * @param oldColumn current column name
	 * @param newColumn replacement column name
	 */
	public void renameColumn(String oldColumn, String newColumn) {
		runMutation("rename", cleanColumn(oldColumn), newColumn);
	}

	/**
	 * Drops physical columns and refreshes frame metadata.
	 *
	 * @param columns columns to drop
	 */
	public void dropColumns(List<String> columns) {
		List<String> cleaned = new ArrayList<>();
		for (String column : columns) {
			cleaned.add(cleanColumn(column));
		}
		runMutation("drop", cleaned);
	}

	/**
	 * Duplicates a physical column.
	 *
	 * @param source source column
	 * @param target new column
	 */
	public void duplicateColumn(String source, String target) {
		runMutation("duplicate", cleanColumn(source), target);
	}

	/**
	 * Strictly converts a column to a supported SEMOSS scalar type.
	 *
	 * @param column column to convert
	 * @param type   SEMOSS type name
	 */
	public void changeColumnType(String column, String type) {
		runMutation("cast", cleanColumn(column), type);
	}

	/**
	 * Applies a supported string operation to the selected columns.
	 *
	 * @param columns   columns to transform
	 * @param operation {@code trim}, {@code upper}, or {@code lower}
	 */
	public void stringTransform(List<String> columns, String operation) {
		List<String> cleaned = new ArrayList<>();
		for (String column : columns) {
			cleaned.add(cleanColumn(column));
		}
		runMutation("string_transform", cleaned, operation);
	}

	/**
	 * Replaces values in one column.
	 *
	 * @param column   target column
	 * @param oldValue value to replace
	 * @param newValue replacement value
	 * @param regex    whether to use the string replacement path
	 */
	public void replaceValue(String column, Object oldValue, Object newValue, boolean regex) {
		runMutation("replace", cleanColumn(column), oldValue, newValue, regex);
	}

	/**
	 * Deletes rows matching a structural filter plan.
	 *
	 * @param filterPlan non-empty filter plan
	 */
	public void dropRows(Map<String, Object> filterPlan) {
		runMutation("drop_rows", filterPlan);
	}

	/**
	 * Updates one column in rows matching a structural filter plan.
	 *
	 * @param filterPlan non-empty filter plan
	 * @param column     column to update
	 * @param value      replacement value
	 */
	public void updateRows(Map<String, Object> filterPlan, String column, Object value) {
		runMutation("update_rows", filterPlan, cleanColumn(column), value);
	}

	/**
	 * Appends another Polars frame using strict schema alignment.
	 *
	 * @param other    frame in the same Python runtime
	 * @param distinct whether to remove duplicate rows
	 */
	public void unionWith(PolarsFrame other, boolean distinct) {
		runMutation("union", other.runtimeName, distinct);
	}

	/**
	 * Joins typed rows into this frame using equality keys.
	 *
	 * @param headers source headers
	 * @param rows    source rows
	 * @param schema  source SEMOSS types by header
	 * @param leftOn  left join columns
	 * @param rightOn right join columns
	 * @param how     Polars join type
	 */
	public void mergeRows(List<String> headers, List<List<Object>> rows, Map<String, String> schema,
			List<String> leftOn, List<String> rightOn, String how) {
		runMutation("merge_rows", headers, rows, schema, leftOn, rightOn, how);
	}

	@Override
	public void addRow(Object[] cleanCells, String[] headers) {
		List<List<Object>> rows = new ArrayList<>();
		List<Object> row = new ArrayList<>();
		for (Object value : cleanCells) {
			row.add(value);
		}
		rows.add(row);
		runMutation("append_rows", rows);
	}

	@Override
	public void removeColumn(String columnHeader) {
		dropColumns(List.of(columnHeader));
	}

	@Override
	public void processDataMakerComponent(DataMakerComponent component) {
		// Legacy playsheet components do not mutate Python-backed frames directly.
	}

	@Override
	public String createVarFrame() {
		String variableName = "_smss_polars_var_" + UUID.randomUUID().toString().replace("-", "_");
		this.pyTranslator.runEmptyPy(variableName + " = " + this.runtimeName + ".data.clone()");
		return variableName;
	}

	private void runMutation(String method, Object... args) {
		runMethod(method, args);
		recreateMeta();
		updateDataId();
	}

	private Object runMethod(String method, Object... args) {
		if ("union".equals(method)) {
			String otherRuntimeName = String.valueOf(args[0]);
			if (!PYTHON_IDENTIFIER.matcher(otherRuntimeName).matches()) {
				throw new IllegalArgumentException("Invalid Polars runtime identifier");
			}
			return this.pyTranslator.runScript(this.runtimeName + ".union(" + otherRuntimeName + ", "
					+ Boolean.TRUE.equals(args[1]) + ")");
		}
		String jsonArgs = pythonArgs(args);
		return this.pyTranslator.runScript(this.runtimeName + "." + method
				+ "(*__import__('json').loads(" + GSON.toJson(jsonArgs) + "))");
	}

	private static String pythonArgs(Object... args) {
		return GSON.toJson(args);
	}

	private static String cleanColumn(String column) {
		if (column != null && column.contains("__")) {
			return column.split("__", 2)[1];
		}
		return column;
	}
}
