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
package prerna.reactor.frame.py;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.py.PandasFrame;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.ds.py.PyTranslator;
import prerna.om.IStringExportProcessor;
import prerna.query.interpreters.PandasInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.reactor.task.TaskBuilderReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.ConstantDataTask;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.om.task.TaskUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class CollectPivotReactor extends TaskBuilderReactor {

	/**
	 * This class is responsible for collecting data from a task and returning it
	 */

	// need to see this
	// https://stackoverflow.com/questions/46220167/add-columns-to-pivot-table-with-pandas

	private static final Logger classLogger = LogManager.getLogger(CollectPivotReactor.class);

	private static final String NEW_LINE = "\n";

	// python variable the generated pivot script accumulates its JSON output into
	private static final String PIVOT_OUT = "_pivot_out";
	public static final String ALL_SECTIONS = "**ALL_SECTIONS**";
	int row_max = -1;
	int col_max = -1;

	public CollectPivotReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ROW_GROUPS.getKey(), ReactorKeysEnum.COLUMNS.getKey(),
				ReactorKeysEnum.VALUES.getKey(), ReactorKeysEnum.SUBTOTALS.getKey(), "json", "margins", "sections",
				"optional" };
	}

	@Override
	public NounMetadata execute() {
		// TODO: DOING THIS BECAUSE WE NEED THE QS TO ALWAYS BE DISTINCT FALSE
		// TODO: ADDING UNTIL WE CAN HAVE FE BE EXPLICIT
		// always ensure the task is distinct false
		// as long as this is made through FE
		// the task iterator hasn't been executed yet
		this.task = getTask();
		SelectQueryStruct qs = null;
		ITableDataFrame queryFrame = null;
		if (this.task instanceof BasicIteratorTask) {
			qs = ((BasicIteratorTask) this.task).getQueryStruct();
			qs.setDistinct(false);
			queryFrame = qs.getFrame();
		}

		PyTranslator pyt = insight.getPyTranslator();
		if (pyt == null) {
			return getError("Pivot requires Python. Python is not enabled in this instance");
		}

		// this is the payload that is coming
		// Frame ( frame = [ FRAME890385 ] ) | Select ( Genre , Studio, MovieBudget )
		// .as ( [ Genre , Studio, MovieBudget ] ) | CollectPivot( rowGroups=["Genre"],
		// columns=["Studio"], values=["sum(MovieBudget)"] ) ;
		// pandas format is - pd.pivot_table(mv, index=['Genre', 'Nominated'],
		// values=['MovieBudget', 'RevenueDomestic'], aggfunc={'MovieBudget':np.sum,
		// 'RevenueDomestic':np.mean}, columns='Studio')

		// I need to convert the values into aggregate functions
		// I need to change this check later

		String pivotFrameName = Utility.getRandomString(6);
		String makePivotFrame = null;
		String outputFile = null;
		if (task instanceof BasicIteratorTask && queryFrame instanceof PandasFrame) {
			PandasInterpreter interp = new PandasInterpreter();
			PandasFrame frame = (PandasFrame) queryFrame;
			interp.setDataTableName(frame.getName(), frame.getWrapperName() + ".cache['data']");
			interp.setDataTypeMap(frame.getMetaData().getHeaderToTypeMap());
			interp.setPyTranslator(pyt);

			// Convert the alias-based query struct to physical so each frame column
			// resolves to its real name (the same conversion the Seaborn collect does).
			// Without it, every frame column reports the PRIM_KEY_PLACEHOLDER sentinel, so
			// the generated column-rename dict collides on that duplicate key and drops a
			// column. getPhysicalQs returns a copy, so the task's original qs (used later
			// for the view options) is untouched.
			SelectQueryStruct frameQs = QSAliasToPhysicalConverter.getPhysicalQs(qs, frame.getMetaData());

			// pd.pivot_table does the aggregation, so the frame query must return RAW rows.
			// Swap any aggregate selector (e.g. Count(CHOL)) for its underlying column so
			// composeQuery does not aggregate - otherwise, with no explicit GROUP BY, it
			// collapses every row into a single group and drops the row/column dimensions.
			List<IQuerySelector> rawSelectors = new ArrayList<>();
			for (IQuerySelector sel : frameQs.getSelectors()) {
				if (sel.getSelectorType() == IQuerySelector.SELECTOR_TYPE.FUNCTION
						&& !((QueryFunctionSelector) sel).getInnerSelector().isEmpty()) {
					rawSelectors.add(((QueryFunctionSelector) sel).getInnerSelector().get(0));
				} else {
					rawSelectors.add(sel);
				}
			}
			frameQs.setSelectors(rawSelectors);

			interp.setQueryStruct(frameQs);
			String frameQuery = interp.composeQuery();

			// aaw8Ciq = mvw.cache['data'][['Genre', 'Nominated',
			// 'MovieBudget']].drop_duplicates().iloc[0:].to_dict('split')
			if (qs.isDistinct()) {
				frameQuery = frameQuery.replace(".drop_duplicates().iloc[0:].to_dict('split')", "");
			} else {
				frameQuery = frameQuery.replace(".iloc[0:].to_dict('split')", "");
			}

			makePivotFrame = pivotFrameName + " = " + frameQuery;
		} else {
			Map<String, SemossDataType> typesMap = TaskUtility.getTypesMapFromTask(this.task);

			String fileName = Utility.getRandomString(6);
			String dir = (insight.getUserFolder() + "/Temp").replace('\\', '/');
			File tempDir = new File(dir);
			if (!tempDir.exists()) {
				tempDir.mkdirs();
			}
			outputFile = dir + "/" + fileName + ".csv";
			Utility.writeResultToFile(outputFile, this.task, typesMap, ",", new IStringExportProcessor() {
				// we need to replace all inner quotes with ""
				@Override
				public String processString(String input) {
					return input.replace("\"", "\"\"");
				}
			});

			String importPandasS = new StringBuilder(PandasFrame.PANDAS_IMPORT_STRING).toString();
			String importNumpyS = new StringBuilder(PandasFrame.NUMPY_IMPORT_STRING).toString();
			pyt.runEmptyPy(importPandasS, importNumpyS);

			// generate the script
			makePivotFrame = PandasSyntaxHelper.getCsvFileRead(PandasFrame.PANDAS_IMPORT_VAR,
					PandasFrame.NUMPY_IMPORT_VAR, outputFile, pivotFrameName, ",", "\"", "\\\\", null, typesMap, -1);
		}

		// so this is going to come in as vectors
		List<String> rowGroups = this.store.getGenRowStruct(keysToGet[0]).getAllStrValues();
		List<String> colGroups = this.store.getGenRowStruct(keysToGet[1]).getAllStrValues();
		List<String> values = this.store.getGenRowStruct(keysToGet[2]).getAllStrValues();
		List<String> optional = null;

		boolean json = false;

		if (this.store.getNounKeys().contains("json")) {
			json = this.store.getGenRowStruct(keysToGet[4]).get(0).toString().equalsIgnoreCase("true");
		}
		List<String> sections = null;

		if (this.store.getNounKeys().contains(keysToGet[6])) {
			sections = this.store.getGenRowStruct(keysToGet[6]).getAllStrValues();
		}
		if (this.store.getNounKeys().contains(keysToGet[7])) {
			optional = this.store.getGenRowStruct(keysToGet[7]).getAllStrValues();
		}

		List<String> newValues = new ArrayList<>();
		List<String> functions = new ArrayList<>();

		// lastly the values
		// need to create a pivot map for the FE
		Map<String, Object> pivotMap = new HashMap<>();
		pivotMap.put(keysToGet[0], rowGroups);
		pivotMap.put(keysToGet[1], colGroups);
		pivotMap.put(keysToGet[6], sections);
		pivotMap.put(keysToGet[7], optional);

		List<Map<String, String>> valuesList = new ArrayList<>();

		for (int valIndex = 0; valIndex < values.size(); valIndex++) {
			Map<String, String> valueMap = new HashMap<>();
			String curValue = values.get(valIndex);

			// get the operator and selector
			if (curValue.contains("(")) {
				String operator = curValue.substring(0, curValue.indexOf("(")).trim();
				String operand = curValue.substring(curValue.indexOf("(") + 1, curValue.length() - 1).trim();
				newValues.add(operand);
				functions.add(operator);
				// pass back the original operator before converting
				valueMap.put("math", operator);
				valueMap.put("alias", operand);
				valuesList.add(valueMap);
			} else {
				newValues.add(curValue);
				valueMap.put("math", "");
				valueMap.put("alias", curValue);
				valuesList.add(valueMap);
			}
		}

		// make the frame
		// we have to do this do that we can determine the proper limits
		pyt.runEmptyPy(makePivotFrame);

		String commands = null;
		List<String> pivotNames = new ArrayList<>();
		if (sections == null) {
			sections = new ArrayList<>();
			sections.add(ALL_SECTIONS);
			commands = genSections(sections.get(0), sections, "", pivotFrameName, rowGroups, colGroups, newValues,
					functions, json, pivotNames);

		} else {
			String sectionColumnName = sections.get(0);

			// get the values of the section and pass it in
			// mv[['Genre']].drop_duplicates().to_dict('list')
			String sectionNames = pivotFrameName + "[['" + sections.get(0) + "']].drop_duplicates().to_dict('list')";
			Map<String, Object> nameToList = (Map<String, Object>) pyt.runDirectPy(sectionNames);
			Object objList = nameToList.get(sectionColumnName);
			List<String> allSections = new ArrayList<>();
			if (objList instanceof List) {
				for (int itemIndex = 0; itemIndex < ((List<Object>) objList).size(); itemIndex++) {
					allSections.add(((List<Object>) objList).get(itemIndex) + "");
				}
			} else if (objList instanceof String) {
				allSections = new ArrayList<>();
				allSections.add((String) objList);
			}
			String quote = getQuote(sectionColumnName);

			if (allSections != null && allSections.size() > 0) {
				commands = genSections(sections.get(0), allSections, quote, pivotFrameName, rowGroups, colGroups,
						newValues, functions, json, pivotNames);
			}
		}

		pivotMap.put(keysToGet[2], valuesList);

		// genSections builds 'commands' so its final statement evaluates to the
		// _pivot_out
		// string (the full pivot JSON). The python transport deserializes that JSON
		// back
		// into Java collections, so pivotResultToJson re-serializes it for the view.
		String jsonOutput = pivotResultToJson(pyt.runDirectPy(commands));

		/*** check to see if the pivot is within limits **/
		NounMetadata pivotCheck = checkPivotLimits(pivotFrameName, colGroups, pivotNames);

		// if a file was made delete it
		if (outputFile != null) {
			File outputF = new File(outputFile);
			outputF.delete();
		}
		// check if pivot has any validation message
		if (pivotCheck != null) {
			return pivotCheck;
		}
		ConstantDataTask cdt = new ConstantDataTask();
		// need to do all the sets
		cdt.setFormat("TABLE");
		cdt.setTaskOptions(task.getTaskOptions());
		// Derive header metadata from the query struct's selectors rather than
		// task.getHeaderInfo(), which would lazily iterate the pivot's source task.
		// That task selects an aggregate with no GROUP BY and cannot be iterated
		// normally (pd.pivot_table does the aggregation on the raw rows instead).
		// qs is null only for the non-PandasFrame CSV path, whose task is a normal
		// iterable task.
		List<Map<String, Object>> pivotHeaderInfo = (qs != null) ? qs.getHeaderInfo() : task.getHeaderInfo();
		cdt.setHeaderInfo(new ArrayList<>(pivotHeaderInfo));
		// return the correct header info with the wrapped around math that is used on
		// the column
		for (Map<String, Object> header : cdt.getHeaderInfo()) {
			String alias = (String) header.get("alias");
			for (Map<String, String> value : valuesList) {
				if (value.get("math") == null || value.get("math").isEmpty()) {
					continue;
				}
				if (alias != null && alias.equals(value.get("alias"))) {
					header.put("calculatedBy", alias);
					header.put("math", value.get("math"));
					header.put("derived", true);
				}
			}
		}
		cdt.setSortInfo(new ArrayList<>(task.getSortInfo()));
		cdt.setId(task.getId());
		Map<String, Object> formatMap = new HashMap<>();
		formatMap.put("type", "TABLE");
		cdt.setFormatMap(formatMap);

		// set the output so it can give it
		Map<String, Object> outputMap = new HashMap<>();
		outputMap.put("headers", new String[] {});
		outputMap.put("rawHeaders", new String[] {});
		outputMap.put("values", new String[] { jsonOutput });
		outputMap.put("pivotData", pivotMap);
		cdt.setOutputData(outputMap);

		// need to set the task options
		// hopefully this is the current one I am working with
		if (this.task.getTaskOptions() != null) {
			// I really hope this is only one
			Iterator<String> panelIds = task.getTaskOptions().getPanelIds().iterator();
			while (panelIds.hasNext()) {
				String panelId = panelIds.next();
				// store the noun store as well for refreshing
				task.getTaskOptions().setCollectStore(this.store);
				this.insight.setFinalViewOptions(panelId, qs, task.getTaskOptions(), task.getFormatter());
			}
		}

		// close the original task
		try {
			this.task.close();
		} catch (IOException e) {
			classLogger.error("Failed to close source task after collecting pivot output.", e);
		}

		return new NounMetadata(cdt, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
	}

	private NounMetadata checkPivotLimits(String frameName, List<String> colGroups, List<String> pivotTableNames) {
		getPivotLimits();
		// preparing the delete script
		StringBuffer totalSize = new StringBuffer();
		StringBuffer pivotDeleteScript = new StringBuffer();
		long rowCount = 0;
		// calculating pivot row count by iterating the generated pivot pandas
		for (int i = 0; i < pivotTableNames.size(); i++) {
			if (i > 0) {
				pivotDeleteScript.append(",");
				totalSize.append(" + ");
			}
			pivotDeleteScript.append(pivotTableNames.get(i));
			totalSize.append(pivotTableNames.get(i)).append(".shape[0]");
		}
		try {
			rowCount = this.insight.getPyTranslator().getLong(totalSize.toString());
			classLogger.info("Pivot Table Row Count:::{}", rowCount);

			if (rowCount > row_max) {
				return getError("Max number of rows allowed : " + row_max + ". This pivot has " + rowCount
						+ ". Please filter and try again");
			}
			long colCount = getCount(frameName, colGroups);
			if (colCount > col_max) {
				return getError("Max number of columns allowed : " + col_max + ". This pivot has " + colCount
						+ ". Please filter and try again");
			}
		} finally {
			// deleting the pivots
			this.insight.getPyTranslator().runScript("del(" + pivotDeleteScript + ")");
		}

		return null;
	}

	private void getPivotLimits() {
		if (row_max < 0 || col_max < 0) {
			if (Utility.getDIHelperProperty(Constants.PIVOT_ROW_MAX) != null) {
				row_max = Integer.parseInt(Utility.getDIHelperProperty(Constants.PIVOT_ROW_MAX));
			} else {
				row_max = 1000;
			}
			if (Utility.getDIHelperProperty(Constants.PIVOT_COL_MAX) != null) {
				col_max = Integer.parseInt(Utility.getDIHelperProperty(Constants.PIVOT_COL_MAX));
			} else {
				col_max = 100;
			}
		}
	}

	public void setTask(ITask task) {
		this.task = task;
	}

	// based on data type suggests if we need to add the ' or not
	private String getQuote(String columnName) {
		String quote = "'";
		// use the query struct's header metadata rather than task.getHeaderInfo(),
		// which would iterate the pivot's source task (an aggregate with no GROUP BY
		// that cannot be iterated normally). qs-level metadata is enough to know the
		// column's type.
		List<Map<String, Object>> headersInfo = (task instanceof BasicIteratorTask)
				? ((BasicIteratorTask) task).getQueryStruct().getHeaderInfo()
				: task.getHeaderInfo();
		for (Map<String, Object> headerMap : headersInfo) {
			String name = (String) headerMap.get("alias");
			SemossDataType type = SemossDataType.convertStringToDataType(headerMap.get("type").toString());
			if (name.equalsIgnoreCase(columnName)) {
				if (type == SemossDataType.INT || type == SemossDataType.DOUBLE) {
					quote = "";
				}
				break;
			}
		}
		return quote;
	}

	@Override
	public List<NounMetadata> getOutputs() {
		List<NounMetadata> outputs = super.getOutputs();
		if (outputs != null && !outputs.isEmpty()) {
			return outputs;
		}

		outputs = new ArrayList<>();
		NounMetadata output = new NounMetadata(this.signature, PixelDataType.FORMATTED_DATA_SET,
				PixelOperationType.TASK_DATA);
		outputs.add(output);
		return outputs;
	}

	@Override
	protected void buildTask() {
		// do nothing

	}

	private String pivotResultToJson(Object pyResult) {
		if (pyResult == null) {
			return "";
		}
		if (pyResult instanceof String) {
			return (String) pyResult;
		}
		// The python transport auto-deserializes a JSON response into Java collections
		// (List/Map), so calling toString() on it would not be valid JSON. Re-serialize
		// it back to real JSON for the pivot view (values[0] is JSON.parsed on the FE).
		if (pyResult instanceof java.util.Collection) {
			return new JSONArray((java.util.Collection<?>) pyResult).toString();
		}
		if (pyResult instanceof java.util.Map) {
			return new JSONObject((java.util.Map<?, ?>) pyResult).toString();
		}
		return pyResult + "";
	}

	public String genSections(String sectionName, List<String> sections, String quote, String frameName,
			List<String> rows, List<String> columns, List<String> values, List<String> functions, boolean json,
			List<String> pivotNames) {
		// mv[['Genre']].drop_duplicates().to_dict('list') - Values of this list is an
		// array and for every array .. pass that as a filter
		// mv[['Genre']].drop_duplicates().to_dict('list')
		// filtered as - mv[mv['Genre']=='Drama']

		StringBuilder allSections = new StringBuilder("");
		StringBuilder deleteSectionFrames = new StringBuilder("del(");
		StringBuilder sectionBlock = new StringBuilder("[");

		// get the sections
		// need to find a way to pass the pivot and other things
		if (!sections.get(0).equalsIgnoreCase(ALL_SECTIONS)) {
			for (int sectionIndex = 0; sectionIndex < sections.size(); sectionIndex++) {
				if (sectionIndex != 0) {
					allSections.append(PIVOT_OUT).append(" += ', '").append(NEW_LINE);
					deleteSectionFrames.append(", ");
					sectionBlock.append(", ");
				}
				Object thisSectionValue = sections.get(sectionIndex).toString();
				String sectionSpecificFrame = Utility.getRandomString(5);
				allSections.append(sectionSpecificFrame).append(" = ").append(frameName).append("[").append(frameName)
						.append("['").append(sectionName).append("'] == ").append(quote).append(thisSectionValue)
						.append(quote).append("]").append(NEW_LINE)
						.append(genPivot(sectionSpecificFrame, rows, columns, values, functions, json, pivotNames))
						.append(NEW_LINE);
				deleteSectionFrames.append(sectionSpecificFrame);
				sectionBlock.append("\\\"").append(thisSectionValue).append("\\\"");
			}
			sectionBlock.append("], ");
			deleteSectionFrames.append(")");
			allSections = new StringBuilder(PIVOT_OUT).append(" = '['").append(NEW_LINE).append(PIVOT_OUT)
					.append(" += \"").append(sectionBlock).append("\"").append(NEW_LINE).append(PIVOT_OUT)
					.append(" += '['").append(NEW_LINE).append(allSections).append(PIVOT_OUT).append(" += ']'")
					.append(NEW_LINE).append(PIVOT_OUT).append(" += ']'").append(NEW_LINE);
			// delete the section frames now that their pivot json has been captured
			allSections.append(deleteSectionFrames).append(NEW_LINE);
		} else {
			sectionBlock.append("\\\"").append(ALL_SECTIONS).append("\\\"").append("], ");
			allSections.append(genPivot(frameName, rows, columns, values, functions, json, pivotNames))
					.append(NEW_LINE);
			allSections = new StringBuilder(PIVOT_OUT).append(" = '['").append(NEW_LINE).append(PIVOT_OUT)
					.append(" += \"").append(sectionBlock).append("\"").append(NEW_LINE).append(PIVOT_OUT)
					.append(" += '['").append(NEW_LINE).append(allSections).append(PIVOT_OUT).append(" += ']'")
					.append(NEW_LINE).append(PIVOT_OUT).append(" += ']'").append(NEW_LINE);
		}

		// the final statement is the assembled JSON string; runDirectPy returns it
		allSections.append(PIVOT_OUT).append(NEW_LINE);
		return allSections.toString();
	}

	public String genPivot(String frameName, List<String> rows, List<String> columns, List<String> values,
			List<String> functions, boolean json, List<String> pivotNames) {
		StringBuilder retString = new StringBuilder();
		// pd.pivot_table(df, values='D', index=['A', 'B'],
		// columns=['C'], aggfunc=np.sum, fill_value=0)

		// generate the index string
		StringBuilder idxString = new StringBuilder("");

		List<String> rowsAndColumns = new ArrayList<>();
		rowsAndColumns.addAll(rows);

		// take care of the column order
		StringBuilder column_order = new StringBuilder("[");
		for (int valIndex = 0; valIndex < values.size(); valIndex++) {
			if (valIndex > 0) {
				column_order.append(", ");
			}
			column_order.append("'").append(values.get(valIndex)).append("'");
		}
		column_order.append("]");

		// generate rows
		for (int idxIndex = 0; idxIndex < rowsAndColumns.size(); idxIndex++) {
			if (idxIndex != 0) {
				idxString.append(", ");
			}

			idxString.append("'").append(rowsAndColumns.get(idxIndex)).append("'");
		}

		if (idxString.length() > 0) {
			idxString = new StringBuilder("index = [").append(idxString).append("], ");
		}

		// generate the column string
		StringBuilder colString = new StringBuilder("");
		if (columns != null) {
			for (int colIndex = 0; colIndex < columns.size(); colIndex++) {
				if (colIndex != 0) {
					colString.append(", ");
				}
				colString.append("'").append(columns.get(colIndex)).append("'");
			}

			if (colString.length() > 0) {
				colString = new StringBuilder("columns = [").append(colString).append("], ");
			}
		}

		// generate agg functions
		// should be the same size as the values
		StringBuilder funString = new StringBuilder("");
		for (int funIndex = 0; funIndex < functions.size(); funIndex++) {
			// following functions are available
			// np.sum, np.mean, min, max, count, numpy.size, pd.Series.nunique
			String fun = functions.get(funIndex);
			String value = values.get(funIndex);

			fun = QueryFunctionHelper.convertFunctionToPandasSyntax(fun);
			if (funIndex != 0) {
				funString.append(", ");
			}
			funString.append("'").append(value).append("' : ");
			funString.append("'").append(fun).append("'");

		}

		if (funString.length() > 0) {
			funString = new StringBuilder("aggfunc = {").append(funString).append("}, ");
		}
		// generate the values string
		StringBuilder pdValuesString = new StringBuilder("");
		for (int valIndex = 0; valIndex < values.size(); valIndex++) {
			if (valIndex != 0) {
				pdValuesString.append(", ");
			}
			pdValuesString.append("'").append(values.get(valIndex)).append("'");
		}
		if (pdValuesString.length() > 0) {
			pdValuesString = new StringBuilder("values = [").append(pdValuesString).append("], ");
		}

		// handle drop na
		// handle fillvalues

		// create the pivot
		// generate the pivot first
		String labelsCheat = "zzzzpp";
		if (rows.size() == 1) {
			labelsCheat = "Row Total";
		}

		String marginValue = "False";

		StringBuilder pivotString = new StringBuilder("");
		String pivotName = Utility.getRandomString(5);
		pivotString.append(pivotName).append(" = ").append("pd.pivot_table(").append(frameName).append(",")
				.append(pdValuesString).append(colString).append(idxString).append(funString).append("dropna=True,")
				.append("margins=").append(marginValue).append(", margins_name='" + labelsCheat + "'")
				.append(").fillna('')");

		classLogger.info("{}", pivotString);
		// append the formatter to start.. need a better way for this.. but for now
		// 2 decimal places
		retString.append("pd.set_option('display.float_format', lambda x: '%.2f' % x)").append(NEW_LINE);
		retString.append(pivotString).append(NEW_LINE);

		String outputFormat = ".to_html()";
		if (json) {
			outputFormat = ".to_json(orient='split')";
		}

		retString.append(PIVOT_OUT).append(" += ").append(pivotName).append("[").append(column_order).append("]")
				.append(outputFormat).append(NEW_LINE);
		// storing the pivot name for future to get the pivot row count
		pivotNames.add(pivotName);

		classLogger.debug("{}", retString);
		return retString.toString();
	}

	private long getCount(String frameName, List<String> items) {
		long retCount = 1;
		for (int itemIndex = 0; itemIndex < items.size(); itemIndex++) {
			StringBuilder sb = new StringBuilder(frameName).append("['").append(items.get(itemIndex))
					.append("'].nunique()");
			long count = this.insight.getPyTranslator().getLong(sb.toString());
			retCount = retCount * count;
		}
		return retCount;
	}

	public static String getJson2HTML(JSONObject mainObj, List<String> rows) {
		String[] values = null;

		// each record is a combination of this
		JSONArray colArray = mainObj.getJSONArray("columns");
		values = new String[colArray.length()];

		// get the index
		JSONArray index = mainObj.getJSONArray("index");
		StringWriter outputString = new StringWriter();

		// the data
		// process the row
		JSONArray data = mainObj.getJSONArray("data");
		double[][] dataArray = new double[data.length()][colArray.length()];

		for (int dataIndex = 0; dataIndex < data.length(); dataIndex++) {
			JSONArray record = data.getJSONArray(dataIndex);
			for (int recIndex = 0; recIndex < record.length(); recIndex++) {
				if (record.get(recIndex) instanceof Double) {
					dataArray[dataIndex][recIndex] = record.getDouble(recIndex);
				} else {
					dataArray[dataIndex][recIndex] = 0;
				}
			}
		}

		Map<String, Object> itemLevelColSpan = new HashMap<>();
		String[][] columns = null;
		String[][] columnsData = null;

		for (int columnIndex = 0; columnIndex < colArray.length(); columnIndex++) {
			// this is a multi level
			if (colArray.get(columnIndex) instanceof JSONArray) {
				JSONArray thisLevel = colArray.getJSONArray(columnIndex);
				if (columns == null) {
					columns = new String[thisLevel.length()][rows.size() + values.length + 1];
					columnsData = new String[thisLevel.length()][rows.size() + values.length + 1];
				}
				String parent = "";
				for (int colLevelIndex = 0; colLevelIndex < thisLevel.length(); colLevelIndex++) {
					String colName = thisLevel.getString(colLevelIndex);
					String key = colName + "__" + colLevelIndex;
					String location = colName + "__" + colLevelIndex + "__" + (columnIndex + rows.size());
					if (parent.length() > 0) {
						key = parent + ":" + key;
					}

					int colSpan = 1;
					if (itemLevelColSpan.containsKey(key)) {
						colSpan = (Integer) itemLevelColSpan.get(key);
						location = itemLevelColSpan.get(key + "__LOCATION") + "";
						colSpan++;
					} else {
						columns[colLevelIndex][rows.size() + columnIndex] = colName;
					}
					itemLevelColSpan.put(key, colSpan);
					itemLevelColSpan.put(key + "__LOCATION", location);
					itemLevelColSpan.put(location, colSpan);
					parent = key;
					columnsData[colLevelIndex][rows.size() + columnIndex] = colName;
				}
			}
			// this is a single level column
			else {
				if (columns == null) {
					columns = new String[1][rows.size() + values.length + 1];
					columnsData = new String[1][rows.size() + values.length + 1];
				}

				columns[0][rows.size() + columnIndex] = colArray.getString(columnIndex);
				columnsData[0][rows.size() + columnIndex] = colArray.getString(columnIndex);
			}
		}
		// add the sum column last
		columns[0][rows.size() + values.length] = "All Total";

		// walk the hash of hash and the items recursively
		String[][] rowDataArrayOutput = new String[index.length()][rows.size() + values.length + 1];
		// this keeps track of actual parent etc.
		// required when we print
		String[][] rowDataArray = new String[index.length()][rows.size() + values.length + 1];

		Map<String, Integer> itemLevelRowSpan = new HashMap<>();
		Map<String, double[]> itemLevelTotals = new HashMap<>();
		double[] allRowTotal = new double[values.length + 1];

		// filling in the rows
		for (int rowIndex = 0; rowIndex < index.length(); rowIndex++) {
			double summer = 0;
			String rowKey = null;
			if (index.get(rowIndex) instanceof JSONArray) {
				JSONArray singleRow = index.getJSONArray(rowIndex);
				String parent = "";
				rowKey = singleRow.getString(0);
				for (int rowItemIndex = 0; rowItemIndex < singleRow.length(); rowItemIndex++) {
					String thisItem = singleRow.getString(rowItemIndex);
					String key = thisItem + "__" + rowItemIndex;
					if (parent.length() > 0) {
						key = parent + ":" + key;
					}
					int span = 0;
					if (itemLevelRowSpan.containsKey(key)) {
						span = itemLevelRowSpan.get(key);
						rowDataArrayOutput[rowIndex][rowItemIndex] = "";
					} else {
						rowDataArrayOutput[rowIndex][rowItemIndex] = thisItem;
					}
					rowDataArray[rowIndex][rowItemIndex] = thisItem;
					span++;
					itemLevelRowSpan.put(key, span);
					parent = key;
					// need to check for number but..
				}
			} else {
				rowKey = index.getString(0);
				String thisItem = index.getString(rowIndex);
				rowDataArrayOutput[rowIndex][0] = thisItem;
			}

			// fill the data in parallel
			// plus 1 is for total
			double[] totals = new double[values.length + 1];
			for (int columnIndex = 0; columnIndex < values.length + 1; columnIndex++) {
				// fill the data
				String key = rowKey + "__" + columnIndex; // get the first level
				if (itemLevelTotals.containsKey(key)) {
					totals = itemLevelTotals.get(key);
				} else {
					totals[columnIndex] = 0;
				}

				if (columnIndex < values.length) {
					totals[columnIndex] += dataArray[rowIndex][columnIndex];
					allRowTotal[columnIndex] += dataArray[rowIndex][columnIndex];
					rowDataArrayOutput[rowIndex][rows.size() + columnIndex] = dataArray[rowIndex][columnIndex] + "";
					summer = summer + dataArray[rowIndex][columnIndex];
				} else {
					totals[columnIndex] += summer;
					allRowTotal[columnIndex] += summer;
				}
				itemLevelTotals.put(key, totals);
			}
			rowDataArrayOutput[rowIndex][rows.size() + values.length] = summer + "";
		}

		// generate html
		outputString.append("<table>");
		outputString.append("<thead>");

		String curLevelItem = null;
		boolean newItem = true;

		// columns first
		for (int trIndex = 0; trIndex < columns.length; trIndex++) {
			outputString.append("<tr>");
			String[] thisRow = columns[trIndex];
			String[] thisDataRow = columnsData[trIndex];
			String parent = "";
			for (int tdIndex = 0; tdIndex < rows.size(); tdIndex++) {
				outputString.append("<th></th>");
			}

			// need something that keeps the parent at this level as we process all of these
			// it has to be based on index
			for (int tdIndex = rows.size(); tdIndex < thisRow.length; tdIndex++) {
				String thisItem = thisRow[tdIndex];
				String dataItem = thisDataRow[tdIndex];
				String cardinalKey = dataItem + "__" + trIndex + "__" + tdIndex;
				String key = dataItem + "__" + trIndex;
				if (parent.length() > 0) {
					key = parent + ":" + key;
				}

				if (thisItem != null && thisItem.length() > 0) {
					outputString.append("<th style=\"width=200px;background-color:#F6F6F6;color:#1E1E1E;\"");
					int colSpan = 0;
					if (itemLevelColSpan.containsKey(cardinalKey)) {
						colSpan = (Integer) itemLevelColSpan.get(cardinalKey);
						if (newItem && tdIndex == 0) {
							// rowSpan++;
							newItem = false;
						}
						outputString.append(" colspan=" + colSpan + " >");
						tdIndex += (colSpan - 1); // account for the tdindex++
					} else {
						outputString.append(">");
					}
					outputString.append(thisItem);
					outputString.append("</th>");

				} else if (thisItem == null) {
				}
				parent = key;
			}

			outputString.append("<tr>");
		}

		// generate row headers next
		outputString.append("<tr>");
		for (int tdIndex = 0; tdIndex < rows.size(); tdIndex++) {
			outputString.append(
					"<th style=\"width:200px;background-color:#F6F6F6;color:#1E1E1E;\">" + rows.get(tdIndex) + "</th>");
		}
		// fill other tds
		for (int tdIndex = rows.size(); tdIndex < columns[0].length; tdIndex++) {
			// width:200px;background-color:#F6F6F6;color:#1E1E1E;
			outputString.append("<th style=\"width:200px;background-color:#F6F6F6;color:#1E1E1E;\"></th>");
		}

		outputString.append("</tr>");
		outputString.append("</thead>");
		outputString.append("<tbody>");

		// write the data
		for (int trIndex = 0; trIndex < rowDataArrayOutput.length; trIndex++) {
			String[] thisRow = rowDataArrayOutput[trIndex];
			String[] thisDataRow = rowDataArray[trIndex];
			String parent = "";
			outputString.append("<tr>");
			for (int tdIndex = 0; tdIndex < thisRow.length; tdIndex++) {
				String thisItem = thisRow[tdIndex];
				String dataItem = thisDataRow[tdIndex];
				String key = dataItem + "__" + tdIndex;
				if (parent.length() > 0) {
					key = parent + ":" + key;
				}
				// logic for doing totals
				if (tdIndex == 0 && curLevelItem != null && !dataItem.equalsIgnoreCase(curLevelItem)) {
					newItem = true;
					// add the total for this column
					outputString.append("<th style=\"background-color:#F6F6F6;color:#1E1E1E;\" colspan="
							+ (thisRow.length - (values.length + 1)) + ">");
					outputString.append(curLevelItem + " -- Total </th>");
					double[] totals = itemLevelTotals.get(curLevelItem + "__" + tdIndex);

					if (totals != null) {
						for (int totalIndex = 0; totalIndex < totals.length; totalIndex++) {
							outputString.append("<td style=\"font-weight:bold;\">" + totals[totalIndex] + "</td>");
						}
					}
					curLevelItem = dataItem;
					outputString.append("</tr><tr>");
				} else if (curLevelItem == null) {
					curLevelItem = dataItem;
				}

				if (thisItem != null && thisItem.length() > 0) {
					if (tdIndex < rows.size()) {
						outputString.append("<th style=\"background-color:#F6F6F6;color:#1E1E1E;\"");
					} else {
						outputString.append("<td");
					}
					int rowSpan = 0;
					if (itemLevelRowSpan.containsKey(key)) {
						rowSpan = itemLevelRowSpan.get(key);
						if (newItem && tdIndex == 0) {
							// rowSpan++;
							newItem = false;
						}
						outputString.append(" rowspan=" + rowSpan + " >");
					} else {
						outputString.append(">");
					}
					outputString.append(thisItem);
					if (tdIndex < rows.size()) {
						outputString.append("</th>");
					} else {
						outputString.append("</td>");
					}

				}
				parent = key;

			}
			outputString.append("</tr>");

		}

		// print out the last total
		if (curLevelItem != null) {
			outputString.append("<tr>");
			outputString.append("<th style=\"width:200px;background-color:#F6F6F6;color:#1E1E1E;\" colspan="
					+ (rowDataArrayOutput[0].length - (values.length + 1)) + ">");
			outputString.append(curLevelItem + " -- TOTAL </th>");
			double[] totals = itemLevelTotals.get(curLevelItem + "__" + 0);

			if (totals != null) {
				for (int totalIndex = 0; totalIndex < totals.length; totalIndex++) {
					outputString.append("<td style=\"font-weight:bold;\">" + totals[totalIndex] + "</td>");
				}
			}
			outputString.append("</tr>");
		}

		// finally the grand total
		outputString.append("<tr>");
		outputString.append("<th style=\"background-color:#F6F6F6;color:#1E1E1E;font-weight:bold;\" colspan="
				+ (rowDataArrayOutput[0].length - (values.length + 1)) + ">All Total</th>");

		for (int tdIndex = 0; tdIndex < allRowTotal.length; tdIndex++) {
			outputString.append("<td style=\"font-weight:bold;\">" + allRowTotal[tdIndex] + "</td>");
		}
		outputString.append("</tr>");

		outputString.append("</tbody>");
		outputString.append("</table>");

		return outputString.toString();
	}

}
