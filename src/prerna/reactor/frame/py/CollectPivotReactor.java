package prerna.reactor.frame.py;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

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
import prerna.query.querystruct.selectors.QueryFunctionHelper;
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
import prerna.util.DIHelper;
import prerna.util.Utility;

public class CollectPivotReactor extends TaskBuilderReactor {

	/**
	 * This class is responsible for collecting data from a task and returning it
	 */
	
	// need to see this https://stackoverflow.com/questions/46220167/add-columns-to-pivot-table-with-pandas
	
	private static final Logger classLogger = LogManager.getLogger(CollectPivotReactor.class);

	private static final String NEW_LINE = "\n";
	
	private static String curEncoding = null;
	public static final String ALL_SECTIONS = "**ALL_SECTIONS**";
	int row_max = -1;
	int col_max = -1;

	private static Map<String, String> mathMap = new HashMap<String, String>();
	static {
		mathMap.put("Sum", "sum");
		mathMap.put("Average", "mean");
		mathMap.put("Min", "min");
		mathMap.put("Max", "max");
		mathMap.put("Median", "median");
		mathMap.put("StandardDeviation", "std");
		mathMap.put("Count", "count");
	}

	public CollectPivotReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ROW_GROUPS.getKey(), ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.VALUES.getKey(),  ReactorKeysEnum.SUBTOTALS.getKey(), "json", "margins", "sections", "optional"};
	}

	public NounMetadata execute() {
		// TODO: DOING THIS BECAUSE WE NEED THE QS TO ALWAYS BE DISTINCT FALSE
		// TODO: ADDING UNTIL WE CAN HAVE FE BE EXPLICIT
		// always ensure the task is distinct false
		// as long as this is made through FE
		// the task iterator hasn't been executed yet
		this.task = getTask();
		SelectQueryStruct qs = null;
		ITableDataFrame queryFrame = null;
		if(this.task instanceof BasicIteratorTask) {
			qs = ((BasicIteratorTask) this.task).getQueryStruct();
			qs.setDistinct(false);
			queryFrame = qs.getFrame();
		}
		
		PyTranslator pyt = insight.getPyTranslator();
		if(pyt == null) {
			return getError("Pivot requires Python. Python is not enabled in this instance");
		}
		pyt.setLogger(this.getLogger(this.getClass().getName()));

		// this is the payload that is coming
		// Frame ( frame = [ FRAME890385 ] ) | Select ( Genre , Studio, MovieBudget ) .as ( [ Genre , Studio, MovieBudget ] ) | CollectPivot( rowGroups=["Genre"], columns=["Studio"], values=["sum(MovieBudget)"] ) ;
		// pandas format is - pd.pivot_table(mv, index=['Genre', 'Nominated'], values=['MovieBudget', 'RevenueDomestic'], aggfunc={'MovieBudget':np.sum, 'RevenueDomestic':np.mean}, columns='Studio')
		
		// I need to convert the values into aggregate functions
		// I need to change this check later

		String pivotFrameName = Utility.getRandomString(6);
		String makePivotFrame = null;
		String outputFile = null;
		if(task instanceof BasicIteratorTask && queryFrame instanceof PandasFrame) {
			PandasInterpreter interp = new PandasInterpreter();
			PandasFrame frame = (PandasFrame) queryFrame;
			interp.setDataTableName(frame.getName(), frame.getWrapperName()+ ".cache['data']");
			interp.setDataTypeMap(frame.getMetaData().getHeaderToTypeMap());
			interp.setQueryStruct(qs);
			interp.setKeyCache(frame.keyCache);
			// I should also possibly set up pytranslator so I can run command for creating filter
			interp.setPyTranslator(pyt);
			String frameQuery = interp.composeQuery();
			
			//aaw8Ciq = mvw.cache['data'][['Genre', 'Nominated', 'MovieBudget']].drop_duplicates().iloc[0:].to_dict('split')
			if(qs.isDistinct()) {
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
			if(!tempDir.exists()) {
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
			makePivotFrame = PandasSyntaxHelper.getCsvFileRead(PandasFrame.PANDAS_IMPORT_VAR, PandasFrame.NUMPY_IMPORT_VAR, 
					outputFile, pivotFrameName, ",", "\"", "\\\\", pyt.getCurEncoding(), typesMap);
		}
		
		// so this is going to come in as vectors
		List<String> rowGroups = this.store.getNoun(keysToGet[0]).getAllStrValues();
		List<String> colGroups = this.store.getNoun(keysToGet[1]).getAllStrValues();
		List<String> values = this.store.getNoun(keysToGet[2]).getAllStrValues();
		List<String> optional = null;
		
		List<String> subtotals = rowGroups;
		if(keyValue.containsKey(keysToGet[3])) {
			subtotals = this.store.getNoun(keysToGet[3]).getAllStrValues();
		}
		boolean json = false;
		boolean margins = true;

		if(this.store.getNounKeys().contains("json")) {
			json = this.store.getNoun(keysToGet[4]).get(0).toString().equalsIgnoreCase("true");
		}
		if(this.store.getNounKeys().contains("margins")) {
			margins = this.store.getNoun(keysToGet[5]).get(0).toString().equalsIgnoreCase("true");
		}
		List<String> sections = null;
		
		if(this.store.getNounKeys().contains(keysToGet[6])) {
			sections = this.store.getNoun(keysToGet[6]).getAllStrValues();
		}
		if(this.store.getNounKeys().contains(keysToGet[7])) {
			optional = this.store.getNoun(keysToGet[7]).getAllStrValues();
		}

		if(curEncoding == null) {
			curEncoding = pyt.getCurEncoding();
		}
		
		List <String> newValues = new Vector<String>();
		List <String> functions = new Vector<String>();

		// lastly the values
		// need to create a pivot map for the FE
		Map<String, Object> pivotMap = new HashMap<String, Object>();
		pivotMap.put(keysToGet[0], rowGroups);
		pivotMap.put(keysToGet[1], colGroups);
		pivotMap.put(keysToGet[6], sections);
		pivotMap.put(keysToGet[7], optional);

		List<Map<String, String>> valuesList = new Vector<Map<String, String>>();
		
		for(int valIndex = 0;valIndex < values.size();valIndex++)
		{
			Map<String, String> valueMap = new HashMap<String, String>();
			String curValue = values.get(valIndex);

			// get the operator and selector
			//String [] composite = curValue.split("(");
			if(curValue.contains("(")) {
				String operator = curValue.substring(0, curValue.indexOf("(")).trim();
				String operand = curValue.substring(curValue.indexOf("(") + 1, curValue.length()-1).trim();
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
		if(sections == null) {
			sections = new Vector<String>();
			sections.add(ALL_SECTIONS);
			commands = genSections(sections.get(0), sections, "", pivotFrameName, rowGroups, colGroups, 
					subtotals, newValues, functions, true, true, json, margins,pivotNames);
			
		} else {
			String sectionColumnName = sections.get(0);
			
			// get the values of the section and pass it in
			// mv[['Genre']].drop_duplicates().to_dict('list')	
			String sectionNames = pivotFrameName + "[['" + sections.get(0) + "']].drop_duplicates().to_dict('list')";
			HashMap nameToList = (HashMap) pyt.runScript(sectionNames);
			//makeFrame = ""; // null the make frame it has been made now
			Object objList = nameToList.get(sectionColumnName);
			List <String> allSections = new Vector<String>();
			if(objList instanceof List) {
				for(int itemIndex = 0;itemIndex < ((List)objList).size();itemIndex++) {
					allSections.add(((List) objList).get(itemIndex) + "");
				}
			}
			else if(objList instanceof String) {
				allSections = new ArrayList<String>();
				allSections.add((String)objList); 
			}
			String quote = getQuote(sectionColumnName);
			
			if(allSections != null && allSections.size() > 0) {
				commands = genSections(sections.get(0), allSections, quote, pivotFrameName, rowGroups, colGroups, subtotals, newValues, functions, true, true, json, margins,pivotNames);
			}
		}
		
		pivotMap.put(keysToGet[2], valuesList);
		String jsonOutput = pyt.runPyAndReturnOutput(commands); 
		
		/*** check to see if the pivot is within limits **/
		NounMetadata pivotCheck = checkPivotLimits(pivotFrameName, colGroups, pivotNames);
		
		//this.insight.getPyTranslator().runEmptyPy("del(" + frameName + ")");
		// if a file was made delete it
		if(outputFile != null) {
			File outputF = new File(outputFile);
			outputF.delete();
		}
		// check if pivot has any validation message
		if(pivotCheck != null) {
			return pivotCheck;
		}
		ConstantDataTask cdt = new ConstantDataTask();
		// need to do all the sets
		cdt.setFormat("TABLE");
		cdt.setTaskOptions(task.getTaskOptions());
		cdt.setHeaderInfo(new ArrayList<>(task.getHeaderInfo()));
		// return the correct header info with the wrapped around math that is used on the column
		for(Map<String, Object> header : cdt.getHeaderInfo()) {
			String alias = (String) header.get("alias");
			for(Map<String, String> value : valuesList) {
				if(value.get("math") == null || value.get("math").isEmpty()) {
					continue;
				}
				if(alias != null && alias.equals(value.get("alias"))) {
					header.put("calculatedBy", alias);
					header.put("math", value.get("math"));
					header.put("derived", true);
				}
			}
		}
		cdt.setSortInfo(new ArrayList<>(task.getSortInfo()));
		cdt.setId(task.getId());
		Map<String, Object> formatMap = new Hashtable<String, Object>();
		formatMap.put("type", "TABLE");
		cdt.setFormatMap(formatMap);
		
		// set the output so it can give it
		Map<String, Object> outputMap = new HashMap<String, Object>();
		outputMap.put("headers", new String[] {});
		outputMap.put("rawHeaders", new String[] {});
		outputMap.put("values", jsonOutput);
		outputMap.put("pivotData", pivotMap);
		cdt.setOutputData(outputMap);
		
		// need to set the task options
		// hopefully this is the current one I am working with
		if(this.task.getTaskOptions() != null) {
			// I really hope this is only one
			Iterator <String> panelIds = task.getTaskOptions().getPanelIds().iterator();
			while(panelIds.hasNext()) {
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
			classLogger.error(Constants.STACKTRACE, e);
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
			rowCount = (long) this.insight.getPyTranslator().runScript(totalSize.toString());
			System.out.println("Pivot Table Row Count:::" + rowCount);

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
			//deleting the pivots
			this.insight.getPyTranslator().runScript("del(" + pivotDeleteScript + ")");
		}

		return null;
	}
	
	private void getPivotLimits() {
		if(row_max < 0 || col_max < 0) {
			if(DIHelper.getInstance().getProperty(Constants.PIVOT_ROW_MAX) != null) {
				row_max = Integer.parseInt(DIHelper.getInstance().getProperty(Constants.PIVOT_ROW_MAX));
			} else {
				row_max = 1000;
			}
			if(DIHelper.getInstance().getProperty(Constants.PIVOT_COL_MAX) != null) {
				col_max = Integer.parseInt(DIHelper.getInstance().getProperty(Constants.PIVOT_COL_MAX));
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
		List<Map<String, Object>> headersInfo = task.getHeaderInfo();
		for (Map<String, Object> headerMap : headersInfo) 
		{
			String name = (String) headerMap.get("alias");
			SemossDataType type = SemossDataType.convertStringToDataType(headerMap.get("type").toString());
			if(name.equalsIgnoreCase(columnName))
			{
				if(type == SemossDataType.INT || type == SemossDataType.DOUBLE)
					quote = "";
				break;
			}
		}
		return quote;
	}

	@Override
	public List<NounMetadata> getOutputs() {
		List<NounMetadata> outputs = super.getOutputs();
		if(outputs != null && !outputs.isEmpty()) return outputs;

		outputs = new Vector<NounMetadata>();
		NounMetadata output = new NounMetadata(this.signature, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
		outputs.add(output);
		return outputs;
	}

	@Override
	protected void buildTask() {
		// do nothing
		
	}
	
	public String genSections(String sectionName, List<String> sections, String quote, String frameName, 
			List<String> rows, List<String> columns, List<String> subtotalColumns, List<String> values, 
			List<String> functions, boolean dropNA, boolean fill_value, boolean json, boolean margins, List<String> pivotNames)
	{
		// mv[['Genre']].drop_duplicates().to_dict('list') - Values of this list is an array and for every array .. pass that as a filter
		 // mv[['Genre']].drop_duplicates().to_dict('list')		
		// filtered as -  mv[mv['Genre']=='Drama']
		
		// need to accomodate when the section is not a string
		// start the main json array
		// TODO: need to send the sections too - once davy decides the format
		
		StringBuilder allSections = new StringBuilder("");
		StringBuilder deleteSectionFrames = new StringBuilder("del(");
		StringBuilder sectionBlock = new StringBuilder("[");

		// get the sections
		// need to find a way to pass the pivot and other things
		if(!sections.get(0).equalsIgnoreCase(ALL_SECTIONS)) {
			for(int sectionIndex = 0;sectionIndex < sections.size();sectionIndex++) {
				if(sectionIndex != 0) {
					allSections.append("print(', ')").append(NEW_LINE);
					deleteSectionFrames.append(", ");
					sectionBlock.append(", ");
				}
				Object thisSectionValue = sections.get(sectionIndex).toString();
				String sectionSpecificFrame = Utility.getRandomString(5);
				allSections.append(sectionSpecificFrame).append(" = ").append(frameName).append("[")
					.append(frameName).append("['").append(sectionName).append("'] == ")
					.append(quote).append(thisSectionValue).append(quote).append("]").append(NEW_LINE)
					.append(genPivot(sectionSpecificFrame, rows, columns, subtotalColumns, values, functions, dropNA, fill_value, json, margins, pivotNames))
					.append(NEW_LINE);		
				deleteSectionFrames.append(sectionSpecificFrame);
				sectionBlock.append("\\\"").append(thisSectionValue).append("\\\"");
			}
			sectionBlock.append("], ");
			deleteSectionFrames.append(")");
			allSections = new StringBuilder("print('[')").append(NEW_LINE)
					.append("print(\"").append(sectionBlock).append("\")").append(NEW_LINE)
					.append("print('[')").append(NEW_LINE).append(allSections).append("print(']')").append(NEW_LINE)
					.append("print(']')").append(NEW_LINE);
			// delete them
			allSections.append(deleteSectionFrames).append(NEW_LINE);
		} else {
			sectionBlock.append("\\\"").append(ALL_SECTIONS).append("\\\"").append("], ");
			allSections.append(genPivot(frameName, rows, columns, subtotalColumns, values, functions, dropNA, fill_value, json, margins, pivotNames)).append(NEW_LINE);		
			allSections = new StringBuilder("print('[')").append(NEW_LINE)
					.append("print(\"").append(sectionBlock).append("\")").append(NEW_LINE)
					.append("print('[')").append(NEW_LINE).append(allSections).append("print(']')").append(NEW_LINE)
					.append("print(']')").append(NEW_LINE);
		}
		// close the main json array
		//allSections.append("print(']')").append(NEW_LINE);
		
		return allSections.toString();
	}
	
	
	public String genPivot(String frameName, List <String> rows, List <String> columns, List <String> subtotalColumns, 
			List <String> values, List <String> functions, boolean dropNA, boolean fill_value, 
			boolean json, boolean margins,List<String>  pivotNames){
		StringBuilder retString = new StringBuilder();
		// pd.pivot_table(df, values='D', index=['A', 'B'],
        //columns=['C'], aggfunc=np.sum, fill_value=0)
		// this will never happen
		//if(functions.size() != values.size())
		//	return; // ciao
		
		
		// generate the index string
		StringBuilder idxString = new StringBuilder("");
		StringBuilder crosstabRows = new StringBuilder("");
		
		List <String> rowsAndColumns = new Vector<String>();
		rowsAndColumns.addAll(rows);
		//rowsAndColumns.addAll(columns);
		
		// set it up for subtotals as well
		subtotalColumns = rowsAndColumns;
		
		// take care of the column order
		StringBuilder column_order = new StringBuilder("[");
		for(int valIndex = 0;valIndex < values.size();valIndex++) {
			if(valIndex > 0) {
				column_order.append(", ");
			}
			column_order.append("'").append(values.get(valIndex)).append("'");
		}
		// add the column total to be included
		//column_order.append(", 'Column Total'");
		column_order.append("]");

		// geenerate rows
		for(int idxIndex = 0;idxIndex < rowsAndColumns.size();idxIndex++) {
			if(idxIndex != 0) {
				idxString.append(", ");
			}
				
			idxString.append("'").append(rowsAndColumns.get(idxIndex)).append("'");			
		}
		
		if(idxString.length() > 0) {
			idxString = new StringBuilder("index = [").append(idxString).append("], ");
		}
		
		// do it for cross tab also
		for(int idxIndex = 0;idxIndex < rows.size();idxIndex++) {
			if(idxIndex != 0) {
				crosstabRows.append(", ");
			}
			crosstabRows.append(frameName).append(".").append(rows.get(idxIndex));
		}		
		if(idxString.length() > 0) {
			crosstabRows = new StringBuilder("[").append(crosstabRows).append("], ");
		}
		
		// generate the column string
		StringBuilder colString = new StringBuilder(""); // we dont need colstring anymore for pivot. We append it to rows
		StringBuilder crosstabCols = new StringBuilder("");
		if(columns != null) {
			for(int colIndex = 0;colIndex < columns.size();colIndex++) {
				if(colIndex != 0) {
					colString.append(", ");
					crosstabCols.append(", ");
				}
				colString.append("'").append(columns.get(colIndex)).append("'");			
				crosstabCols.append(frameName).append(".").append(columns.get(colIndex));
			}
			
			if(colString.length() > 0) {
				colString = new StringBuilder("columns = [").append(colString).append("], ");
				crosstabCols = new StringBuilder("[").append(crosstabCols).append("], ");
			}
		}
		
		// generate agg functions
		// should be the same size as the values
		StringBuilder funString = new StringBuilder("");
		StringBuilder crosstabVal = new StringBuilder("");
		StringBuilder crosstabAgg = new StringBuilder("");
		for(int funIndex = 0;funIndex < functions.size();funIndex++) {
			// following functions are available
			// np.sum, np.mean, min, max, count, numpy.size, pd.Series.nunique
			String fun = functions.get(funIndex);
			String value = values.get(funIndex);
			
			fun = QueryFunctionHelper.convertFunctionToPandasSyntax(fun);
			if(funIndex != 0) {
				funString.append(", ");
				crosstabVal.append(", ");
				crosstabAgg.append(", ");
			}
			funString.append("'").append(value).append("' : ");
			funString.append("'").append(fun).append("'");
			
			crosstabVal.append(frameName).append(".").append(value);
			crosstabAgg.append("'").append(fun).append("'");
		}		
		
		if(funString.length() > 0) {
			funString = new StringBuilder("aggfunc = {").append(funString).append("}, ");
			crosstabVal = new StringBuilder("values = ").append(crosstabVal).append(", ");
			crosstabAgg = new StringBuilder("aggfunc = ").append(crosstabAgg).append(", "); // need to put margins finally
		}
		// generate the values string
		StringBuilder pdValuesString = new StringBuilder("");
		for(int valIndex = 0;valIndex < values.size();valIndex++) {
			if(valIndex != 0) {
				pdValuesString.append(", ");
			}
			pdValuesString.append("'").append(values.get(valIndex)).append("'");
		}
		if(pdValuesString.length() > 0) {
			pdValuesString = new StringBuilder("values = [").append(pdValuesString).append("], ");
		}
		
		// handle drop na
		// handle fillvalues
		
		// create the pivot
		// generate the pivot first
		String totalAppender = "";
		String marginName = " ...All Total... ";
		String labelsCheat = "zzzzpp";
		if(rows.size() == 1)
			labelsCheat = "Row Total";
			
		
		margins = rows.size() == 1 || values.size() == 1;
		String marginValue = margins?"True":"False";
		marginValue = "False";

		StringBuilder pivotString = new StringBuilder("");
		String pivotName = Utility.getRandomString(5);
		pivotString.append(pivotName)
				.append(" = ")
				.append("pd.pivot_table(")
				.append(frameName).append(",")
				.append(pdValuesString)
				.append(colString)
				.append(idxString)
				.append(funString)
				.append("dropna=True,")
				.append("margins=").append(marginValue)
				.append(", margins_name='" + labelsCheat + "'")
				.append(").fillna('')");
		
		StringBuilder crosstabString = new StringBuilder("");
		crosstabString.append(pivotName)
		.append(" = ")
		.append("pd.crosstab(")
		.append(crosstabRows)
		//.append(pdValuesString)
		//.append(colString)
		.append(crosstabCols)
		.append(crosstabVal)
		.append(crosstabAgg)
		.append("margins=True,")
		.append("margins_name='..ALL'")
		.append(").fillna('')");
		
		
		System.out.println(pivotString);
		System.out.println(crosstabString);
		// append the formatter to start.. need a better way for this.. but for now
		// 2 decimal places
		retString.append("pd.set_option('display.float_format', lambda x: '%.2f' % x)").append(NEW_LINE);
		// check to see size of values and then append
//		if(values.size() == 1)
//			retString.append(crosstabString).append(NEW_LINE);
//		else 
		retString.append(pivotString).append(NEW_LINE);
		
		// this one generates the totals and such
		//pivotName  = generateTotals(rows, columns, values, pivotName, labelsCheat, retString);

		
		String outputFormat = ".to_html()";
		if(json) {
			outputFormat = ".to_json(orient='split')";
		}


		retString.append(NEW_LINE).append("print(").append(pivotName).append("[").append(column_order).append("]")
				.append(outputFormat).append(")").append(NEW_LINE);// .append("del(").append(pivotName).append(")");
		// storing the pivot name for future to get the pivot row count
		pivotNames.add(pivotName);
		
		System.err.println(retString);
		return retString.toString();
	}
	
	private String generateTotals(List <String> rows, List <String> columns, List <String> values, String pivotName, String labelCheat, StringBuilder totalString)
	{
		/*
		 * --- the stuff that finally works -- 
			piv1 = pd.pivot_table(d,values = ['id', 'age'], index = ['frame', 'gender'], aggfunc = {'id' : 'mean', 'age': 'mean'}, dropna=True,margins=False, margins_name='zzzzzzzzzzzz').fillna('')
			sac26Tn= piv1.sum(level='frame')
			gaeAfkZ = piv1[['id', 'age']].sum()
			fpiv1 = piv1.append(sac26Tn.assign(gender='zzzzzzzz').set_index('gender', append=True).sort_index().append(pd.DataFrame([gaeAfkZ.values], columns=gaeAfkZ.index, index = pd.MultiIndex.from_tuples([('zzzzpp', '')], names = ['id','age']))).fillna(''))
			fpiv1 = fpiv1.sort_index(level=['frame', 'gender'])
			
			fpiv1.index = pd.MultiIndex.from_tuples([(x[0].replace('zzzzpp', 'Row Total'), x[1].replace('zzzzzzzz', 'Total')) for x in fpiv1.index], names=fpiv1.index.names)
			
			There are three variables - rows, columns, values
			
			when rows > 1 no column - this is the first bifurcation. If it is 0 nothing to do
			When rows > 1 and columns > 0 third
			when rows = 1 but columns > 0 - Second bifurcatation
			
			
		 */
		
		//StringBuilder totalString = new StringBuilder();
		String finalPivotName = pivotName;
		
		if(rows.size() > 1)
		{
			finalPivotName = Utility.getRandomString(5);
			String lastColumn = rows.get(rows.size() - 1);

			StringBuilder valueString = new StringBuilder("[");
			for(int valIndex = 0;valIndex < values.size();valIndex++) {
				if(valIndex > 0) {
					valueString.append(", ");
				}
				valueString.append("'").append(values.get(valIndex)).append("'");
			}
			valueString.append("]");
			
			//aftgto = ahF34A.append(sapwSx8.assign(gender='', location= 'zzzzzz').set_index(['gender', 'location'], append=True).sort_index())

			StringBuilder rowSumColumnAdderString = new StringBuilder("");
			StringBuilder rowSumColumnIndexString = new StringBuilder("[");
			for(int rowIndex = 1;rowIndex < rows.size() ;rowIndex++)
			{
				if(rowIndex > 1) {
					rowSumColumnAdderString.append(", ");
					rowSumColumnIndexString.append(", ");
				}
				
				rowSumColumnIndexString.append("'").append(rows.get(rowIndex)).append("'");
				//if(rowIndex + 1 == rows.size())
				{
					rowSumColumnAdderString.append(rows.get(rowIndex)).append("='zzzzzz'");
				}
//				else
//					rowSumColumnAdderString.append(rows.get(rowIndex)).append(" = ''");
				
			}
			rowSumColumnIndexString.append("]");
			
			String rowTotal = "s" + Utility.getRandomString(5);
			totalString.append(rowTotal + "= " + pivotName + ".sum(level=('" + rows.get(0) + "'))"); // this is multiple levels - you have to always do 1 less
			totalString.append(NEW_LINE);

			totalString.append(finalPivotName + " = ");
			totalString.append(pivotName + ".append(");
			
			// add the row level totals first
			//saAdRws= acaENC.sum(level='frame')
			//aDzHlB = acaENC.append(saAdRws.assign(frame= 'zzzzzz').set_index('frame', append=True).sort_index())
	
			totalString.append(rowTotal + ".assign(" + rowSumColumnAdderString + ")"); // create a new column
			totalString.append(".set_index(" + rowSumColumnIndexString + ", append=True).sort_index())");// add this column as index and sort it
			
			String grandTotal = "g" + Utility.getRandomString(5);
			totalString.append(NEW_LINE).append(grandTotal + " = " + pivotName + "[" + valueString + "].sum()");
			
	
			StringBuilder totalValueIndex = new StringBuilder("[(");
			StringBuilder totalValueIndexNames = new StringBuilder("[");
			StringBuilder valueAdderString = new StringBuilder();
			for(int valIndex = 0;valIndex < values.size();valIndex++) {
				if(valIndex > 0) {
					totalValueIndex.append(", ");
					valueAdderString.append(", ");
					totalValueIndexNames.append(", ");
				}
				if(valIndex == 0)	
					totalValueIndex.append("'zzzzpp'");
				else
					totalValueIndex.append("''");
				
				totalValueIndexNames.append("'").append(values.get(valIndex)).append("'");
				valueAdderString.append(finalPivotName).append(".").append(values.get(valIndex));
			}
			totalValueIndex.append(")]");
			totalValueIndexNames.append("]");
		
			// now comes the hard part
			// creating it and appending it
	
			totalString.append(NEW_LINE);
			
			StringBuilder rowTotalReplacer = new StringBuilder("(");
			StringBuilder rowDropper = new StringBuilder("(");
			for(int rowIndex = 0;rowIndex < rows.size();rowIndex++)
			{
				if(rowIndex > 0) {
					rowTotalReplacer.append(", ");
					rowDropper.append(", ");
				}
				
				// only need the first and last rows
				if(rowIndex == 0)
				{
					rowTotalReplacer.append("x[0].replace('zzzzpp', 'Row Total')");
					rowDropper.append("'zzzzpp'");
				}
				else if(rowIndex+1 == rows.size())
				{
					rowDropper.append("'zzzzzz'");
					rowTotalReplacer.append("x[").append(rowIndex).append("].replace('zzzzzz', 'Total')");
				}
				else
				{
					rowDropper.append("'zzzzzz'"); // empty nothing to replace
					rowTotalReplacer.append("x[").append(rowIndex).append("]");
				}
			}
			rowTotalReplacer.append(")");
			rowDropper.append(")");
			
			// now add the column totalsgapHkVh = acaENC[['id']].sum()
			//aDzHlB = aDzHlB.append(pd.DataFrame([gapHkVh.values], columns=gapHkVh.index, index = pd.MultiIndex.from_tuples([('zzzzpp', '')], names = ['id', 'age']))).fillna('')
			if(values.size() > 1)
			{
				totalString.append(finalPivotName).append(" = ").append(finalPivotName);
				totalString.append(".append(");
				totalString.append("pd.DataFrame(["+ grandTotal + ".values], columns=" + grandTotal + ".index, index = pd.MultiIndex.from_tuples(" + totalValueIndex + ", ");
				totalString.append("names = " + totalValueIndexNames).append("))).fillna('')" );			
				totalString.append(NEW_LINE);
			}
			else
			{
				// drop the row
				totalString.append(finalPivotName).append(" = ").append(finalPivotName);
				totalString.append(".drop([");
				totalString.append(rowDropper);
				totalString.append("])");
				totalString.append(NEW_LINE);
			}
			// sort final pivot
			//fpiv1 = fpiv1.sort_index(level=['frame', 'gender'])
			StringBuilder rowString = new StringBuilder("[");
			for(int rowIndex = 0;rowIndex < rows.size();rowIndex++) {
				if(rowIndex > 0) {
					rowString.append(", ");
				}
				rowString.append("'").append(rows.get(rowIndex)).append("'");
			}
			rowString.append("]");
			totalString.append(finalPivotName).append(" = ").append(finalPivotName).append(".sort_index(level=").append(rowString).append(")");
		
			// now replace the indices
			totalString.append(NEW_LINE);
			//fpiv1.index = pd.MultiIndex.from_tuples([(x[0].replace('zzzzpp', 'Row Total'), x[1].replace('zzzzzzzz', 'Total')) for x in fpiv1.index], names=fpiv1.index.names)
	

			
			totalString.append(finalPivotName).append(".index = ");
			totalString.append("pd.MultiIndex.from_tuples([").append(rowTotalReplacer).append(" for x in ")
								.append(finalPivotName).append(".index], names = ")
								.append(finalPivotName).append(".index.names)");
			
			totalString.append(NEW_LINE);
		}
		else if(values.size() == 1)
		{
			
		}

		// lastly add the column total
		if(rows.size() == 1 && columns.size() > 0) // bifur 3
		{
			// it already has a column total by the way of margin
			totalString.append(finalPivotName).append(" = ");
			totalString.append(finalPivotName).append(".rename(columns={'").append(labelCheat).append("': 'Column Total'})");
		}
		else // bifur 2
		{
			StringBuilder valueAdderString = new StringBuilder();
			for(int valIndex = 0;valIndex < values.size();valIndex++) {
				if(valIndex > 0) {
					valueAdderString.append(" + ");
				}
				valueAdderString.append(finalPivotName).append(".").append(values.get(valIndex));
			}
			totalString.append(finalPivotName).append("['Column Total'] = ").append(valueAdderString);
		}
		
		
		System.err.println("This sets up the total.. ");
		System.err.println(totalString);
		
		
		return finalPivotName;
	}
	
	private long getCount(String frameName, List <String> items) 
	{
		long retCount = 1;
		for(int itemIndex = 0;itemIndex < items.size();itemIndex++) {
			StringBuilder sb = new StringBuilder(frameName).append("['").append(items.get(itemIndex)).append("'].nunique()");
			long count = (Long)this.insight.getPyTranslator().runScript(sb.toString());
			retCount = retCount * count;
		}
		return retCount;
	}
	
	public static String getJson2HTML(JSONObject mainObj, List <String> rows)
	{
		//String [] rows = new String[] {"frame", "location"};
		String [] values = null;
		
		// each record is a combination of this
		JSONArray colArray = mainObj.getJSONArray("columns");
		values = new String[colArray.length()];
		
		
		// get the index
		JSONArray index = mainObj.getJSONArray("index");
		String [] rowValues = new String[rows.size()];
		
		// tells the number of times this item is there
		// and the childs
		Map rowMap = new HashMap();
		Map levelItemCount = new HashMap();
		StringWriter outputString = new StringWriter();
		
		// the data
		// process the row
		JSONArray data = mainObj.getJSONArray("data");
		double [][] dataArray = new double[data.length()][colArray.length()];
		
		for(int dataIndex = 0;dataIndex < data.length();dataIndex++)
		{
			JSONArray record = data.getJSONArray(dataIndex);
			for(int recIndex = 0;recIndex < record.length();recIndex++)
			{
				if(record.get(recIndex) instanceof Double)
					dataArray[dataIndex][recIndex] = record.getDouble(recIndex);
				else
					dataArray[dataIndex][recIndex] = 0;
					
			}
		}
		
		// get columns
		// PROCESS ALL THE COLUMNS
		// chol, id
		// male, female
		Map itemLevelColSpan = new HashMap(); // keeps the column span
		String[][] columns = null;//
		String[][] columnsData = null;//
		
		for(int columnIndex = 0;columnIndex < colArray.length();columnIndex++)
		{
			if(colArray.get(columnIndex) instanceof JSONArray) // this is a multi level
			{
				JSONArray thisLevel = colArray.getJSONArray(columnIndex);
				if(columns == null)
				{
					columns = new String[thisLevel.length()][rows.size() + values.length + 1];
					columnsData = new String[thisLevel.length()][rows.size() + values.length + 1];
				}
				String parent = "";
				for(int colLevelIndex = 0;colLevelIndex < thisLevel.length();colLevelIndex++)
				{
					String colName = thisLevel.getString(colLevelIndex);
					String key = colName + "__" + colLevelIndex;
					String location = colName + "__" + colLevelIndex + "__" + (columnIndex + rows.size());
					if(parent.length() > 0)
						key = parent + ":" + key;
					
					int colSpan = 1;
					if(itemLevelColSpan.containsKey(key))
					{
						colSpan = (Integer)itemLevelColSpan.get(key);
						location = itemLevelColSpan.get(key + "__LOCATION") +"";
						colSpan++;
					}
					else
					{
						columns[colLevelIndex][rows.size() + columnIndex] = colName;	
					}
					itemLevelColSpan.put(key, colSpan);
					itemLevelColSpan.put(key+"__LOCATION", location);
					itemLevelColSpan.put(location, colSpan);
					parent = key;
					columnsData[colLevelIndex][rows.size() + columnIndex] = colName;	
				}
			}
			else // this is a single level column
			{
				if(columns == null)
				{
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
		String [][] rowDataArrayOutput = new String[index.length()][rows.size() + values.length + 1];
		String [][] rowDataArray = new String[index.length()][rows.size() + values.length + 1]; // this keeyps track of actual parent etc. required when we print
		
		Map itemLevelRowSpan = new HashMap();
		Map <String, double[]> itemLevelTotals = new HashMap<String, double[]>();
		double [] allRowTotal = new double[values.length + 1];
		
		// filling in the rows
		for(int rowIndex = 0;rowIndex < index.length();rowIndex++)
		{
			double summer = 0;
			String rowKey = null;
			if(index.get(rowIndex) instanceof JSONArray)
			{
				JSONArray singleRow = index.getJSONArray(rowIndex);
				String parent = "";
				rowKey = singleRow.getString(0);
				for(int rowItemIndex = 0;rowItemIndex < singleRow.length();rowItemIndex++)
				{
					String thisItem = singleRow.getString(rowItemIndex);
					String key = thisItem + "__" + rowItemIndex;
					if(parent.length() > 0)
						key = parent + ":" + key;
					int span = 0;
					if(itemLevelRowSpan.containsKey(key))
					{
						span = (Integer)itemLevelRowSpan.get(key);
						rowDataArrayOutput[rowIndex][rowItemIndex] = "";
					}
					else
					{
						rowDataArrayOutput[rowIndex][rowItemIndex] = thisItem;
					}
					rowDataArray[rowIndex][rowItemIndex] = thisItem;
					span++;
					itemLevelRowSpan.put(key, span);
					parent = key;
					// need to check for number but..
				}
			}
			else
			{
				rowKey = index.getString(0);
				String thisItem = index.getString(rowIndex);
				rowDataArrayOutput[rowIndex][0] = thisItem;				
			}
			
			// fill the data in parallel
			// plus 1 is for total
			double [] totals = new double[values.length + 1];
			for(int columnIndex = 0;columnIndex < values.length + 1;columnIndex++)
			{
				// fill the data
				String key = rowKey + "__" + columnIndex; // get the first level
				if(itemLevelTotals.containsKey(key))
				{
					totals = itemLevelTotals.get(key);
				}
				else
					totals[columnIndex] = 0;
				
				if(columnIndex < values.length)
				{
					totals[columnIndex] += dataArray[rowIndex][columnIndex];
					allRowTotal[columnIndex] += dataArray[rowIndex][columnIndex];
					rowDataArrayOutput[rowIndex][rows.size() + columnIndex] = dataArray[rowIndex][columnIndex] + "";
					summer = summer+dataArray[rowIndex][columnIndex];
				}
				else
				{
					totals[columnIndex] += summer;
					allRowTotal[columnIndex] += summer;
				}
				itemLevelTotals.put(key, totals);
			}
			rowDataArrayOutput[rowIndex][rows.size() + values.length] = summer + "";
		}
		
		// generate html
		//System.err.println("<table border=1>");
		outputString.append("<table>");
		outputString.append("<thead>");
		
		String curLevelItem = null;
		boolean newItem = true;
		
		// columns first
		for(int trIndex = 0;trIndex < columns.length;trIndex++)
		{
			//System.err.println("<tr>");
			outputString.append("<tr>");
			String [] thisRow = columns[trIndex];
			String [] thisDataRow = columnsData[trIndex];
			String parent = "";
			for(int tdIndex = 0;tdIndex < rows.size();tdIndex++)
				//System.err.println("<td></td>");
				outputString.append("<th></th>");
			
			// need something that keeps the parent at this level as we process all of these
			// we canot keep it
			// it has to be based on index
			
			for(int tdIndex = rows.size();tdIndex < thisRow.length;tdIndex++)
			{
				String thisItem = thisRow[tdIndex];
				String dataItem = thisDataRow[tdIndex];
				String cardinalKey = dataItem + "__" + trIndex + "__" + tdIndex;
				String key = dataItem + "__" + trIndex;
				if(parent.length() > 0)
					key = parent + ":" + key;
				
				if(thisItem != null && thisItem.length() > 0)
				{
					//System.err.print("<td");
					outputString.append("<th style=\"width=200px;background-color:#F6F6F6;color:#1E1E1E;\"");
					int colSpan = 0;
					if(itemLevelColSpan.containsKey(cardinalKey))
					{
						colSpan = (Integer)itemLevelColSpan.get(cardinalKey);
						if(newItem && tdIndex == 0)
						{
							//rowSpan++;
							newItem = false;
						}
						//System.err.print(" rowspan=" + rowSpan + " >");
						outputString.append(" colspan=" + colSpan + " >");
						tdIndex += (colSpan - 1); // account for the tdindex++
					}
					else
						//System.err.println(">");
						outputString.append(">");
					//System.err.println(thisItem);
					//System.err.println("</td>");
					outputString.append(thisItem);
					outputString.append("</th>");
					
				}
				else if(thisItem == null)
				{
					//System.err.println("<td>k</td>");
					//outputString.append("<td></td>");
				}
				parent = key;
			}
			
			//System.err.println("<tr>");
			outputString.append("<tr>");
		}
		
		// generate row headers next
		//System.err.println("<tr>");
		outputString.append("<tr>");
		for(int tdIndex = 0;tdIndex < rows.size();tdIndex++)
			//System.err.println("<td>" + rows[tdIndex] + "</td>");			
			outputString.append("<th style=\"width:200px;background-color:#F6F6F6;color:#1E1E1E;\">" + rows.get(tdIndex) + "</th>");			
		// fill other tds
		for(int tdIndex = rows.size();tdIndex < columns[0].length;tdIndex++)
			//System.err.println("<td></td>");
			//width:200px;background-color:#F6F6F6;color:#1E1E1E;
			outputString.append("<th style=\"width:200px;background-color:#F6F6F6;color:#1E1E1E;\"></th>");
		
		//System.err.println("</tr>");
		outputString.append("</tr>");
		outputString.append("</thead>");
		outputString.append("<tbody>");

		// write the data
		for(int trIndex = 0;trIndex < rowDataArrayOutput.length;trIndex++)
		{
			String [] thisRow = rowDataArrayOutput[trIndex];
			String [] thisDataRow = rowDataArray[trIndex];
			String parent = "";			
			//System.err.println("<tr>");
			outputString.append("<tr>");
			for(int tdIndex = 0;tdIndex < thisRow.length;tdIndex++)
			{
				String thisItem = thisRow[tdIndex];
				String dataItem = thisDataRow[tdIndex];
				String key = dataItem + "__" + tdIndex;
				if(parent.length() > 0)
					key = parent + ":" + key;
				
				if(tdIndex == 0 && curLevelItem != null && !dataItem.equalsIgnoreCase(curLevelItem)) // logic for doing totals
				{
					newItem = true;
					// add the total for this column
					//System.err.println("<td colspan=" + (thisRow.length - (values.length + 1)) +">");					
					outputString.append("<th style=\"background-color:#F6F6F6;color:#1E1E1E;\" colspan=" + (thisRow.length - (values.length + 1)) +">");
					//System.err.println(curLevelItem + " -- TOTAL </td>");
					outputString.append(curLevelItem + " -- Total </th>");
					double [] totals = itemLevelTotals.get(curLevelItem + "__" + tdIndex);
					
					if(totals != null)
					{
						for(int totalIndex = 0;totalIndex < totals.length;totalIndex++)
						{
							//System.err.println("<td>" + totals[totalIndex] + "</td>");
							outputString.append("<td style=\"font-weight:bold;\">" + totals[totalIndex] + "</td>");
						}
					}
					curLevelItem = dataItem;
					//System.err.println("</tr><tr>");
					outputString.append("</tr><tr>");
				}
				else if(curLevelItem == null)
				{
					curLevelItem = dataItem;
				}
				
				if(thisItem != null && thisItem.length() > 0)
				{
					//System.err.print("<td");
					if(tdIndex < rows.size())
						outputString.append("<th style=\"background-color:#F6F6F6;color:#1E1E1E;\"");
					else
						outputString.append("<td");
					int rowSpan = 0;
					if(itemLevelRowSpan.containsKey(key))
					{
						rowSpan = (Integer)itemLevelRowSpan.get(key);
						if(newItem && tdIndex == 0)
						{
							//rowSpan++;
							newItem = false;
						}
						//System.err.print(" rowspan=" + rowSpan + " >");
						outputString.append(" rowspan=" + rowSpan + " >");
					}
					else
						//System.err.println(">");
						outputString.append(">");
					//System.err.println(thisItem);
					//System.err.println("</td>");
					outputString.append(thisItem);
					if(tdIndex < rows.size())
						outputString.append("</th>");
					else
						outputString.append("</td>");
					
				}
				parent = key;
				
			}
			//System.err.println("</tr>");
			outputString.append("</tr>");
			
		}
		
		// print out the last total
		//System.err.println("<tr>");
		if(curLevelItem != null)
		{
			outputString.append("<tr>");
			//System.err.println("<td colspan=" + (rowDataArrayOutput[0].length - (values.length + 1)) +">");
			//System.err.println(curLevelItem + " -- TOTAL </td>");
			outputString.append("<th style=\"width:200px;background-color:#F6F6F6;color:#1E1E1E;\" colspan=" + (rowDataArrayOutput[0].length - (values.length + 1)) +">");
			outputString.append(curLevelItem + " -- TOTAL </th>");
			double [] totals = itemLevelTotals.get(curLevelItem + "__" + 0);
			
			if(totals != null)
			{
				for(int totalIndex = 0;totalIndex < totals.length;totalIndex++)
				{
					//System.err.println("<td>" + totals[totalIndex] + "</td>");
					outputString.append("<td style=\"font-weight:bold;\">" + totals[totalIndex] + "</td>");
				}
			}
			//System.err.println("</tr>");
			outputString.append("</tr>");
		}
		
		// finally the grand total
		//System.err.println("<tr>");
		outputString.append("<tr>");
		//System.err.println("<td colspan=" + (rowDataArrayOutput[0].length - (values.length + 1)) + ">All Total</td>");
		outputString.append("<th style=\"background-color:#F6F6F6;color:#1E1E1E;font-weight:bold;\" colspan=" + (rowDataArrayOutput[0].length - (values.length + 1)) + ">All Total</th>");
		
		for(int tdIndex = 0;tdIndex < allRowTotal.length;tdIndex++)
		{
			//System.err.println("<td>" + allRowTotal[tdIndex] + "</td>");
			outputString.append("<td style=\"font-weight:bold;\">" + allRowTotal[tdIndex] + "</td>");
		}
		//System.err.println("</tr>");
		outputString.append("</tr>");

		//System.out.println("</table>");
		outputString.append("</tbody>");
		outputString.append("</table>");
		
		//System.err.println(outputString);
		
		return outputString.toString();

	}
		
}
