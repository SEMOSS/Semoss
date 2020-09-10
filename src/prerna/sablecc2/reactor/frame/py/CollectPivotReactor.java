package prerna.sablecc2.reactor.frame.py;

import java.io.File;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import oracle.net.aso.a;
import prerna.ds.py.PyTranslator;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.ConstantDataTask;
import prerna.sablecc2.reactor.task.TaskBuilderReactor;
import prerna.util.Utility;

public class CollectPivotReactor extends TaskBuilderReactor {

	/**
	 * This class is responsible for collecting data from a task and returning it
	 */
	
	private static String curEncoding = null;

	private static Map<String, String> mathMap = new HashMap<String, String>();
	static {
		mathMap.put("Sum", "sum");
		mathMap.put("Average", "mean");
		mathMap.put("Min", "min");
		mathMap.put("Max", "max");
		mathMap.put("Median", "median");
		mathMap.put("StandardDeviation", "std");
		mathMap.put("Count", "count");
	};

	public CollectPivotReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ROW_GROUPS.getKey(), ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.VALUES.getKey(),  ReactorKeysEnum.SUBTOTALS.getKey(), "json"};
	}

	public NounMetadata execute() {
		this.task = getTask();

		PyTranslator pyt = insight.getPyTranslator();
		// just for testing
		//pyt = null;
		if(pyt == null)
			return getError("Pivot requires Python. Python is not enabled in this instance");
		
		pyt.setLogger(this.getLogger(this.getClass().getName()));

		
		// this is the payload that is coming
		// Frame ( frame = [ FRAME890385 ] ) | Select ( Genre , Studio, MovieBudget ) .as ( [ Genre , Studio, MovieBudget ] ) | CollectPivot( rowGroups=["Genre"], columns=["Studio"], values=["sum(MovieBudget)"] ) ;
		
		// pandas format is - pd.pivot_table(mv, index=['Genre', 'Nominated'], values=['MovieBudget', 'RevenueDomestic'], aggfunc={'MovieBudget':np.sum, 'RevenueDomestic':np.mean}, columns='Studio')
		
		// I need to convert the values into aggregate functions
		
		// I need to change this check later

		String fileName = Utility.getRandomString(6);
		String dir = (insight.getUserFolder() + "/Temp").replace('\\', '/');
		File tempDir = new File(dir);
		if(!tempDir.exists()) {
			tempDir.mkdir();
		}
		String outputFile = dir + "/" + fileName + ".csv";
		Utility.writeResultToFile(outputFile, this.task, ",");

		// so this is going to come in as vectors
		List<String> rowGroups = this.store.getNoun(keysToGet[0]).getAllStrValues();
		List<String> colGroups = this.store.getNoun(keysToGet[1]).getAllStrValues();
		List<String> values = this.store.getNoun(keysToGet[2]).getAllStrValues();
		List<String> subtotals = rowGroups;
		if(keyValue.containsKey(keysToGet[3]))
			subtotals = this.store.getNoun(keysToGet[3]).getAllStrValues();

		boolean json = false;

		if(this.store.getNounKeys().contains("json"))
			json = this.store.getNoun(keysToGet[4]).get(0).toString().equalsIgnoreCase("true");

		if(curEncoding == null)
			curEncoding = pyt.runPyAndReturnOutput("print(sys.stdout.encoding)");
		
		String frameName = Utility.getRandomString(6);
		String makeFrame =frameName + " = pd.read_csv('" + outputFile + "', encoding='" + curEncoding + "')"; //.replace(np.nan, '', regex=True)";
		
		List <String> newValues = new Vector<String>();
		List <String> functions = new Vector<String>();

		// lastly the values
		// need to create a pivot map for the FE
		Map<String, Object> pivotMap = new HashMap<String, Object>();
		pivotMap.put(keysToGet[0], rowGroups);
		pivotMap.put(keysToGet[1], colGroups);
		List<Map<String, String>> valuesList = new Vector<Map<String, String>>();

		
		for(int valIndex = 0;valIndex < values.size();valIndex++)
		{
			Map<String, String> valueMap = new HashMap<String, String>();
			String curValue = values.get(valIndex);

			// get the operator and selector
			//String [] composite = curValue.split("(");
			String operator = curValue.substring(0, curValue.indexOf("(")).trim();
			String operand = curValue.substring(curValue.indexOf("(") + 1, curValue.length()-1).trim();
			newValues.add(operand);
			functions.add(operator);
			// pass back the original operator before converting
			valueMap.put("math", operator);

			valueMap.put("alias", operand);
			valuesList.add(valueMap);
		}

		// making the new call here
		// columns are not really required
		// going to ignore for now
		String commands = genPivot(frameName, rowGroups, colGroups, subtotals, newValues, functions, true, true, json);
		commands = makeFrame + "\n" + commands;
		
//		// convert the inputs into a cgroup
//		StringBuilder rows = new StringBuilder("index=[");
//		for(int rowIndex = 0;rowIndex < rowGroups.size();rowIndex++)
//		{
//			String curValue = rowGroups.get(rowIndex);
//			if(rowIndex != 0)
//				rows.append(",");
//			rows.append("'").append(curValue).append("'");
//		}
//		rows.append("]");
//
//		StringBuilder cols = new StringBuilder("columns=[");
//		for(int colIndex = 0;colIndex < colGroups.size();colIndex++)
//		{
//			String curValue = colGroups.get(colIndex);
//			if(colIndex != 0)
//				cols.append(",");
//			cols.append("'").append(curValue).append("'");
//		}
//		if(cols.toString().equalsIgnoreCase("columns["))
//			cols = new StringBuilder("");
//		else
//			cols.append("]").insert(0, " , ");
		

		pivotMap.put(keysToGet[2], valuesList);

		String htmlOutput = pyt.runPyAndReturnOutput(commands); 
		
		File outputF = new File(outputFile);
		outputF.delete();
		
		ConstantDataTask cdt = new ConstantDataTask();
		// need to do all the sets
		cdt.setFormat("TABLE");
		cdt.setTaskOptions(task.getTaskOptions());
		cdt.setHeaderInfo(task.getHeaderInfo());
		cdt.setSortInfo(task.getSortInfo());
		cdt.setId(task.getId());
		Map<String, Object> formatMap = new Hashtable<String, Object>();
		formatMap.put("type", "TABLE");
		cdt.setFormatMap(formatMap);
		
		// set the output so it can give it
		Map<String, Object> outputMap = new HashMap<String, Object>();
		outputMap.put("headers", new String[] {});
		outputMap.put("rawHeaders", new String[] {});
		outputMap.put("values", new String[] {htmlOutput});
		outputMap.put("pivotData", pivotMap);
		cdt.setOutputData(outputMap);
		
		// need to set the task options
		// hopefully this is the current one I am working with
		if(this.task.getTaskOptions() != null)
		{
			prerna.query.querystruct.SelectQueryStruct sqs = (prerna.query.querystruct.SelectQueryStruct)((BasicIteratorTask)task).getQueryStruct();
			// I really hope this is only one
			Iterator <String> panelIds = task.getTaskOptions().getPanelIds().iterator();
			while(panelIds.hasNext()) {
				String panelId = panelIds.next();
				// this is a bit silly
				// but will set the formatter into the task options
				// so if we pull the task options we have that information
				// this is for {{@link RefreshPanelTaskReactor}}
				task.getTaskOptions().setFormatter(task.getFormatter());
				task.getTaskOptions().getOptions().put("values", values);
				this.insight.setFinalViewOptions(panelId, sqs, task.getTaskOptions());
			}
		}
		
		return new NounMetadata(cdt, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
		
		
		
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
	
	public String genPivot(String frameName, List <String> rows, List <String> columns, List <String> subtotalColumns, List <String> values, List <String> functions, boolean dropNA, boolean fill_value, boolean json)
	{
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
		
		// geenerate rows
		for(int idxIndex = 0;idxIndex < rowsAndColumns.size();idxIndex++)
		{
			if(idxIndex != 0)
			{
				idxString.append(", ");
			}
				
			idxString.append("'").append(rowsAndColumns.get(idxIndex)).append("'");			
		}		
		if(idxString.length() > 0)
			idxString = new StringBuilder("index = [").append(idxString).append("], ");

		// do it for cross tab also
		for(int idxIndex = 0;idxIndex < rows.size();idxIndex++)
		{
			if(idxIndex != 0)
				crosstabRows.append(", ");
				
			crosstabRows.append(frameName).append(".").append(rows.get(idxIndex));
		}		
		if(idxString.length() > 0)
			crosstabRows = new StringBuilder("[").append(crosstabRows).append("], ");

		// generate the column string
		StringBuilder colString = new StringBuilder(""); // we dont need colstring anymore for pivot. We append it to rows
		StringBuilder crosstabCols = new StringBuilder("");
		if(columns != null)
		{
			for(int colIndex = 0;colIndex < columns.size();colIndex++)
			{
				if(colIndex != 0)
				{
					colString.append(", ");
					crosstabCols.append(", ");
				}
				colString.append("'").append(columns.get(colIndex)).append("'");			
				crosstabCols.append(frameName).append(".").append(columns.get(colIndex));
			}		
			if(colString.length() > 0)
			{
				colString = new StringBuilder("columns = [").append(colString).append("], ");
				crosstabCols = new StringBuilder("[").append(crosstabCols).append("], ");
			}
		}
		
		// generate agg functions
		// should be the same size as the values
		StringBuilder funString = new StringBuilder("");
		StringBuilder crosstabVal = new StringBuilder("");
		StringBuilder crosstabAgg = new StringBuilder("");
		for(int funIndex = 0;funIndex < functions.size();funIndex++)
		{
			// following functions are available
			// np.sum, np.mean, min, max, count, numpy.size, pd.Series.nunique
			String fun = functions.get(funIndex);
			String value = values.get(funIndex);
			
			fun = QueryFunctionHelper.convertFunctionToPandasSyntax(fun);
			if(funIndex != 0)
			{
				funString.append(", ");
				crosstabVal.append(", ");
				crosstabAgg.append(", ");
			}
			funString.append("'").append(value).append("' : ");
			funString.append("'").append(fun).append("'");
			
			crosstabVal.append(frameName).append(".").append(value);
			crosstabAgg.append("'").append(fun).append("'");
			
			
		}		
		if(funString.length() > 0)
		{
			funString = new StringBuilder("aggfunc = {").append(funString).append("}, ");
			crosstabVal = new StringBuilder("values = ").append(crosstabVal).append(", ");
			crosstabAgg = new StringBuilder("aggfunc = ").append(crosstabAgg).append(", "); // need to put margins finally
		}
		// generate the values string
		StringBuilder pdValuesString = new StringBuilder("");
		for(int valIndex = 0;valIndex < values.size();valIndex++)
		{
			if(valIndex != 0)
				pdValuesString.append(", ");
			pdValuesString.append("'").append(values.get(valIndex)).append("'");
		}
		if(pdValuesString.length() > 0)
			pdValuesString = new StringBuilder("values = [").append(pdValuesString).append("], ");

		// handle drop na
		// handle fillvalues
		
		// create the pivot
		// generate the pivot first
		String totalAppender = "";
		String marginName = " ...All Total... ";
		String labelsCheat = "zzzzzzzzzzzz";

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
				.append("margins=True")
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
		retString.append("pd.set_option('display.float_format', lambda x: '%.2f' % x)").append("\n");
		// check to see size of values and then append
//		if(values.size() == 1)
//			retString.append(crosstabString).append("\n");
//		else 
		retString.append(pivotString).append("\n");
		
		// need to convert the index as well
		// I am just going to convert everything to a string
		//id1 = aVLQIp.index.levels[0]
		//id1 = id1.astype('str')
		// make all of the indices string so it doesnt complain later
		if(rowsAndColumns.size() > 1)
		{
			for(int idx = 0;idx < rowsAndColumns.size();idx++)
			{
				String customIdxName = Utility.getRandomString(5);
				retString.append(customIdxName).append("=").append(pivotName).append(".index.levels[").append(idx).append("]").append("\n");
				retString.append(customIdxName).append("=").append(customIdxName).append(".astype('str')").append("\n");
				// set up this level immediately
				retString.append(pivotName).append(".index.set_levels(").append(customIdxName).append(", level=").append(idx).append(", inplace=True)").append("\n");
			}
		}		
		// generate the subtotals now
		// need to do this for every level
		//df1 = table.groupby(level=[0,1]).sum()
		//		df1.index = pd.MultiIndex.from_arrays([df1.index.get_level_values(0), 
		//		                                       df1.index.get_level_values(1)+ '_sum', 
		//		                                       len(df1.index) * ['']])
		
		// df2 = table.groupby(level=0).sum()
		//df2.index = pd.MultiIndex.from_arrays([df2.index.values + '_sum',
		 //                                      len(df2.index) * [''], 
		 //                                      len(df2.index) * ['']])
		
		// see which subtotal columns we want
		// stop at that level when we get to
		//df = pd.concat([table, df1, df2]).sort_index(level=[0])
		String outputFormat = ".to_html()";
		if(json)
			outputFormat = ".to_json(orient='split')";
		
		if(subtotalColumns != null)
		{
			StringBuilder groupBy = new StringBuilder();
			
			StringBuilder concat = new StringBuilder("pd.concat([").append(pivotName);
			StringBuilder deleter = new StringBuilder("del(").append(pivotName);
						
			
			for(int subIndex = 0;subIndex < subtotalColumns.size();subIndex++)
			{
				String groupFrameName = Utility.getRandomString(5);
				String thisSub = subtotalColumns.get(subIndex);
				String function = "sum()";
				int totalerIndex = 0; // this is the place to start
	
				StringBuilder leveler = new StringBuilder("");
	
				// set up the multiindex
				StringBuilder indexer = new StringBuilder("");
				indexer.append("pd.MultiIndex.from_arrays([");
				
				boolean processLevel = true;
				boolean totalColumn = true;
				
				// need to find which level this one is at
				for(int idx = 0;idx < rowsAndColumns.size();idx++)
				{
					//System.err.println("Current Sub is " + thisSub + " and rows is " + rows.get(idx));
					
					if(processLevel)
					{
						// create the lever for the groupby
						if(idx != 0)
							leveler.append(", ");
						leveler.append(idx);
					}

					if(idx != 0)
						indexer.append(", ");
					// add the rows
					if(rowsAndColumns.get(idx).equalsIgnoreCase(thisSub))
					{
						totalerIndex = idx;
						processLevel = false;
						totalColumn = true;
						totalAppender = "  Total  ";
					}
					
					// add these only if they are not the last level
					//if(idx + 1 < rows.size())
					{
						if(totalColumn)
						{
							indexer.append(groupFrameName).append(".index.get_level_values(").append(idx).append(")").append(" + '").append(totalAppender).append("'");
							totalColumn = false;
							totalAppender  = "";
						}
						else 
						{
							indexer.append("len(").append(groupFrameName).append(".index)* ['']");
						}
					}						
				}
				
				if(!rowsAndColumns.get(rowsAndColumns.size() -1).equalsIgnoreCase(thisSub))
				{
					// set up the groupby
					StringBuffer curGroup = new StringBuffer("");
					curGroup.append(groupFrameName).append(" = ").append(pivotName).append(".groupby(level=[").append(leveler).append("]).").append(function);
					curGroup.append("\n");
					groupBy.append(curGroup.toString()).append("\n");
					
					System.err.println(curGroup);
					// set the indexer into the groupby
					indexer.append("]");
					indexer = new StringBuilder().append(groupFrameName).append(".index = ").append(indexer).append(")");
					groupBy.append(indexer.toString()).append("\n");
					
					// should do this only if it is the first column I think ?
					// I need to really drop the last row
					// everytime
					groupBy.append("totalRows = len(").append(groupFrameName).append(")").append("\n");
					groupBy.append(groupFrameName).append("=").append(groupFrameName).append(".iloc[:").append("totalRows -1]").append("\n");
					//groupBy.append(groupFrameName).append("=").append(groupFrameName).append(".drop('").append(labelsCheat).append("  Total  ").append("', level=0, errors='ignore')");
					System.err.println(indexer);
					
					concat.append(", ").append(groupFrameName);
					deleter.append(", ").append(groupFrameName);
				}
			}
			
			concat.append("])")
			
			//.append(".rename(index={'").append(marginName).append("': '").append(labelsCheat).append("'})")
			//.append(".rename(columns={'").append(marginName).append("' : '").append(labelsCheat).append("'}, levels=1)")
			.append(".sort_index(level=[0]).fillna('')");
			if(rowsAndColumns.size() == 1) // nothing to concat
				concat = new StringBuilder(pivotName);
			//if(columns.size() > 1) // this happens only when more than 1 column
			concat.append(".rename(columns={'").append(labelsCheat).append("' : '").append(marginName).append("'}, level=1)");
			concat.append(".rename(index={'").append(labelsCheat).append("': '").append(marginName).append("'})");
			
			String finalPivotName = Utility.getRandomString(5);
			deleter.append(", ").append(finalPivotName);
			deleter.append(")");
			String finalPivot = finalPivotName + " = " + concat;
			
			// change the margin name to seomthing 
			
			// need to drop the all total from 
			String dropAllTotal = "";
			
			//if(rows.size() > 1) // else there is no total
			//	dropAllTotal = finalPivotName + " = " + finalPivotName +".drop('" + marginName + "  Total  '" +", level=0)";
			
			System.err.println(finalPivot);
			System.err.println(deleter);
			String output = "print(" + finalPivotName + outputFormat + ")"; //.encode('utf-8'))";
			String deleteLast = "del(" + finalPivotName + ")";
			
			retString.append(groupBy).append("\n").append(finalPivot).append("\n").append(dropAllTotal).append("\n").append(output).append("\n").append(deleter);
		}		
		else
			retString.append("\n").append(pivotName).append(outputFormat).append("\n").append("del(").append(pivotName).append(")");
		
		System.err.println(retString);
		return retString.toString();
	}

	
	
}
