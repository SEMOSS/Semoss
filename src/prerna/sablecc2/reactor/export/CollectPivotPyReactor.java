package prerna.sablecc2.reactor.export;

import java.io.File;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.ds.py.PyTranslator;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ConstantDataTask;
import prerna.sablecc2.reactor.task.TaskBuilderReactor;
import prerna.util.Utility;

public class CollectPivotPyReactor extends TaskBuilderReactor {

	/**
	 * This class is responsible for collecting data from a task and returning it
	 */

	private static Map<String, String> mathMap = new HashMap<String, String>();
	static {
		mathMap.put("Sum", "sum");
		mathMap.put("Average", "mean");
		mathMap.put("Min", "min");
		mathMap.put("Max", "max");
		mathMap.put("Median", "median");
		mathMap.put("StandardDeviation", "std");
	};

	public CollectPivotPyReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ROW_GROUPS.getKey(), ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.VALUES.getKey() };
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

		// convert the inputs into a cgroup
		
		StringBuilder rows = new StringBuilder("index=[");
		for(int rowIndex = 0;rowIndex < rowGroups.size();rowIndex++)
		{
			String curValue = rowGroups.get(rowIndex);
			if(rowIndex != 0)
				rows.append(",");
			rows.append("'").append(curValue).append("'");
		}
		rows.append("]");

		StringBuilder cols = new StringBuilder("columns=[");
		for(int colIndex = 0;colIndex < colGroups.size();colIndex++)
		{
			String curValue = colGroups.get(colIndex);
			if(colIndex != 0)
				cols.append(",");
			cols.append("'").append(curValue).append("'");
		}
		if(cols.toString().equalsIgnoreCase("columns["))
			cols = new StringBuilder("");
		else
			cols.append("]").insert(0, " , ");
		

		// lastly the values
		// need to create a pivot map for the FE
		Map<String, Object> pivotMap = new HashMap<String, Object>();
		pivotMap.put(keysToGet[0], rowGroups);
		pivotMap.put(keysToGet[1], colGroups);
		List<Map<String, String>> valuesList = new Vector<Map<String, String>>();
		StringBuilder aggFunctions = new StringBuilder(", aggfunc={");
		StringBuilder valueSelects = new StringBuilder(", values=[");
		for(int valIndex = 0;valIndex < values.size();valIndex++)
		{
			Map<String, String> valueMap = new HashMap<String, String>();
			String curValue = values.get(valIndex);

			// get the operator and selector
			//String [] composite = curValue.split("(");
			String operator = curValue.substring(0, curValue.indexOf("(")).trim();
			String operand = curValue.substring(curValue.indexOf("(") + 1, curValue.length()-1).trim();
			
			
			if(valIndex != 0)
			{
				aggFunctions.append(",");
				valueSelects.append(",");
			}
			
			// pass back the original operator before converting
			valueMap.put("math", operator);

			for (Map.Entry<String, String> mathElement : mathMap.entrySet()) {
				String key = (String) mathElement.getKey();
				String value = (String) mathElement.getValue();
				// convert syntax for operator
				operator = operator.replace(key, value);
			}

			
			valueSelects.append("'").append(operand).append("'");
			
			
			aggFunctions.append("'").append(operand).append("'");
			aggFunctions.append(":");
			
			// do the replacement logic
			// step 1 do it simple
			aggFunctions.append("np.").append(operator);
			
			valueMap.put("alias", operand);
			valuesList.add(valueMap);
		}
		aggFunctions.append("}");
		valueSelects.append("]");
		pivotMap.put(keysToGet[2], valuesList);

		// random variable
		String frameName = Utility.getRandomString(6);
		String makeFrame =frameName + " = pd.read_csv('" + outputFile + "')";
		String pivot = "print(pd.pivot_table(" + frameName + ", " + rows + cols + valueSelects + aggFunctions + ").to_html())";
		String deleteFrame = "del(" + frameName + ")";
		
		String [] inscript = new String[]{makeFrame, pivot, deleteFrame}; 
		// now compose the whole thing
		String htmlOutput = pyt.runPyAndReturnOutput(inscript);
		
		
		
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
}
