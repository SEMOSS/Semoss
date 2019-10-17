package prerna.sablecc2.reactor.export;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IRawSelectWrapper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.ConstantDataTask;
import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.reactor.task.TaskBuilderReactor;
import prerna.util.Utility;

public class CollectPivotReactor extends TaskBuilderReactor {

	/**
	 * This class is responsible for collecting data from a task and returning it
	 */

	private int limit = 0;
	
	public CollectPivotReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ROW_GROUPS.getKey(), ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.VALUES.getKey() };
	}
	
	public NounMetadata execute() {

		AbstractRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(this.getLogger(this.getClass().getName()));
		rJavaTranslator.startR(); 
		
		// I need to change this check later
		String[] packages = { "pivottabler" };
		//rJavaTranslator.checkPackages(packages);

		this.task = getTask();
		
		// I neeed to get the basic iterator and then get types from there
		// 
		task.getMetaMap();
		SemossDataType[] sTypes = ((IRawSelectWrapper) ((BasicIteratorTask)(task)).getIterator()).getTypes();
		String[] headers = ((IRawSelectWrapper) ((BasicIteratorTask)(task)).getIterator()).getHeaders();
		System.out.println(".");
		
		Map<String, SemossDataType> typeMap = new HashMap<String, SemossDataType>();
		for(int i = 0; i < headers.length; i++) {
			typeMap.put(headers[i],sTypes[i]);
		}
		
		// I need to see how to get this to temp
		String fileName = Utility.getRandomString(6);
		String dir = insight.getUserFolder() + "/Temp";
		dir = dir.replaceAll("\\\\", "/");
		File tempDir = new File(dir);
		if(!tempDir.exists())
			tempDir.mkdir();
		String outputFile = dir + "/" + fileName + ".csv";
		Utility.writeResultToFile(outputFile, this.task, typeMap, ",");
		
		// so this is going to come in as vectors
		List rowGroups = this.store.getNoun(keysToGet[0]).getAllValues();
		List colGroups = this.store.getNoun(keysToGet[1]).getAllValues();
		List values = this.store.getNoun(keysToGet[2]).getAllValues();
		

		
		StringBuilder pivoter = new StringBuilder("{library(\"pivottabler\");");
		pivoter.append(fileName + " <- fread(\"" + outputFile + "\");");
		// get the frame
		// get frame name
		String table = fileName;

		// convert the inputs into a cgroup
		StringBuilder rows = new StringBuilder("c(");
		for(int rowIndex = 0;rowIndex < rowGroups.size();rowIndex++)
		{
			if(rowIndex > 0)
				rows.append(",");
			rows.append("\"").append(rowGroups.get(rowIndex)).append("\"");
		}
		rows.append(")");
		
		// convert columns next
		StringBuilder cols = new StringBuilder("c(");
		for(int colIndex = 0;colIndex < colGroups.size();colIndex++)
		{
			if(colIndex > 0)
				cols.append(",");
			cols.append("\"").append(colGroups.get(colIndex)).append("\"");
		}
		cols.append(")");
		
		// last piece is the calculations
		// not putting headers right now
		StringBuilder calcs = new StringBuilder("c(");
		for(int calcIndex = 0;calcIndex < values.size();calcIndex++)
		{
			if(calcIndex > 0)
				calcs.append(",");
			calcs.append("\"").append(values.get(calcIndex)).append("\"");
		}
		calcs.append(")");
		
		String pivotName = "pivot" + Utility.getRandomString(5);
		String htmlName = pivotName + ".html";
		
		pivoter.append(pivotName + " <- qpvt(" + table + "," + rows + "," + cols + "," + calcs + ");");
		
		// create the html
		pivoter.append(pivotName + "$saveHtml(paste(ROOT" + ",\"/" + htmlName +"\", sep=\"\"));");
				
				
		pivoter.append("}");
		
		// make the pivot
		rJavaTranslator.runRAndReturnOutput(pivoter.toString());
		// get the output
		String htmlOutput = rJavaTranslator.runRAndReturnOutput("print(" + pivotName + "$getHtml());");

		pivoter = new StringBuilder("{");
		// delete the variable and pivot
		pivoter.append("rm(" + pivotName + "," + table + ");");
		pivoter.append("}");
		rJavaTranslator.runRAndReturnOutput(pivoter.toString());
		
		File outputF = new File(outputFile);
		outputF.delete();
		
		ConstantDataTask cdt = new ConstantDataTask();
		// need to do all the sets
		cdt.setFormat("TABLE");
		
		// I need to create the options here
		Map optionMap = new HashMap<String, Object>();
		optionMap.put(keysToGet[0], rowGroups);
		optionMap.put(keysToGet[1], colGroups);
		optionMap.put(keysToGet[2], values);
		
		TaskOptions options = new TaskOptions(optionMap);
		cdt.setTaskOptions(options);
		cdt.setHeaderInfo(task.getHeaderInfo());
		cdt.setSortInfo(task.getSortInfo());
		
		// set the output so it can give it
		cdt.setOutputData(htmlOutput);
		// I dont think the filter information is required
		//cdt.setFilterInfo(task.getFilterInfo());
		// delete the pivot later
		return new NounMetadata(cdt, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
	}
	

	// keeping these methods for now.. I am not sure I require them
	
	@Override
	protected void buildTask() {
		// if the task was already passed in
		// we do not need to optimize/recreate the iterator
		if(this.task.isOptimized()) {
			this.task.optimizeQuery(this.limit);
		}
	}
	
	private TaskOptions genOrnamentTaskOptions() {
		if(this.subAdditionalReturn != null && this.subAdditionalReturn.size() == 1) {
			NounMetadata noun = this.subAdditionalReturn.get(0);
			if(noun.getNounType() == PixelDataType.ORNAMENT_MAP) {
				// we will use this map as task options
				TaskOptions options = new TaskOptions((Map<String, Object>) noun.getValue());
				options.setOrnament(true);
				return options;
			}
		}
		return null;
	}
	
	//returns how much do we need to collect
	private int getTotalToCollect() {
		// try the key
		GenRowStruct numGrs = store.getNoun(keysToGet[1]);
		if(numGrs != null && !numGrs.isEmpty()) {
			return ((Number) numGrs.get(0)).intValue();
		}
		
		// try the cur row
		List<Object> allNumericInputs = this.curRow.getAllNumericColumns();
		if(allNumericInputs != null && !allNumericInputs.isEmpty()) {
			return ((Number) allNumericInputs.get(0)).intValue();
		}
		
		// default to 500
		return 500;
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
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "The number to collect";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
