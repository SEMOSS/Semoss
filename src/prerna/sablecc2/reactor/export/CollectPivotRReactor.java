package prerna.sablecc2.reactor.export;

import java.io.File;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.ds.r.RSyntaxHelper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ConstantDataTask;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.reactor.task.TaskBuilderReactor;
import prerna.util.Utility;

public class CollectPivotRReactor extends TaskBuilderReactor {

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
		mathMap.put("StandardDeviation", "sd");
		mathMap.put("Count", "count");
	};

	public CollectPivotRReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ROW_GROUPS.getKey(), ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.VALUES.getKey() };
	}

	public NounMetadata execute() {
		this.task = getTask();

		AbstractRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(this.getLogger(this.getClass().getName()));
		rJavaTranslator.startR(); 

		// I need to change this check later
		String[] packages = { "pivottabler" };
		rJavaTranslator.checkPackages(packages);

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
		String rows = RSyntaxHelper.createStringRColVec(rowGroups);
		String cols = RSyntaxHelper.createStringRColVec(colGroups);

		// last piece is the calculations
		// not putting headers right now
		StringBuilder calcs = new StringBuilder("c(");
		for(int calcIndex = 0; calcIndex < values.size(); calcIndex++) {
			if(calcIndex > 0) {
				calcs.append(",");
			}
			String newValue = values.get(calcIndex).toString();
			// replace the generic math with the pivottabler math
			for (Map.Entry<String, String> mathElement : mathMap.entrySet()) {
				String key = (String) mathElement.getKey();
				String value = (String) mathElement.getValue();

				newValue = newValue.replace(key, value);
			}
			calcs.append("\"").append(newValue).append("\"");
		}
		calcs.append(")");

		String pivotName = "pivot" + Utility.getRandomString(5);
		String htmlName = pivotName + ".html";

		// load html
		StringBuilder pivoter = new StringBuilder("library(\"pivottabler\");");
		pivoter.append(RSyntaxHelper.getFReadSyntax(fileName, outputFile, ","));
		pivoter.append(pivotName + " <- qpvt(" + fileName + "," + rows + "," + cols + "," + calcs + ");");
		// create the html
		pivoter.append(pivotName + "$saveHtml(paste(ROOT" + ",\"/" + htmlName +"\", sep=\"\"));");

		// make the pivot
		rJavaTranslator.runR(pivoter.toString());
		// get the output
		String htmlOutput = rJavaTranslator.runRAndReturnOutput("print(" + pivotName + "$getHtml());");

		// delete the variable and pivot
		rJavaTranslator.runR("rm(" + pivotName + "," + fileName + ");");
		File outputF = new File(outputFile);
		outputF.delete();

		// need to create a pivot map for the FE
		Map<String, Object> pivotMap = new HashMap<String, Object>();
		pivotMap.put(keysToGet[0], rowGroups);
		pivotMap.put(keysToGet[1], colGroups);
		List<Map<String, String>> valuesList = new Vector<Map<String, String>>();
		for(int i = 0; i < values.size(); i++) {
			String value = (String) values.get(i);
			
			Map<String, String> valueMap = new HashMap<String, String>();
			for (Map.Entry<String, String> mathElement : mathMap.entrySet()) {
				String key = (String) mathElement.getKey();
				if (value.contains(key)) {
					// string manipulate to get the alias inside of the math
					String alias = ((String)values.get(i)).replace(key, "").trim();
					alias = alias.substring(1, alias.length() - 1);

					valueMap.put("alias", alias);
					valueMap.put("math", key);
					valuesList.add(valueMap);
					continue;
				}
			}

			// if we get to this point
			// no math was fun
			// just put it as is
			valueMap.put("alias", value);
			valueMap.put("math", "");
			valuesList.add(valueMap);
		}
		pivotMap.put(keysToGet[2], valuesList);
		
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
