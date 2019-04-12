package prerna.sablecc2.reactor.algorithms.dataquality;

import java.util.List;
import java.util.Vector;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/*
 * This is the class of DQ that performs data quality from the input information
 */
public class RunDataQualityReactor extends AbstractRFrameReactor {

	private static final String DIR_SEP = System.getProperty("file.separator");

	// Define rules because ReactorKeyEnum doesn't have default values for our purposes
	private static final String RULE_KEY = "rule";
	private static final String COLUMNS_KEY = "column";
	private static final String OPTIONS_KEY = "options";
	private static final String INPUT_TABLE_KEY = "inputTable";

	// This gets the basic format of the data. The information put in here will be in the format of the 
	// information we want to read in. I.E. assigning a var [INT]
	public RunDataQualityReactor() {
		this.keysToGet = new String[]{
				RULE_KEY, COLUMNS_KEY, OPTIONS_KEY, INPUT_TABLE_KEY
		};
	}

	@Override
	/*
	 * (non-Javadoc)
	 * @see prerna.sablecc2.reactor.IReactor#execute()
	 * 
	 * This function creates Meta Data on the variable. It takes in the value and also takes the pixel
	 * and assigns a type. We use this to create the variables to pass to R
	 */
	public NounMetadata execute() {
		// Initiate R
		init();
		organizeKeys();
		RDataTable frame = (RDataTable) getFrame();
		String rFrameVariable = frame.getName();
		String rule = getData(RULE_KEY);
		String column = getData(COLUMNS_KEY);
		List<String> optionsList = getOptions(OPTIONS_KEY);
		RDataTable inputTable = getInputTable();
		
		StringBuilder str = new StringBuilder();
		str.append("list(");
		str.append("rule = \"").append(rule).append("\"");
		str.append(", col = \"").append(column).append("\"");
		str.append(", options = ");
		if (!optionsList.isEmpty()) {
			System.out.println("in if");
			int optListSize = optionsList.size();
//			if(optListSize > 1) {
				str.append("c(");
//			}
			for(int i = 0; i < optListSize; i++) {
				str.append("\"").append(optionsList.get(i)).append("\"");
				if( (i + 1) != optListSize) {
					str.append(",");
				}
			}
//			if(optListSize > 1) {
				str.append(")");
//			} 
		}
		else {
			System.out.println("in else");
			str.append("NULL");
		}

		str.append(")");		
		System.out.println("WE in HERE");
		////////  Variable that will be set to map of rules/ input of mission control //////
		StringBuilder inputString = new StringBuilder();
		String inputVariable = "inputRules_" + Utility.getRandomString(5);
		inputString.append(inputVariable + " <- " + str + ";"); 
		
		// will call the script with all the source calls 
		
		StringBuilder rScript = new StringBuilder();
		String base = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		base = base.replace("\\", "/");
		String dqDirLoc = base + DIR_SEP + "R" + DIR_SEP + "DQ" + DIR_SEP;
		rScript.append("source(\"" + dqDirLoc + "sourceFile.R" + "\");");
		rScript.append("sourceFiles(\"" + dqDirLoc + "\");");
		
		
		String retRVariableName = null;
		if(inputTable != null) {
			retRVariableName = inputTable.getName();
		} else {
			retRVariableName = "dataQualityTable_" + Utility.getRandomString(5);
			appendFrameGeneration(rScript, retRVariableName);
		}
		rScript.append(inputString.toString());
		
		//create a return variable that holds the updated dt and the output data table so we can pass both back
		String wholeReturn = "return_" + Utility.getRandomString(5);
		rScript.append(wholeReturn).append(" <- missionControl(" + rFrameVariable + ", " + inputVariable + ", " + retRVariableName + ");");
		rScript.append(rFrameVariable).append(" <- " + wholeReturn + "[[1]];");
		rScript.append(retRVariableName).append(" <-  " + wholeReturn + "[[2]];");
		

		// you will run this rScript
		this.rJavaTranslator.runR(rScript.toString());
		recreateMetadata(rFrameVariable);
		
		if(inputTable != null) {
			return new NounMetadata(inputTable, PixelDataType.FRAME);
		}
		System.out.println(rScript);
		// make a new frame
		RDataTable newFrame = createFrameFromVariable(retRVariableName);
		NounMetadata noun = new NounMetadata(newFrame, PixelDataType.FRAME);
		this.insight.getVarStore().put(retRVariableName, noun);
		return noun;
	}

	private List<String> getOptions(String key) {
		// instantiate var ruleList as a list of strings 
		List<String> optionList = new Vector<String>();
		// Class call to make grs to get the Noun of getRules
		GenRowStruct grs = this.store.getNoun(key);

		if(grs == null || grs.isEmpty()) {
			optionList.add("NULL");
			return optionList;
		}
		// Assign size to the length of grs
		int size = grs.size();
		// Iterate through the rule and add the value to the list
		for(int i = 0; i < size; i++) {
			optionList.add(grs.get(i).toString());
		}
		return optionList;
	}
	
	private String getData(String key) {
		GenRowStruct grs = this.store.getNoun(key);

		if(grs == null || grs.isEmpty()) {
			throw new IllegalArgumentException("Missing Necessary Value to Run");
		}

		return grs.get(0).toString();
	}
	
	
	private RDataTable getInputTable() {
		GenRowStruct grs = this.store.getNoun(INPUT_TABLE_KEY);
		if(grs == null || grs.isEmpty()) {
			return null;
		}
		NounMetadata input = grs.getNoun(0);
		if(input.getNounType() == PixelDataType.CONST_STRING) {
			// we are making the frame here
			RDataTable table = new RDataTable();
			String retRVariableName = input.getValue().toString();
			table.setName(retRVariableName);
			
			StringBuilder rScript = new StringBuilder();
			appendFrameGeneration(rScript, retRVariableName);
			this.rJavaTranslator.runR(rScript.toString());
			// store as a varaible in the insight
			this.insight.getVarStore().put(retRVariableName, new NounMetadata(table, PixelDataType.FRAME));
			return table;
		}
		return (RDataTable) input.getValue();
	}
	
	private void appendFrameGeneration(StringBuilder rScript, String retRVariableName) {
		rScript.append(retRVariableName).append(" <- data.table(Columns=character(), Errors=integer(), Valid=integer(), Total=integer(), Rules=character(), Description=character(), ruleID = character(), toColor = character());");
	}
}
