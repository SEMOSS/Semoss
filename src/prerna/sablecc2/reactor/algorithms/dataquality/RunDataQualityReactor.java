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
	private static final String RULE_KEY = "rules";
	private static final String OPTIONS_KEY = "options";
	private static final String COLUMNS_KEY = "columns";
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
		List<String> rulesList = getData(RULE_KEY);
		List<String> columnsList = getData(COLUMNS_KEY);
		List<List<String>> optionsList = getOptions();
		RDataTable inputTable = getInputTable();
		
		////// build the list of maps that will be sent to mission control
		StringBuilder str = new StringBuilder();
		str.append("list(");
		int size = rulesList.size();
		for(int i = 0; i < size; i++) {
			str.append("list(");
			str.append("rule = \"").append(rulesList.get(i)).append("\"");
			str.append(", col = \"").append(columnsList.get(i)).append("\"");
			str.append(", options = ");
			if (optionsList != null) {
				List<String> optionsListTemp = optionsList.get(i);
				int valSize = optionsListTemp.size();
				if(valSize > 1){str.append("c(");} 
				for(int j = 0; j < valSize; j++) {
					if(optionsListTemp.get(j) != null && optionsListTemp.get(j) != ""){
						str.append("\"").append(optionsListTemp.get(j)).append("\"");
						if( (j+1) != valSize) {
							str.append(",");
						}
					}
					else{str.append("NULL");}
				}
				if(valSize > 1){str.append(")");} 
			}
			else {
				str.append("NULL");
			}
			
			str.append(")");
			if(i + 1 != size){str.append(",");}
		}
		str.append(")");		
		
		////////  Variable that will be set to map of rules/ input of mission control //////
		StringBuilder inputString = new StringBuilder();
		String inputVariable = "inputRules_" + Utility.getRandomString(5);
		inputString.append(inputVariable + " <- " + str + ";"); 
		
		// will call the script with all the source calls 
		StringBuilder rScript = new StringBuilder();
		String base = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		base = base.replace("\\", "/");
		String fileLoc = base + DIR_SEP + "R" + DIR_SEP + "DQ" + DIR_SEP + "sourceFile.R";
		rScript.append("source(\"" + fileLoc + "\");");
		
		
		String retRVariableName = null;
		if(inputTable != null) {
			retRVariableName = inputTable.getName();
		} else {
			retRVariableName = "dataQualityTable_" + Utility.getRandomString(5);
			rScript.append(retRVariableName).append(" <- data.table(Columns=character(), Errors=integer(), Valid=integer(), Total=integer(), Rules=character(), Description=character());");
		}
		rScript.append(inputString.toString());
		
		//create a return variable that holds the updated dt and the output data table so we can pass both back
		String wholeReturn = "return" + Utility.getRandomString(5);
		rScript.append(wholeReturn).append(" <- missionControl(" + rFrameVariable + ", " + inputVariable + ", " + retRVariableName + ");");
		rScript.append(rFrameVariable).append(" <- " + wholeReturn + "[[1]];");
		rScript.append(retRVariableName).append(" <-  " + wholeReturn + "[[2]];");
		

		// you will run this rScript
		this.rJavaTranslator.runR(rScript.toString());
		recreateMetadata(rFrameVariable);
		
		if(inputTable != null) {
			return new NounMetadata(inputTable, PixelDataType.FRAME);
		}
		
		// make a new frame
		RDataTable newFrame = createFrameFromVariable(retRVariableName);
		NounMetadata noun = new NounMetadata(newFrame, PixelDataType.FRAME);
		this.insight.getVarStore().put(retRVariableName, noun);
		return noun;
	}

	
	
	/*
	 * Grabbing pixel inputs
	 */
	
	/**
	 * 
	 * @return 
	 */
	private List<String> getData(String key) {
		// instantiate var ruleList as a list of strings 
		List<String> inputList = new Vector<String>();
		// Class call to make grs to get the Noun of getRules
		GenRowStruct grs = this.store.getNoun(key);
		/*
		 * Error Check for No Rule input
		 */
		if(grs == null || grs.isEmpty()) {
			throw new IllegalArgumentException("Must define rules to run");
		}
		// Assign size to the length of grs
		int size = grs.size();
		// Iterate through the rule and add the value to the list
		for(int i = 0; i < size; i++) {
			inputList.add(grs.get(i).toString());
		}
		return inputList;
	}
		
		/**
		 * 
		 * @return
		 */
	private List<List<String>> getOptions() {
		// Instantiates attributesList as a List of List of Strings <- the columns wanted
		List<List<String>> optionsList = new Vector<List<String>>();
		// Create object grs and we get the values and store in grs
		GenRowStruct grs = this.store.getNoun(OPTIONS_KEY);
		if(grs == null || grs.isEmpty()) {
			return null;
		}
		int size = grs.size();
		for(int i = 0; i < size; i++) {
			NounMetadata noun = grs.getNoun(i);
			Object val = noun.getValue();
			
			List<String> row = new Vector<String>();
			if(val instanceof List) {
				int rowSize = ((List) val).size();
				for(int j = 0; j < rowSize; j++) {
					if(((List)val).get(j) instanceof NounMetadata){
						row.add(((NounMetadata) ((List)val).get(j)).getValue().toString());
					}
					else{
						row.add(((List) val).get(j).toString());
					}
				}
			}
			else {
				row.add(val.toString());
			}
			optionsList.add(row);
		}
		return optionsList;	
	}
	
	private RDataTable getInputTable() {
		GenRowStruct grs = this.store.getNoun(INPUT_TABLE_KEY);
		if(grs == null || grs.isEmpty()) {
			return null;
		}
		return (RDataTable) grs.get(0);
	}
}
