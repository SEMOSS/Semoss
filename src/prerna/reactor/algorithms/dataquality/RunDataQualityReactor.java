package prerna.reactor.algorithms.dataquality;

import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.r.RDataTable;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/*
 * This is the class of DQ that performs data quality from the input information
 */
public class RunDataQualityReactor extends AbstractRFrameReactor {

	// Define rules because ReactorKeyEnum doesn't have default values for our purposes
	private static final String RULE_KEY = "rule";
	private static final String COLUMNS_KEY = "column";
	private static final String OPTIONS_KEY = "options";
	private static final String INPUT_TABLE_KEY = "inputTable";

	// This gets the basic format of the data. The information put in here will be in the format of the 
	// information we want to read in. I.E. assigning a var [INT]
	public RunDataQualityReactor() {
		this.keysToGet = new String[] {
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
		// only apply char length to string cols
		if(rule.equals("Character Length")) {
			SemossDataType dt = frame.getMetaData().getHeaderTypeAsEnum(frame.getName() + "__" + column);
			if(!dt.equals(SemossDataType.STRING)) {
				throw new IllegalArgumentException("Character length rule only applies to STRING column");
			}
		}
		List<String> optionsList = getOptions(OPTIONS_KEY);
		RDataTable inputTable = getInputTable();
		
		String retRVariableName = null;
		if(inputTable != null) {
			retRVariableName = inputTable.getName();
		} else {
			// did user define output table?
			retRVariableName = getInputTableName();
			// no, make one up
			if(retRVariableName == null) {
				retRVariableName = "dataQualityTable_" + Utility.getRandomString(5);
			}
		}
		
		StringBuilder str = new StringBuilder();
		str.append("list(");
		str.append("rule = \"").append(rule).append("\"");
		str.append(", col = \"").append(column).append("\"");
		str.append(", options = ");
		if (!optionsList.isEmpty()) {
			int optListSize = optionsList.size();
			str.append("c(");
			for(int i = 0; i < optListSize; i++) {
				str.append("\"").append(optionsList.get(i)).append("\"");
				if( (i + 1) != optListSize) {
					str.append(",");
				}
			}
			str.append(")");
		}
		else {
			str.append("NULL");
		}

		str.append(")");		
		////////  Variable that will be set to map of rules/ input of mission control //////
		StringBuilder inputString = new StringBuilder();
		String inputVariable = "inputRules_" + Utility.getRandomString(5);
		inputString.append(inputVariable + " <- " + str + ";"); 
		
		// will call the script with all the source calls 
		
		StringBuilder rScript = new StringBuilder();
		String base = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String dqDirLoc = null;
		if(base.endsWith(DIR_SEPARATOR)) {
			dqDirLoc = base + "R" + DIR_SEPARATOR + "DQ" + DIR_SEPARATOR;
		} else {
			dqDirLoc = base + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "DQ" + DIR_SEPARATOR;
		}
		dqDirLoc = dqDirLoc.replace("\\", "/");
		rScript.append("source(\"" + dqDirLoc + "sourceFile.R" + "\");");
		rScript.append("sourceFiles(\"" + dqDirLoc + "\");");
		
		if(inputTable == null) {
			rScript.append(retRVariableName).append(" <- data.table(Columns=character(), Errors=integer(), Valid=integer(), Total=integer(), Rules=character(), Description=character(), toColor = character());");
		}
		rScript.append(inputString.toString());
		
		rScript.append(retRVariableName).append(" <- missionControl(" + rFrameVariable + ", " + inputVariable + ", " + retRVariableName + ");");
		
		
		// R garbage collection
		rScript.append("source(\"" + dqDirLoc + "fileCleanup.R" + "\");");
		rScript.append("rm(" + inputVariable + ");");
//		System.out.println(rScript);

		// you will run this rScript
		this.rJavaTranslator.runR(rScript.toString());
		frame.recreateMeta();
		
		if(inputTable != null) {
			return new NounMetadata(inputTable, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
		}
		// make a new frame
		RDataTable newFrame = createNewFrameFromVariable(retRVariableName);
		NounMetadata noun = new NounMetadata(newFrame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
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
		NounMetadata noun = grs.getNoun(0);
		if(noun.getNounType() == PixelDataType.FRAME) {
			return (RDataTable) grs.get(0);
		}
		return null;
	}
	
	private String getInputTableName() {
		GenRowStruct grs = this.store.getNoun(INPUT_TABLE_KEY);
		if(grs == null || grs.isEmpty()) {
			return null;
		}
		NounMetadata noun = grs.getNoun(0);
		if(noun.getNounType() == PixelDataType.CONST_STRING) {
			return grs.get(0).toString();
		}
		return null;
	}
}
