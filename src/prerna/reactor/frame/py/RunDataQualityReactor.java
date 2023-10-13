package prerna.reactor.frame.py;

import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.py.PandasFrame;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.reactor.frame.FrameFactory;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class RunDataQualityReactor extends AbstractPyFrameReactor {

	private static final String RULE_KEY = "rule";
	private static final String COLUMNS_KEY = "column";
	private static final String OPTIONS_KEY = "options";
	private static final String INPUT_TABLE_KEY = "inputTable";

	public RunDataQualityReactor() {
		this.keysToGet = new String[] { RULE_KEY, COLUMNS_KEY, OPTIONS_KEY, INPUT_TABLE_KEY };
	}

	public NounMetadata execute() {
		organizeKeys();
		PandasFrame frame = (PandasFrame) getFrame();
		String frameWrapper = frame.getWrapperName();
		String rule = getData(RULE_KEY);
		String column = getData(COLUMNS_KEY);
		List<Object> optionsList = getOptions(OPTIONS_KEY);
		PandasFrame inputTable = getInputTable();

		String retPyFrameName = null;
		if (inputTable != null) {
			retPyFrameName = inputTable.getName();
		} else {
			// did user define output table?
			retPyFrameName = getInputTableName();
			// no, make one up
			if (retPyFrameName == null) {
				retPyFrameName = "dataQualityTable_" + Utility.getRandomString(5);
			}
		}

		// load python module
		// SemossBase/py/DQ
		frame.runScript("from DQ import missionControl as mc");

		// create rule object
		// map of rules/ input for mission control
		StringBuilder str = new StringBuilder();
		String opt = PandasSyntaxHelper.createPandasColVec(optionsList, SemossDataType.STRING);
		String pyRule = "rule" + Utility.getRandomString(5);
		str.append(pyRule + " = {'rule': '" + rule + "', 'col': '" + column + "', 'options': " + opt + "}");
		frame.runScript(str.toString());

		if (inputTable == null) {
			// create empty frame to append rows to
			StringBuilder pyScript = new StringBuilder();
			pyScript.append(retPyFrameName).append(" = pd.DataFrame(columns=['Columns', 'Errors', 'Valid', 'Total', 'Rules', 'Description', 'toColor'])");
			frame.runScript(pyScript.toString());
		}

		// run mission control
		StringBuilder pyScript = new StringBuilder();
		pyScript.append(retPyFrameName).append(" = mc.missionControl(" + frameWrapper + ", " + pyRule + ", " + retPyFrameName + ")");
		frame.runScript(pyScript.toString());

		if (inputTable != null) {
			return new NounMetadata(inputTable, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
		}
		// make a new frame
		PandasFrame newFrame = null;
		try {
			newFrame = (PandasFrame) FrameFactory.getFrame(this.insight, "PY", retPyFrameName);
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e.getMessage());
		}
		
		// set data for new frame object
		frame.runScript(PandasSyntaxHelper.makeWrapper(newFrame.getWrapperName(), retPyFrameName));
		newFrame = (PandasFrame) recreateMetadata(newFrame, false);
		NounMetadata noun = new NounMetadata(newFrame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
		this.insight.getVarStore().put(retPyFrameName, noun);
		return noun;
	}

	private List<Object> getOptions(String key) {
		// instantiate var ruleList as a list of strings
		List<Object> optionList = new Vector<>();
		GenRowStruct grs = this.store.getNoun(key);
		if (grs == null || grs.isEmpty()) {
			return optionList;
		}
		// Assign size to the length of grs
		int size = grs.size();
		// Iterate through the rule and add the value to the list
		for (int i = 0; i < size; i++) {
			optionList.add(grs.get(i) + "");
		}
		return optionList;
	}

	private String getData(String key) {
		GenRowStruct grs = this.store.getNoun(key);
		if (grs == null || grs.isEmpty()) {
			throw new IllegalArgumentException("Must set " + key);
		}
		return grs.get(0).toString();
	}

	private PandasFrame getInputTable() {
		GenRowStruct grs = this.store.getNoun(INPUT_TABLE_KEY);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		NounMetadata noun = grs.getNoun(0);
		if (noun.getNounType() == PixelDataType.FRAME) {
			return (PandasFrame) grs.get(0);
		}
		return null;
	}

	private String getInputTableName() {
		GenRowStruct grs = this.store.getNoun(INPUT_TABLE_KEY);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		NounMetadata noun = grs.getNoun(0);
		if (noun.getNounType() == PixelDataType.CONST_STRING) {
			return grs.get(0).toString();
		}
		return null;
	}
}
