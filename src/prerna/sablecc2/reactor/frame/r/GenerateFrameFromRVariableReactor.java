package prerna.sablecc2.reactor.frame.r;

import org.apache.log4j.Logger;

import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.imports.ImportUtility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class GenerateFrameFromRVariableReactor extends AbstractRFrameReactor {
	
	private static final String CLASS_NAME = GenerateFrameFromRVariableReactor.class.getName();
	
	public GenerateFrameFromRVariableReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.VARIABLE.getKey(), ReactorKeysEnum.OVERRIDE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		init();
		organizeKeys();
		String varName = getVarName();
		this.rJavaTranslator.executeEmptyR(RSyntaxHelper.asDataTable(varName, varName));
		// recreate a new frame and set the frame name
		String[] colNames = this.rJavaTranslator.getColumns(varName);
		this.rJavaTranslator.runR(RSyntaxHelper.cleanFrameHeaders(varName, colNames));
		colNames = this.rJavaTranslator.getColumns(varName);
		String[] colTypes = this.rJavaTranslator.getColumnTypes(varName);

		if(colNames == null || colTypes == null) {
			throw new IllegalArgumentException("Please make sure the variable " + varName + " exists and can be a valid data.table object");
		}
		
		RDataTable newTable = new RDataTable(this.insight.getRJavaTranslator(logger), varName);
		ImportUtility.parserRTableColumnsAndTypesToFlatTable(newTable, colNames, colTypes, varName);
		NounMetadata noun = new NounMetadata(newTable, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);		
		if(overrideFrame()) {
			this.insight.setDataMaker(newTable);
		}
		// add the alias as a noun by default
		if(varName != null && !varName.isEmpty()) {
			this.insight.getVarStore().put(varName, noun);
		}
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				null, 
				"GenerateFrameFromRVariable", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		return noun;
	}

	private boolean overrideFrame() {
		GenRowStruct overrideGrs = this.store.getNoun(ReactorKeysEnum.OVERRIDE.getKey());
		if(overrideGrs != null && !overrideGrs.isEmpty()) {
			return (boolean) overrideGrs.get(0);
		}
		// default is to override
		return true;
	}
	
	/**
	 * Get the input being the r variable name
	 * @return
	 */
	private String getVarName() {
		// key based
		GenRowStruct overrideGrs = this.store.getNoun(ReactorKeysEnum.VARIABLE.getKey());
		if(overrideGrs != null && !overrideGrs.isEmpty()) {
			return  (String) overrideGrs.get(0);
		}
		// first input
		return this.curRow.get(0).toString();
	}
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.VARIABLE.getKey())) {
			return "Name of the r variable";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
