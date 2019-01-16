package prerna.sablecc2.reactor.frame.r;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.h2.H2Frame;
import prerna.ds.r.RSyntaxHelper;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.reactor.imports.H2Importer;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class GenerateH2FrameFromRVariableReactor extends AbstractRFrameReactor {

	/**
	 * This reactor takes an r frame and synchronizes it to an h2 frame in
	 * semoss inputs are: 1) r data table name
	 */
	
	public GenerateH2FrameFromRVariableReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.VARIABLE.getKey(), ReactorKeysEnum.OVERRIDE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		// get rFrameName
		String varName = getVarName();
		H2Frame newTable = new H2Frame(varName);

		//sync R dataframe to H2Frame
		syncFromR(this.rJavaTranslator,varName, newTable);
		if(overrideFrame()) {
			this.insight.setDataMaker(newTable);
		}
		NounMetadata noun = new NounMetadata(newTable, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
		// add the alias as a noun by default
		if(varName != null && !varName.isEmpty()) {
			this.insight.getVarStore().put(varName, noun);
		}
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				null, 
				"GenerateH2FrameFromRVariable", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		return noun;
	}

	/**
	 * 
	 * @param rFrameName
	 * @param frame
	 */
	public void syncFromR(AbstractRJavaTranslator rJavaTranslator, String rFrameName, H2Frame frame) {
		// generate the QS
		// set the column names and types
		rJavaTranslator.executeR(RSyntaxHelper.asDataTable(rFrameName, rFrameName));
		// recreate a new frame and set the frame name
		String[] colNames = rJavaTranslator.getColumns(rFrameName);
		rJavaTranslator.runR(RSyntaxHelper.cleanFrameHeaders(rFrameName, colNames));
		colNames = rJavaTranslator.getColumns(rFrameName);
		String[] colTypes = rJavaTranslator.getColumnTypes(rFrameName);
		// change r dataTypes such as dates, logicals, etc to be displayed as strings
		StringBuilder dataTypeConversion = new StringBuilder();
		for (int i = 0; i < colTypes.length; i++) {
			SemossDataType smssType = SemossDataType.convertStringToDataType(colTypes[i]);
			if (smssType == SemossDataType.INT || smssType == SemossDataType.DOUBLE) {
				dataTypeConversion.append(RSyntaxHelper.alterColumnTypeToNumeric(rFrameName, colNames[i]) + ";");
			}
			if (smssType == SemossDataType.STRING || smssType == SemossDataType.DATE) {
				dataTypeConversion.append(RSyntaxHelper.alterColumnTypeToCharacter(rFrameName, colNames[i]) + ";");
			}
		}
		if (dataTypeConversion.toString().length() > 0) {
			rJavaTranslator.runR(dataTypeConversion.toString());
		}

		if (colNames == null || colTypes == null) {
			throw new IllegalArgumentException("Please make sure the variable " + rFrameName + " exists and can be a valid data.table object");
		}
		
		CsvQueryStruct qs = new CsvQueryStruct();
		qs.setSelectorsAndTypes(colNames, colTypes);

		// we will make a temp file
		String tempFileLocation = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "\\"
				+ DIHelper.getInstance().getProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
		tempFileLocation += "\\" + Utility.getRandomString(10) + ".csv";
		tempFileLocation = tempFileLocation.replace("\\", "/");
		rJavaTranslator.executeR("fwrite(" + rFrameName + ", file='" + tempFileLocation + "')");

		// iterate through file and insert values
		qs.setFilePath(tempFileLocation);
		H2Importer importer = new H2Importer(frame, qs);
		// importer will create the necessary meta information
		importer.insertData();
	}
	
	/**
	 * Get the input being the r variable name
	 * 
	 * @return
	 */
	private String getVarName() {
		// key based
		GenRowStruct overrideGrs = this.store.getNoun(ReactorKeysEnum.VARIABLE.getKey());
		if (overrideGrs != null && !overrideGrs.isEmpty()) {
			return (String) overrideGrs.get(0);
		}
		// first input
		return this.curRow.get(0).toString();
	}
	
	private boolean overrideFrame() {
		GenRowStruct overrideGrs = this.store.getNoun(ReactorKeysEnum.OVERRIDE.getKey());
		if (overrideGrs != null && !overrideGrs.isEmpty()) {
			return (boolean) overrideGrs.get(0);
		}
		// default is to override
		return true;
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
