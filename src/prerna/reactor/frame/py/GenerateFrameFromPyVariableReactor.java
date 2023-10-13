package prerna.reactor.frame.py;

import org.apache.logging.log4j.Logger;

import prerna.ds.py.PandasFrame;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.ds.py.PyTranslator;
import prerna.reactor.imports.ImportUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class GenerateFrameFromPyVariableReactor extends AbstractPyFrameReactor {
	
	private static final String CLASS_NAME = GenerateFrameFromPyVariableReactor.class.getName();
	
	public GenerateFrameFromPyVariableReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.VARIABLE.getKey(), ReactorKeysEnum.OVERRIDE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		//init();
		organizeKeys();
		String varName = getVarName();
		PyTranslator pyT = this.insight.getPyTranslator();
		logger.info("Getting the columns for :" + varName);
		String[] colNames = pyT.getStringArray(PandasSyntaxHelper.getColumns(varName));;
		
		// I bet this is being done for pixel.. I will keep the same
		logger.info("Cleaning the columns for :" + varName);
		pyT.runScript(PandasSyntaxHelper.cleanFrameHeaders(varName, colNames));
		colNames = pyT.getStringArray(PandasSyntaxHelper.getColumns(varName));
		
		logger.info("Getting the column types for :" + varName);
		String[] colTypes = pyT.getStringArray(PandasSyntaxHelper.getTypes(varName));

		if(colNames == null || colTypes == null) {
			throw new IllegalArgumentException("Please make sure the variable " + varName + " exists and can be a valid data.table object");
		}
		PandasFrame frame = new PandasFrame(varName);
		//frame.setTranslator(pyT);
		pyT.runPyAndReturnOutput(PandasSyntaxHelper.makeWrapper(frame.getWrapperName(), varName));
		//frame.setJep(this.insight.getPy());
		frame.setTranslator(this.insight.getPyTranslator());

		// create the pandas frame
		// and set up teverything else
		ImportUtility.parseTableColumnsAndTypesToFlatTable(frame.getMetaData(), colNames, colTypes, varName);

		NounMetadata noun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);		
		if(overrideFrame()) {
			this.insight.setDataMaker(frame);
		}
		// add the alias as a noun by default
		if(varName != null && !varName.isEmpty()) {
			this.insight.getVarStore().put(varName, noun);
		}
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				null, 
				"GenerateFrameFromPyVariable", 
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
			return "Name of the py variable";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
