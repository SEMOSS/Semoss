package prerna.reactor.frame.py;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class ToPercentReactor extends AbstractPyFrameReactor {
	
	private static final String BY100 = "by100";
	private static final String SIG_DIGITS = "sigDigits";
	
	public ToPercentReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), SIG_DIGITS, BY100,
				ReactorKeysEnum.NEW_COLUMN.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();
		
		// get the wrapper name
		String wrapperFrameName = frame.getWrapperName();
		
		// get inputs
		String srcCol = this.keyValue.get(ReactorKeysEnum.COLUMN.getKey());
		int sigDigits = getValue(SIG_DIGITS);
		boolean by100 = getBoolean(BY100);
		String newColName = this.keyValue.get(ReactorKeysEnum.NEW_COLUMN.getKey());
		String script = null;
		// create script
		if (newColName != null && !newColName.equals("") && !newColName.equals("null")) {
			script = wrapperFrameName + ".to_pct('" + srcCol + "', '" + srcCol + "', " + sigDigits + ", ";
		} else {
			script = wrapperFrameName + ".to_pct('" + srcCol + "', '" + newColName + "', " + sigDigits + ", ";
		}
		if (by100) script += "True)";
		else script += "False)";

		String by100v = by100 ? "True" : "False";
		
		if (sigDigits < 0) {
			throw new IllegalArgumentException("Significant digits must be greater than or equal to zero.");
		}
		
        script = wrapperFrameName + ".cache['data']['" + newColName + "'] = " + 
                wrapperFrameName + ".cache['data']['" + srcCol + "'].apply(lambda row: " +
                "str(round(row, " + sigDigits + ") * 100) + '%' if " + by100v + 
                " else " +
                "str(round(row, " + sigDigits + ") * 1) + '%' )" +
                ".replace(\'nan%\','null')"; //this check is for replacing nan% with null

		// run script
		// converting to lambda
		//  mv['add'] = mv.apply(lambda x: clean.PyFrame.to_pct_l(x['MovieBudget'], 2, 1) , axis=1)
		insight.getPyTranslator().runEmptyPy(script);
		this.addExecutedCode(script);
		
		// update meta data
		OwlTemporalEngineMeta metaData = frame.getMetaData();
		String frameName = frame.getName();
		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
		
		if (newColName != null && !newColName.equals("") && !newColName.equals("null")) {
			retNoun.addAdditionalOpTypes(PixelOperationType.FRAME_HEADERS_CHANGE);
			String addedColumnDataType = SemossDataType.STRING.toString();
			metaData.addProperty(frameName, frameName + "__" + newColName);
			metaData.setAliasToProperty(frameName + "__" + newColName, newColName);
			metaData.setDataTypeToProperty(frameName + "__" + newColName, addedColumnDataType);
			metaData.setDerivedToProperty(frameName + "__" + newColName, true);
			frame.syncHeaders();
		} else {
			metaData.modifyDataTypeToProperty(frameName + "__" + srcCol, frameName, SemossDataType.STRING.toString());
		}

		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"ToPercent", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		// return the output
		return retNoun;
	}
	
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////

	private boolean getBoolean(String key) {
		GenRowStruct grs = this.store.getNoun(key);
		if (grs != null && !grs.isEmpty()) {
			return (boolean) grs.get(0);
		}
		// default is false
		return false;
	}

	private int getValue(String key) {
		GenRowStruct grs = this.store.getNoun(key);
		NounMetadata noun = grs.getNoun(0);
		
		if (noun.getNounType() == PixelDataType.CONST_INT) {
			return (int) grs.get(0);
		} else {
			throw new IllegalArgumentException("Input of " + grs.get(0) + " is invalid. Significant digits must be an integer value.");
		}
	}
}

