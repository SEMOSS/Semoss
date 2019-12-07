package prerna.sablecc2.reactor.frame.py;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.py.PandasFrame;
import prerna.query.querystruct.transform.QSRenameColumnConverter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.ModifyHeaderNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;
import prerna.util.Utility;
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
		
		// create script
		String script = wrapperFrameName + ".to_pct('" + srcCol + "', '" + newColName + "', " + sigDigits + ", ";
		if (by100) script += "True)";
		else script += "False)";
		
		// run script
		insight.getPyTranslator().runEmptyPy(script);
		
		// update meta data
		OwlTemporalEngineMeta metaData = frame.getMetaData();
		String frameName = frame.getName();
		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
		
		if (newColName != null && !newColName.equals("")) {
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
		int defaultValue = 4;

		NounMetadata noun = grs.getNoun(0);
		if (noun.getNounType() == PixelDataType.CONST_INT) {
			return (int) grs.get(0);
		}
		return defaultValue;
	}
}

