package prerna.reactor.frame.r;

import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ToPercentReactor extends AbstractRFrameReactor {

	private static final String BY100 = "by100";
	private static final String SIG_DIGITS = "sigDigits";

	public ToPercentReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.COLUMN.getKey(), 
				SIG_DIGITS, BY100, ReactorKeysEnum.NEW_COLUMN.getKey() };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		// get frame
		RDataTable frame = (RDataTable) getFrame();
		String rFrameName = frame.getName();
		String srcCol = this.keyValue.get(ReactorKeysEnum.COLUMN.getKey());
		int sigDigits = getValue(SIG_DIGITS);
		boolean by100 = getBoolean(BY100);
		String newColName = this.keyValue.get(ReactorKeysEnum.NEW_COLUMN.getKey());

		// need to check data types to make sure user passes in numeric col
		OwlTemporalEngineMeta metaData = frame.getMetaData();

		// make sure column exists
		String[] startingColumns = getColumns(rFrameName);
		List<String> startingColumnsList = new Vector<String>(startingColumns.length);
		startingColumnsList.addAll(Arrays.asList(startingColumns));
		if (srcCol == null || !startingColumnsList.contains(srcCol)
				|| !Utility.isNumericType(metaData.getHeaderTypeAsEnum(rFrameName + "__" + srcCol).toString())) {
			throw new IllegalArgumentException("Need to define existing numeric column.");
		}

		// build and run r script
//		StringBuilder rScript = new StringBuilder();
//		rScript.append(rFrameName).append("$");
//		if (newColName == null || newColName.equals("")) {
//			rScript.append(srcCol);
//		} else {
//			rScript.append(newColName);
//		}
//		rScript.append(" <- paste0(");
//		if (by100) {
//			rScript.append("100 * ");
//		}
//		rScript.append("round(").append(rFrameName).append("$").append(srcCol);
//		rScript.append(", digits = ").append(sigDigits).append("), ").append("\"%\");");
		
		StringBuilder script = new StringBuilder();
		script.append(rFrameName).append("$");
		String replaceNA = "";
		if (newColName == null || newColName.equals("") || newColName.equals("null")) {
			script.append(srcCol);
			replaceNA = rFrameName + "$" + srcCol + "[" + rFrameName + "$" + srcCol + " %like% \"NA%\" | " 
					+ rFrameName + "$" + srcCol + " %like% \"NaN%\"] <- NA; ";

		} else {
			script.append(newColName);
			replaceNA = rFrameName + "$" + newColName + "[" + rFrameName + "$" + newColName + " %like% \"NA%\" | "
					+ rFrameName + "$" + newColName + " %like% \"NaN%\"] <- NA; ";
		}
		script.append(" <- paste0(format(round(" + rFrameName + "$" + srcCol);
		if (by100) {
			script.append(" * 100 ");
		}
		// Validate SigDigits
		if (sigDigits < 0 || sigDigits > 20) {
			throw new IllegalArgumentException("Significant digits must be integer values between 0 and 20, inclusive.");
		}
		
		script.append(", " + sigDigits + "), nsmall = " + sigDigits + "), '%');");
		// replace NA% with NA
		script.append(replaceNA);
		this.rJavaTranslator.runR(script.toString());
		this.addExecutedCode(script.toString());

		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
		if (newColName != null && !newColName.equals("")) {
			retNoun.addAdditionalOpTypes(PixelOperationType.FRAME_HEADERS_CHANGE);
			String addedColumnDataType = SemossDataType.STRING.toString();
			metaData.addProperty(rFrameName, rFrameName + "__" + newColName);
			metaData.setAliasToProperty(rFrameName + "__" + newColName, newColName);
			metaData.setDataTypeToProperty(rFrameName + "__" + newColName, addedColumnDataType);
			metaData.setDerivedToProperty(rFrameName + "__" + newColName, true);
			frame.syncHeaders();
		} else {
			metaData.modifyDataTypeToProperty(rFrameName + "__" + srcCol, rFrameName, SemossDataType.STRING.toString());
		}
		return retNoun;
	}

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

	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(SIG_DIGITS)) {
			return "Indicates the number of significant digits you'd like to keep";
		} else if (key.equals(BY100)) {
			return "Indicates if you want to multiply by 100 to get in percent form.";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
