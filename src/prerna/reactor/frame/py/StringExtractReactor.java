package prerna.reactor.frame.py;

import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class StringExtractReactor extends AbstractPyFrameReactor {

	/*
	 * here are the keys/options that can be passed into the reactor
	 */
	private static final String RIGHT = "right";
	private static final String LEFT = "left";
	private static final String KEEP = "keep";
	private static final String REMOVE = "remove";
	private static final String WHERE = "where";
	private static final String AMOUNT = "amount";
	private static final String OPTION = "option";

	public StringExtractReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), WHERE, OPTION, AMOUNT,
				ReactorKeysEnum.NEW_COLUMN.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();
		//get table name
		String wrapperFrameName = frame.getWrapperName();
		
		// variables of things passed into the reactor
		String srcCol = this.keyValue.get(ReactorKeysEnum.COLUMN.getKey());
		String where = this.keyValue.get(WHERE);
		int amount = getValue(AMOUNT);
		// this is to keep the amount passed or delete the amount passed
		String option = this.keyValue.get(OPTION);
		String newColName = this.keyValue.get(ReactorKeysEnum.NEW_COLUMN.getKey());
		boolean replace = true;
		if (newColName != null && !newColName.isEmpty()) {
			replace = false;
			newColName = getCleanNewColName(newColName);
		}

		// make sure all inputs are valid/present
		String[] startingColumns = getColNames(frame);
		List<String> startingColumnsList = new Vector<String>(startingColumns.length);
		startingColumnsList.addAll(Arrays.asList(startingColumns));
		if (srcCol == null || !startingColumnsList.contains(srcCol)) {
			throw new IllegalArgumentException("Need to define existing source column.");
		}
		if (where == null || (!where.equals(LEFT) && !where.equals(RIGHT))) {
			throw new IllegalArgumentException("Need to pass in 'where' to keep or remove characters (right or left)");
		}
		if (option == null || (!option.equals(KEEP) && !option.equals(REMOVE))) {
			throw new IllegalArgumentException(
					"Need to pass in 'option' - to keep or remove the designated characters");
		}

		// build script
		StringBuilder script = new StringBuilder();
		script.append(wrapperFrameName + ".string_trim_col('" + srcCol + "', '" + newColName + "', '");
		if (option.equals(KEEP)) {
			script.append("keep, ");
		} else if (option.equals(REMOVE)) {
			script.append("remove, ");
		}
		if (option.equals(LEFT)) {
			script.append("left, ");
		} else if (option.equals(RIGHT)) {
			script.append("right, ");
		}
		script.append(amount + ")");
		
		// run script
		frame.runScript(script.toString());
		this.addExecutedCode(script.toString());
		
		// if not replacing vals (creating a new col, update metadata)
		String frameName = frame.getName();
		if (!replace) {
			// check if new column exists
			String colExistsScript = "if '" + newColName + "' in " + frameName;
			boolean colExists = (boolean) frame.runScript(colExistsScript);
			if (!colExists) {
				NounMetadata error = NounMetadata.getErrorNounMessage("Unable to perform string extraction");
				SemossPixelException exception = new SemossPixelException(error);
				exception.setContinueThreadOfExecution(false);
				throw exception;
			}
			OwlTemporalEngineMeta metaData = frame.getMetaData();
			metaData.addProperty(frameName, frameName + "__" + newColName);
			metaData.setAliasToProperty(frameName + "__" + newColName, newColName);
			metaData.setDataTypeToProperty(frameName + "__" + newColName, SemossDataType.STRING.toString());
			metaData.setDerivedToProperty(frameName + "__" + newColName, true);
			frame.syncHeaders();
		}
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"StringExtract", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));

		// create retNoun and if not replacing vals (creating a new col, add
		// that pixeloptype)
		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
		if (!replace) {
			retNoun.addAdditionalOpTypes(PixelOperationType.FRAME_HEADERS_CHANGE);
		}
		retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully performed string extraction!"));
		return retNoun;
	}

	private int getValue(String key) {
		GenRowStruct grs = this.store.getNoun(key);
		if (grs == null || grs.isEmpty()) {
			throw new IllegalArgumentException("Need to define an amount to keep or remove.");
		}
		int value = (int) grs.getNoun(0).getValue();

		return value;
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(WHERE)) {
			return "Side of the string to work with (left/right)";
		} else if (key.equals(OPTION)) {
			return "Whether you want to keep or remove the characters (keep/remove)";
		} else if (key.equals(AMOUNT)) {
			return "Amount of characters to remove or keep";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}