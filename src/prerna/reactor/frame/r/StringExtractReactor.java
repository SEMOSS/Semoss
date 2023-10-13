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
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class StringExtractReactor extends AbstractRFrameReactor {

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
		init();
		organizeKeys();
		// get frame
		RDataTable frame = (RDataTable) getFrame();
		OwlTemporalEngineMeta metaData = frame.getMetaData();

		
		String frameName = frame.getName();
		String table = frameName;
				
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
		
		String dataType = metaData.getHeaderTypeAsString(table + "__" + srcCol);
		if(dataType == null)
			return getWarning("Frame is out of sync / No Such Column. Cannot perform this operation");


		// make sure all inputs are valid/present
		String[] startingColumns = getColumns(frameName);
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

		// build and run r script
		StringBuilder script = new StringBuilder();
		script.append(frameName).append("$");
		if (replace) {
			script.append(srcCol);
		} else {
			script.append(newColName);
		}
		script.append(" <- str_sub(").append(frameName).append("$").append(srcCol).append(", ");
		if (option.equals(KEEP)) {
			if (where.equals(LEFT)) {
				script.append("1, ").append(amount);
			} else if (where.equals(RIGHT)) {
				script.append("-").append(amount);
			}
		} else if (option.equals(REMOVE)) {
			if (where.equals(LEFT)) {
				script.append(amount + 1).append(", nchar(").append(frameName).append("$").append(srcCol).append(")");
			} else if (where.equals(RIGHT)) {
				script.append("1").append(", nchar(").append(frameName).append("$").append(srcCol).append(") - ").append(amount);
			}
		}
		script.append(");");
		this.rJavaTranslator.runR(script.toString());
		this.addExecutedCode(script.toString());

		// if not replacing vals (creating a new col, update metadata)
		if (!replace) {
			// check if new column exists
			String colExistsScript = "\"" + newColName + "\" %in% colnames(" + frameName + ")";
			boolean colExists = this.rJavaTranslator.getBoolean(colExistsScript);
			if (!colExists) {
				NounMetadata error = NounMetadata.getErrorNounMessage("Unable to perform string extraction");
				SemossPixelException exception = new SemossPixelException(error);
				exception.setContinueThreadOfExecution(false);
				throw exception;
			}
			metaData.addProperty(frameName, frameName + "__" + newColName);
			metaData.setAliasToProperty(frameName + "__" + newColName, newColName);
			metaData.setDataTypeToProperty(frameName + "__" + newColName, SemossDataType.STRING.toString());
			metaData.setDerivedToProperty(frameName + "__" + newColName, true);
			frame.syncHeaders();
		}

		// create retNoun and if not replaceing vals (creating a new col, add
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
