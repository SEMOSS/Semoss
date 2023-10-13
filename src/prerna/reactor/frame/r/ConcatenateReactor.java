package prerna.reactor.frame.r;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.AddHeaderNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class ConcatenateReactor extends AbstractRFrameReactor {

	public ConcatenateReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DELIMITER.getKey(), ReactorKeysEnum.NEW_COLUMN.getKey(),
				ReactorKeysEnum.VALUES.getKey() };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		RDataTable rFrame = (RDataTable) getFrame();
		String frameName = rFrame.getName();
		String delim = this.keyValue.get(ReactorKeysEnum.DELIMITER.getKey());
		String newColName = this.keyValue.get(ReactorKeysEnum.NEW_COLUMN.getKey());
		if (newColName == null || newColName.isEmpty()) {
			throw new IllegalArgumentException("Need to define the new column name");
		}
		// clean the column name to ensure that it is valid
		newColName = getCleanNewColName(rFrame, newColName);

		GenRowStruct val_grs = this.store.getNoun(this.keysToGet[2]);
		StringBuilder rsb = new StringBuilder();
		for (int i = 0; i < val_grs.size(); i++) {
			NounMetadata noun = val_grs.getNoun(i);
			Object val = noun.getValue();
			if (noun.getNounType().equals(PixelDataType.COLUMN)) {
				rsb.append(frameName + "$" + val);
			} else {
				rsb.append(val);
			}
			if ((i + 1) != val_grs.size()) {
				rsb.append(", ");
			}
		}
		// Execute RSyntax
		// paste(val,val2,..., sep="delim")
		String rScript = frameName + "$" + newColName + " <- paste(" + rsb.toString() + ", sep=\"" + delim + "\")";
		this.rJavaTranslator.executeEmptyR(rScript);
		this.addExecutedCode(rScript);

		// check if new column exists
		String colExistsScript = "\"" + newColName + "\" %in% colnames(" + frameName + ")";
		boolean colExists = this.rJavaTranslator.getBoolean(colExistsScript);
		if (!colExists) {
			NounMetadata error = NounMetadata.getErrorNounMessage("Unable to Concatenate values");
			SemossPixelException exception = new SemossPixelException(error);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		// update the metadata to include this new column
		OwlTemporalEngineMeta metaData = this.getFrame().getMetaData();
		metaData.addProperty(frameName, frameName + "__" + newColName);
		metaData.setAliasToProperty(frameName + "__" + newColName, newColName);
		metaData.setDataTypeToProperty(frameName + "__" + newColName, SemossDataType.STRING.toString());
		rFrame.syncHeaders();

		UserTrackerFactory.getInstance().trackAnalyticsWidget(this.insight, rFrame, "Concatenate",
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));

		NounMetadata retNoun = new NounMetadata(rFrame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE, PixelOperationType.FRAME_DATA_CHANGE);
		retNoun.addAdditionalReturn(new AddHeaderNounMetadata(newColName));
		retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully Concatenated values into " + newColName));
		return retNoun;
	}

}
