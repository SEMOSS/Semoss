package prerna.reactor.frame.r;

import java.util.List;
import java.util.Map;
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
import prerna.util.Utility;

public class ColumnAverageReactor extends AbstractRFrameReactor {

	/*
	 * Keys that can be passed in
	 */

	public ColumnAverageReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.NEW_COLUMN.getKey() };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		RDataTable frame = (RDataTable) getFrame();
		String frameName = frame.getName();
		List<String> columns = getCols(ReactorKeysEnum.COLUMNS.getKey());
		String newColName = this.keyValue.get(ReactorKeysEnum.NEW_COLUMN.getKey());
		newColName = getCleanNewColName(newColName);

		// build and run r script
		StringBuilder script = new StringBuilder();
		script.append(frameName).append("$").append(newColName).append(" <- round(((");
		int i;
		OwlTemporalEngineMeta metadata = frame.getMetaData();
		Map<String, SemossDataType> dataTypeMap = metadata.getHeaderToTypeMap();
		for (i = 0; i < columns.size() - 1; i++) {
			String column = columns.get(i);
			SemossDataType dataType = dataTypeMap.get(frameName + "__" + column);
			if (!Utility.isNumericType(dataType.toString())) {
				throw new IllegalArgumentException(column + " must be a numeric column");
			}
			script.append(frameName).append("$").append(column).append(" + ");
		}
		script.append(frameName).append("$").append(columns.get(i)).append(") / ");
		script.append(columns.size()).append("), digits = 2);");
		this.rJavaTranslator.runR(script.toString());
		this.addExecutedCode(script.toString());

		// check if new column exists
		String colExistsScript = "\"" + newColName + "\" %in% colnames(" + frameName + ")";
		boolean colExists = this.rJavaTranslator.getBoolean(colExistsScript);
		if (!colExists) {
			NounMetadata error = NounMetadata.getErrorNounMessage("Unable to perform average across columns");
			SemossPixelException exception = new SemossPixelException(error);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		// update meta data
		OwlTemporalEngineMeta metaData = frame.getMetaData();
		metaData.addProperty(frameName, frameName + "__" + newColName);
		metaData.setAliasToProperty(frameName + "__" + newColName, newColName);
		metaData.setDataTypeToProperty(frameName + "__" + newColName, SemossDataType.DOUBLE.toString());
		metaData.setDerivedToProperty(frameName + "__" + newColName, true);
		frame.syncHeaders();

		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE,PixelOperationType.FRAME_DATA_CHANGE);
		retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully performed average across columns."));
		return retNoun;
	}

	private List<String> getCols(String key) {
		List<String> columnsList = new Vector<String>();
		GenRowStruct grs = this.store.getNoun(key);
		if (grs == null || grs.isEmpty()) {
			throw new IllegalArgumentException("Please pass at least one numeric column.");
		}
		for (int i = 0; i < grs.size(); i++) {
			columnsList.add(grs.get(i).toString());
		}
		return columnsList;
	}
}
