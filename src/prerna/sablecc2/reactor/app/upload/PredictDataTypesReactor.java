package prerna.sablecc2.reactor.app.upload;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.poi.main.helper.CSVFileHelper;
import prerna.poi.main.helper.FileHelperUtil;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class PredictDataTypesReactor extends AbstractReactor {
	protected static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	public PredictDataTypesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.DELIMITER.getKey(),
				ReactorKeysEnum.ROW_COUNT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String filePath = this.keyValue.get(this.keysToGet[0]);
		if (filePath == null) {
			NounMetadata noun = new NounMetadata("Need to define " + this.keysToGet[0], PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException exception = new SemossPixelException(noun);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		String delimiter = this.keyValue.get(this.keysToGet[1]);
		if (delimiter == null) {
			delimiter = ",";
		}
		boolean rowCount = getRowCount();

		char delim = delimiter.charAt(0);

		CSVFileHelper helper = new CSVFileHelper();
		helper.setDelimiter(delim);
		helper.parse(filePath);
		Map[] predictionMaps = FileHelperUtil.generateDataTypeMapsFromPrediction(helper.getHeaders(), helper.predictTypes());
		Map<String, Object> retMap = new HashMap<>();
		retMap.put("dataTypes", predictionMaps[0]);
		retMap.put("additionalTypes", predictionMaps[1]);
		if (rowCount) {
			// get the row count
			int count = 1;
			while ((helper.getNextRow()) != null) {
				count++;
			}
			retMap.put("endRow", count);
		}
		helper.clear();
		return new NounMetadata(retMap, PixelDataType.MAP);
	}

	/**
	 * Get the end row count from file
	 * 
	 * @return
	 */
	private boolean getRowCount() {
		GenRowStruct boolGrs = this.store.getNoun(this.keysToGet[2]);
		if (boolGrs != null) {
			if (boolGrs.size() > 0) {
				List<Object> val = boolGrs.getValuesOfType(PixelDataType.BOOLEAN);
				return (boolean) val.get(0);
			}
		}
		return false;
	}
}
