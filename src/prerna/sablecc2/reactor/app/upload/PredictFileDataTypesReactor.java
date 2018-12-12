package prerna.sablecc2.reactor.app.upload;

import java.util.HashMap;
import java.util.Map;

import prerna.poi.main.helper.CSVFileHelper;
import prerna.poi.main.helper.FileHelperUtil;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class PredictFileDataTypesReactor extends AbstractReactor {

	public PredictFileDataTypesReactor() {
		this.keysToGet = new String[] { UploadInputUtility.FILE_PATH, UploadInputUtility.DELIMITER, UploadInputUtility.ROW_COUNT };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String filePath = UploadInputUtility.getFilePath(this.store);
		String delimiter = UploadInputUtility.getDelimiter(this.store);
		char delim = delimiter.charAt(0);
		boolean rowCount = UploadInputUtility.getRowCount(this.store);
		CSVFileHelper helper = new CSVFileHelper();
		helper.setDelimiter(delim);
		helper.parse(filePath);
		Map[] predictionMaps = FileHelperUtil.generateDataTypeMapsFromPrediction(helper.getHeaders(), helper.predictTypes());
		Map<String, Object> retMap = new HashMap<>();
		retMap.put("headers", helper.getFileOriginalHeaders());
		retMap.put("cleanHeaders", helper.getHeaders());
		retMap.put("dataTypes", predictionMaps[0]);
		retMap.put("additionalDataTypes", predictionMaps[1]);
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
}
