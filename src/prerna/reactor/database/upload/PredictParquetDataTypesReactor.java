package prerna.reactor.database.upload;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.poi.main.helper.ParquetFileHelper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.upload.UploadInputUtility;

public class PredictParquetDataTypesReactor extends AbstractReactor {
	
	private static final String CLASS_NAME = PredictParquetDataTypesReactor.class.getName();
	
	public PredictParquetDataTypesReactor() {
		this.keysToGet = new String[] { UploadInputUtility.FILE_PATH, UploadInputUtility.SPACE };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		Logger logger = getLogger(CLASS_NAME);
		logger.info("Extracting file headers and determinig data types");
		
		String filePath = UploadInputUtility.getFilePath(this.store, this.insight);
		if(!new File(filePath).exists()) {
			throw new IllegalArgumentException("Unable to locate file");
		}
		// check if file is valid
		if(!ParquetFileHelper.isParquetFile(filePath)) {
			NounMetadata error = new NounMetadata("Invalid file. Must be .parquet", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException e = new SemossPixelException(error);
			e.setContinueThreadOfExecution(false);
			throw e;
		}
		
		// store all the data in the map
		Map<String, Object> fileData = new HashMap<String, Object>();
	
		//ParquetFileHelper helper = new ParquetFileHelper();
		//helper.setLogger(logger);
		try {
			logger.info("Retrieving parquet file headers and data types");
			Map<String, SemossDataType> predictionMaps = ParquetFileHelper.getHeadersAndDataTypes(filePath);
			logger.info("Headers and Data Types retrieved.");
			fileData.put("headers", predictionMaps.keySet());
			fileData.put("cleanHeaders", predictionMaps.keySet());
			fileData.put("dataTypes", predictionMaps);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error("Unable to read parquet file");
			logger.error(e.toString());
			throw new IllegalArgumentException(e);
		}
		return new NounMetadata(fileData, PixelDataType.MAP);
	}

}
