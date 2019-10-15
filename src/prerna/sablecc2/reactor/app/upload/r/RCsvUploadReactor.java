package prerna.sablecc2.reactor.app.upload.r;

import java.io.File;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.User;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.engine.api.impl.util.Owler;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.r.RNativeEngine;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.sablecc2.reactor.app.upload.AbstractUploadFileReactor;
import prerna.sablecc2.reactor.app.upload.UploadInputUtility;
import prerna.sablecc2.reactor.app.upload.UploadUtilities;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class RCsvUploadReactor extends AbstractUploadFileReactor {
	private CSVFileHelper helper;

	public RCsvUploadReactor() {
		this.keysToGet = new String[] { UploadInputUtility.APP, UploadInputUtility.FILE_PATH,
				UploadInputUtility.DELIMITER, UploadInputUtility.DATA_TYPE_MAP, UploadInputUtility.NEW_HEADERS,
				UploadInputUtility.ADDITIONAL_DATA_TYPES};
	}

	@Override
	public void generateNewApp(User user, String newAppName, String filePath) throws Exception {
		// grab inputs passed in
		final String delimiter = UploadInputUtility.getDelimiter(this.store);
		Map<String, String> dataTypesMap = UploadInputUtility.getCsvDataTypeMap(this.store);
		Map<String, String> newHeaders = UploadInputUtility.getNewCsvHeaders(this.store);
		Map<String, String> additionalDataTypeMap = UploadInputUtility.getAdditionalCsvDataTypes(this.store);
		File uploadFile = new File(filePath);
		String fileName = uploadFile.getName();
		// TODO do we still need this????
		if (fileName.contains("_____UNIQUE")) {
			// ... yeah, this is not intuitive at all,
			// but I add a timestamp at the end to make sure every file is unique
			// but i want to remove it so things are "pretty"
			fileName = fileName.substring(0, fileName.indexOf("_____UNIQUE"));
		}
		
		int stepCounter = 1;
		logger.info(stepCounter + ". Create smss file for database...");
		File owlFile = UploadUtilities.generateOwlFile(this.appId, newAppName);
		this.tempSmss = UploadUtilities.createTemporaryRSmss(this.appId, newAppName, owlFile, fileName, newHeaders, dataTypesMap, additionalDataTypeMap);
		DIHelper.getInstance().getCoreProp().setProperty(this.appId + "_" + Constants.STORE, this.tempSmss.getAbsolutePath());
		logger.info(stepCounter + ". Complete");
		stepCounter++;
		
		logger.info(stepCounter + ". Parse data types...");
		this.helper = UploadUtilities.getHelper(filePath, delimiter, dataTypesMap, newHeaders);
		// parse the information
		Object[] headerTypesArr = UploadUtilities.getHeadersAndTypes(this.helper, dataTypesMap, additionalDataTypeMap);
		String[] headers = (String[]) headerTypesArr[0];
		SemossDataType[] types = (SemossDataType[]) headerTypesArr[1];
		String[] additionalTypes = (String[]) headerTypesArr[2];
		logger.info(stepCounter + ". Complete");
		stepCounter++;
		

		logger.info(stepCounter + ". Start generating engine metadata");
		Owler owler = new Owler(owlFile.getAbsolutePath(), ENGINE_TYPE.R);
		// table name is the file name
		String tableName = fileName.substring(0, fileName.indexOf("."));
		// add the table
		owler.addConcept(tableName, null, null);
		// add the props
		for (int i = 0; i < headers.length; i++) {
			owler.addProp(tableName, headers[i], types[i].toString(), additionalTypes[i]);
		}
		// add descriptions and logical names
		UploadUtilities.insertFlatOwlMetadata(owler, tableName, headers, UploadInputUtility.getCsvDescriptions(this.store), UploadInputUtility.getCsvLogicalNames(this.store));

		owler.commit();
		owler.export();
		logger.info(stepCounter + ". Complete");
		stepCounter++;
		
		// move file
		File dataFile = SmssUtilities.getDataFile(Utility.loadProperties(this.tempSmss.getAbsolutePath()));
		FileUtils.copyFile(uploadFile, dataFile);
		
		logger.info(stepCounter + ". Create database store...");
		this.engine = new RNativeEngine();
		this.engine.openDB(this.tempSmss.getAbsolutePath());
		logger.info(stepCounter + ". Complete");
		stepCounter++;
	}

	@Override
	public void addToExistingApp(String filePath) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void closeFileHelpers() {
		if (this.helper != null) {
			this.helper.clear();
		}
	}

}
