package prerna.reactor.database.upload.r;

import java.io.File;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import net.snowflake.client.jdbc.internal.apache.commons.io.FilenameUtils;
import prerna.algorithm.api.SemossDataType;
import prerna.auth.User;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.engine.impl.r.RNativeEngine;
import prerna.poi.main.RDBMSEngineCreationHelper;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.reactor.database.upload.AbstractUploadFileReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.upload.UploadInputUtility;
import prerna.util.upload.UploadUtilities;

public class RCsvUploadReactor extends AbstractUploadFileReactor {
	
	private CSVFileHelper helper;

	public RCsvUploadReactor() {
		this.keysToGet = new String[] { UploadInputUtility.DATABASE, UploadInputUtility.FILE_PATH,
				UploadInputUtility.DELIMITER, UploadInputUtility.DATA_TYPE_MAP, UploadInputUtility.NEW_HEADERS,
				UploadInputUtility.ADDITIONAL_DATA_TYPES};
	}

	@Override
	public void generateNewDatabase(User user, String newDatabaseName, String filePath) throws Exception {
		// grab inputs passed in
		final String delimiter = UploadInputUtility.getDelimiter(this.store);
		Map<String, String> dataTypesMap = UploadInputUtility.getCsvDataTypeMap(this.store);
		Map<String, String> newHeaders = UploadInputUtility.getNewCsvHeaders(this.store);
		Map<String, String> additionalDataTypeMap = UploadInputUtility.getAdditionalCsvDataTypes(this.store);
		File uploadFile = new File(filePath);
		String fileName = FilenameUtils.getBaseName(filePath);
		// TODO do we still need this????
		if (fileName.contains("_____UNIQUE")) {
			// ... yeah, this is not intuitive at all,
			// but I add a timestamp at the end to make sure every file is unique
			// but i want to remove it so things are "pretty"
			fileName = fileName.substring(0, fileName.indexOf("_____UNIQUE"));
		}
		
		int stepCounter = 1;
		logger.info(stepCounter + ". Create smss file for database...");
		File owlFile = UploadUtilities.generateOwlFile(this.databaseId, newDatabaseName);
		this.tempSmss = UploadUtilities.createTemporaryRSmss(this.databaseId, newDatabaseName, owlFile, fileName, newHeaders, dataTypesMap, additionalDataTypeMap);
		DIHelper.getInstance().setEngineProperty(this.databaseId + "_" + Constants.STORE, this.tempSmss.getAbsolutePath());
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
		

		logger.info(stepCounter + ". Start generating database metadata");
		WriteOWLEngine owlEngine = this.database.getOWLEngineFactory().getWriteOWL();

		// table name is the file name
		String tableName = RDBMSEngineCreationHelper.cleanTableName(fileName).toUpperCase();
		// add the table
		owlEngine.addConcept(tableName, null, null);
		// add the props
		for (int i = 0; i < headers.length; i++) {
			owlEngine.addProp(tableName, headers[i], types[i].toString(), additionalTypes[i]);
		}
		// add descriptions and logical names
		UploadUtilities.insertFlatOwlMetadata(owlEngine, tableName, headers, UploadInputUtility.getCsvDescriptions(this.store), UploadInputUtility.getCsvLogicalNames(this.store));
		owlEngine.commit();
		owlEngine.export();
		owlEngine.close();
		logger.info(stepCounter + ". Complete");
		stepCounter++;
		
		// move file
		File dataFile = SmssUtilities.getDataFile(Utility.loadProperties(this.tempSmss.getAbsolutePath()));
		FileUtils.copyFile(uploadFile, dataFile);
		
		logger.info(stepCounter + ". Create database store...");
		this.database = new RNativeEngine();
		this.database.open(this.tempSmss.getAbsolutePath());
		logger.info(stepCounter + ". Complete");
		stepCounter++;
	}

	@Override
	public void addToExistingDatabase(String filePath) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void closeFileHelpers() {
		if (this.helper != null) {
			this.helper.clear();
		}
	}

}
