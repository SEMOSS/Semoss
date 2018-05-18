package prerna.sablecc2.reactor.app.upload.rdbms.excel;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.Insight;
import prerna.poi.main.helper.XLFileHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.PixelPlanner;
import prerna.sablecc2.reactor.app.upload.AbstractRdbmsUploadReactor;
import prerna.sablecc2.reactor.app.upload.UploadUtilities;
import prerna.test.TestUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class RdbmsFlatExcelUploadReactor extends AbstractRdbmsUploadReactor {
	
	/*
	 * There are quite a few things that we need
	 * 1) app -> name of the app to create
	 * 1) filePath -> string contianing the path of the file
	 * 2) dataTypes -> map of the sheet to another map of the header to the type, this will contain the original headers we send to FE
	 * 3) newHeaders -> map of the sheet to another map containing old header to new headers for the csv file
	 * 4) additionalTypes -> map of the sheet to another map containing header to an additional type specification
	 * 						additional inputs would be {header : currency, header : date_format, ... }
	 * 5) clean -> boolean if we should clean up the strings before insertion, default is true
	 * TODO: 6) deduplicate -> boolean if we should remove duplicate rows in the relational database
	 * 7) existing -> boolean if we should add to an existing app, defualt is false
	 */
	
	private static final String CLASS_NAME = RdbmsFlatExcelUploadReactor.class.getName();

	public RdbmsFlatExcelUploadReactor() {
		this.keysToGet = new String[]{APP, FILE_PATH, DATA_TYPE_MAP, NEW_HEADERS, 
				ADDITIONAL_TYPES, CLEAN_STRING_VALUES, REMOVE_DUPLICATE_ROWS, ADD_TO_EXISTING};
	}
	
	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);

		final String appName = getAppName();
		final boolean existing = getExisting();
		final String filePath = getFilePath();
		final File file = new File(filePath);
		if(!file.exists()) {
			throw new IllegalArgumentException("Could not find the file path specified");
		}
		
		if(existing) {
			addToExistingApp(appName, filePath, logger);
		} else {
			generateNewApp(appName, filePath, logger);
		}
		
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.MARKET_PLACE_ADDITION);
	}
	

	@Override
	public void addToExistingApp(String appName, String filePath, Logger logger) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void generateNewApp(String newAppName, String filePath, Logger logger) {
		/*
		 * Things we need to do
		 * 1) make directory
		 * 2) make owl
		 * 3) make temporary smss
		 * 4) make engine class
		 * 5) load actual data
		 * 6) load owl metadata
		 * 7) load default insights
		 * 8) add to localmaster and solr
		 */
		
		Map<String, Map<String, String>> dataTypesMap = getDataTypeMap();
		Map<String, Map<String, String>> newHeaders = getNewHeaders();
		Map<String, Map<String, String>> additionalDataTypeMap = getAdditionalTypes();
		final boolean clean = getClean();

		// now that I have everything, let us go through and insert

		// start by validation
		logger.info("Start validating app");
		try {
			UploadUtilities.validateApp(newAppName);
		} catch (IOException e) {
			throw new IllegalArgumentException(e.getMessage());
		}
		logger.info("Done validating app");

		logger.info("Starting app creation");

		logger.info("1. Start generating app folder");
		UploadUtilities.generateAppFolder(newAppName);
		logger.info("1. Complete");

		logger.info("Generate new app database");
		logger.info("2. Create metadata for database...");
		File owlFile = UploadUtilities.generateOwlFile(newAppName);
		logger.info("2. Complete");

		logger.info("3. Create properties file for database...");
		File tempSmss = null;
		try {
			tempSmss = UploadUtilities.createTemporaryRdbmsSmss(newAppName, owlFile, "H2_DB", null);
			DIHelper.getInstance().getCoreProp().setProperty(newAppName + "_" + Constants.STORE, tempSmss.getAbsolutePath());
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e.getMessage());
		}
		logger.info("3. Complete");

		logger.info("4. Create database store...");
		IEngine engine = new RDBMSNativeEngine();
		engine.setEngineName(newAppName);
		Properties props = Utility.loadProperties(tempSmss.getAbsolutePath());
		props.put("TEMP", true);
		engine.setProp(props);
		engine.openDB(null);
		logger.info("4. Complete");
		
		logger.info("5. Start loading data..");
		logger.info("Load excel file...");
		XLFileHelper helper = getHelper(filePath, newHeaders);
		logger.info("Done loading excel file");
		processExcelSheets(helper, dataTypesMap, additionalDataTypeMap, clean, logger); 
//		Object[] headerTypesArr = getHeadersAndTypes(helper, dataTypesMap, additionalDataTypeMap);
//		String[] headers = (String[]) headerTypesArr[0];
//		SemossDataType[] types = (SemossDataType[]) headerTypesArr[1];
//		String[] additionalTypes = (String[]) headerTypesArr[2];
//		logger.info("Done parsing file metadata");
		
	}

	/**
	 * Process all the excel sheets using the data type map
	 * @param helper
	 * @param dataTypesMap
	 * @param additionalDataTypeMap 
	 */
	private void processExcelSheets(XLFileHelper helper, Map<String, Map<String, String>> dataTypesMap, Map<String, Map<String, String>> additionalDataTypeMap, boolean clean, Logger logger) {
		if(dataTypesMap == null || dataTypesMap.isEmpty()) {
			String[] sheetNames = helper.getTables();
			for(String sheet : sheetNames) {
				String[] headers = helper.getHeaders(sheet);
				String[] types = helper.predictRowTypes(sheet);
				Map<String, String> innerMap = new HashMap<String, String>();
				int numHeaders = headers.length;
				for(int i = 0; i < numHeaders; i++) {
					innerMap.put(headers[i], types[i]);
				}
				dataTypesMap.put(sheet, innerMap);
			}
		}

		int counter = 1;
		int numSheets = dataTypesMap.keySet().size();
		logger.info("Start processing sheets. Total sheets to load = " + numSheets);
		for(String sheetname : dataTypesMap.keySet()) {
			logger.info("Start process sheet " + counter + " = "+ sheetname);
			
			logger.info("Start parsing sheet metadata");
			// get the sheet types
			Map<String, String> sheetDataTypesMap = dataTypesMap.get(sheetname);
			Map<String, String> sheetAdditionalDataTypesMap = null;
			if(additionalDataTypeMap != null & additionalDataTypeMap.containsKey(sheetname)) {
				sheetAdditionalDataTypesMap = additionalDataTypeMap.get(sheetname);
			}
			Object[] headerTypesArr = getHeadersAndTypes(helper, sheetname, sheetDataTypesMap, sheetAdditionalDataTypesMap);
			String[] headers = (String[]) headerTypesArr[0];
			SemossDataType[] types = (SemossDataType[]) headerTypesArr[1];
			String[] additionalTypes = (String[]) headerTypesArr[2];
			logger.info("Done parsing sheet metadata");
			
			
			
			logger.info("Done process sheet " + counter + " = "+ sheetname);
			counter++;
		}
	}

	private XLFileHelper getHelper(final String filePath, Map<String, Map<String, String>> newHeaders) {
		XLFileHelper xlHelper = new XLFileHelper();
		xlHelper.parse(filePath);
		
		// set the new headers we want
		if(newHeaders != null && !newHeaders.isEmpty()) {
			xlHelper.modifyCleanedHeaders(newHeaders);
		}

		return xlHelper;
	}
	
	/**
	 * Figure out the types and how to use them
	 * Will return an object[]
	 * Index 0 of the return is an array of the headers
	 * Index 1 of the return is an array of the types
	 * Index 2 of the return is an array of the additional type information
	 * The 3 arrays all match based on index
	 * @param helper
	 * @param dataTypesMap
	 * @param additionalDataTypeMap
	 * @return
	 */
	private Object[] getHeadersAndTypes(XLFileHelper helper, String sheetName, Map<String, String> dataTypesMap, Map<String, String> additionalDataTypeMap) {
		String[] headers = helper.getHeaders(sheetName);
		int numHeaders = headers.length;
		// we want types
		// and we want additional types
		SemossDataType[] types = new SemossDataType[numHeaders];
		String[] additionalTypes = new String[numHeaders];

		// get the types
		if(dataTypesMap != null && !dataTypesMap.isEmpty()) {
			for(int i = 0; i < numHeaders; i++) {
				types[i] = SemossDataType.convertStringToDataType(dataTypesMap.get(headers[i]));
			}
		} else {
			String[] predictedTypes = helper.predictRowTypes(sheetName);
			for(int i = 0; i < predictedTypes.length; i++) {
				types[i] = SemossDataType.convertStringToDataType(predictedTypes[i]);
			}
		}

		// get additional type information
		if(additionalDataTypeMap != null && !additionalDataTypeMap.isEmpty()) {
			for(int i = 0 ; i < numHeaders; i++) {
				additionalTypes[i] = additionalDataTypeMap.get(headers[i]);
			}
		}

		return new Object[]{headers, types, additionalTypes};
	}

	
	///////////////////////////////////////////////////////

	/*
	 * Getters from noun store
	 */

	private Map<String, Map<String, String>> getDataTypeMap() {
		GenRowStruct grs = this.store.getNoun(DATA_TYPE_MAP);
		if(grs == null || grs.isEmpty()) {
			return null;
		}
		return (Map<String, Map<String, String>>) grs.get(0);
	}

	private Map<String, Map<String, String>> getNewHeaders() {
		GenRowStruct grs = this.store.getNoun(NEW_HEADERS);
		if(grs == null || grs.isEmpty()) {
			return null;
		}
		return (Map<String, Map<String, String>>) grs.get(0);
	}

	private Map<String, Map<String, String>> getAdditionalTypes() {
		GenRowStruct grs = this.store.getNoun(ADDITIONAL_TYPES);
		if(grs == null || grs.isEmpty()) {
			return null;
		}
		return (Map<String, Map<String, String>>) grs.get(0);
	}
	
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////

	public static void main(String[] args) {
		TestUtilityMethods.loadDIHelper("C:\\workspace\\Semoss_Dev\\RDF_Map.prop");
		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\LocalMasterDatabase.smss";
		IEngine coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineName("LocalMasterDatabase");
		coreEngine.openDB(engineProp);
		coreEngine.setEngineName("LocalMasterDatabase");
		DIHelper.getInstance().setLocalProperty("LocalMasterDatabase", coreEngine);

		String filePath = "C:/Users/SEMOSS/Desktop/Movie Data.csv";

		Insight in = new Insight();
		PixelPlanner planner = new PixelPlanner();
		planner.setVarStore(in.getVarStore());
		in.getVarStore().put("$JOB_ID", new NounMetadata("test", PixelDataType.CONST_STRING));
		in.getVarStore().put("$INSIGHT_ID", new NounMetadata("test", PixelDataType.CONST_STRING));

		RdbmsFlatExcelUploadReactor reactor = new RdbmsFlatExcelUploadReactor();
		reactor.setInsight(in);
		reactor.setPixelPlanner(planner);
		NounStore nStore = reactor.getNounStore();
		// app name struct
		{
			GenRowStruct struct = new GenRowStruct();
			struct.add(new NounMetadata("ztest" + Utility.getRandomString(6), PixelDataType.CONST_STRING));
			nStore.addNoun(ReactorKeysEnum.APP.getKey(), struct);
		}
		// file path
		{
			GenRowStruct struct = new GenRowStruct();
			struct.add(new NounMetadata(filePath, PixelDataType.CONST_STRING));
			nStore.addNoun(ReactorKeysEnum.FILE_PATH.getKey(), struct);
		}

		reactor.In();
		reactor.execute();
	}
	
}
