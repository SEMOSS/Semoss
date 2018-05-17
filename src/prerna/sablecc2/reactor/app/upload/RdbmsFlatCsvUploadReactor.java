package prerna.sablecc2.reactor.app.upload;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.Insight;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.PixelPlanner;
import prerna.test.TestUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class RdbmsFlatCsvUploadReactor extends AbstractUploadReactor {

	/*
	 * There are quite a few things that we need
	 * 1) app -> name of the app to create
	 * 1) filePath -> string contianing the path of the file
	 * 2) delimiter -> delimiter for the file
	 * 3) dataTypes -> map of the header to the type, this will contain the original headers we send to FE
 	 * 4) newHeaders -> map containign old header to new headers for the csv file
	 * 5) additionalTypes -> map containing header to an additional type specification
	 * 						additional inputs would be {header : currency, header : date_format, ... }
	 * 6) clean -> boolean if we should clean up the strings before insertion, default is true
	 * 7) deduplicate -> boolean if we should remove duplicate rows in the relational database
	 */
	
	private static final String CLASS_NAME = RdbmsFlatCsvUploadReactor.class.getName();

	public RdbmsFlatCsvUploadReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.DELIMITER.getKey(), 
				ReactorKeysEnum.DATA_TYPE_MAP.getKey(), ReactorKeysEnum.NEW_HEADER_NAMES.getKey(), "additionalTypes", "clean", "deduplicate"};
	}

	@Override
	public NounMetadata execute() {
		String newAppName = getAppName();
		String filePath = getFilePath();
		String delimiter = getDelimiter();
		Map<String, String> dataTypesMap = getDataTypeMap();
		Map<String, String> newHeaders = getNewHeaders();
		Map<String, String> additionalDataTypeMap = getAdditionalTypes();
		boolean clean = getClean();
		boolean deduplicate = getDeduplicateRows();

		// now that I have everything, let us go through and just insert everything
		Logger logger = getLogger(CLASS_NAME);
		
		// start by validation
//		logger.info("Start validating app");
//		try {
//			UploadUtilities.validateApp(newAppName);
//		} catch (IOException e) {
//			throw new IllegalArgumentException(e.getMessage());
//		}
//		logger.info("Done validating app");
		
		/*
		 * Things we need to do
		 * 1) make directory
		 * 2) make engine
		 * 3) load data into engine
		 * 4) make insights database
		 * 5) make smss
		 * 6) load into local master
		 * 7) load into solr
		 */
		
		logger.info("Starting app creation");
		
		logger.info("Start generating app folder");
		UploadUtilities.generateAppFolder(newAppName);
		logger.info("Done generating app folder");

		logger.info("Generate new app database");
		logger.info("1. Create metadata for database...");
		File owlFile = UploadUtilities.generateOwlFile(newAppName);
		logger.info("1. Complete");
		logger.info("2. Create properties file for database...");
		File tempSmss = null;
		try {
			tempSmss = UploadUtilities.createTemporaryRdbmsSmss(newAppName, owlFile, "H2_DB", null);
			DIHelper.getInstance().getCoreProp().setProperty(newAppName + "_" + Constants.STORE, tempSmss.getAbsolutePath());
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e.getMessage());
		}
		logger.info("2. Complete");
		logger.info("3. Create database store...");
		RDBMSNativeEngine engine = new RDBMSNativeEngine();
		engine.setEngineName(newAppName);
		Properties props = Utility.loadProperties(tempSmss.getAbsolutePath());
		props.put("TEMP", true);
		engine.setProp(props);
		engine.openDB(null);
		logger.info("3. Complete");

//		logger.info("Start generating insights database");
//		IEngine insightDb = UploadUtilities.generateInsightsDatabase(newAppName);
//		logger.info("Done generating insights database");
//
//
//		logger.info("Start loading into solr");
//		try {
//			SolrUtility.addAppToSolr(newAppName);
//		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e) {
//			e.printStackTrace();
//		}
//		logger.info("Done loading into solr");
//
//		AppEngine appEng = new AppEngine();
//		appEng.setInsightDatabase(insightDb);
//		// only at end do we add to DIHelper
//		DIHelper.getInstance().setLocalProperty(newAppName, appEng);
//		String appNames = (String) DIHelper.getInstance().getLocalProp(Constants.ENGINES);
//		appNames = appNames + ";" + newAppName;
//		DIHelper.getInstance().setLocalProperty(Constants.ENGINES, appNames);
//		
//		// and rename .temp to .smss
//		File smssFile = new File(tempSmss.getAbsolutePath().replace(".temp", ".smss"));
//		try {
//			FileUtils.copyFile(tempSmss, smssFile);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		tempSmss.delete();
				
		
		return null;
	}
	
	///////////////////////////////////////////////////////
	
	/*
	 * Getters from noun store
	 */

	private String getAppName() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
		if(grs == null || grs.isEmpty()) {
			throw new IllegalArgumentException("Must define the new app name using key " + this.keysToGet[0]);
		}
		return grs.get(0).toString();
	}
	
	private String getFilePath() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[1]);
		if(grs == null || grs.isEmpty()) {
			throw new IllegalArgumentException("Must define the file path using key " + this.keysToGet[1]);
		}
		return grs.get(0).toString();
	}
	
	private String getDelimiter() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[2]);
		if(grs == null || grs.isEmpty()) {
			return ",";
		}
		return grs.get(0).toString();
	}

	private Map<String, String> getDataTypeMap() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[3]);
		if(grs == null || grs.isEmpty()) {
			return null;
		}
		return (Map<String, String>) grs.get(0);
	}
	
	private Map<String, String> getNewHeaders() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[4]);
		if(grs == null || grs.isEmpty()) {
			return null;
		}
		return (Map<String, String>) grs.get(0);
	}
	
	private Map<String, String> getAdditionalTypes() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[5]);
		if(grs == null || grs.isEmpty()) {
			return null;
		}
		return (Map<String, String>) grs.get(0);
	}
	
	private boolean getClean() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[6]);
		if(grs == null || grs.isEmpty()) {
			return true;
		}
		return (boolean) grs.get(0);
	}
	
	private boolean getDeduplicateRows() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[7]);
		if(grs == null || grs.isEmpty()) {
			return false;
		}
		return (boolean) grs.get(0);
	}
	
	
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////

	public static void main(String[] args) {
		TestUtilityMethods.loadDIHelper("C:\\workspace\\Semoss_Dev\\RDF_Map.prop");
		String filePath = "C:/Users/SEMOSS/Desktop/Movie Data.csv";
		
		Insight in = new Insight();
		PixelPlanner planner = new PixelPlanner();
		planner.setVarStore(in.getVarStore());
		in.getVarStore().put("$JOB_ID", new NounMetadata("test", PixelDataType.CONST_STRING));
		in.getVarStore().put("$INSIGHT_ID", new NounMetadata("test", PixelDataType.CONST_STRING));
		
		RdbmsFlatCsvUploadReactor reactor = new RdbmsFlatCsvUploadReactor();
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
