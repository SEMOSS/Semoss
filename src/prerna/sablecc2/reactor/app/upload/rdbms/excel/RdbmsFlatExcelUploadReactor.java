package prerna.sablecc2.reactor.app.upload.rdbms.excel;

import java.io.File;
import java.util.Map;

import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.Insight;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.PixelPlanner;
import prerna.sablecc2.reactor.app.upload.AbstractRdbmsUploadReactor;
import prerna.test.TestUtilityMethods;
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

	private void generateNewApp(String appName, String filePath, Logger logger) {
		// TODO Auto-generated method stub
		
	}

	private void addToExistingApp(String appName, String filePath, Logger logger) {
		// TODO Auto-generated method stub
		
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
