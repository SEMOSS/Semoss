package prerna.sablecc2.reactor.app.upload;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.poi.main.RDBMSEngineCreationHelper;
import prerna.poi.main.helper.excel.ExcelValidationUtils;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.OWLER;
import prerna.util.Utility;

public class GenerateExcelFormApp extends AbstractReactor {
	private static final String CLASS_NAME = GenerateExcelFormApp.class.getName();
	
	public GenerateExcelFormApp() {
		this.keysToGet = new String[] { ReactorKeysEnum.APP.getKey(),  "formMap"};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		Logger logger = getLogger(CLASS_NAME);
		String newAppId = UUID.randomUUID().toString();
		String newAppName = this.keyValue.get(this.keysToGet[0]);
		GenRowStruct grs = this.store.getNoun(keysToGet[1]);
		Map<String, Object> formMap = null;
		if (grs != null && !grs.isEmpty()) {
			formMap = (Map<String, Object>) grs.get(0);
		} else {
			throw new IllegalArgumentException("Need to define " + "formMap");
		}

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
		UploadUtilities.generateAppFolder(newAppId, newAppName);
		logger.info("1. Complete");

		logger.info("Generate new app database");
		logger.info("2. Create metadata for database...");
		File owlFile = UploadUtilities.generateOwlFile(newAppId, newAppName);
		logger.info("2. Complete");

		logger.info("3. Create properties file for database...");
		File tempSmss = null;
		try {
			tempSmss = UploadUtilities.createTemporaryRdbmsSmss(newAppId, newAppName, owlFile, "H2_DB", null);
			DIHelper.getInstance().getCoreProp().setProperty(newAppId + "_" + Constants.STORE,
					tempSmss.getAbsolutePath());
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e.getMessage());
		}
		logger.info("3. Complete");

		logger.info("4. Create database store...");
		IEngine engine = new RDBMSNativeEngine();
		engine.setEngineId(newAppId);
		engine.setEngineName(newAppName);
		Properties props = Utility.loadProperties(tempSmss.getAbsolutePath());
		props.put("TEMP", true);
		engine.setProp(props);
		engine.openDB(null);
		logger.info("4. Complete");
		
		OWLER owler = new OWLER(owlFile.getAbsolutePath(), ENGINE_TYPE.RDBMS);

		// process form map to get sheet, headers, insert
		for(String sheetName: formMap.keySet()) {
			Map<String, Object> propMap = new HashMap<>();
			Map<String, Object> sheetMap = (Map<String, Object>) formMap.get(sheetName);
			String[] columns = new String[sheetMap.size()];
			String[] types = new String[sheetMap.size()];
			owler.addConcept(sheetName);
			int i = 0;
			for(String header: sheetMap.keySet()) {
				columns[i] = header;
				types[i]= "VARCHAR(800)";
				propMap.put(header, "VARCHAR(800)");
				i++;
				owler.addProp(sheetName, header, "STRING");
			}
			// insert sheet with headers
			String create = RdbmsQueryBuilder.makeOptionalCreate(sheetName, columns, types);
			engine.insertData(create);
			// create form meta table
			ExcelValidationUtils.insertValidationMap(engine, sheetName, sheetMap);
		}

		try {
			owler.export();
			engine.setOWL(owlFile.getPath());
			engine.commit();
		} catch (IOException e) {
			// ugh... gotta clean up and delete everything... TODO:
			e.printStackTrace();
		}
		logger.info("5. Complete");

		logger.info("6. Start generating default app insights");
		IEngine insightDatabase = UploadUtilities.generateInsightsDatabase(newAppId, newAppName);
		UploadUtilities.addExploreInstanceInsight(newAppId, insightDatabase);
		engine.setInsightDatabase(insightDatabase);
		RDBMSEngineCreationHelper.insertAllTablesAsInsights(engine);
		// insert form insights 
		for(String sheetName : formMap.keySet()) {
			Map<String, Object> sheetMap = (Map<String, Object>) formMap.get(sheetName);
			UploadUtilities.addInsertFormInsight(insightDatabase, newAppId, sheetName, sheetMap);
		}
		logger.info("6. Complete");

		logger.info("7. Process app metadata to allow for traversing across apps	");
		try {
			UploadUtilities.updateMetadata(newAppId);
		} catch (Exception e) {
			e.printStackTrace();
		}
		logger.info("7. Complete");

		// and rename .temp to .smss
		File smssFile = new File(tempSmss.getAbsolutePath().replace(".temp", ".smss"));
		try {
			FileUtils.copyFile(tempSmss, smssFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
		tempSmss.delete();

		// update DIHelper & engine smss file location
		engine.setPropFile(smssFile.getAbsolutePath());
		UploadUtilities.updateDIHelper(newAppId, newAppName, engine, smssFile);

		Map<String, Object> retMap = UploadUtilities.getAppReturnData(this.insight.getUser(), newAppId);
		return new NounMetadata(retMap, PixelDataType.MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}

}
