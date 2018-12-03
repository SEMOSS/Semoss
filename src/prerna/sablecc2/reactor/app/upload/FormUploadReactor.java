package prerna.sablecc2.reactor.app.upload;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.auth.utils.SecurityUpdateUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.poi.main.RDBMSEngineCreationHelper;
import prerna.poi.main.helper.excel.ExcelDataValidationHelper;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.OWLER;
import prerna.util.Utility;

public class FormUploadReactor extends AbstractReactor {
	private static final String CLASS_NAME = FormUploadReactor.class.getName();
	
	public FormUploadReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.APP.getKey(),  "form", UploadInputUtility.ADD_TO_EXISTING};
	}

	@Override
	public NounMetadata execute() {
		User user = null;
		boolean security = AbstractSecurityUtils.securityEnabled();
		if(security) {
			user = this.insight.getUser();
			if(user == null) {
				NounMetadata noun = new NounMetadata("User must be signed into an account in order to create a database", PixelDataType.CONST_STRING, 
						PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
		final String appIdOrName = UploadInputUtility.getAppName(this.store);
		final boolean existing = UploadInputUtility.getExisting(this.store);
		String appId = null;
		
		GenRowStruct grs = this.store.getNoun(keysToGet[1]);
		Map<String, Object> form = null;
		if (grs != null && !grs.isEmpty()) {
			form = (Map<String, Object>) grs.get(0);
		} else {
			throw new IllegalArgumentException("Need to define " + "widgetJson");
		}
		if(existing) {
			
			if(security) {
				if(!SecurityQueryUtils.userCanEditEngine(user, appIdOrName)) {
					NounMetadata noun = new NounMetadata("User does not have sufficient priviledges to update the database", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
					SemossPixelException err = new SemossPixelException(noun);
					err.setContinueThreadOfExecution(false);
					throw err;
				}
			}
			
			appId = addToExistingApp(appIdOrName, form);
		} else {
			appId = generateNewApp(user, appIdOrName, form);
			
			// even if no security, just add user as engine owner
			if(user != null) {
				List<AuthProvider> logins = user.getLogins();
				for(AuthProvider ap : logins) {
					SecurityUpdateUtils.addEngineOwner(appId, user.getAccessToken(ap).getId());
				}
			}
		}

		ClusterUtil.reactorPushApp(appId);
		
		Map<String, Object> retMap = UploadUtilities.getAppReturnData(this.insight.getUser(),appId);
		return new NounMetadata(retMap, PixelDataType.MAP, PixelOperationType.MARKET_PLACE_ADDITION);
			
	}

	private String generateNewApp(User user, String newAppName, Map<String, Object> form) {
		Logger logger = getLogger(CLASS_NAME);
		String newAppId = UUID.randomUUID().toString();

		// start by validation
		logger.info("Start validating app");
		try {
			UploadUtilities.validateApp(user, newAppName, newAppId);
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

		// insert data from form
		insertForm(engine, owler, form);

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
		RDBMSEngineCreationHelper.insertAllTablesAsInsights(engine, owler);
		// insert form insights 
		for(String sheetName : form.keySet()) {
			Map<String, Object> sheetMap = (Map<String, Object>) form.get(sheetName);
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
		return newAppId;
	}



	private String addToExistingApp(String appIdOrName, Map<String, Object> form) {
		Logger logger = getLogger(CLASS_NAME);
		System.out.println(appIdOrName);
		String appId = MasterDatabaseUtility.testEngineIdIfAlias(appIdOrName);
		IEngine engine = Utility.getEngine(appId);
		if(engine == null) {
			throw new IllegalArgumentException("Couldn't find the app " + appId + " to append data into");
		}
		if(!(engine instanceof RDBMSNativeEngine)) {
			throw new IllegalArgumentException("App must be using a relational database");
		}		
		
		int stepCounter = 1;
		logger.info(stepCounter + ". Start loading form...");
		OWLER owler = new OWLER(engine, engine.getOWL());
		insertForm(engine, owler, form);
		try {
			owler.commit();
			owler.export();
			engine.setOWL(engine.getOWL());
			engine.commit();
		} catch (IOException e) {
			// ugh... gotta clean up and delete everything... TODO:
			e.printStackTrace();
		}
		logger.info(stepCounter + ". Complete");
		IEngine insightDatabase = engine.getInsightDatabase();
		// insert form insights 
		for(String sheetName : form.keySet()) {
			Map<String, Object> sheetMap = (Map<String, Object>) form.get(sheetName);
			UploadUtilities.addInsertFormInsight(insightDatabase, appId, sheetName, sheetMap);
		}

		logger.info(stepCounter + ". Process app metadata to allow for traversing across apps	");
		try {
			UploadUtilities.updateMetadata(appId);
		} catch (Exception e) {
			e.printStackTrace();
		}
		logger.info(stepCounter + ". Complete");
		
		return appId;
	}
	
	private void insertForm(IEngine engine, OWLER owler, Map<String, Object> form) {
		// process form map to get sheet, headers, insert
		for (String sheetName : form.keySet()) {
			Map<String, Object> json = (Map<String, Object>) form.get(sheetName);
			Map<String, Object> propMap = new HashMap<>();
			String pixel = (String) json.get("query");
			Object[] inputs = PixelUtility.getFormWidgetInputs(pixel);
			List<String> into = (List<String>) inputs[0];
			List<String> values = (List<String>) inputs[1];
			String[] columns = new String[into.size()+1];
			String[] types = new String[into.size()+1];
			owler.addConcept(sheetName);
			int i = 0;
			List<Map<String, Object>> paramList = (List<Map<String, Object>>) json.get("params");
			for (Map<String, Object> paramMap : paramList) {
				String param = (String) paramMap.get("paramName");
				// check index of param to get header
				if (values.contains(param)) {
					int index = values.indexOf(param);
					if (index >= 0) {
						String header = into.get(index);
						if (header.contains("__")) {
							String[] split = header.split("__");
							header = split[1];
						}
						columns[i] = header;
						// get data type
						Map<String, Object> viewMap = (Map<String, Object>) paramMap.get("view");
						String displayType = (String) viewMap.get("displayType");
						SemossDataType sType = ExcelDataValidationHelper.widgetComponentToDataType(
								ExcelDataValidationHelper.WIDGET_COMPONENT.valueOf(displayType.toUpperCase()));
						if (sType == SemossDataType.STRING || sType == SemossDataType.FACTOR) {
							types[i] = "VARCHAR(2000)";
						} else if (sType == SemossDataType.INT) {
							types[i] = "INT";
						} else if (sType == SemossDataType.DOUBLE) {
							types[i] = "DOUBLE";
						} else if (sType == SemossDataType.DATE) {
							types[i] = "DATE";
						} else if (sType == SemossDataType.TIMESTAMP) {
							types[i] = "TIMESTAMP ";
						}
						propMap.put(header, types[i]);
						i++;
						owler.addProp(sheetName, header, sType.toString());
					}
				}

			}
			// insert sheet with headers
			columns[i] = sheetName;
			types[i] = "IDENTITY";
			String create = RdbmsQueryBuilder.makeOptionalCreate(sheetName, columns, types);
			try {
				engine.insertData(create);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}		
	}

}
