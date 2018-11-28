package prerna.sablecc2.reactor.app.upload.rdbms.external;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityUpdateUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.rdbms.ImpalaEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.poi.main.RDBMSEngineCreationHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.app.upload.UploadInputUtility;
import prerna.sablecc2.reactor.app.upload.UploadUtilities;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.OWLER;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;

public class RdbmsExternalUploadReactor extends AbstractReactor {
	private static final String CLASS_NAME = RdbmsExternalUploadReactor.class.getName();

	public RdbmsExternalUploadReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DB_DRIVER_KEY.getKey(), ReactorKeysEnum.HOST.getKey(),
				ReactorKeysEnum.PORT.getKey(), ReactorKeysEnum.USERNAME.getKey(), ReactorKeysEnum.PASSWORD.getKey(),
				ReactorKeysEnum.SCHEMA.getKey(), ReactorKeysEnum.ADDITIONAL_CONNECTION_PARAMS_KEY.getKey(),
				ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.METAMODEL.getKey() };
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
		
		organizeKeys();
		final String newAppName = UploadInputUtility.getAppName(this.store);
		String appId = generateNewApp(user, newAppName);
		
		// even if no security, just add user as engine owner
		if(user != null) {
			List<AuthProvider> logins = user.getLogins();
			for(AuthProvider ap : logins) {
				SecurityUpdateUtils.addEngineOwner(appId, user.getAccessToken(ap).getId());
			}
		}
		
		ClusterUtil.reactorPushApp(appId);
		
		Map<String, Object> retMap = UploadUtilities.getAppReturnData(this.insight.getUser(),appId);
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}

	private String generateNewApp(User user, String newAppName) {
		Logger logger = getLogger(CLASS_NAME);
		String newAppId = UUID.randomUUID().toString();
		int stepCounter = 1;
		// start by validation
		logger.info(stepCounter + ".Start validating app");
		try {
			UploadUtilities.validateApp(user, newAppName);
		} catch (IOException e) {
			e.printStackTrace();
		}
		logger.info(stepCounter + ".Done validating app");
		stepCounter++;
		
		logger.info(stepCounter + ". Start generating app folder");
		UploadUtilities.generateAppFolder(newAppId, newAppName);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Create metadata for database...");
		File owlFile = UploadUtilities.generateOwlFile(newAppId, newAppName);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		// information for connection details
		String dbType = this.keyValue.get(this.keysToGet[0]);
		String host = this.keyValue.get(this.keysToGet[1]);
		String port = this.keyValue.get(this.keysToGet[2]);
		String username = this.keyValue.get(this.keysToGet[3]);
		String password = this.keyValue.get(this.keysToGet[4]);
		String schema = this.keyValue.get(this.keysToGet[5]);
		String additionalProperties = this.keyValue.get(this.keysToGet[6]);
		
		// the logical metamodel for the upload
		Map<String, Object> externalMetamodel = getMetamodel();
		Map<String, List<String>> nodesAndProps = (Map<String, List<String>>) externalMetamodel.get(ExternalJdbcSchemaReactor.TABLES_KEY);
		List<Map<String, Object>> relationships = (List<Map<String, Object>>) externalMetamodel.get(ExternalJdbcSchemaReactor.RELATIONS_KEY);
		logger.info(stepCounter + ". Create properties file for database...");
		File tempSmss = null;
		// Create default RDBMS engine or Impala
		String engineClassName = RDBMSNativeEngine.class.getName();
		IEngine engine = new RDBMSNativeEngine();
		if (dbType.toUpperCase().equals(RdbmsTypeEnum.IMPALA.getLabel())) {
			engineClassName = ImpalaEngine.class.getName();
			engine = new ImpalaEngine();
		}
		try {
			tempSmss = UploadUtilities.createTemporaryExternalRdbmsSmss(newAppId, newAppName, owlFile, engineClassName, dbType, host, port, schema, username, password, additionalProperties);
			DIHelper.getInstance().getCoreProp().setProperty(newAppId + "_" + Constants.STORE, tempSmss.getAbsolutePath());
		} catch (IOException | SQLException e) {
			e.printStackTrace();
		}
		logger.info(stepCounter + ". Complete");
		stepCounter++;
		
		logger.info(stepCounter + ". Create database store...");
		engine.setEngineId(newAppId);
		engine.setEngineName(newAppName);
		Properties prop = Utility.loadProperties(tempSmss.getAbsolutePath());
		prop.put("TEMP", "TRUE");
		// schema comes from existing db (connect to external db(schema))
		prop.put("SCHEMA", schema);
		((AbstractEngine) engine).setProp(prop);
		engine.openDB(null);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Start generating engine metadata...");
		OWLER owler = new OWLER(owlFile.getAbsolutePath(), engine.getEngineType());
		// get the existing datatypes
		// table names -> column name, column type
		Set<String> cleanTables = new HashSet<String>();
		for(String t : nodesAndProps.keySet()) {
			cleanTables.add(t.split("\\.")[0]);
		}
		Map<String, Map<String, String>> existingRDBMSStructure = RDBMSEngineCreationHelper.getExistingRDBMSStructure(engine, cleanTables);
		// parse the nodes and get the prim keys
		// and write to OWL
		Map<String, String> nodesAndPrimKeys = parseNodesAndProps(owler, nodesAndProps, existingRDBMSStructure);
		// parse the relationships
		// and write to OWL
		parseRelationships(owler, relationships, existingRDBMSStructure, nodesAndPrimKeys);
		// commit / save the owl
		owler.commit();
		try {
			owler.export();
			engine.setOWL(owler.getOwlPath());
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Start generating default app insights");
		IEngine insightDatabase = UploadUtilities.generateInsightsDatabase(newAppId, newAppName);
		UploadUtilities.addExploreInstanceInsight(newAppId, insightDatabase);
		engine.setInsightDatabase(insightDatabase);
		// generate base insights
		RDBMSEngineCreationHelper.insertAllTablesAsInsights(engine, owler);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Process app metadata to allow for traversing across apps	");
		try {
			UploadUtilities.updateMetadata(newAppId);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		logger.info("8. Complete");
		stepCounter++;

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
	
	/**
	 * Add the concepts and properties into the OWL
	 * @param owler
	 * @param nodesAndProps
	 * @param dataTypes
	 * @return
	 */
	private Map<String, String> parseNodesAndProps(OWLER owler, Map<String, List<String>> nodesAndProps, Map<String, Map<String, String>> dataTypes) {
		Map<String, String> nodesAndPrimKeys = new HashMap<String, String>(nodesAndProps.size());
		for (String node : nodesAndProps.keySet()) {
			String[] tableAndPrimaryKey = node.split("\\.");
			String nodeName = tableAndPrimaryKey[0];
			String primaryKey = tableAndPrimaryKey[1];
			nodesAndPrimKeys.put(nodeName, primaryKey);
			String cleanConceptTableName = RDBMSEngineCreationHelper.cleanTableName(nodeName);
			// add concepts
			owler.addConcept(cleanConceptTableName, primaryKey, dataTypes.get(nodeName).get(primaryKey));
			// add concept properties
			for (String prop : nodesAndProps.get(node)) {
				if (!prop.equals(primaryKey)) {
					String cleanProp = RDBMSEngineCreationHelper.cleanTableName(prop);
					owler.addProp(cleanConceptTableName, primaryKey, cleanProp, dataTypes.get(nodeName).get(prop), "");
				}
			}
		}
		return nodesAndPrimKeys;
	}
	
	/**
	 * Add the relationships into the OWL
	 * @param owler
	 * @param relationships
	 * @param dataTypes
	 * @param nodesAndPrimKeys
	 */
	private void parseRelationships(OWLER owler, List<Map<String, Object>> relationships, Map<String, Map<String, String>> dataTypes, Map<String, String> nodesAndPrimKeys) {
		for (Map relation : relationships) {
			String subject = RDBMSEngineCreationHelper.cleanTableName(relation.get(Constants.FROM_TABLE).toString());
			String object = RDBMSEngineCreationHelper.cleanTableName(relation.get(Constants.TO_TABLE).toString());
			// TODO: check if this needs to be cleaned
			String[] joinColumns = relation.get(Constants.REL_NAME).toString().split("\\.");
			// predicate is: "fromTable.fromJoinCol.toTable.toJoinCol"
			String predicate = subject + "." + joinColumns[0] + "." + object + "." + joinColumns[1];
			owler.addRelation(subject, nodesAndPrimKeys.get(subject), object, nodesAndPrimKeys.get(object), predicate);
		}
	}
	
	private Map<String, Object> getMetamodel() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.METAMODEL.getKey());
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		return (Map<String, Object>) grs.get(0);
	}

}
