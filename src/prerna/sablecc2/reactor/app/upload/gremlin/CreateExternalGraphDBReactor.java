package prerna.sablecc2.reactor.app.upload.gremlin;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.engine.impl.tinker.TinkerEngine;
import prerna.poi.main.helper.ImportOptions.TINKER_DRIVER;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.app.upload.UploadUtilities;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.OWLER;
import prerna.util.Utility;

public class CreateExternalGraphDBReactor extends AbstractReactor {

	private static final String CLASS_NAME = CreateExternalGraphDBReactor.class.getName();

	public CreateExternalGraphDBReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.FILE_PATH.getKey(),
				ReactorKeysEnum.GRAPH_TYPE_ID.getKey(), ReactorKeysEnum.GRAPH_NAME_ID.getKey(), 
				ReactorKeysEnum.GRAPH_METAMODEL.getKey() };
	}

	@Override
	public NounMetadata execute() {
		String newAppId = UUID.randomUUID().toString();

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
		
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();

		String newAppName = this.keyValue.get(this.keysToGet[0]).trim().replaceAll("\\s+", "_");
		String fileName = this.keyValue.get(this.keysToGet[1]);
		if (fileName == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires file name to save.", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		String nodeType = this.keyValue.get(this.keysToGet[2]);
		if (nodeType == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires graph type id to save.", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		String nodeName = this.keyValue.get(this.keysToGet[3]);
		if (nodeName == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires graph name id to save.", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		// get metamodel
		GenRowStruct grs = this.store.getNoun(keysToGet[4]);
		Map<String, Object> metaMap = null;
		if(grs != null && !grs.isEmpty()) {
			metaMap = (Map<String, Object>) grs.get(0);
		}
		TINKER_DRIVER tinkerDriver = TINKER_DRIVER.NEO4J;
		if (fileName.contains(".")) {
			String fileExtension = fileName.substring(fileName.indexOf(".") + 1);
			tinkerDriver = TINKER_DRIVER.valueOf(fileExtension.toUpperCase());
		}

		// grab metadata
		Map<String, Object> nodes = (Map<String, Object>) metaMap.get("nodes");
		Map<String, Object> edges = (Map<String, Object>) metaMap.get("edges");
		Set<String> concepts = nodes.keySet();
		Map<String, String> conceptTypes = new HashMap<String, String>();
		Set<String> edgeLabels = new HashSet<>();
		if(edges != null) {
			edgeLabels = edges.keySet();
		}

		Map<String, String> typeMap = new HashMap<String, String>();
		Map<String, String> nameMap = new HashMap<String, String>();
		// create typeMap for smms
		for (String concept : concepts) {
			Map<String, Object> propMap = (Map) nodes.get(concept);
			for (String prop : propMap.keySet()) {
				// get concept type
				if (prop.equals(nodeType)) {
					conceptTypes.put(concept, propMap.get(nodeType).toString());
					typeMap.put(concept, nodeType);
					nameMap.put(concept,  nodeName);
					break;
				}
			}
		}

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
			tempSmss = UploadUtilities.generateTemporaryTinkerSmss(newAppId, newAppName, owlFile, fileName, typeMap, nameMap, tinkerDriver);
			DIHelper.getInstance().getCoreProp().setProperty(newAppId + "_" + Constants.STORE, tempSmss.getAbsolutePath());
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e.getMessage());
		}
		logger.info("3. Complete");

		// create owl file
		logger.info("4. Start generating engine metadata...");
		OWLER owler = new OWLER(owlFile.getAbsolutePath(), ENGINE_TYPE.TINKER);
		// add concepts
		for (String concept : concepts) {
			String conceptType = conceptTypes.get(concept);
			owler.addConcept(concept, conceptType);
			Map<String, Object> propMap = (Map<String, Object>) nodes.get(concept);
			// add properties
			for (String prop : propMap.keySet()) {
				if (!prop.equals(nodeType)) {
					String propType = propMap.get(prop).toString();
					owler.addProp(concept, prop, propType);
				}
			}
		}
		// add relationships
		for(String label : edgeLabels) {
			List<String> rels = (List<String>) edges.get(label);
			owler.addRelation(rels.get(0), rels.get(1), null);
		}

		try {
			owler.commit();
			owler.export();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			owler.closeOwl();
		}
		logger.info("4. Complete");

		logger.info("5. Start generating default app insights");
		IEngine insightDatabase = UploadUtilities.generateInsightsDatabase(newAppId, newAppName);
		UploadUtilities.addExploreInstanceInsight(newAppId, insightDatabase);
		logger.info("5. Complete");

		logger.info("6. Process app metadata to allow for traversing across apps	");
		try {
			UploadUtilities.updateMetadata(newAppId);
		} catch (Exception e) {
			e.printStackTrace();
		}
		logger.info("6. Complete");

		// rename .temp to .smss
		File smssFile = new File(tempSmss.getAbsolutePath().replace(".temp", ".smss"));
		try {
			FileUtils.copyFile(tempSmss, smssFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
		tempSmss.delete();

		DIHelper.getInstance().getCoreProp().setProperty(newAppId + "_" + Constants.STORE, smssFile.getAbsolutePath());
		Utility.synchronizeEngineMetadata(newAppId);

		TinkerEngine tinkerEng = new TinkerEngine();
		tinkerEng.setEngineId(newAppId);
		tinkerEng.setEngineName(newAppName);
		tinkerEng.openDB(smssFile.getAbsolutePath());

		// only at end do we add to DIHelper
		DIHelper.getInstance().setLocalProperty(newAppId, tinkerEng);
		String appNames = (String) DIHelper.getInstance().getLocalProp(Constants.ENGINES);
		appNames = appNames + ";" + newAppId;
		DIHelper.getInstance().setLocalProperty(Constants.ENGINES, appNames);

		// even if no security, just add user as engine owner
		if(user != null) {
			List<AuthProvider> logins = user.getLogins();
			for(AuthProvider ap : logins) {
				SecurityUpdateUtils.addEngineOwner(newAppId, user.getAccessToken(ap).getId());
			}
		}
		
		ClusterUtil.reactorPushApp(newAppId);
		Map<String, Object> retMap = UploadUtilities.getAppReturnData(this.insight.getUser(), newAppId);
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}

}
