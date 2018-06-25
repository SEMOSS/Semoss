package prerna.sablecc2.reactor.app.upload.gremlin;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import prerna.ds.datastax.DataStaxGraphEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
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

public class CreateExternalDSEGraphDBReactor extends AbstractReactor {

	private static final String CLASS_NAME = CreateExternalDSEGraphDBReactor.class.getName();

	public CreateExternalDSEGraphDBReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.HOST.getKey(),
				ReactorKeysEnum.PORT.getKey(), ReactorKeysEnum.USERNAME.getKey(), ReactorKeysEnum.PASSWORD.getKey(),
				ReactorKeysEnum.GRAPH_NAME.getKey(), ReactorKeysEnum.GRAPH_TYPE_ID.getKey(), ReactorKeysEnum.GRAPH_NAME_ID.getKey(),
				ReactorKeysEnum.GRAPH_METAMODEL.getKey() };
	}

	@Override
	public NounMetadata execute() {
		String newAppId = UUID.randomUUID().toString();

		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();
		String newAppName = this.keyValue.get(this.keysToGet[0]).trim().replaceAll("\\s+", "_");;
		if (newAppName == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires database name to save.", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		String host = this.keyValue.get(this.keysToGet[1]);
		if (host == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires host to save.", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		String port = this.keyValue.get(this.keysToGet[2]);
		if (port == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires port to save.", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		String username = this.keyValue.get(this.keysToGet[3]);
		String password = this.keyValue.get(this.keysToGet[4]);
		String graphName = this.keyValue.get(this.keysToGet[5]);
		if (graphName == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires graph name to save.", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		String graphTypeId = this.keyValue.get(this.keysToGet[6]);
		if (graphTypeId == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires graph type id to save.", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		String graphNameId = this.keyValue.get(this.keysToGet[7]);
		if (graphNameId == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires graph name id to save.", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		// meta model
		GenRowStruct grs = this.store.getNoun(keysToGet[8]);
		Map<String, Object> metaMap = null;
		if(grs != null && !grs.isEmpty()) {
			metaMap = (Map<String, Object>) grs.get(0);
		}

		// grab metadata
		Map<String, Object> nodes = (Map<String, Object>) metaMap.get("nodes");
		Map<String, Object> edges = (Map<String, Object>) metaMap.get("edges");
		Set<String> concepts = nodes.keySet();
		Map<String, String> conceptTypes = new HashMap<String, String>();
		Set<String> edgeLabels = edges.keySet();

		Map<String, String> typeMap = new HashMap<String, String>();
		Map<String, String> nameMap = new HashMap<String, String>();
		// create typeMap for smms
		for (String concept : concepts) {
			Map<String, Object> propMap = (Map) nodes.get(concept);
			for (String prop : propMap.keySet()) {
				// get concept type
				if (prop.equals(graphTypeId)) {
					conceptTypes.put(concept, propMap.get(graphTypeId).toString());
					typeMap.put(concept, graphTypeId);
					nameMap.put(concept,  graphNameId);
					break;
				}
			}
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
			tempSmss = UploadUtilities.generateTemporaryDatastaxSmss(newAppId, newAppName, owlFile, host, port, username, password, graphName, typeMap, nameMap);
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
				if (!prop.equals(graphTypeId)) {
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
		insightDatabase.closeDB();
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

		DataStaxGraphEngine dseEngine = new DataStaxGraphEngine();
		dseEngine.setEngineId(newAppId);
		dseEngine.setEngineName(newAppName);
		dseEngine.openDB(smssFile.getAbsolutePath());

		// only at end do we add to DIHelper
		DIHelper.getInstance().setLocalProperty(newAppId, dseEngine);
		String appNames = (String) DIHelper.getInstance().getLocalProp(Constants.ENGINES);
		appNames = appNames + ";" + newAppId;
		DIHelper.getInstance().setLocalProperty(Constants.ENGINES, appNames);

		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

}
