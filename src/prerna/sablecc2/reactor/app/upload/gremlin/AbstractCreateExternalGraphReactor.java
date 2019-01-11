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

public abstract class AbstractCreateExternalGraphReactor extends AbstractReactor {

	protected static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	
	private Logger logger;

	protected transient String newAppId;
	protected transient String newAppName;
	protected transient IEngine engine;
	protected transient File appFolder;
	protected transient File tempSmss;
	protected transient File smssFile;
	
	protected Map<String, String> typeMap = new HashMap<String, String>();
	protected Map<String, String> nameMap = new HashMap<String, String>();
	
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
		
		this.logger = getLogger(this.getClass().getName());
		this.newAppId = UUID.randomUUID().toString();
		this.newAppName = this.keyValue.get(ReactorKeysEnum.APP.getKey()).trim().replaceAll("\\s+", "_");
		
		boolean error = false;
		try {
			// this does the app specific load
			generateNewApp(user);
			
			// this handles the other metadata aspects
			DIHelper.getInstance().getCoreProp().setProperty(this.newAppId + "_" + Constants.STORE, this.smssFile.getAbsolutePath());
			Utility.synchronizeEngineMetadata(this.newAppId);

			// generate the actual engine
			this.engine = generateEngine();

			// only at end do we add to DIHelper
			DIHelper.getInstance().setLocalProperty(this.newAppId, this.engine);
			String appNames = (String) DIHelper.getInstance().getLocalProp(Constants.ENGINES);
			appNames = appNames + ";" + this.newAppId;
			DIHelper.getInstance().setLocalProperty(Constants.ENGINES, appNames);

			// even if no security, just add user as engine owner
			if(user != null) {
				List<AuthProvider> logins = user.getLogins();
				for(AuthProvider ap : logins) {
					SecurityUpdateUtils.addEngineOwner(this.newAppId, user.getAccessToken(ap).getId());
				}
			}
			
			ClusterUtil.reactorPushApp(this.newAppId);
		} catch (Exception e) {
			e.printStackTrace();
			error = true;
			if (e instanceof SemossPixelException) {
				throw (SemossPixelException) e;
			} else {
				NounMetadata noun = new NounMetadata(e.getMessage(), PixelDataType.CONST_STRING, PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		} finally {
			if (error) {
				// need to delete everything...
				cleanUpCreateNewError();
			}
		}

		Map<String, Object> retMap = UploadUtilities.getAppReturnData(this.insight.getUser(), newAppId);
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}
	
	private void generateNewApp(User user) throws Exception {
		// start by validation
		logger.info("Start validating app");
		try {
			UploadUtilities.validateApp(user, this.newAppName, this.newAppId);
		} catch (IOException e) {
			throw new IllegalArgumentException(e.getMessage());
		}
		logger.info("Done validating app");

		logger.info("Starting app creation");

		logger.info("1. Start generating app folder");
		this.appFolder = UploadUtilities.generateAppFolder(this.newAppId, this.newAppName);
		logger.info("1. Complete");
		
		logger.info("Generate new app database");
		logger.info("2. Create metadata for database...");
		File owlFile = UploadUtilities.generateOwlFile(this.newAppId, this.newAppName);
		logger.info("2. Complete");

		////////////////////////////////
		
		/*
		 * This is the portion where we validate user input
		 * And parse through the graph metadata
		 * 
		 * NOTE ::: Nothing before this step should use the typeMap or the nameMap
		 */
		
		validateUserInput();
		String nodeType = this.keyValue.get(ReactorKeysEnum.GRAPH_TYPE_ID.getKey());
		if (nodeType == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires graph type id to save.", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		String nodeName = this.keyValue.get(ReactorKeysEnum.GRAPH_NAME_ID.getKey());
		if (nodeName == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires graph name id to save.", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		// get metamodel
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.GRAPH_METAMODEL.getKey());
		Map<String, Object> metaMap = null;
		if(grs != null && !grs.isEmpty()) {
			metaMap = (Map<String, Object>) grs.get(0);
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

		this.typeMap = new HashMap<String, String>();
		this.nameMap = new HashMap<String, String>();
		// create typeMap for smms
		for (String concept : concepts) {
			Map<String, Object> propMap = (Map) nodes.get(concept);
			for (String prop : propMap.keySet()) {
				// get concept type
				if (prop.equals(nodeType)) {
					conceptTypes.put(concept, propMap.get(nodeType).toString());
					this.typeMap.put(concept, nodeType);
					this.nameMap.put(concept,  nodeName);
					break;
				}
			}
		}
		
		/*
		 * End parsing metadata portion
		 * 
		 */
		
		logger.info("3. Create properties file for database...");
		this.tempSmss = null;
		try {
			this.tempSmss = generateTempSmss(owlFile);
			DIHelper.getInstance().getCoreProp().setProperty(this.newAppId + "_" + Constants.STORE, tempSmss.getAbsolutePath());
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
		// note, on engine creation, we auto create an insights database + add explore an instance
		// TODO: should add some new ones...
		logger.info("5. Complete");

		logger.info("6. Process app metadata to allow for traversing across apps	");
		try {
			UploadUtilities.updateMetadata(this.newAppId);
		} catch (Exception e) {
			e.printStackTrace();
		}
		logger.info("6. Complete");

		// rename .temp to .smss
		this.smssFile = new File(this.tempSmss.getAbsolutePath().replace(".temp", ".smss"));
		try {
			FileUtils.copyFile(this.tempSmss, this.smssFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.tempSmss.delete();
	}

	/**
	 * Delete all the corresponding files that are generated from the upload the failed
	 */
	private void cleanUpCreateNewError() {
		// TODO:clean up DIHelper!
		try {
			// close the DB so we can delete it
			if (this.engine != null) {
				engine.closeDB();
			}

			// delete the .temp file
			if (this.tempSmss != null && this.tempSmss.exists()) {
				FileUtils.forceDelete(this.tempSmss);
			}
			// delete the .smss file
			if (this.smssFile != null && this.smssFile.exists()) {
				FileUtils.forceDelete(this.smssFile);
			}
			// delete the engine folder and all its contents
			if (this.appFolder != null && this.appFolder.exists()) {
				File[] files = this.appFolder.listFiles();
				if (files != null) { // some JVMs return null for empty dirs
					for (File f : files) {
						FileUtils.forceDelete(f);
					}
				}
				FileUtils.forceDelete(this.appFolder);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	///////////////////////////////////////////////////////
	
	/*
	* Execution methods
	* This will be done by every implementation
	*/
	
	protected abstract void validateUserInput() throws IOException;
	
	protected abstract File generateTempSmss(File owlFile) throws IOException;
	
	protected abstract IEngine generateEngine();
	
}
