package prerna.reactor.database.upload.gremlin;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.upload.UploadUtilities;

public abstract class AbstractCreateExternalGraphReactor extends AbstractReactor {

	protected static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	
	private Logger classLogger;

	protected transient String newDatabaseId;
	protected transient String newDatabaseName;
	protected transient IDatabaseEngine database;
	protected transient File databaseFolder;
	protected transient File tempSmss;
	protected transient File smssFile;
	
	protected Map<String, String> typeMap = new HashMap<>();
	protected Map<String, String> nameMap = new HashMap<>();
	
	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		if(user == null) {
			NounMetadata noun = new NounMetadata("User must be signed into an account in order to create a database", PixelDataType.CONST_STRING, 
					PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}
		
		// throw error if user is anonymous
		if (AbstractSecurityUtils.anonymousUsersEnabled() && this.insight.getUser().isAnonymous()) {
			throwAnonymousUserError();
		}
		
		// throw error is user doesn't have rights to publish new databases
		if (AbstractSecurityUtils.adminSetPublisher() && !SecurityQueryUtils.userIsPublisher(this.insight.getUser())) {
			throwUserNotPublisherError();
		}
		
		if (AbstractSecurityUtils.adminOnlyEngineAdd() && !SecurityAdminUtils.userIsAdmin(user)) {
			throwFunctionalityOnlyExposedForAdminsError();
		}
		
		organizeKeys();
		
		this.classLogger = getLogger(this.getClass().getName());
		this.newDatabaseId = UUID.randomUUID().toString();
		this.newDatabaseName = this.keyValue.get(ReactorKeysEnum.DATABASE.getKey()).trim().replaceAll("\\s+", "_");
		
		boolean error = false;
		try {
			// this does the database specific load
			generateNewDatabase(user);
			
			// this handles the other metadata aspects
			DIHelper.getInstance().setEngineProperty(this.newDatabaseId + "_" + Constants.STORE, this.smssFile.getAbsolutePath());
			Utility.synchronizeEngineMetadata(this.newDatabaseId);

			// generate the actual engine
			this.database = generateEngine();

			// only at end do we add to DIHelper
			DIHelper.getInstance().setLocalProperty(this.newDatabaseId, this.database);
			String databaseNames = (String) DIHelper.getInstance().getLocalProp(Constants.ENGINES);
			databaseNames = databaseNames + ";" + this.newDatabaseId;
			DIHelper.getInstance().setLocalProperty(Constants.ENGINES, databaseNames);

			// even if no security, just add user as engine owner
			if(user != null) {
				List<AuthProvider> logins = user.getLogins();
				for(AuthProvider ap : logins) {
					SecurityEngineUtils.addEngineOwner(this.newDatabaseId, user.getAccessToken(ap).getId());
				}
			}
			
			ClusterUtil.pushEngine(this.newDatabaseId);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
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

		Map<String, Object> retMap = UploadUtilities.getEngineReturnData(this.insight.getUser(), newDatabaseId);
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}
	
	private void generateNewDatabase(User user) throws Exception {
		// start by validation
		classLogger.info("Start validating database");
		try {
			UploadUtilities.validateEngine(IEngine.CATALOG_TYPE.DATABASE, user, this.newDatabaseName, this.newDatabaseId);
		} catch (IOException e) {
			throw new IllegalArgumentException(e.getMessage());
		}
		classLogger.info("Done validating database");

		classLogger.info("Starting database creation");

		classLogger.info("1. Start generating database folder");
		this.databaseFolder = UploadUtilities.generateSpecificEngineFolder(IEngine.CATALOG_TYPE.DATABASE, this.newDatabaseId, this.newDatabaseName);
		classLogger.info("1. Complete");
		
		classLogger.info("Generate new database");
		classLogger.info("2. Create metadata for database...");
		File owlFile = UploadUtilities.generateOwlFile(this.newDatabaseId, this.newDatabaseName);
		classLogger.info("2. Complete");

		////////////////////////////////
		
		/*
		 * This is the portion where we validate user input
		 * And parse through the graph metadata
		 * 
		 * NOTE ::: Nothing before this step should use the typeMap or the nameMap
		 */
		
		validateUserInput();

		// check how to query the data using label or property name to get the type
		String nodeType = this.keyValue.get(ReactorKeysEnum.GRAPH_TYPE_ID.getKey());
		String nodeName = this.keyValue.get(ReactorKeysEnum.GRAPH_NAME_ID.getKey());
		boolean useLabel = useLabel();
		if (!useLabel && nodeType == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires graph type id to save.", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
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

		if (metaMap == null) {
			throw new NullPointerException("Must provide a graph metamodel to upload");
		}

		// grab metadata
		Map<String, Object> nodes = (Map<String, Object>) metaMap.get("nodes");
		Map<String, Object> edges = (Map<String, Object>) metaMap.get("edges");
		Set<String> concepts = nodes.keySet();
		Map<String, String> conceptTypes = new HashMap<>();
		Set<String> edgeLabels = new HashSet<>();
		if(edges != null) {
			edgeLabels = edges.keySet();
		}

		this.typeMap = new HashMap<>();
		this.nameMap = new HashMap<>();
		// create typeMap for smms
		for (String concept : concepts) {
			// if we use the label, we assue the concept is a string and only
			// need a name map
			if (useLabel) {
				conceptTypes.put(concept, SemossDataType.STRING.toString());
				this.nameMap.put(concept, nodeName);
			} else {
				// else we need to get type map
				Map<String, Object> propMap = (Map) nodes.get(concept);
				for (String prop : propMap.keySet()) {
					if (prop.equals(nodeType)) {
						conceptTypes.put(concept, propMap.get(nodeType).toString());
						this.typeMap.put(concept, nodeType);
						this.nameMap.put(concept, nodeName);
						break;
					}
				}
			}
		}
		
		/*
		 * End parsing metadata portion
		 * 
		 */
		
		classLogger.info("3. Create properties file for database...");
		this.tempSmss = null;
		try {
			this.tempSmss = generateTempSmss(owlFile);
			DIHelper.getInstance().setEngineProperty(this.newDatabaseId + "_" + Constants.STORE, tempSmss.getAbsolutePath());
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(e.getMessage());
		}
		classLogger.info("3. Complete");

		// create owl file
		classLogger.info("4. Start generating engine metadata...");
		WriteOWLEngine owlEngine = this.database.getOWLEngineFactory().getWriteOWL();
		// add concepts
		for (String concept : concepts) {
			String conceptType = conceptTypes.get(concept);
			owlEngine.addConcept(concept, conceptType);
			Map<String, Object> propMap = (Map<String, Object>) nodes.get(concept);
			// add properties
			for (String prop : propMap.keySet()) {
				if (!prop.equals(nodeType) && !prop.equals(nodeName)) {
					String propType = propMap.get(prop).toString();
					owlEngine.addProp(concept, prop, propType);
				}
			}
		}
		// add relationships
		for(String label : edgeLabels) {
			List<String> rels = (List<String>) edges.get(label);
			owlEngine.addRelation(rels.get(0), rels.get(1), label);
		}

		try {
			owlEngine.commit();
			owlEngine.export();
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			owlEngine.close();
		}
		classLogger.info("4. Complete");


		classLogger.info("5. Process database metadata to allow for traversing across databases	");
		try {
			UploadUtilities.updateMetadata(this.newDatabaseId, user);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		classLogger.info("5. Complete");

		// rename .temp to .smss
		this.smssFile = new File(this.tempSmss.getAbsolutePath().replace(".temp", ".smss"));
		try {
			FileUtils.copyFile(this.tempSmss, this.smssFile);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		this.tempSmss.delete();
		
		// adding all the git here
		// make a version folder if one doesn't exist
		/*
			String versionFolder = 	AssetUtility.getAppAssetVersionFolder(newDatabaseName, newDatabaseId);
			File file = new File(versionFolder);
			if(!file.exists())
				file.mkdir();
			// I will assume the directory is there now
			GitRepoUtils.init(versionFolder);
		*/
	}

	/**
	 * Delete all the corresponding files that are generated from the upload the failed
	 */
	private void cleanUpCreateNewError() {
		// TODO:clean up DIHelper!
		try {
			// close the DB so we can delete it
			if (this.database != null) {
				database.close();
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
			if (this.databaseFolder != null && this.databaseFolder.exists()) {
				File[] files = this.databaseFolder.listFiles();
				if (files != null) { // some JVMs return null for empty dirs
					for (File f : files) {
						FileUtils.forceDelete(f);
					}
				}
				FileUtils.forceDelete(this.databaseFolder);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	/**
	 * Query the external db with a label to get the node
	 * @return
	 */
	protected boolean useLabel() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.USE_LABEL.getKey());
		if(grs != null && !grs.isEmpty()) {
			return (boolean) grs.get(0);
		}
		
		return false;
	}
	
	///////////////////////////////////////////////////////
	
	/*
	* Execution methods
	* This will be done by every implementation
	*/
	
	protected abstract void validateUserInput() throws IOException;
	
	protected abstract File generateTempSmss(File owlFile) throws IOException;
	
	protected abstract IDatabaseEngine generateEngine() throws Exception;
	
}
