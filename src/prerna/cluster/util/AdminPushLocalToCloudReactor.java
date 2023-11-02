package prerna.cluster.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.clients.CentralCloudStorage;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class AdminPushLocalToCloudReactor extends AbstractReactor{

	private static final Logger classLogger = LogManager.getLogger(AdminPushLocalToCloudReactor.class);

	
	public AdminPushLocalToCloudReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DRY_RUN.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		boolean isAdmin = SecurityAdminUtils.userIsAdmin(this.insight.getUser());
		if(!isAdmin) {
			throw new IllegalArgumentException("User must be an admin for this operation!");
		}
		
		organizeKeys();
		boolean dryRun = true;
		String dryRunString = this.keyValue.get(ReactorKeysEnum.DRY_RUN.getKey());
		if (dryRunString != null && !dryRunString.isEmpty() && dryRunString.equalsIgnoreCase("false")) {
			dryRun = false;
		}
		
		Map<String, Object> pushedChanges = new HashMap<String, Object>();
		pushedChanges.put("dryRun", dryRun);
		
		// get all engines
		List<String> dbIds = SecurityEngineUtils.getAllEngineIds(Arrays.asList(IEngine.CATALOG_TYPE.DATABASE.name()));
		List<String> storageIds = SecurityEngineUtils.getAllEngineIds(Arrays.asList(IEngine.CATALOG_TYPE.STORAGE.name()));
		List<String> modelIds = SecurityEngineUtils.getAllEngineIds(Arrays.asList(IEngine.CATALOG_TYPE.MODEL.name()));
		List<String> vectorIds = SecurityEngineUtils.getAllEngineIds(Arrays.asList(IEngine.CATALOG_TYPE.VECTOR.name()));
		List<String> functionIds = SecurityEngineUtils.getAllEngineIds(Arrays.asList(IEngine.CATALOG_TYPE.FUNCTION.name()));
		List<String> projectIds = SecurityProjectUtils.getAllProjectIds();

		try {
			CentralCloudStorage cc = CentralCloudStorage.getInstance();
			Map<String, List<String>> currentContainers = cc.listAllContainersByBucket();
			// push the current files
			for(String key : currentContainers.keySet()) {
				pushedChanges.put(key, currentContainers.get(key));
			}
			
			removeExisitngIds(CentralCloudStorage.DATABASE_BLOB, currentContainers.get(CentralCloudStorage.DATABASE_BLOB), dbIds);
			removeExisitngIds(CentralCloudStorage.STORAGE_BLOB, currentContainers.get(CentralCloudStorage.STORAGE_BLOB), storageIds);
			removeExisitngIds(CentralCloudStorage.MODEL_BLOB, currentContainers.get(CentralCloudStorage.MODEL_BLOB), modelIds);
			removeExisitngIds(CentralCloudStorage.VECTOR_BLOB, currentContainers.get(CentralCloudStorage.VECTOR_BLOB), vectorIds);
			removeExisitngIds(CentralCloudStorage.FUNCTION_BLOB, currentContainers.get(CentralCloudStorage.FUNCTION_BLOB), functionIds);
			removeExisitngIds(CentralCloudStorage.PROJECT_BLOB, currentContainers.get(CentralCloudStorage.PROJECT_BLOB), projectIds);
			
			pushedChanges.put("added_dbIds", dbIds);
			pushedChanges.put("added_storageIds", storageIds);
			pushedChanges.put("added_modelIds", modelIds);
			pushedChanges.put("added_vectorIds", vectorIds);
			pushedChanges.put("added_functionIds", functionIds);
			pushedChanges.put("added_projectIds", projectIds);
			if(!dryRun) {
				for(String e : dbIds) {
					cc.pushEngine(e);
				}
				for(String e : storageIds) {
					cc.pushEngine(e);
				}
				for(String e : modelIds) {
					cc.pushEngine(e);
				}
				for(String e : vectorIds) {
					cc.pushEngine(e);
				}
				for(String e : functionIds) {
					cc.pushEngine(e);
				}
				for(String project : projectIds) {
					cc.pushProject(project);
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return new NounMetadata(pushedChanges, PixelDataType.MAP);
	}
	
	/**
	 * 
	 * @param cloudFiles
	 * @param startingList
	 */
	private void removeExisitngIds(String bucket, List<String> cloudFiles, List<String> startingList) {
		for(String cloudF : cloudFiles) {
			if(!cloudF.endsWith("/")) {
				classLogger.warn("For " + bucket + " there is a file that is not folder = " + cloudF);
				continue;
			}
			// smss folders end with / from list
			if(cloudF.endsWith(CentralCloudStorage.SMSS_POSTFIX+"/")) {
				continue;
			}
			
			cloudF = cloudF.substring(0, cloudF.length()-1);
			// remove it from the list of we have that do not sit in cloud
			startingList.remove(cloudF);
		}
	}

}
