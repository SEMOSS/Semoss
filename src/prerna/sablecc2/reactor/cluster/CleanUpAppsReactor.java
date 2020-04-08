package prerna.sablecc2.reactor.cluster;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.auth.utils.SecurityUpdateUtils;
import prerna.cluster.util.CloudClient;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.nameserver.DeleteFromMasterDB;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class CleanUpAppsReactor extends AbstractReactor {
	private static final Logger logger = LogManager.getLogger(CleanUpAppsReactor.class.getName());
	private static final String CONFIGURATION_FILE = "config.properties";
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	
	public CleanUpAppsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PASSWORD.getKey(), ReactorKeysEnum.DRY_RUN.getKey(), ReactorKeysEnum.CLEAN_UP_CLOUD_STORAGE.getKey()};
	}
	//deploy test
	// This is just so we don't call this on mistake
	// The real security comes from security database
	private static final String PASSWORD = "clean_up_apps_reactor_password";
	
	@Override
	public NounMetadata execute() {
		organizeKeys();	
		if (this.keyValue.size() < 3) {
			throw new IllegalArgumentException("Must input three arguments");
		}

		String password = this.keyValue.get(ReactorKeysEnum.PASSWORD.getKey());
		String dryRunString = this.keyValue.get(ReactorKeysEnum.DRY_RUN.getKey());
		String cleanUpString = this.keyValue.get(ReactorKeysEnum.CLEAN_UP_CLOUD_STORAGE.getKey());
		String configPassword = null;

		try(InputStream input = new FileInputStream(DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + CONFIGURATION_FILE)) {
			Properties prop = new Properties();
			prop.load(input);
			configPassword = prop.getProperty(PASSWORD);
		} catch (IOException ex) {
			logger.error("Error with loading properties in config file" + ex.getMessage());
		}

		//////////////////////////////////////////////////////////////////////////////////////////
		///////////////////////////////// Security Checks ////////////////////////////////////////
		if(password == null || password.isEmpty()) {
			throw new IllegalArgumentException("Must input a password");
		}

		if(!password.equals(configPassword)) {
			throw new IllegalArgumentException("The provided password is not correct!");
		}

		boolean dryRun = true;
		if (dryRunString != null && !dryRunString.isEmpty() && dryRunString.equalsIgnoreCase("false")) {
			dryRun = false;
		}
		
		boolean cleanUpCloudStorage = false;
		if (cleanUpString != null && !cleanUpString.isEmpty() && cleanUpString.equalsIgnoreCase("true")) {
			cleanUpCloudStorage = true;
		}
		
		if(!AbstractSecurityUtils.securityEnabled()) {
			throw new IllegalArgumentException("Security must be enabled for this operation!");
		}

		boolean isAdmin = SecurityAdminUtils.userIsAdmin(this.insight.getUser());
		if(!isAdmin) {
			throw new IllegalArgumentException("User must be an admin for this operation!");
		}
		
		//////////////////////////////////////////////////////////////////////////////////////////
		//////////////////////////////////// Cleanup /////////////////////////////////////////////
		Map<String, Object> cleanupAppsData = new HashMap<>();
		cleanupAppsData.put("dryRun", dryRun);
		if (ClusterUtil.IS_CLUSTER) {
			
			//////////////////////////////////////////////////////////////////////////////////////////
			//////////////////////////////////// Cleanup Apps ////////////////////////////////////////
			Map<String, Object> removedAppsMap = new HashMap<>();
			List<String> appIds = SecurityQueryUtils.getEngineIds();
			for (String appId : appIds) {
				String alias = SecurityQueryUtils.getEngineAliasForId(appId);
				String key = alias + "__" + appId; 
				IEngine engine = null;
				try {
					engine = Utility.getEngine(appId);
				} catch (Exception e) {
					logger.error(e.getStackTrace());
				}
				if (engine == null) {
					
					// Cleanup the app
					if (!dryRun) {
						
						// Actually remove
						
						// Delete from master db
						DeleteFromMasterDB remover = new DeleteFromMasterDB();
						remover.deleteEngineRDBMS(appId);
						SecurityUpdateUtils.deleteApp(appId);
						
						// Delete from cluster
						try {
							CloudClient.getClient().deleteApp(appId);
							
							// Successful cleanup
							removedAppsMap.put(key, "removed");
						} catch (IOException | InterruptedException e) {
							logger.error(e.getStackTrace());
					
							// Partially successful cleanup
							removedAppsMap.put(key, "removed from security and local master, but failed to remove from cloud storage");
						}
					} else {
						// Don't actually remove
						removedAppsMap.put(key, "removed (dry run)");
					}
				} else {
					// Don't cleanup the app
					removedAppsMap.put(key, "preserved");
				}
			}
			cleanupAppsData.put("apps", removedAppsMap);
						
			//////////////////////////////////////////////////////////////////////////////////////////
			//////////////////////////////////// Cleanup Containers ///////////////////////////////////
			cleanupAppsData.put("cleanedUpCloudStorage", cleanUpCloudStorage);
			Map<String, Object> removedContainersMap = new HashMap<>();
			if (cleanUpCloudStorage) {
				try {
					List<String> allContainers = CloudClient.getClient().listAllBlobContainers();
					for (String container : allContainers) {
						String cleanedContainerName = container.replaceAll("-smss", "").replaceAll("/", "");
						//we now have configuration blobs like the image blob we dont want to delete
						if(ClusterUtil.CONFIGURATION_BLOBS.contains(cleanedContainerName)){
							continue;
						}
						if (!appIds.contains(cleanedContainerName)) {
							// Cleanup the container
							if (!dryRun) {
								// Actually remove
								try {
									CloudClient.getClient().deleteContainer(container);

									// Successful cleanup
									removedContainersMap.put(container, "removed");
								} catch (IOException | InterruptedException e) {
									logger.error(e.getStackTrace());

									// Unsuccessful cleanup
									removedContainersMap.put(container, "failed to remove");
								}
							} else {
								// Don't actually remove
								removedContainersMap.put(container, "removed (dry run)");
							}
						} else {
							// Don't cleanup the container
							removedContainersMap.put(container, "preserved");
						}
					}
				} catch (IOException | InterruptedException e) {
					logger.error(e.getStackTrace());

					// Error reading the cloud storage account
					removedContainersMap.put("error", "failed to list containers in cloud storage account");
				}
				cleanupAppsData.put("containers", removedContainersMap);
			}
		}
		return new NounMetadata(cleanupAppsData, PixelDataType.MAP, PixelOperationType.CLEANUP_APPS);
	}
}