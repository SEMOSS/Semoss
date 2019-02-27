package prerna.util.git.reactors;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.DIHelper;
import prerna.util.MosfetSyncHelper;
import prerna.util.Utility;
import prerna.util.git.GitSynchronizer;

public class SyncAppFilesOAuth extends GitBaseReactor {

	public SyncAppFilesOAuth() {
		this.keysToGet = new String[]{
				ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.REPOSITORY.getKey(), 
				ReactorKeysEnum.SYNC_PULL.getKey(), ReactorKeysEnum.SYNC_DATABASE.getKey(),
				"files"};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String appId = this.keyValue.get(this.keysToGet[0]);
		if(appId == null || appId.isEmpty()) {
			throw new IllegalArgumentException("Need to specify the app name");
		}
		String appName = null;
		
		// you can only push
		// if you are the owner
		if(AbstractSecurityUtils.securityEnabled()) {
			appId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), appId);
			if(!SecurityAppUtils.userCanEditEngine(this.insight.getUser(), appId)) {
				throw new IllegalArgumentException("App does not exist or user does not have access to edit database");
			}
			appName = SecurityQueryUtils.getEngineAliasForId(appId);
		} else {
			appName = MasterDatabaseUtility.getEngineAliasForId(appId);
		}
		
		String repository = this.keyValue.get(this.keysToGet[1]);
		String dualStr = this.keyValue.get(this.keysToGet[2]);
		String databaseStr = this.keyValue.get(this.keysToGet[3]);
		List<String> filesToSync = getFilesToSync();

		Logger logger = getLogger(this.getClass().getName());
		logger.info("Starting the synchronization process");

		// default for dual is true if nothing is sent it means it is dual
		boolean dual = false;
		if(dualStr == null || dualStr.equals("true")) {
			dual = true;
		}

		boolean database = false;
		if(databaseStr != null && databaseStr.equals("true")) {
			database = true;
		}
		
		String token = getToken();
		if(database) {
			try {
				logger.info("Synchronizing Database Now... ");
				// remove the app
				Utility.getEngine(appId).closeDB();
				DIHelper.getInstance().removeLocalProperty(appId);
				GitSynchronizer.syncDatabases(appId, appName, repository, token, logger);
				logger.info("Synchronize Database Complete");
			} finally {
				// open it back up
				Utility.getEngine(appId);
			}
		}

		// if it is null or true dont worry
		logger.info("Synchronizing now... ");
		Map<String, List<String>> filesChanged = GitSynchronizer.synchronizeSpecific(appId, appName, repository, token, filesToSync, dual);
		logger.info("Synchronize Complete");

		StringBuffer output = new StringBuffer("SUCCESS \r\n ");
		output.append("ADDED : ");
		if(filesChanged.containsKey("ADD")) {
			output.append(filesChanged.get("ADD").size());
		} else {
			output.append("0");
		}
		output.append(" , MODIFIED : ");
		if(filesChanged.containsKey("MOD")) {
			output.append(filesChanged.get("MOD").size());
		} else {
			output.append("0");
		}
		output.append(" , RENAMED : ");
		if(filesChanged.containsKey("REN")) {
			output.append(filesChanged.get("REN").size());
		} else {
			output.append("0");
		}
		output.append(" , DELETED : ");
		if(filesChanged.containsKey("DEL")) {
			output.append(filesChanged.get("DEL").size());
		} else {
			output.append("0");
		}

		// will update solr and in the engine rdbms insights database
		Map<String, List<String>> mosfetFiles = getMosfetFiles(filesChanged, filesToSync);
		if(!mosfetFiles.isEmpty()) {
			logger.info("Indexing your insight changes");
			MosfetSyncHelper.synchronizeInsightChanges(mosfetFiles, logger);
			logger.info("Index complete");
		} else {
			logger.info("No insight indexing required");
		}


		return new NounMetadata(output.toString(), PixelDataType.CONST_STRING, PixelOperationType.MARKET_PLACE);
	}

	/**
	 * Need to get the appropriate files to perform indexing!
	 * @param filesChanged
	 * @return
	 */
	private Map<String, List<String>> getMosfetFiles(Map <String, List<String>> filesChanged, List<String> filesToSync) {
		Map<String, List<String>> mosfetFiles = new Hashtable<String, List<String>>();
		if(filesChanged.containsKey("ADD")) {
			List<String> files = getMosfetFiles(filesChanged.get("ADD"), filesToSync);
			if(!files.isEmpty()) {
				mosfetFiles.put("ADD", files);
			}
		}

		if(filesChanged.containsKey("MOD")) {
			List<String> files = getMosfetFiles(filesChanged.get("MOD"), filesToSync);
			if(!files.isEmpty()) {
				mosfetFiles.put("MOD", files);
			}		
		}

		if(filesChanged.containsKey("REN")) {
			List<String> files = getMosfetFiles(filesChanged.get("REN"), filesToSync);
			if(!files.isEmpty()) {
				mosfetFiles.put("REN", files);
			}		
		}

		if(filesChanged.containsKey("DEL")) {
			List<String> files = getMosfetFiles(filesChanged.get("DEL"), filesToSync);
			if(!files.isEmpty()) {
				mosfetFiles.put("DEL", files);
			}		
		}
		return mosfetFiles;
	}

	private List<String> getMosfetFiles(List<String> potentialFiles, List<String> filesToSync) {
		List<String> mosfetFiles = new Vector<String>();
		if(potentialFiles != null) {
			for(String f : potentialFiles) {
				if(f.endsWith(".mosfet") && filesToSync.contains(f)) {
					mosfetFiles.add(f);
				}
			}
		}
		return mosfetFiles;
	}

	private List<String> getFilesToSync() {
		List<String> filesToSync = new Vector<String>();
		GenRowStruct grs = this.store.getNoun(this.keysToGet[4]);
		for(int i = 0; i < grs.size(); i++) {
			filesToSync.add(grs.get(i).toString());
		}
		return filesToSync;
	}
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("files")) {
			return "The files to sync";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
