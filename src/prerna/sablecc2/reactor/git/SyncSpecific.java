package prerna.sablecc2.reactor.git;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.MosfetSyncHelper;
import prerna.util.git.GitSynchronizer;

public class SyncSpecific extends AbstractReactor {

	public SyncSpecific() {
		this.keysToGet = new String[]{"app", "remoteApp", "username", "password", "dual", "files"};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String appName = this.keyValue.get(this.keysToGet[0]);
		String remoteApp = this.keyValue.get(this.keysToGet[1]);
		String username = this.keyValue.get(this.keysToGet[2]);
		String password = this.keyValue.get(this.keysToGet[3]);
		String dualStr = this.keyValue.get(this.keysToGet[4]);
		List<String> filesToSync = getFilesToSync();


		Logger logger = getLogger(this.getClass().getName());
		logger.info("Starting the synchronization process");

		// default for dual is true if nothing is sent it means it is dual
		boolean dual = false;
		if(dualStr == null || dualStr.equals("true")) {
			dual = true;
		}

		// if it is null or true dont worry
		logger.info("Synchronizing now... ");
		Map<String, List<String>> filesChanged = GitSynchronizer.synchronizeSpecific(appName, remoteApp, username, password, filesToSync, dual);
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
		GenRowStruct grs = this.store.getNoun(this.keysToGet[5]);
		for(int i = 0; i < grs.size(); i++) {
			filesToSync.add(grs.get(i).toString());
		}
		return filesToSync;
	}

}
