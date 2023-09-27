package prerna.util;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;


/**
 * This class opens a thread and watches a specific SMSS file.
 */
public class SMSSVectorWatcher extends AbstractFileWatcher {

	private static final Logger logger = LogManager.getLogger(SMSSVectorWatcher.class);

	/**
	 * Processes SMSS files.
	 * @param	Name of the file.
	 */
	@Override
	public void process(String fileName) {
		catalogEngine(fileName, folderToWatch);
	}

	/**
	 * Returns an array of strings naming the files in the directory. Goes through list and loads an existing database.
	 */
	public String loadExistingEngine(String fileName) {
		return loadNewEngine(fileName, folderToWatch);
	}

	/**
	 * Loads a new database by setting a specific engine with associated properties.
	 * @param 	Specifies properties to load 
	 */
	public static String loadNewEngine(String newFile, String folderToWatch) {
		String engines = DIHelper.getInstance().getEngineProperty(Constants.ENGINES) + "";
		String engineId = null;
		try{
			Properties prop = Utility.loadProperties(Utility.normalizePath(folderToWatch) + "/"  + Utility.normalizePath(newFile));
			if(prop == null) {
				throw new NullPointerException("Unable to find/load properties file '" + newFile + "'");
			}
			
			engineId = prop.getProperty(Constants.ENGINE);
			if(engines.startsWith(engineId) || engines.contains(";"+engineId+";") || engines.endsWith(";"+engineId)) {
				logger.debug("Model " + folderToWatch + "<>" + newFile + " is already loaded...");
			} else {
				String filePath = folderToWatch + "/" + newFile;
				Utility.catalogEngineByType(filePath, prop, engineId);
			}
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
		}
		
		return engineId;
	}
	
	// this is an alternate method.. which will not load the database but would merely keep the name of the engine
	// and the SMSS file
	/**
	 * Loads a new database by setting a specific engine with associated properties.
	 * @param 	Specifies properties to load 
	 */	
	public static String catalogEngine(String newFile, String folderToWatch) {
		String engines = DIHelper.getInstance().getEngineProperty(Constants.ENGINES) + "";
		String engineId = null;
		try{
			Properties prop = Utility.loadProperties(Utility.normalizePath(folderToWatch) + "/"  + Utility.normalizePath(newFile));
			if(prop == null) {
				throw new NullPointerException("Unable to find/load properties file '" + newFile + "'");
			}
			engineId = prop.getProperty(Constants.ENGINE);
			
			if(engines.startsWith(engineId) || engines.contains(";"+engineId+";") || engines.endsWith(";"+engineId)) {
				logger.debug("Model " + folderToWatch + "<>" + newFile + " is already loaded...");
			} else {
				String filePath = folderToWatch + "/" + newFile;
				Utility.catalogEngineByType(filePath, prop, engineId);
			}
		} catch(Exception e){
			logger.error(Constants.STACKTRACE, e);
		}
		
		return engineId;
	}

	/**
	 * Used in the starter class for processing SMSS files.
	 */
	@Override
	public void loadFirst() {
		// I need to get all the SMSS files
		// Read the engine names and profile the SMSS files i.e. capture that in some kind of hashtable
		// and let it go that is it
		File dir = new File(folderToWatch);
		String[] fileNames = dir.list(this);
		String[] engineIds = new String[fileNames.length];

		// loop through and load all the engines
		// but we will ignore the local master and security database
		for (int fileIdx = 0; fileIdx < fileNames.length; fileIdx++) {
			try {
				String fileName = fileNames[fileIdx];
				
				// I really dont want to load anything here
				// I only want to keep track of what are the engine names and their corresponding SMSS files
				// so we will catalog instead of load
				String loadedEngineId = catalogEngine(fileName, folderToWatch);
				engineIds[fileIdx] = loadedEngineId;
			} catch (RuntimeException ex) {
				logger.error(Constants.STACKTRACE, ex);
				logger.fatal("Model engine Failed " + folderToWatch + "/" + fileNames[fileIdx]);
			}
		}
		
		// remove unused databases
		if (!ClusterUtil.IS_CLUSTER) {
			List<String> engines = SecurityEngineUtils.getAllEngineIds(Arrays.asList(IEngine.CATALOG_TYPE.VECTOR.toString()));
			for(String engine : engines) {
				if(!ArrayUtilityMethods.arrayContainsValue(engineIds, engine)) {
					logger.info("Deleting the model engine from security..... " + Utility.cleanLogString(engine));
					SecurityEngineUtils.deleteEngine(engine);
				}
			}
		}
	}

	/**
	 * Processes new SMSS files.
	 */
	@Override
	public void run() {
		logger.info("Starting SMSSVectorWatcher thread");
		synchronized(monitor) {
			loadFirst();
			super.run();
		}
	}

}