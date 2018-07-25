package prerna.cache;

import java.io.File;
import java.io.FilenameFilter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

import prerna.algorithm.api.ITableDataFrame;
import prerna.om.Insight;
import prerna.sablecc.PKQLRunner;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

@Deprecated
public abstract class InsightCache implements ICache {

	protected final String INSIGHT_CACHE_PATH;
	protected static Map<String, String> extensionMap = new HashMap<>();
	public static final String JSON_EXTENSION = "_VizData.json";
	public static final String R_EXTENSION = "_R.RData";
	private static String cacheMode = DIHelper.getInstance().getProperty("CACHE_SETTING");

	private ConcurrentMap<String, ReentrantLock> locks = new ConcurrentHashMap<>();
	/**
	 * Default constructor for an insight cache
	 * If there is a different type of data maker that needs to be cached
	 * 		it needs to be added into the extension map.  The map stores the
	 * 		the data maker name to the extension of the cached data maker
	 */
	protected InsightCache() {
		// get the main insight cache file path for this machine
		INSIGHT_CACHE_PATH = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR);		
		
		// this must be set for all the code below
		// need to store the extensions for each data frame
		extensionMap.put("TinkerFrame", ".tg");
		extensionMap.put("H2Frame", ".gz");
	}
	
	public void setCacheMode(boolean cacheSetting) {
		if(cacheSetting) {
			cacheMode = "ON";
			Utility.changePropMapFileValue(DIHelper.getInstance().getProperty(Constants.DIHELPER_PROP_FILE_LOCATION), "CACHE_SETTING", "ON");
		} else {
			cacheMode = "OFF";
			Utility.changePropMapFileValue(DIHelper.getInstance().getProperty(Constants.DIHELPER_PROP_FILE_LOCATION), "CACHE_SETTING", "OFF");
		}
	}
	
	/**
	 * Getting the base folder will be defined by the specific implementation of InsightCache
	 * That is because there is a different folder structure when storing insights that are
	 * generated from a database versus insights that are cached through a "drag and drop csv file"
	 * @param in			uses the insight to determine the structure
	 * @return				the string containing the base folder
	 */
	abstract public String getBaseFolder(Insight in);
	
	/**
	 * Getting the unique id for the cache will be defined by the specific implementation of InsightCache
	 * That is because there is a different unique id required if the insight is generated from a 
	 * database versus insight that are cached through a "drag and drop csv file"
	 * @param in			uses the insight to determine the unique id
	 * @return				the string containing the unique id
	 */
	abstract public String createUniqueId(Insight in);
	
	/**
	 * Get the data maker path for the cached insight
	 * @param in			The insight to get the data maker path for
	 * @return				The path for the cached data maker
	 */
	public String getDMFilePath(Insight in) {
		String baseFile = getBaseFolder(in) + FILE_SEPARATOR + createUniqueId(in);
		String dataMakerName = in.getDataMakerName();
		
		String fileName = "";
		// determine the extension of the cache file based on the data maker
		if(extensionMap.containsKey(dataMakerName)) {
			fileName = baseFile + extensionMap.get(dataMakerName);
		}
		
		return fileName;
	}
	
	/**
	 * Get the path to the json of the cached insight
	 * @param in			The insight to get the cached json data for
	 * @return				The path for the cached json
	 */
	public String getVizFilePath(Insight in) {
		return getBaseFilePath(in) + JSON_EXTENSION;
	}
	
	/**
	 * Get the path to the R data of the cached insight
	 * @param in
	 * @return
	 */
	public String getRDataFilePath(Insight in) {
		return getBaseFilePath(in) + R_EXTENSION;
	}
	
	/**
	 * 
	 * @param in
	 * @return
	 */
	private String getBaseFilePath(Insight in) {
		String baseFile = getBaseFolder(in) + FILE_SEPARATOR + createUniqueId(in);
		return baseFile;
	}
	/////////////// START CACHEING CODE ///////////////
	
	/*
	 * There are two different ways to cache an insight
	 * 1) pass in the insight
	 * 		-> this is used when we are saving a csv insight which caches the data but
	 * 			it doesn't have access to the json data being used so it needs to re-run 
	 * 			the insight to cache the json viz
	 * 2) pass in the insight and the json viz
	 * 		-> this is used when we go through create output (running an existing insight).
	 * 			that means the insight is being run and has never been cached before. since 
	 * 			we need to send the data to the FE, no need to re-run to get the output again,
	 * 			so we just pass it in and write it to the file
	 */
	
	/**
	 * Caches a given insight
	 * This caches both the data maker and the JSON data sent to visualize on the FE
	 * @param in			The insight to cache
	 * @return				Returns the baseFile for the given insight
	 */
	public String cacheInsight(Insight in) {
		String baseFile = null;
		if("ON".equals(cacheMode)) {
			baseFile = getBaseFilePath(in);
			cacheDataMaker(in.getDataMakerName(), in.getDataMaker(), baseFile);
			cacheJSONData(in.getWebData(), baseFile);
			cacheRData(in.getPkqlRunner(), baseFile);
		}
		return baseFile;
	}

	/**
	 * Cache a given insight
	 * This caches the data maker and the input vizData
	 * @param in			The insight to cache
	 * @param vizData		The json viz data for the view
	 * @return
	 */
	public String cacheInsight(Insight in, Map<String, Object> vizData) {
		String baseFile = null;
		if("ON".equals(cacheMode)) {
			baseFile = getBaseFilePath(in);
			cacheDataMaker(in.getDataMakerName(), in.getDataMaker(), baseFile);
			cacheJSONData(vizData, baseFile);
			cacheRData(in.getPkqlRunner(), baseFile);
		}
		return baseFile;
	}	
	
	/**
	 * Caches the data maker for an insight
	 * @param dataMakerName				The name of the data maker
	 * @param dm						The data maker
	 * @param baseFile					The base file for the data maker
	 */
	private void cacheDataMaker(String dataMakerName, IDataMaker dm, String baseFile) {
		String extension = extensionMap.get(dataMakerName);
		if(extension != null && dm instanceof ITableDataFrame) {
			// determine the path of the cache file based on the data maker
			String dmFilePath = baseFile + extension;
			// if the insight isn't already cached
			if(!(new File(dmFilePath).exists())) {
				// use the data makers save function passing in the file path
				((ITableDataFrame)dm).save(dmFilePath);
			}
		}
	}

	/**
	 * Caches the json viz data for an insight
	 * @param jsonData					The json to cache for the insight
	 * @param baseFile					The base file for the json viz
	 */
	private void cacheJSONData(Map<String, Object> jsonData, String baseFile) {
		String jsonFilePath = baseFile + JSON_EXTENSION;
		if(!(new File(jsonFilePath).exists())) {
			// take the json data and put it in a map
			Map<String, Object> saveObj = new HashMap<>();
			saveObj.putAll(jsonData);
			saveObj.put("insightID", null);
			// write the object
			// this method calls gson.ToJson() based on the input and writes it
			// to the given file path
			ICache.writeToFile(jsonFilePath, saveObj);
		}
	}
	
	private void cacheRData(PKQLRunner pkqlRunner, String baseFile) {
//		String rDataFile = baseFile + R_EXTENSION;
//		if(pkqlRunner.getVariableValue(AbstractRJavaReactor.R_CONN) != null) {
//			String rScript = "save(list=ls(), file=\"" + rDataFile.replace("\\", "/") + "\")";
//			try {
//				((org.rosuda.REngine.Rserve.RConnection) pkqlRunner.getVariableValue(AbstractRJavaReactor.R_CONN)).eval(rScript);
//			} catch (RserveException e) {
//				e.printStackTrace();
//			}
//		} else if(pkqlRunner.getVariableValue(AbstractRJavaReactor.R_ENGINE) != null) {
//			String rScript = "save(list=ls(), file=\"" + rDataFile.replace("\\", "/") + "\")";
//			((org.rosuda.JRI.Rengine) pkqlRunner.getVariableValue(AbstractRJavaReactor.R_ENGINE)).eval(rScript);
//		}
	}
	
	/////////////// END CACHEING CODE ///////////////

	
	/////////////// START UN-CACHEING CODE ///////////////

	/**
	 * Get the cached json viz data for a given insight
	 * @param in			The insight to get the json viz data
	 * @return				The string containing the view data
	 */
	public String getVizData(Insight in) {
		if(!"ON".equals(cacheMode)) return null;
		
		String fileLoc = getBaseFolder(in) + FILE_SEPARATOR + createUniqueId(in) + JSON_EXTENSION;
		return ICache.readFromFileString(fileLoc);		
	}

	/**
	 * Get the cached data maker for a given insight
	 * @param in			The insight to get the cached data maker
	 * @return				The cached data maker
	 */
	public ITableDataFrame getDMCache(Insight in) {
		if(!"ON".equals(cacheMode)) return null;
		
		ITableDataFrame dataFrame = null;
		
		// determine the cache location
		String cacheLoc = getBaseFolder(in) + FILE_SEPARATOR + createUniqueId(in);
		String dataMakerName = in.getDataMakerName();
		// if the data maker is not defined in extension map then it cannot be cached
		String extension = extensionMap.get(dataMakerName);
		if(extension != null) {
			// the actual path is the location plus the extension
			String fileName = cacheLoc + extension;
			File file = new File(fileName);
			if(file.exists() && file.isFile()) {
				ITableDataFrame instanceFrame;
				try {
					// also need to get the package location to generically create a default constructor
					// such that you can deserialize the file into the correct data frame
					String framePackageLocation = DIHelper.getInstance().getProperty(dataMakerName);
					if(framePackageLocation != null) {
						Class frame = Class.forName(framePackageLocation);
						Constructor<ITableDataFrame> cons = frame.getConstructor();
						instanceFrame = cons.newInstance();
						
						ReentrantLock lock = getLock(fileName);
						lock.lock();
						try {
//							dataFrame = instanceFrame.open(fileName, in.getUserId());
						} finally {
							//TODO : how to remove locks thread safely?
//							if(!lock.hasQueuedThreads()) {
//								removeLock(fileName);
//							}
							lock.unlock();
						}						
					}
				} catch(NoSuchMethodException e) {
					e.printStackTrace();
				} catch (InstantiationException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
		}
		return dataFrame;
	}
	
	public void getRCache(Insight in) {
//		if(!"ON".equals(cacheMode)) return;
//		
//		// determine the cache location
//		String cacheLoc = getBaseFolder(in) + FILE_SEPARATOR + createUniqueId(in) + R_EXTENSION;
//		File rFile = new File(cacheLoc);
//		if(rFile.exists() && rFile.isFile()) {
//			boolean foundRConnection = false;
//			PKQLRunner pkqlRunner = in.getPkqlRunner();
//			if(pkqlRunner.getVariableValue(AbstractRJavaReactor.R_CONN) != null) {
//				foundRConnection = true;
//				String rScript = "load(\"" + rFile.getAbsolutePath().replace("\\", "/") + "\")";
//				try {
//					((org.rosuda.REngine.Rserve.RConnection) pkqlRunner.getVariableValue(AbstractRJavaReactor.R_CONN)).eval(rScript);
//				} catch (RserveException e) {
//					e.printStackTrace();
//				}
//			} else if(pkqlRunner.getVariableValue(AbstractRJavaReactor.R_ENGINE) != null) {
//				foundRConnection = true;
//				String rScript = "load(\"" + rFile.getAbsolutePath().replace("\\", "/") + "\")";
//				((org.rosuda.JRI.Rengine) pkqlRunner.getVariableValue(AbstractRJavaReactor.R_ENGINE)).eval(rScript);
//			}
//			
//			// if we did not find a connection, we need to make one
//			// and then we need to set it in the runner so it is used by the insight
//			if(!foundRConnection) {
//				String useJriStr = DIHelper.getInstance().getProperty(Constants.R_CONNECTION_JRI);
//				boolean useJri = false;
//				if(useJriStr != null) {
//					useJri = Boolean.valueOf(useJriStr);
//				}
//				
//				RCacheUtility.generateNewRSessionAndLoadWorkspace(pkqlRunner, rFile.getAbsolutePath(), useJri);		
//			}
//		}
	}
	
	/////////////// END UN-CACHEING CODE ///////////////

	
	/////////////// START DELETE CACHE CODE ///////////////

	/**
	 * Delete all cached information for a given insight
	 * @param in				The insight to delete the cached information for
	 */
	public void deleteInsightCache(Insight in) {
		ICache.deleteFolder(getBaseFolder(in));
	}

	/**
	 * Delete cached information with a given extension
	 * @param in				The insight to delete the cached information for
	 * @param extensions		The specific extensions of the files to delete
	 */
	public void deleteCacheFiles(Insight in, String... extensions) {
		String baseFolderPath = getBaseFolder(in);
		String fileNameStart = createUniqueId(in);
		File basefolder = new File(baseFolderPath);
		if(basefolder.isDirectory()) {
			File[] files = basefolder.listFiles(new FilenameFilter(){
				@Override
				public boolean accept(File dir, String fileName) {
					if(extensions.length == 0) {
						return fileName.startsWith(fileNameStart);
					} else {
						for(int i = 0; i < extensions.length; i++) {
							if(fileName.startsWith(fileNameStart + extensions[i])) {
								return true;
							}
						}
						return false;
					}
				}
			});
			for(File f : files) {
				ICache.deleteFile(f);
			}
		}
	}
	
	public void deleteAllCache() {
		File file = new File(INSIGHT_CACHE_PATH);
		if(file.exists()) {
			String[] cacheFolders = file.list();
			for(String folder : cacheFolders) {
				ICache.deleteFolder(INSIGHT_CACHE_PATH+FILE_SEPARATOR+folder);
			}
		}
	}
	
	/////////////// END DELETE CACHE CODE ///////////////
	
	/**
	 * 
	 * @param key
	 * @return
	 * 
	 * get the thread lock for a key, create one if necessary
	 */
	private ReentrantLock getLock(String key) {
		locks.putIfAbsent(key, new ReentrantLock());
		return locks.get(key);
	}
	
	/**
	 * 
	 * @param key
	 * 
	 * remove lock if no other threads have a handle on it
	 */
	private void removeLock(String key) {
		locks.remove(key);
		System.out.println("Removed Lock for key: "+key);
	}
}
