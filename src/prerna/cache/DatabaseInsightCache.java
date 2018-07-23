package prerna.cache;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import prerna.om.Insight;

@Deprecated
public class DatabaseInsightCache extends InsightCache {

	private static DatabaseInsightCache singleton;
	
	private DatabaseInsightCache() {
		
	}
	
	protected static DatabaseInsightCache getInstance() {
		if(singleton == null) {
			singleton = new DatabaseInsightCache();
		}
		return singleton;
	}

	@Override
	public String getBaseFolder(Insight in) {
		String id = ICache.cleanFolderAndFileName(in.getRdbmsId());
		String questionName = ICache.cleanFolderAndFileName(in.getInsightName());
		if(questionName.length() > 25) {
			questionName = questionName.substring(0, 25);
		}
		String base = getBaseDBFolder(in.getEngineId()) + FILE_SEPARATOR + id + "_" + questionName;
		File basefolder = new File(base);
		if(!basefolder.exists()) {
			try {
				FileUtils.forceMkdir(basefolder);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return base;
	}
	
	public String getBaseDBFolder(String engineName) {
		engineName = ICache.cleanFolderAndFileName(engineName);
		String base = INSIGHT_CACHE_PATH + FILE_SEPARATOR + engineName;
		return base;
	}

	@Override
	public String createUniqueId(Insight in) {
		String id = ICache.cleanFolderAndFileName(in.getRdbmsId());
		String questionName = ICache.cleanFolderAndFileName(in.getInsightName());
		if(questionName.length() > 25) {
			questionName = questionName.substring(0, 25);
		}
		id += "_" + questionName;
//		String paramStr = getParamString(in.getParamHash());
//		if(!paramStr.isEmpty()) {
//			id += "_" + paramStr;
//		}
		return id;
	}
	
	/**
	 * @param paramHash
	 * @return
	 * Converts paramHash to a string that will be used for file naming
	 * The purpose of this is to guarantee paramHashes will always yield the same value and the file name does not become too long
	 * 
	 * Note: ~77,000 random strings, 50% chance two hashes will collide
	 * 		
	 */
	private static String getParamString(Map<String, List<Object>> paramHash) {
		if(paramHash == null || paramHash.isEmpty()) return "";
		
		List<String> keys = new ArrayList<String>(paramHash.keySet());
		Collections.sort(keys);
		
		StringBuilder paramString = new StringBuilder();
		
		for(String key : keys) {
			List<Object> params = paramHash.get(key);
			Collections.sort(params, new Comparator<Object>() {
				public int compare(Object o1, Object o2) {
					return o1.toString().toLowerCase().compareTo(o2.toString().toLowerCase());
				}
			});
			
			paramString.append(key+":::");
			for(Object param : params) {
				paramString.append(param);
			}
		}
		
		//use other types of hashing if this won't be sufficient
		return paramString.toString().hashCode()+"";
	}
	
	public void deleteDBCache(String dbName) {
		ICache.deleteFolder(getBaseDBFolder(dbName));
	}
}
