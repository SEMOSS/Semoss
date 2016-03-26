package prerna.cache;

import java.io.File;
import java.io.FilenameFilter;
import java.util.HashMap;
import java.util.Map;

import prerna.ds.TinkerFrame;
import prerna.om.Insight;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.util.Constants;
import prerna.util.DIHelper;

public abstract class InsightCache implements ICache {

	public static final String DM_EXTENSION = ".tg";
	public static final String JSON_EXTENSION = "_VizData.json";
	protected final String INSIGHT_CACHE_PATH;
	
	protected InsightCache() {
		INSIGHT_CACHE_PATH = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR);
	}
	
	abstract public String getBaseFolder(Insight in);
	
	abstract public String createUniqueId(Insight in);
	
	public String getDMFilePath(Insight in) {
		String baseFile = getBaseFolder(in) + FILE_SEPARATOR + createUniqueId(in);
		return baseFile + DM_EXTENSION;
	}
	
	public String getVizFilePath(Insight in) {
		String baseFile = getBaseFolder(in) + FILE_SEPARATOR + createUniqueId(in);
		return baseFile + JSON_EXTENSION;
	}
	
	public String cacheInsight(Insight in) {
		String baseFile = getBaseFolder(in) + FILE_SEPARATOR + createUniqueId(in);
		cacheDataMaker(in.getDataMaker(), baseFile);
		cacheJSONData(in.getWebData(), baseFile);
		
		return baseFile;
	}

	public String cacheInsight(Insight in, Map<String, Object> vizData) {
		String baseFile = getBaseFolder(in) + FILE_SEPARATOR + createUniqueId(in);
		cacheDataMaker(in.getDataMaker(), baseFile);
		cacheJSONData(vizData, baseFile);

		return baseFile;
	}	
	
	public String getVizData(Insight in) {
		String fileLoc = getBaseFolder(in) + FILE_SEPARATOR + createUniqueId(in) + JSON_EXTENSION;
		return ICache.readFromFileString(fileLoc);
	}

	public TinkerFrame getDMCache(Insight in) {
		String fileLoc = getBaseFolder(in) + FILE_SEPARATOR + createUniqueId(in) + DM_EXTENSION;
		return reloadTinkerCache(fileLoc);
	}
	
	public void cacheDataMaker(IDataMaker dm, String baseFile) {
		if(dm instanceof TinkerFrame) {
			String dmFilePath = baseFile + DM_EXTENSION;
			if(!(new File(dmFilePath).exists())) {
				((TinkerFrame)dm).save(dmFilePath);
			}
		}
	}
	
	public TinkerFrame reloadTinkerCache(String tinkerCacheLoc) {
		TinkerFrame tf = null;
		File f = new File(tinkerCacheLoc);
		// if that graph cache exists load it and sent to the FE
		if(f.exists() && f.isFile()) {
			tf = TinkerFrame.open(tinkerCacheLoc);
		}
		
		return tf;
	}
	
	public void cacheJSONData(Map<String, Object> jsonData, String baseFile) {
		String jsonFilePath = baseFile + JSON_EXTENSION;
		if(!(new File(jsonFilePath).exists())) {
			Map<String, Object> saveObj = new HashMap<>();
			saveObj.putAll(jsonData);
			saveObj.put("insightID", null);
			ICache.writeToFile(jsonFilePath, saveObj);
		}
	}
	
	public void deleteCacheFolder(Insight in) {
		ICache.deleteFolder(getBaseFolder(in));
	}

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
}
