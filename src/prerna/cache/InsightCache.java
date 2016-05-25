package prerna.cache;

import java.io.File;
import java.io.FilenameFilter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.om.Insight;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.util.Constants;
import prerna.util.DIHelper;

public abstract class InsightCache implements ICache {

//	public static final String DM_EXTENSION = ".tg";
	public static final String JSON_EXTENSION = "_VizData.json";
	protected final String INSIGHT_CACHE_PATH;
	protected static Map<String, Class> classMap = new HashMap<>();
	protected static Map<String, String> extensionMap = new HashMap<>();
	 
	protected InsightCache() {
		INSIGHT_CACHE_PATH = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR);
		
		//put this in a separate class or a prop file
		try {
			classMap.put(".tg", Class.forName("prerna.ds.TinkerFrame"));
			extensionMap.put("prerna.ds.TinkerFrame", ".tg");
		} catch (ClassNotFoundException e) {}
		
		try {
			classMap.put(".gz", Class.forName("prerna.ds.H2.H2Frame"));
			extensionMap.put("prerna.ds.H2.H2Frame", ".gz");
		} catch(ClassNotFoundException e) {}
	}
	
	abstract public String getBaseFolder(Insight in);
	
	abstract public String createUniqueId(Insight in);
	
	public String getDMFilePath(Insight in) {
		String baseFile = getBaseFolder(in) + FILE_SEPARATOR + createUniqueId(in);
		
		String fileName = "";
		for(String extension : classMap.keySet()) {
			fileName = baseFile + extension;
			File file = new File(fileName);
			if(file.exists() && file.isFile()) {
				break;
			}
		}
		return fileName;
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

	//Needs to return either IDataMaker or ITableDataFrame
	public ITableDataFrame getDMCache(Insight in) {
		String fileLoc = getBaseFolder(in) + FILE_SEPARATOR + createUniqueId(in);//+dm extension
		return reloadTinkerCache(fileLoc);
	}
	
	public void cacheDataMaker(IDataMaker dm, String baseFile) {
		if(dm instanceof ITableDataFrame) {
		String dataFrame = dm.getClass().getName();
		String extension = extensionMap.get(dataFrame);
			String dmFilePath = baseFile + extension;
			if(!(new File(dmFilePath).exists())) {
				((ITableDataFrame)dm).save(dmFilePath);
			}
		}
	}
	
	//needs to return either IDataMaker or ITableDataFrame
	public ITableDataFrame reloadTinkerCache(String tinkerCacheLoc) {
		ITableDataFrame dataFrame = null;

		String fileName = "";
		File file;
		String ext;
		
		for(String extension : classMap.keySet()) {
			fileName = tinkerCacheLoc + extension;
			file = new File(fileName);
			if(file.exists() && file.isFile()) {
//				Class frame = classMap.get(extension);
//				Constructor<ITableDataFrame> cons = frame.getConstructor();
				ITableDataFrame instanceFrame;
				try {
					Class frame = classMap.get(extension);
					Constructor<ITableDataFrame> cons = frame.getConstructor();
					instanceFrame = cons.newInstance();
					dataFrame = instanceFrame.open(fileName);
					break;
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
				}
			}
		}
	
		return dataFrame;
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
