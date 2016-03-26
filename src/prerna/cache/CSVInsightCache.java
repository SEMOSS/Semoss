package prerna.cache;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class CSVInsightCache extends InsightCache {

	private static CSVInsightCache singleton;
	private final String CSV_INSIGHT_FOLDER;
	
	private final String CSV_PATH;
	
	private CSVInsightCache() {
		CSV_INSIGHT_FOLDER = DIHelper.getInstance().getProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
		CSV_PATH = INSIGHT_CACHE_PATH + FILE_SEPARATOR + CSV_INSIGHT_FOLDER;
	}
	
	protected static CSVInsightCache getInstance() {
		if(singleton == null) {
			singleton = new CSVInsightCache();
		}
		return singleton;
	}
	
	@Override
	public String getBaseFolder(Insight in) {
		File basefolder = new File(CSV_PATH);
		if(!basefolder.exists()) {
			try {
				FileUtils.forceMkdir(basefolder);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return CSV_PATH;
	}

	@Override
	public String createUniqueId(Insight in) {
		String uniqueId = ICache.cleanFolderAndFileName(in.getDatabaseID());
		String questionName = ICache.cleanFolderAndFileName(in.getInsightName());
		if(questionName.length() > 25) {
			questionName = questionName.substring(0, 25);
		}
		uniqueId += "_" + questionName;
		return uniqueId;
	}
	
}
