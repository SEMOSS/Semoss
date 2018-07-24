package prerna.om;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import prerna.engine.impl.SmssUtilities;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.gson.GsonUtility;

public class InsightCacheUtility {

	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	private InsightCacheUtility() {
		
	}
	
	/**
	 * Main method to cache a full insight
	 * @param insight
	 * @throws IOException 
	 */
	public static File cacheInsight(Insight insight) throws IOException {
		String rdbmsId = insight.getRdbmsId();
		String engineId = insight.getEngineId();
		String engineName = insight.getEngineName();
		
		if(engineId == null || rdbmsId == null || engineName == null) {
			throw new IOException("Cannot jsonify an insight that is not saved");
		}
		
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String folderDir = baseFolder + DIR_SEPARATOR + "db" + DIR_SEPARATOR + SmssUtilities.getUniqueName(engineName, engineId) 
				+ DIR_SEPARATOR + "version" + DIR_SEPARATOR + rdbmsId;
		if(!(new File(folderDir).exists())) {
			new File(folderDir).mkdirs();
		}
		String insightLoc = folderDir + "\\InsightCache.json";
		
		Gson gson = GsonUtility.getDefaultGson();
		File insightFile = new File(insightLoc);
		try {
			FileUtils.writeStringToFile(insightFile, gson.toJson(insight));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return insightFile;
	}
	
	/**
	 * Main method to read in a full insight
	 * @param insightDir
	 * @return
	 */
	public static Insight readInsightCache(File insightFile) {
		Gson gson = GsonUtility.getDefaultGson();
		try {
			Insight insight = gson.fromJson(FileUtils.readFileToString(insightFile), Insight.class);
			return insight;
		} catch (JsonSyntaxException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	/**
	 * Main method to read in a full insight
	 * @param insightPath
	 * @return
	 */
	public static Insight readInsightCache(String insightPath) {
		File insightFile = new File(insightPath);
		return readInsightCache(insightFile);
	}
	
}
