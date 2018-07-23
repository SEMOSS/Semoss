package prerna.om;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import prerna.util.gson.GsonUtility;

public class InsightCacheUtility {

	private InsightCacheUtility() {
		
	}
	
	/**
	 * Main method to cache a full insight
	 * @param insight
	 */
	public static void cacheInsight(Insight insight, String insightDir) {
		String insightLoc = insightDir + "\\InsightCache.json";
		
		Gson gson = GsonUtility.getDefaultGson();
		try {
			FileUtils.writeStringToFile(new File(insightLoc), gson.toJson(insight));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * Main method to read in a full insight
	 * @param insightDir
	 * @return
	 */
	public static Insight readInsightCache(String insightDir) {
		String insightLoc = insightDir + "\\InsightCache.json";
		
		Gson gson = GsonUtility.getDefaultGson();
		try {
			Insight insight = gson.fromJson(FileUtils.readFileToString(new File(insightLoc)), Insight.class);
			return insight;
		} catch (JsonSyntaxException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
}
