package prerna.reactor.insights.recipemanagement;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityInsightUtils;
import prerna.om.Insight;
import prerna.om.InsightFile;
import prerna.om.OldInsight;
import prerna.reactor.insights.AbstractInsightReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class DownloadInsightRecipeReactor extends AbstractInsightReactor {

	private static final Logger logger = LogManager.getLogger(DownloadInsightRecipeReactor.class);

	public DownloadInsightRecipeReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.ID.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// get the recipe for the insight
		// need the engine name and id that has the recipe
		String projectId = this.keyValue.get(this.keysToGet[0]);
		String rdbmsId = this.keyValue.get(this.keysToGet[1]);
		
		// pull the insight from the security db
		Insight newInsight = SecurityInsightUtils.getInsight(projectId, rdbmsId);

		// OLD INSIGHT
		if(newInsight instanceof OldInsight) {
			Map<String, Object> insightMap = new HashMap<String, Object>();
			// return to the FE the recipe
			insightMap.put("name", newInsight.getInsightName());
			// keys below match those in solr
			insightMap.put("core_engine", newInsight.getProjectId());
			insightMap.put("core_engine_id", newInsight.getRdbmsId());
			return new NounMetadata(insightMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OLD_INSIGHT);
		}

		// get a random file name
		Date date = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss_SSSS");
		formatter.setTimeZone(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
		String modifiedDate = formatter.format(date);
		String fileLocation = this.insight.getInsightFolder() + DIR_SEPARATOR + Utility.normalizePath("insight_recipe_" + rdbmsId + "_" + modifiedDate) + ".txt";
		File recipeFile = new File(fileLocation);
		recipeFile.getParentFile().mkdirs();
		try {
			recipeFile.createNewFile();
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred creating new file with message: " + e.getMessage());
		}
		
		List<String> recipeSteps = newInsight.getPixelList().getPixelRecipe();

		String downloadKey = UUID.randomUUID().toString();
		InsightFile insightFile = new InsightFile();
		insightFile.setFileKey(downloadKey);
		insightFile.setFilePath(fileLocation);
		insightFile.setDeleteOnInsightClose(true);
		
		FileWriter fw = null;
		PrintWriter pw = null;
		try {
			fw = new FileWriter(recipeFile);
			pw = new PrintWriter(fw);
			for(String step : recipeSteps) {
				pw.println(step);
			}
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred writing the recipe to file with message: " + e.getMessage());
		} finally {
			if(pw != null) {
				pw.close();
			}
			if(fw != null) {
				try {
					fw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		// store the insight file 
		// in the insight so the FE can download it
		// only from the given insight
		this.insight.addExportFile(downloadKey, insightFile);

		NounMetadata retNoun = new NounMetadata(downloadKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
		retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully generated the csv file"));
		return retNoun;
	}
}