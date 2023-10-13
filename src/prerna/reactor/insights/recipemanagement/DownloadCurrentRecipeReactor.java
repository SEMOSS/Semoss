package prerna.reactor.insights.recipemanagement;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.om.InsightFile;
import prerna.om.Pixel;
import prerna.om.PixelList;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class DownloadCurrentRecipeReactor extends AbstractReactor {

	private static final Logger logger = LogManager.getLogger(DownloadCurrentRecipeReactor.class);
	
	@Override
	public NounMetadata execute() {
		PixelList pixelList = this.insight.getPixelList();

		// get a random file name
		Date date = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss_SSSS");
		formatter.setTimeZone(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
		String modifiedDate = formatter.format(date);
		String fileLocation = this.insight.getInsightFolder() + DIR_SEPARATOR + Utility.normalizePath("insight_recipe_" + modifiedDate) + ".txt";
		File recipeFile = new File(fileLocation);
		recipeFile.getParentFile().mkdirs();
		try {
			recipeFile.createNewFile();
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred creating new file with message: " + e.getMessage());
		}
		
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
			for(Pixel pixel : pixelList) {
				pw.println(pixel.getPixelString());
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
