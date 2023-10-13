package prerna.reactor.project;

import java.io.File;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.om.InsightFile;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class DownloadProjectInsightsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(DownloadProjectInsightsReactor.class);

	/*
	 * This class is used to construct a new project
	 * This project only contains insights
	 */

	public DownloadProjectInsightsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		
		String projectId = this.keyValue.get(this.keysToGet[0]);
		File insightsFile = null;
		try {
			insightsFile = SecurityProjectUtils.createInsightsDatabase(projectId, this.insight.getInsightFolder());
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred attemping to generate the insights database for this project");
		}
		
		String downloadKey = UUID.randomUUID().toString();
		InsightFile insightFile = new InsightFile();
		insightFile.setFileKey(downloadKey);
		insightFile.setDeleteOnInsightClose(false);
		insightFile.setFilePath(insightsFile.getAbsolutePath());

		// store the insight file 
		// in the insight so the FE can download it
		// only from the given insight
		this.insight.addExportFile(downloadKey, insightFile);

		NounMetadata retNoun = new NounMetadata(downloadKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
		retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully generated the csv file"));
		return retNoun;
	}
	
}
