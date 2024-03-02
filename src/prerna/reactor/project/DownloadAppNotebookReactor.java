package prerna.reactor.project;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.project.api.IProject.PROJECT_TYPE;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class DownloadAppNotebookReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(DownloadAppNotebookReactor.class);

	/*
	 * This class is used to construct a new project
	 * This project only contains insights
	 */

	public DownloadAppNotebookReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		
		String projectId = this.keyValue.get(this.keysToGet[0]);
		if(!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
			// you don't have access
			throw new IllegalArgumentException("Project/App does not exist or user does not have access to the project");
		}
		
		IProject project = Utility.getProject(projectId);
		if(project.getProjectType() != PROJECT_TYPE.BLOCKS) {
			throw new IllegalArgumentException("Project/App must be type 'BLOCKS'");
		}
		
		String portalsFolder = AssetUtility.getProjectPortalsFolder(projectId);
		String blocksJson = portalsFolder + "/" + IProject.BLOCK_FILE_NAME;
		
		File blockJF = new File(blocksJson);
		if(!blockJF.exists() || !blockJF.isFile()) {
			throw new IllegalArgumentException("Could not find the blocks json");
		}
		
		try(InputStream is = new FileInputStream(blockJF)) {
			JSONObject json = new JSONObject(IOUtils.toString(is, "UTF-8"));
			
			
			
			
			
			
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		
		String downloadKey = UUID.randomUUID().toString();
		
//		InsightFile insightFile = new InsightFile();
//		insightFile.setFileKey(downloadKey);
//		insightFile.setDeleteOnInsightClose(false);
//		insightFile.setFilePath(insightsFile.getAbsolutePath());
//
//		// store the insight file 
//		// in the insight so the FE can download it
//		// only from the given insight
//		this.insight.addExportFile(downloadKey, insightFile);

		NounMetadata retNoun = new NounMetadata(downloadKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
		retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully generated the csv file"));
		return retNoun;
	}
	
}
