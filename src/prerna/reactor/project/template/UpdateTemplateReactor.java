package prerna.reactor.project.template;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class UpdateTemplateReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(UpdateTemplateReactor.class);
	private static final String CLASS_NAME = UpdateTemplateReactor.class.getName();
	
	public UpdateTemplateReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.TEMPLATE_NAME.getKey(),
				ReactorKeysEnum.TEMPLATE_FILE.getKey() };
	}

	public NounMetadata execute() {
		organizeKeys();
		Logger logger = getLogger(CLASS_NAME);
		
		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		String templateFile = this.keyValue.get(ReactorKeysEnum.TEMPLATE_FILE.getKey());
		String templateName = this.keyValue.get(ReactorKeysEnum.TEMPLATE_NAME.getKey());
		
		IProject project = Utility.getProject(projectId);
		String versionFolder = AssetUtility.getProjectAssetFolder(project.getProjectName(), projectId);
		String fileToMove = versionFolder;
		if(templateFile.startsWith("/") || templateFile.startsWith("\\")) {
			fileToMove += templateFile;
		} else {
			fileToMove += "/" + templateFile;
		}
		fileToMove = fileToMove.replace('\\', '/');
		File f = new File(fileToMove);
		// we will move this file over
		String baseF = this.insight.getInsightFolder();
		String tempMove = baseF + "/" + UUID.randomUUID().toString() + "." + FilenameUtils.getExtension(fileToMove);
		File newF = new File(tempMove);
		if(newF.getParentFile().exists()) {
			newF.getParentFile().mkdirs();
		}
		try {
			FileUtils.moveFile(f, newF);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred moving the template into the template folder", e);
		}
		
		logger.info("Starting to synchronize templates with template directory");
		// pull from cloud
		ClusterUtil.pullProjectFolder(project, AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId));
		// move back the file
		try {
			FileUtils.moveFile(newF, f);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred moving the template into the template folder", e);
		}
		// write/update to properties file
		Map<String, String> templateDataMap = TemplateUtility.editTemplate(projectId, templateFile, templateName);
		// push to cloud
		ClusterUtil.pushProjectFolder(project, AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId));
		logger.info("Finished synchronizing templates with template directory");

		// returning back the updated template information which will contain all the 
		// template information with template name as key and file name as the value
		return new NounMetadata(templateDataMap, PixelDataType.MAP);
	}
}
