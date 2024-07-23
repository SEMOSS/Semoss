package prerna.reactor.project;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.project.impl.ProjectHelper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

public class CreateAppFromTemplateReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CreateAppFromTemplateReactor.class);

	private static final String CLASS_NAME = CreateAppFromTemplateReactor.class.getName();

	/*
	 * This class is used to construct a new project using an existing project as a template.
	 * It can be considered a deep copy in that all insights from the template are also copied to the new project
	 */

	public CreateAppFromTemplateReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), "projectTemplate", 
				ReactorKeysEnum.GLOBAL.getKey(), 
				ReactorKeysEnum.PROVIDER.getKey(), ReactorKeysEnum.URL.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		
		organizeKeys();
		int index = 0;
		String newProjectName = this.keyValue.get(this.keysToGet[index++]);
		String projectTemplateId = this.keyValue.get(this.keysToGet[index++]);
		boolean global = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[index++])+"");
		String gitProvider = this.keyValue.get(this.keysToGet[index++]);
		String gitCloneUrl = this.keyValue.get(this.keysToGet[index++]);
		
		// make sure valid id for user
		if(!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectTemplateId)) {
			// you dont have access
			throw new IllegalArgumentException("The template you are attempting to use does not exist or user does not have access to the project");
		}
		// Instantiate an object using the projectTemplate
		IProject templateProject = Utility.getProject(projectTemplateId);

		// Use the template to populate the parameters needed to create the new project
		IProject.PROJECT_TYPE projectEnumType = templateProject.getProjectType();
		boolean templateHasPortal = templateProject.isHasPortal();
		String templatePortalName = templateProject.getPortalName();
		
		// Create new project
		IProject newProject = ProjectHelper.generateNewProject(newProjectName, projectEnumType, global, templateHasPortal, templatePortalName, 
				gitProvider, gitCloneUrl, this.insight.getUser(), logger);
		
		// now we just need to move over the files for assets
		String templateProjectAssetFolder = AssetUtility.getProjectAssetFolder(projectTemplateId);
		String newProjectAssetFolder = AssetUtility.getProjectAssetFolder(newProject.getProjectId());
		
		Path sourceDir = Paths.get(templateProjectAssetFolder);
        Path destinationDir = Paths.get(newProjectAssetFolder);
        
        try {
        	Files.walk(sourceDir)
        	.forEach(sourcePath -> {
        		try {
        			Path targetPath = destinationDir.resolve(sourceDir.relativize(sourcePath));
        			if(sourcePath.toFile().isFile()) {
        				targetPath.toFile().getParentFile().mkdirs();
        				Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
        			}
        		} catch (IOException ex) {
        			classLogger.warn("Error with moving file from template");
        			classLogger.error(Constants.STACKTRACE, ex);
        		}
        	});
        	if (ClusterUtil.IS_CLUSTER) {
        		logger.info("Syncing project for cloud backup");
        		ClusterUtil.pushProjectFolder(newProject, newProjectAssetFolder);
        	}
        } catch (IOException e) {
        	classLogger.error(Constants.STACKTRACE, e);
        	throw new IllegalArgumentException("New project was created but could not transfer over the assets from the template. Errror = " + e.getMessage());
        }
		
		Map<String, Object> retMap = UploadUtilities.getProjectReturnData(this.insight.getUser(), newProject.getProjectId());
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The name for this project. Note: the project ID is randomly generated and is not passed into this method";
		} else if(key.equals("projectTemplate")) {
			return "The id of the existing project that is serving as the template for the copy";
		} else if(key.equals(ReactorKeysEnum.PROVIDER.getKey())) {
			return "The GIT provider - user must be logged in with this provider for credentials";
		} else if(key.equals(ReactorKeysEnum.URL.getKey())) {
			return "The GIT repository URL to clone for this project";
		}
		return super.getDescriptionForKey(key);
	}
	
}
