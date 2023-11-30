package prerna.reactor.project;

import java.util.Map;

import org.apache.logging.log4j.Logger;

import prerna.project.api.IProject;
import prerna.project.impl.ProjectHelper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.upload.UploadUtilities;

public class CreateProjectReactor extends AbstractReactor {

	private static final String CLASS_NAME = CreateProjectReactor.class.getName();

	/*
	 * This class is used to construct a new project
	 * This project only contains insights
	 */

	public CreateProjectReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.PROJECT_TYPE.getKey(),
				ReactorKeysEnum.PORTAL.getKey(), ReactorKeysEnum.PORTAL_NAME.getKey(),
				ReactorKeysEnum.PROVIDER.getKey(), ReactorKeysEnum.URL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		
		this.organizeKeys();
		IProject.PROJECT_TYPE projectType = null;

		int index = 0;
		String projectName = this.keyValue.get(this.keysToGet[index++]);
		String projectTypeStr = this.keyValue.get(this.keysToGet[index++]);
		boolean hasPortal = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[index++]));
		
		// project type is new
		// if has portal
		// will assume code if not provided
		// else will assume it is insight
		// TODO: potentially remove hasportal entirely
		if(hasPortal) {
			if(projectTypeStr == null || (projectTypeStr=projectTypeStr.trim()).isEmpty()) {
				projectType = IProject.PROJECT_TYPE.CODE;
			} else {
				projectType = IProject.PROJECT_TYPE.valueOf(projectTypeStr);
			}
		} else {
			projectType = IProject.PROJECT_TYPE.INSIGHTS;
		}
		String portalName = this.keyValue.get(this.keysToGet[index++]);
		String gitProvider = this.keyValue.get(this.keysToGet[index++]);
		String gitCloneUrl = this.keyValue.get(this.keysToGet[index++]);
		
		IProject project = ProjectHelper.generateNewProject(projectName, projectType, hasPortal, portalName, 
				gitProvider, gitCloneUrl, this.insight.getUser(), logger);

		Map<String, Object> retMap = UploadUtilities.getProjectReturnData(this.insight.getUser(), project.getProjectId());
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The name for this project. Note: the project ID is randomly generated and is not passed into this method";
		} else if(key.equals(ReactorKeysEnum.PROVIDER.getKey())) {
			return "The GIT provider - user must be logged in with this provider for credentials";
		} else if(key.equals(ReactorKeysEnum.URL.getKey())) {
			return "The GIT repository URL to clone for this project";
		}
		return super.getDescriptionForKey(key);
	}
	
}
