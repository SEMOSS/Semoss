package prerna.reactor.project;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.GsonBuilder;

import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.project.impl.ProjectHelper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.gson.GsonUtility;
import prerna.util.upload.UploadUtilities;

public class CreateAppFromBlocksReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CreateAppFromBlocksReactor.class);

	private static final String CLASS_NAME = CreateAppFromBlocksReactor.class.getName();

	/*
	 * This class is used to construct a new project using an existing project as a template.
	 * It can be considered a deep copy in that all insights from the template are also copied to the new project
	 */

	public CreateAppFromBlocksReactor() {
		this.keysToGet = new String[]{ ReactorKeysEnum.PROJECT.getKey(), 
				ReactorKeysEnum.PORTAL_NAME.getKey(),
				ReactorKeysEnum.PROVIDER.getKey(), ReactorKeysEnum.URL.getKey(),
				ReactorKeysEnum.JSON.getKey() };
	}
	
	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		
		organizeKeys();
		int index = 0;
		String newProjectName = this.keyValue.get(this.keysToGet[index++]);
		String portalName = this.keyValue.get(this.keysToGet[index++]);
		String gitProvider = this.keyValue.get(this.keysToGet[index++]);
		String gitCloneUrl = this.keyValue.get(this.keysToGet[index++]);
		
		Map<String, Object> json = getPromptJSON();
		if(json == null || json.isEmpty()) {
			throw new IllegalArgumentException("Must provide the prompt builder JSON");
		}
		
		// Create new project
		IProject newProject = ProjectHelper.generateNewProject(newProjectName, IProject.PROJECT_TYPE.BLOCKS, true, portalName, 
				gitProvider, gitCloneUrl, this.insight.getUser(), logger);
		
		String portalsFolder = AssetUtility.getProjectPortalsFolder(newProject.getProjectId());
		File blocksJsonFile = new File(portalsFolder+"/"+IProject.BLOCK_FILE_NAME);
		try {
			GsonUtility.writeObjectToJsonFile(blocksJsonFile, new GsonBuilder().setPrettyPrinting().create(), json);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("New project was created but could not write the blocks json to the project folder. Errror = " + e.getMessage());
		}
		
		String newProjectAssetFolder = AssetUtility.getProjectAssetFolder(newProject.getProjectId());
		if (ClusterUtil.IS_CLUSTER) {
			logger.info("Syncing project for cloud backup");
			ClusterUtil.pushProjectFolder(newProject, newProjectAssetFolder);
		}
		
		Map<String, Object> retMap = UploadUtilities.getProjectReturnData(this.insight.getUser(), newProject.getProjectId());
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}
	
	private Map<String,Object> getPromptJSON() {
		GenRowStruct mapGrs = this.store.getNoun(ReactorKeysEnum.JSON.getKey());
		if(mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if(mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Object>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if(mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, Object>) mapInputs.get(0).getValue();
		}
		return null;
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The name for this project. Note: the project ID is randomly generated and is not passed into this method";
		} else if(key.equals(ReactorKeysEnum.PROVIDER.getKey())) {
			return "The GIT provider - user must be logged in with this provider for credentials";
		} else if(key.equals(ReactorKeysEnum.URL.getKey())) {
			return "The GIT repository URL to clone for this project";
		} else if(key.equals(ReactorKeysEnum.JSON.getKey())) {
			return "The JSON that represents the prompt builder";
		}
		return super.getDescriptionForKey(key);
	}
	
}
