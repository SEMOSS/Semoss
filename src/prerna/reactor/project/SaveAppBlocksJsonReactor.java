package prerna.reactor.project;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;
import prerna.util.gson.GsonUtility;

public class SaveAppBlocksJsonReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(SaveAppBlocksJsonReactor.class);

	private static final String CLASS_NAME = SaveAppBlocksJsonReactor.class.getName();

	public SaveAppBlocksJsonReactor() {
		this.keysToGet = new String[]{ ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.JSON.getKey(), ReactorKeysEnum.COMMENT_KEY.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		
		User user = this.insight.getUser();
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		
		if(projectId == null || projectId.isEmpty()) {
			throw new IllegalArgumentException("Must input an project id");
		}
		
		// make sure valid id for user
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(user, projectId);
		if(!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
			// you dont have access
			throw new IllegalArgumentException("Project does not exist or user does not have access to the project");
		}
		
		Map<String, Object> json = getBlocksJSON();
		if(json == null || json.isEmpty()) {
			throw new IllegalArgumentException("Must provide the blocks JSON");
		}
		
		String comment = this.keyValue.get(this.keysToGet[2]);
		if(comment != null && !comment.isEmpty()) {
			comment = Utility.decodeURIComponent(comment);
		}
		
		IProject project = Utility.getProject(projectId);
		String portalsFolder = AssetUtility.getProjectPortalsFolder(projectId);
		File blocksJsonFile = new File(portalsFolder+"/"+IProject.BLOCK_FILE_NAME);
		if(blocksJsonFile.exists() && blocksJsonFile.isFile()) {
			blocksJsonFile.delete();
		}
		
		try {
			GsonUtility.writeObjectToJsonFile(blocksJsonFile, new GsonBuilder().setPrettyPrinting().create(), json);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Was unable to save the blocks json to the project folder. Errror = " + e.getMessage());
		}
		
		// add file to git
		List<String> files = new Vector<>();
		files.add(blocksJsonFile.getAbsolutePath());
		String projectVersionFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId);
		GitRepoUtils.addSpecificFiles(projectVersionFolder, files);
		// commit it
		GitRepoUtils.commitAddedFiles(projectVersionFolder, comment, user);
		
		if (ClusterUtil.IS_CLUSTER) {
			logger.info("Syncing project for cloud backup");
			ClusterUtil.pushProjectFolder(project, projectVersionFolder);
			SecurityProjectUtils.setPortalPublish(user, projectId);
		}
		
		return new NounMetadata(true, PixelDataType.MAP);
	}
	
	private Map<String,Object> getBlocksJSON() {
		GenRowStruct mapGrs = this.store.getNoun(ReactorKeysEnum.JSON.getKey());
		if(mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if(mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Object>) mapInputs.get(0).getValue();
			}
			
			List<NounMetadata> encodedStrGrs = mapGrs.getNounsOfType(PixelDataType.CONST_STRING);
			if(encodedStrGrs != null && !encodedStrGrs.isEmpty()) {
				String encodedStr = (String) encodedStrGrs.get(0).getValue();
				String mapStr = Utility.decodeURIComponent(encodedStr);
				return new Gson().fromJson(mapStr, Map.class);
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
		if(key.equals(ReactorKeysEnum.JSON.getKey())) {
			return "The JSON that represents the blocks for the app";
		}
		return super.getDescriptionForKey(key);
	}
	

}
