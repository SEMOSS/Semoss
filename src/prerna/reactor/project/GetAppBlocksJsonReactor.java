package prerna.reactor.project;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.reflect.TypeToken;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.gson.GsonUtility;

public class GetAppBlocksJsonReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(GetAppBlocksJsonReactor.class);

	public GetAppBlocksJsonReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
	
		if(projectId == null || projectId.isEmpty()) {
			throw new IllegalArgumentException("Must input an project id");
		}
		
		// make sure valid id for user
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if(!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
			// you dont have access
			throw new IllegalArgumentException("Project does not exist or user does not have access to the project");
		}
		
		IProject project = Utility.getProject(projectId);
		String portalsFolder = AssetUtility.getProjectPortalsFolder(project.getProjectId());
		File blocksJsonFile = new File(portalsFolder+"/"+IProject.BLOCK_FILE_NAME);
		if(!blocksJsonFile.exists() && !blocksJsonFile.isFile()) {
			throw new IllegalArgumentException("No blocks json file exists for this app");
		}
		Map<String, Object> blocksJson;
		try {
			blocksJson = (Map<String, Object>) GsonUtility.readJsonFileToObject(blocksJsonFile, new TypeToken<Map<String, Object>>() {}.getType());
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Unable to read the blocks json file. Error = " + e.getMessage());
		}
		
		return new NounMetadata(blocksJson, PixelDataType.MAP);
	}

}
