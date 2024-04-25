package prerna.util.git.reactors;

import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineUtility;
import prerna.util.git.GitUtils;

public class IsGit extends AbstractReactor {

	public IsGit() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(this.getClass().getName());
		organizeKeys();
		String projectId = keyValue.get(keysToGet[0]);
		if(projectId == null || projectId.isEmpty()) {
			throw new IllegalArgumentException("Need to specify the project id");
		}

		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if(!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access to edit the project");
		}
		String projectName = SecurityProjectUtils.getProjectAliasForId(projectId);
		
		logger.info("Checking - Please wait");
		// get the path of the git location
		String thisProjectFolder = EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.PROJECT, projectId, projectName);
		boolean isGit = GitUtils.isGit(thisProjectFolder);
		logger.info("Complete");
		return new NounMetadata(isGit, PixelDataType.BOOLEAN, PixelOperationType.MARKET_PLACE);
	}

}
