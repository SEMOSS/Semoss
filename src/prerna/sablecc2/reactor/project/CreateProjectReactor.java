package prerna.sablecc2.reactor.project;

import java.util.Map;

import org.apache.logging.log4j.Logger;

import prerna.engine.impl.ProjectHelper;
import prerna.project.api.IProject;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.app.upload.UploadUtilities;

public class CreateProjectReactor extends AbstractReactor {

	private static final String CLASS_NAME = CreateProjectReactor.class.getName();

	/*
	 * This class is used to construct a new project
	 * This project only contains insights
	 */

	public CreateProjectReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		this.organizeKeys();
		String projectName = this.keyValue.get(this.keysToGet[0]);
		IProject project = ProjectHelper.generateNewProject(projectName, this.insight.getUser(), logger);
				
		Map<String, Object> retMap = UploadUtilities.getProjectReturnData(this.insight.getUser(), project.getProjectId());
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}
}
