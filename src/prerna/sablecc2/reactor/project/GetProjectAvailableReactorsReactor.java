package prerna.sablecc2.reactor.project;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class GetProjectAvailableReactorsReactor extends AbstractReactor {
	
	public GetProjectAvailableReactorsReactor() {
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
		return new NounMetadata(project.getAvailableReactors(), PixelDataType.CONST_STRING);
	}

}