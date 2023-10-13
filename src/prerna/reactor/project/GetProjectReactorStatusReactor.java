package prerna.reactor.project;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ProjectCustomReactorCompilator;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetProjectReactorStatusReactor extends AbstractReactor {

	public GetProjectReactorStatusReactor() {
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
		
		Map<String, Object> retObj = new HashMap<>();
		retObj.put("isCompiled", ProjectCustomReactorCompilator.isCompiled(projectId));
		retObj.put("isFailed", ProjectCustomReactorCompilator.isFailed(projectId));
	
		return new NounMetadata(retObj, PixelDataType.MAP);
	}
	
}
