package prerna.reactor.project;

import java.util.ArrayList;
import java.util.List;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.frame.r.util.TCPRTranslator;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ProjectRReactor extends AbstractReactor {
	
	public ProjectRReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.CODE.getKey(), ReactorKeysEnum.PROJECT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		if(projectId == null || (projectId=projectId.trim()).isEmpty()) {
			projectId = this.insight.getContextProjectId();
			if(projectId == null || (projectId=projectId.trim()).isEmpty()) {
				projectId = this.insight.getProjectId();
			}
		}
		if(projectId == null || (projectId=projectId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must input an project id");
		}
		
		String code = Utility.decodeURIComponent(this.keyValue.get(ReactorKeysEnum.CODE.getKey()));
		
		// make sure valid id for user
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if(!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
			// you don't have access
			throw new IllegalArgumentException("Project does not exist or user does not have access to the project");
		}

		IProject project = Utility.getProject(projectId);
		TCPRTranslator projectRTranslator = project.getProjectRTranslator();
		String output = projectRTranslator.runRAndReturnOutput(code, insight.getUser().getVarMap());
		
		List<NounMetadata> outputs = new ArrayList<>(1);
		outputs.add(new NounMetadata(output, PixelDataType.CONST_STRING));
		return new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
	}

}