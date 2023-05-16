package prerna.sablecc2.reactor.project;

import java.util.ArrayList;
import java.util.List;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.ds.py.TCPPyTranslator;
import prerna.project.api.IProject;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class ProjectPyReactor extends AbstractReactor {
	
	public ProjectPyReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.CODE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		String code = Utility.decodeURIComponent(this.keyValue.get(this.keysToGet[1]));

		if(projectId == null || projectId.isEmpty()) {
			throw new IllegalArgumentException("Must input an project id");
		}
		
		if(AbstractSecurityUtils.securityEnabled()) {
			// make sure valid id for user
			projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
			if(!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
				// you don't have access
				throw new IllegalArgumentException("Project does not exist or user does not have access to the project");
			}
		}

		IProject project = Utility.getProject(projectId);
		TCPPyTranslator projectPyTranslator = project.getProjectPyTranslator();
		String output = projectPyTranslator.runSingle(this.insight.getUser().getVarMap(), code, null);
		
		List<NounMetadata> outputs = new ArrayList<>(1);
		outputs.add(new NounMetadata(output, PixelDataType.CONST_STRING));
		return new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
	}

}