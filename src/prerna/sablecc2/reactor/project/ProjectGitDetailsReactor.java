package prerna.sablecc2.reactor.project;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class ProjectGitDetailsReactor extends AbstractReactor {
	
	public ProjectGitDetailsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		
		if(projectId == null || (projectId=projectId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must input an project id");
		}
		
		User user = this.insight.getUser();
		if(!SecurityProjectUtils.userCanViewProject(user, projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access to the project");
		}

		IProject project = Utility.getProject(projectId);
		
		String gitProvider = project.getProjectGitProvider();
		if(gitProvider == null) {
			gitProvider = "";
		}
		String gitRepo = project.getProjectGitRepo();
		if(gitRepo == null) {
			gitRepo = "";
		}
		
		Map<String, String> gitDetails = new HashMap<>();
		gitDetails.put("gitProvider", gitProvider);
		gitDetails.put("gitRepo", gitRepo);
		return new NounMetadata(gitDetails, PixelDataType.MAP);
	}

}