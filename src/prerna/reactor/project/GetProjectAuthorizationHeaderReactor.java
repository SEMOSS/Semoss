package prerna.reactor.project;

import org.apache.commons.lang3.StringUtils;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.project.impl.ProjectHeaderAuthEvaluator;
import prerna.project.impl.ProjectProperties;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetProjectAuthorizationHeaderReactor extends AbstractReactor {
		
	public GetProjectAuthorizationHeaderReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		
		if(StringUtils.isBlank(projectId)) {
			throw new IllegalArgumentException("Must input an project id");
		}
			
		if(!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have edit access to get the authorization headers");
		}
		
		// make sure we have the value or throw a null pointer
		IProject project = Utility.getProject(projectId);
		ProjectProperties props = project.getProjectProperties();
		
		//TODO assuming right now it is Basic with access/secret key
		
		ProjectHeaderAuthEvaluator eval = new ProjectHeaderAuthEvaluator();
		eval.setProjectId(projectId);
		eval.setAccessKey(props.getProperty("accessKey"));
		eval.setSecretKey(props.getProperty("secretKey"));
		NounMetadata noun = new NounMetadata(eval, PixelDataType.PROJECT_AUTHORIZATION_HEADER);
		return noun;
	}
	

}
