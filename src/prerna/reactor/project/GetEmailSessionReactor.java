package prerna.reactor.project;

import org.apache.commons.lang3.StringUtils;

import jakarta.mail.Session;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.project.impl.ProjectProperties;
import prerna.project.impl.ProjectPropertyEvaluator;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetEmailSessionReactor extends AbstractReactor {
		
	public GetEmailSessionReactor() {
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
			throw new IllegalArgumentException("Project does not exist or user does not have access to edit");
		}
		
		// make sure we have the value or throw a null pointer
		IProject project = Utility.getProject(projectId);
		ProjectProperties props = project.getProjectProperties();
		
		Session emailSession = props.getEmailSession();
		if(emailSession == null) {
			throw new IllegalArgumentException("Email Session is not defined for this project");
		}

		ProjectPropertyEvaluator eval = new ProjectPropertyEvaluator();
		eval.setProjectId(projectId);
		eval.setMethodName("getEmailSession");
		NounMetadata noun = new NounMetadata(eval, PixelDataType.EMAIL_SESSION);
		return noun;
	}
	

}
