package prerna.sablecc2.reactor.project;

import javax.mail.Session;

import org.apache.commons.lang3.StringUtils;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.project.impl.ProjectProperties;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
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
			
		if(AbstractSecurityUtils.securityEnabled()) {
			if(!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
				throw new IllegalArgumentException("Project does not exist or user does not have access to edit");
			}
		}
		
		IProject project = Utility.getProject(projectId);
		ProjectProperties props = project.getProjectProperties();
		
		Session emailSession = props.getEmailSession();
		if(emailSession == null) {
			throw new IllegalArgumentException("Email Session is not defined for this project");
		}
		NounMetadata noun = new NounMetadata(emailSession, PixelDataType.EMAIL_SESSION);
		return noun;
	}
	

}
