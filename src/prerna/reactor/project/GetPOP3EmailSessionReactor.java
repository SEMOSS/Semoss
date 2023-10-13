package prerna.reactor.project;

import org.apache.commons.lang3.StringUtils;

import jakarta.mail.Store;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.project.impl.ProjectProperties;
import prerna.project.impl.ProjectPropertyEvaluator;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetPOP3EmailSessionReactor extends AbstractReactor {
		
	public GetPOP3EmailSessionReactor() {
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
		
		Store pop3Store = props.getPop3EmailStore();
		if(pop3Store  == null) {
			throw new IllegalArgumentException("POP3 Email Store is not defined for this project");
		}

		ProjectPropertyEvaluator eval = new ProjectPropertyEvaluator();
		eval.setProjectId(projectId);
		eval.setMethodName("getPop3EmailStore");
		NounMetadata noun = new NounMetadata(eval, PixelDataType.EMAIL_SESSION);
		return noun;
	}
	

}
