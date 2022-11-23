package prerna.sablecc2.reactor.project;

import org.apache.commons.lang3.StringUtils;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class PublishProjectReactor extends AbstractReactor {
	
	public PublishProjectReactor() {
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
			if(!SecurityProjectUtils.userIsOwner(this.insight.getUser(), projectId)) {
				throw new IllegalArgumentException("Project does not exist or user is not an owner of the project");
			}
		}
		
		IProject project = Utility.getProject(projectId);
		project.setRepublish(true);
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully set the project to publish"));
		return noun;
	}

}
