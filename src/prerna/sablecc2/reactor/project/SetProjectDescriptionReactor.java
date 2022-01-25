package prerna.sablecc2.reactor.project;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.app.upload.UploadInputUtility;

public class SetProjectDescriptionReactor extends AbstractReactor {
	
	public static final String DESCRIPTIONS = "description";

	public SetProjectDescriptionReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.DESCRIPTION.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = UploadInputUtility.getProjectNameOrId(this.store);
		
		if(AbstractSecurityUtils.securityEnabled()) {
			projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
			if(!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
				throw new IllegalArgumentException("Project does not exist or user does not have access to edit");
			}
		}

		String description = this.keyValue.get(this.keysToGet[1]);
		SecurityProjectUtils.updateProjectDescription(projectId, description);

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully saved new description for project"));
		return noun;
	}
}
