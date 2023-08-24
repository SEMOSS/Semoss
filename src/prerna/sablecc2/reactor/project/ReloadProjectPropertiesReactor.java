package prerna.sablecc2.reactor.project;

import org.apache.commons.lang3.StringUtils;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.project.impl.ProjectProperties;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class ReloadProjectPropertiesReactor extends AbstractReactor {
		
	public ReloadProjectPropertiesReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		
		if(StringUtils.isBlank(projectId)) {
			throw new IllegalArgumentException("Must input an project id");
		}
			
		if(!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access to edit");
		}
		
		IProject project = Utility.getProject(projectId);
		ProjectProperties props = project.getProjectProperties();
		props.reloadProps();
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully set new properties for project"));
		return noun;
	}
}