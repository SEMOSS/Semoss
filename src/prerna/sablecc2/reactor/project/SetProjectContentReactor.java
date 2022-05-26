package prerna.sablecc2.reactor.project;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.project.impl.ProjectProperties;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class SetProjectContentReactor extends AbstractReactor {
	
	public SetProjectContentReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.PROJECT_PROPERTIES_MAP.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		
		/*
		 * Checks to see if project based on project id exists 
		 */
		if(StringUtils.isBlank(projectId)) {
			throw new IllegalArgumentException("Must input an project id");
		}
		
		/*
		 * Checks to make sure only the owner of project can run this reactor 
		 */
		if(AbstractSecurityUtils.securityEnabled()) {
			if(!SecurityProjectUtils.userIsOwner(this.insight.getUser(), projectId)) {
				throw new IllegalArgumentException("Project does not exist or user does not have access to edit");
			}
		}
				
		IProject project = Utility.getProject(projectId);
		ProjectProperties props = project.getProjectProperties();
		
		Map<String, String> mods = getMods();
		props.updateAllProperties(mods);
		System.out.println(props);
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully set new properties for project"));
		return noun;
	}
	
	/*
	 * Converts inputed map of pixel call into a Map<string, string>
	 */
	private Map<String, String> getMods() {
		GenRowStruct mapGrs = this.store.getNoun(ReactorKeysEnum.PROJECT_PROPERTIES_MAP.getKey());
		if (mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if (mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, String>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, String>) mapInputs.get(0).getValue();
		}

		throw new IllegalArgumentException("Invalid submit request");
	}

}
