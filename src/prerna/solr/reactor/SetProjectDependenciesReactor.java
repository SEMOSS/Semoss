package prerna.solr.reactor;

import java.util.List;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.upload.UploadInputUtility;

public class SetProjectDependenciesReactor extends AbstractSetMetadataReactor {
	
	public SetProjectDependenciesReactor() {
		this.keysToGet = new String[]{
				ReactorKeysEnum.PROJECT.getKey(), "dependencies", 
			};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		String projectId = UploadInputUtility.getProjectNameOrId(this.store);
		List<String> dependentEngineIds = getDependentEngineIds();

		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		SecurityProjectUtils.updateProjectDependencies(user, projectId, dependentEngineIds);

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully set the new dependencies"));
		return noun;
	}
	
	@Override
	public String getReactorDescription() {
		return "Set the engine dependencies for a project";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals("dependencies")) {
			return "The list of engineid's that this project depends on for full functionality";
		}
		return super.getDescriptionForKey(key);
	}
	
	
	private List<String> getDependentEngineIds() {
		GenRowStruct colGrs = this.store.getNoun("dependencies");
		if (colGrs != null && !colGrs.isEmpty()) {
			return colGrs.getAllStrValues();
		}

		return null;
	}

}
