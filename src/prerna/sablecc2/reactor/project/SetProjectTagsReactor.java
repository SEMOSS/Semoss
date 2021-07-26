package prerna.sablecc2.reactor.project;

import java.util.List;
import java.util.Vector;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.app.upload.UploadInputUtility;

public class SetProjectTagsReactor extends AbstractReactor {
	
	public SetProjectTagsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.TAGS.getKey()};
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
		
		List<String> tags = getTags();
		SecurityProjectUtils.updateProjectTags(projectId, tags);
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully saved new tags for project"));
		return noun;
	}
	
	/**
	 * Get the tags to set for the insight
	 * @return
	 */
	protected List<String> getTags() {
		List<String> tags = new Vector<String>();
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.TAGS.getKey());
		if(grs != null && !grs.isEmpty()) {
			for(int i = 0; i < grs.size(); i++) {
				tags.add(grs.get(i).toString());
			}
		}
		
		return tags;
	}

}
