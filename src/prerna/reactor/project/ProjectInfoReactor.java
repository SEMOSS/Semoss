package prerna.reactor.project;

import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class ProjectInfoReactor extends AbstractReactor {
	
	public ProjectInfoReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.META_KEYS.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		
		if(projectId == null || (projectId=projectId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must input an project id");
		}
		
		// make sure valid id for user
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if(!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
			// you dont have access
			throw new IllegalArgumentException("Project does not exist or user does not have access to the project");
		}
		List<Map<String, Object>> baseInfo = SecurityProjectUtils.getUserProjectList(this.insight.getUser(), projectId);
		
		if(baseInfo == null || baseInfo.isEmpty()) {
			throw new IllegalArgumentException("Could not find any project data");
		}
		
		// we filtered to a single project
		Map<String, Object> projectInfo = baseInfo.get(0);
		projectInfo.putAll(SecurityProjectUtils.getAggregateProjectMetadata(projectId, getMetaKeys(), true));
		// also return the portal url if there is a portal
		if(Boolean.parseBoolean(projectInfo.get("project_has_portal")+"")) {
			String url = Utility.getApplicationUrl() + "/" + Utility.getPublicHomeFolder() + "/" + projectId + "/" + Constants.PORTALS_FOLDER + "/";
			projectInfo.put("project_portal_url", url);
		}
		return new NounMetadata(projectInfo, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.PROJECT_INFO);
	}
	
	private List<String> getMetaKeys() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.META_KEYS.getKey());
		if(grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues();
		}
		
		return null;
	}

}