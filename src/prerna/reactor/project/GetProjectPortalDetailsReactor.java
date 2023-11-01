package prerna.reactor.project;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class GetProjectPortalDetailsReactor extends AbstractReactor {
	
	public GetProjectPortalDetailsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		
		if(projectId == null || projectId.isEmpty()) {
			throw new IllegalArgumentException("Must input an project id");
		}
		
		// make sure valid id for user
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if(!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
			// you dont have access
			throw new IllegalArgumentException("Project does not exist or user does not have access to the project");
		}
		
		Map<String, Object> portalDetails = new HashMap<>();

		IProject project = Utility.getProject(projectId);
		boolean hasPortal = project.isHasPortal();
		portalDetails.put("project_has_portal", hasPortal);
		portalDetails.put("project_is_published", project.isPublished());
		// TODO: old - will remove once confirmed from FE
		portalDetails.put("isPublished", project.isPublished());
		if(hasPortal) {
			String url = Utility.getApplicationUrl() + "/" + Utility.getPublicHomeFolder() + "/" + projectId + "/" + Constants.PORTALS_FOLDER + "/";
			portalDetails.put("project_portal_url", url);
			// TODO: old - will remove once confirmed from FE
			portalDetails.put("url", url);
		}
		return new NounMetadata(portalDetails, PixelDataType.MAP);
	}

}
