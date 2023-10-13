package prerna.reactor.project;

import org.apache.commons.lang3.StringUtils;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class PublishProjectReactor extends AbstractReactor {
	
	public PublishProjectReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.RELEASE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		Boolean release = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[1])+"");
		if(StringUtils.isBlank(projectId)) {
			throw new IllegalArgumentException("Must input an project id");
		}
		
		User user = this.insight.getUser();
		if(!SecurityProjectUtils.userIsOwner(user, projectId)) {
			throw new IllegalArgumentException("Project does not exist or user is not an owner of the project");
		}
		
		IProject project = Utility.getProject(projectId);
		project.setRepublish(true);
		if(release) {
			SecurityProjectUtils.setPortalPublish(user, projectId);
			ClusterUtil.pushProjectFolder(project, AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId), 
					Constants.ASSETS_FOLDER + "/" + Constants.PORTALS_FOLDER);
		}
		
		String url = Utility.getApplicationUrl() + "/" + Utility.getPublicHomeFolder() + "/" + projectId + "/" + Constants.PORTALS_FOLDER + "/";
		NounMetadata noun = new NounMetadata(url, PixelDataType.CONST_STRING);
		if(release) {
			noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully published and released the project"));
		} else {
			noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully published the project"));
		}
		return noun;
	}

}
