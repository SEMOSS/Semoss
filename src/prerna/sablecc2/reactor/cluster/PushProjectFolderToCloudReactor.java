package prerna.sablecc2.reactor.cluster;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.AssetUtility;
import prerna.util.Utility;

public class PushProjectFolderToCloudReactor extends AbstractReactor {
	
	public PushProjectFolderToCloudReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		if(projectId == null || (projectId=projectId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must input an project id");
		}
		
		// make sure valid id for user
		if(!SecurityProjectUtils.userIsOwner(this.insight.getUser(), projectId)) {
			// you dont have access
			throw new IllegalArgumentException("Project does not exist or user is not an owner to force pulling from cloud storage");
		}
		
		IProject project = Utility.getProject(projectId);
		String projectFolderPath = AssetUtility.getProjectBaseFolder(project.getProjectName(), projectId).replace("\\", "/");
		ClusterUtil.pushProjectFolder(project, projectFolderPath);

		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

}