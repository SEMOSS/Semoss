package prerna.reactor.cluster;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;

public class PullProjectFolderFromCloudReactor extends AbstractReactor {
	
	public PullProjectFolderFromCloudReactor() {
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
		ClusterUtil.pullProjectFolder(project, projectFolderPath);

		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

}