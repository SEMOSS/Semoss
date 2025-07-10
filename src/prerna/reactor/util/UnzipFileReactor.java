package prerna.reactor.util;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;
import prerna.util.ZipUtils;

public class UnzipFileReactor extends AbstractReactor {

	public UnzipFileReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey()};
		this.keyRequired = new int[] {1, 0};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}
		
		// specify the folder from the base
		String fileRelativePath = Utility.normalizePath(keyValue.get(keysToGet[0]));
		String space = this.keyValue.get(this.keysToGet[1]);
		
		// if security enables, you need proper permissions
		// this takes in the insight and does a user check that the user has access to perform the operations
		String baseFolder = AssetUtility.getRootFolderPath(this.insight, space, true);
		String zipFileLocation = (baseFolder + "/" + fileRelativePath).replace('\\', '/');
		File zipFile = new File(zipFileLocation);
		if(zipFile.exists() && !zipFile.isFile()) {
			throw new IllegalArgumentException("Cannot find zip file '" + fileRelativePath + "')");
		}

		try {
			ZipUtils.unzip(zipFileLocation, zipFile.getParent());
		} catch (IOException e) {
			throw new IllegalArgumentException("Unable to unzip file. Detailed error = " + e.getMessage());
		}
		
		if(ClusterUtil.IS_CLUSTER) {
			//is it in the user space?
			if (AssetUtility.USER_SPACE_KEY.equalsIgnoreCase(space)) {
				AuthProvider provider = user.getPrimaryLogin();
				String projectId = user.getAssetProjectId(provider);
				if(projectId!=null && !(projectId.isEmpty())) {
					ClusterUtil.pushUserWorkspace(projectId, true);
				}
			// is it in the insight space of a saved insight?
			} else if(space == null || space.trim().isEmpty() || space.equals(AssetUtility.INSIGHT_SPACE_KEY)) {
				if(this.insight.isSavedInsight()) {
					IProject project = Utility.getProject(this.insight.getProjectId());
					ClusterUtil.pushProjectFolder(project, zipFile.getParent());
				}
			// this is in the project space where space = project id
			} else {
				IProject project = Utility.getProject(space);
				ClusterUtil.pushProjectFolder(project, zipFile.getParent());
			}
		}
		
		// extract engineIds from project
		// then process and set project dependencies
		File finalProjectFolder = new File(zipFile.getParent());
		
		String[] engineIds = UploadInputUtility.getEngineIdsFromProject(finalProjectFolder);
		Map<String, Object> engineInfo = UploadInputUtility.processAndSetProjectDependencies(engineIds, space, user);
		
		Map<String, Object> retMap = new HashMap<>();
		retMap.put("success", true);
		retMap.put("engineIds", engineInfo);
		
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}
	
	@Override
	public String getReactorDescription() {
	    return "Unzips the updated project and routes the extracted files";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
	    if(key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
	        return "This is a required value containing the relative file path of the single zip file to be imported";
	    } else if(key.equals(ReactorKeysEnum.SPACE.getKey())) {
	        return "This is an optional field to determine the space in which the relative file path exists (user project space, current insight space, project id space).";
	    }
	    return super.getDescriptionForKey(key);
	}
	
}
