package prerna.reactor.project;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;

public class BrowseAppAssetsReactor extends AbstractReactor {

	public BrowseAppAssetsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.FILE_PATH.getKey() };
		this.keyRequired = new int[] {1,0};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String projectId = this.keyValue.get(this.keysToGet[0]);
		if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
			throw new IllegalArgumentException("Project " + projectId + " does not exist or user does not have access to edit assets.");
		}
		IProject project = Utility.getProject(projectId);

		String relativeFilePath = this.keyValue.get(this.keysToGet[1]);
		if(relativeFilePath != null) {
			relativeFilePath = Utility.normalizePath(relativeFilePath.trim());
			if(!relativeFilePath.isEmpty()) {
				relativeFilePath = relativeFilePath.replace('\\', '/');
				if(!relativeFilePath.startsWith("/")) {
					relativeFilePath = "/" + relativeFilePath;
				}
			}
		}
		
		String pathSubstring = AssetUtility.getProjectAppRootFolder(project.getProjectName(), project.getProjectId());
		int pathSubstringIndex = pathSubstring.length();
		String filePath = AssetUtility.getProjectAssetsFolder(project.getProjectName(), project.getProjectId());
		if(relativeFilePath != null && !relativeFilePath.isEmpty()) {
			filePath += relativeFilePath;
		}
		
		File directory = new File(filePath);
		if(!directory.exists()) {
			throw new IllegalArgumentException("The directory " + relativeFilePath + " does not exist within the assets folder");
		}
		if(!directory.isDirectory()) {
			throw new IllegalArgumentException("The path " + relativeFilePath + " exists within the assets folder but is not a directory");
		}
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
		
		List<Map<String, Object>> retObj = new ArrayList<>();
		File[] allFiles = directory.listFiles();
		for(File f : allFiles) {
			if(f.getName().startsWith(".") && f.isDirectory()) {
				// we dont want to show this
				continue;
			}
			Map<String, Object> fileMap = new HashMap<>();
			fileMap.put("name", f.getName());
			if (f.isDirectory()) {
				fileMap.put("type", "directory");
			} else {
				fileMap.put("type", FilenameUtils.getExtension(f.getName()));
			}
			fileMap.put("lastModified", dateFormat.format(f.lastModified()));
			fileMap.put("path", f.getAbsolutePath().substring(pathSubstringIndex));
			retObj.add(fileMap);
		}

		NounMetadata retNoun = new NounMetadata(retObj, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		return retNoun;
	}

	@Override
	public String getReactorDescription() {
		return "List the files and directories from a relative filePath input from within the projects assets folder";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The unique id for the project/app";
		} else if(key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "The relative file path to list contents from. This relative path should assume the prefix of '/version/assets/' and not include the prefix in the string value.";
		}
		return super.getDescriptionForKey(key);
	}

}
