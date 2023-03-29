package prerna.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.SmssUtilities;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.util.git.GitRepoUtils;

public class AssetUtility {

	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	public static String USER_SPACE_KEY = "USER";
	public static String INSIGHT_SPACE_KEY = "INSIGHT";

	/**
	 * Grab the workspace to work with asset files
	 * 
	 * PROJECT-ID: project/project_folder/version/assets 
	 * USER: project/userApp/version/assets 
	 * INSIGHT: project/project_folder/app_root/version/insightID
	 * 
	 * @param in
	 * @param space
	 * @return
	 */
	public static String getAssetBasePath(Insight in, String space, boolean editRequired) {
		String assetFolder = in.getInsightFolder();
		// find out what space the user wants to use to get the base asset path
		if (space != null && !space.isEmpty()) {
			if (USER_SPACE_KEY.equalsIgnoreCase(space)) {
				if (AbstractSecurityUtils.securityEnabled()) {
					User user = in.getUser();
					if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
						throw new IllegalArgumentException("Must be logged in to access user specific assets");
					}
					AuthProvider provider = user.getPrimaryLogin();
					String projectId = user.getAssetProjectId(provider);
					String projectName = "Asset";
					assetFolder = getUserAssetAndWorkspaceBaseFolder(projectName, projectId);
				}
			} else if (INSIGHT_SPACE_KEY.equalsIgnoreCase(space)) {
				// default
				// but need to perform check
				if(editRequired && in.isSavedInsight() && !SecurityInsightUtils.userCanEditInsight(in.getUser(), in.getProjectId(), in.getRdbmsId())) {
					throw new IllegalArgumentException("User does not have permission for this insight");
				}
			} else {
				// user has passed an id
				String projectId = space;
				// check if the user has permission for the app
				if (AbstractSecurityUtils.securityEnabled()) {
					if(editRequired) {
						if(!SecurityProjectUtils.userCanEditProject(in.getUser(), projectId)) {
							throw new IllegalArgumentException("User does not have permission for this project");
						}
					} else {
						// only read access
						if(!SecurityProjectUtils.userCanViewProject(in.getUser(), projectId)) {
							throw new IllegalArgumentException("User does not have permission for this project");
						}
					}
				}
				IProject project = Utility.getProject(projectId);
				String projectName = project.getProjectName();
				// assetFolder = getAppAssetFolder(appName, appId);
				assetFolder = getProjectBaseFolder(projectName, projectId);
			}
		} else if(in.isSavedInsight() && editRequired){
			// we are about to send back the insight folder 
			// since that is the default
			// FE very rarely sends the INSIGHT_SPACE_KEY
			// and edit is required
			// make sure user has access
			if(!SecurityInsightUtils.userCanEditInsight(in.getUser(), in.getProjectId(), in.getRdbmsId())) {
				throw new IllegalArgumentException("User does not have permission for this insight");
			}
		}
		assetFolder = assetFolder.replace('\\', '/');
		return assetFolder;
	}
	
	/**
	 * Grab the git version base path
	 * 
	 * @param in
	 * @param space
	 * @param editRequired
	 * @return
	 */
	public static String getAssetVersionBasePath(Insight in, String space, boolean editRequired) {
		String assetFolder = null;
		if(in.isSavedInsight()) {
			assetFolder = getProjectVersionFolder(in.getProjectName(), in.getProjectId());
		} else {
			assetFolder = in.getInsightFolder();
		}
		
		// find out what space the user wants to use to get the base asset path
		if (space != null) {
			if (USER_SPACE_KEY.equalsIgnoreCase(space)) {
				if (AbstractSecurityUtils.securityEnabled()) {
					User user = in.getUser();
					if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
						throw new IllegalArgumentException("Must be logged in to perform this operation");
					}
					AuthProvider provider = user.getPrimaryLogin();
					String projectId = user.getAssetProjectId(provider);
					String projectName = "Asset";
					assetFolder = getUserAssetAndWorkspaceBaseFolder(projectName, projectId);
				}
			} else if (INSIGHT_SPACE_KEY.equalsIgnoreCase(space)) {
				// default
				// but need to perform check
				if(editRequired && in.isSavedInsight() && !SecurityInsightUtils.userCanEditInsight(in.getUser(), in.getProjectId(), in.getRdbmsId())) {
					throw new IllegalArgumentException("User does not have permission for this insight");
				}
			} else {
				// user has passed an id
				String projectId = space;
				// check if the user has permission for the app
				if (AbstractSecurityUtils.securityEnabled()) {
					if(editRequired) {
						if(!SecurityProjectUtils.userCanEditProject(in.getUser(), space)) {
							throw new IllegalArgumentException("User does not have permission for this project");
						}
					} else {
						// only read access
						if(!SecurityProjectUtils.userCanViewProject(in.getUser(), space)) {
							throw new IllegalArgumentException("User does not have permission for this project");
						}
					}
				}
				IProject project = Utility.getProject(projectId);
				String projectName = project.getProjectName();
				assetFolder = getProjectBaseFolder(projectName, projectId);
			}
		} else if(in.isSavedInsight() && editRequired){
			// we are about to send back the insight folder 
			// since that is the default
			// FE very rarely sends the INSIGHT_SPACE_KEY
			// and edit is required
			// make sure user has access
			if(!SecurityInsightUtils.userCanEditInsight(in.getUser(), in.getProjectId(), in.getRdbmsId())) {
				throw new IllegalArgumentException("User does not have permission for this insight");
			}
		}
		assetFolder = assetFolder.replace('\\', '/');
		
		// need to make adjustment here so that if it is not version then ignore initing here
		if(in.isSavedInsight() && !isGit(assetFolder) && !assetFolder.trim().endsWith(Constants.APP_ROOT_FOLDER)) {
			GitRepoUtils.init(assetFolder);
		}
		return assetFolder;
	}
	
	public static String getProjectAssetFolder(String projectId) {
		IProject project = Utility.getProject(projectId);
		String projectName = project.getProjectName();
		return AssetUtility.getProjectAssetFolder(projectName, projectId);
	}
	
	public static String getProjectAssetFolder(String projectName, String projectId) {
		String projectVersionBaseFolder = getProjectVersionFolder(projectName, projectId);
		String projectFolder = projectVersionBaseFolder + DIR_SEPARATOR + "assets";

		// if this folder does not exist create it
		File file = new File(projectFolder);
		if (!file.exists()) {
			file.mkdir();
		}
		return projectFolder;
	}
	
	public static String getProjectVersionFolder(String projectName, String projectId) {
		return getProjectVersionFolder(projectName, projectId, false);
	}
	
	public static String getProjectVersionFolder(String projectName, String projectId, boolean ignoreGit) {
		String appBaseFolder = getProjectBaseFolder(projectName, projectId);
		String gitFolder = appBaseFolder + DIR_SEPARATOR + Constants.VERSION_FOLDER;
		// if this folder does not exist create it
		File file = new File(Utility.normalizePath(gitFolder));
		if (!file.exists()) {			
			file.mkdir();
		}
		
		if(!ignoreGit && !isGit(gitFolder)) {
			GitRepoUtils.init(gitFolder);
		}
		return gitFolder;
	}
	
	public static String getAssetRelativePath(Insight in, String space) {
		String relativePath = "";
		if(space == null || space.equals(INSIGHT_SPACE_KEY)) {
			relativePath = in.getRdbmsId();
		} else {
			// user space or asset app
			// asset app - no relative space ?
			relativePath = "";
			//relativePath = "assets";
		}	
		return relativePath;
	}
	
	public static boolean isGit(String assetFolder) {
		File file = new File(Utility.normalizePath(assetFolder) + DIR_SEPARATOR + ".git");
		return file.exists();
	}

	public static String getProjectBaseFolder(String projectId) {
        IProject project = Utility.getProject(projectId);
        String projectName = project.getProjectName();
        return AssetUtility.getProjectBaseFolder(projectName, projectId);
    }
	
	public static String getProjectBaseFolder(String projectName, String projectId) {
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		if( !(baseFolder.endsWith("/") || baseFolder.endsWith("\\")) ) {
			baseFolder += DIR_SEPARATOR;
		}
		
		String baseProjectFolder = Utility.normalizePath(baseFolder + Constants.PROJECT_FOLDER + DIR_SEPARATOR 
				+ SmssUtilities.getUniqueName(projectName, projectId) + DIR_SEPARATOR + Constants.APP_ROOT_FOLDER );

		File baseProjectFolderFile = new File(baseProjectFolder);
		if(!baseProjectFolderFile.exists()) {
			baseProjectFolderFile.mkdir();
			// if you are creating this.. there is a possibility we need to fix this project
			rehomeDB(projectName, projectId, baseProjectFolder);
		}
		// try to see if there is a version folder and if so move it into app_root
		return baseProjectFolder;
	}
	
	private static void rehomeDB(String projectName, String projectId, String newRoot) {
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		if( !(baseFolder.endsWith("/") || baseFolder.endsWith("\\")) ) {
			baseFolder += DIR_SEPARATOR;
		}

		String oldBaseAppFolder = Utility.normalizePath(baseFolder + Constants.PROJECT_FOLDER + DIR_SEPARATOR 
				+ SmssUtilities.getUniqueName(projectName, projectId) + DIR_SEPARATOR + Constants.VERSION_FOLDER );

		File oldBaseAppFolderFile = new File(oldBaseAppFolder);

		if(oldBaseAppFolderFile.exists()) {
			try {
				System.err.println(">>>>> Rehoming Catalog : " + projectName + " <<<<<<");
				Files.move(oldBaseAppFolderFile.toPath(), new File(newRoot + DIR_SEPARATOR + Constants.VERSION_FOLDER).toPath(), StandardCopyOption.REPLACE_EXISTING);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/*
	 * USER ASSET METHODS
	 */
	
	public static String getUserAssetAndWorkspaceVersionFolder(String projectName, String projectId) {
		// get the base folder
		String baseFodler = getUserAssetAndWorkspaceBaseFolder(projectName, projectId);
		return baseFodler + "/version";
	}
	
	public static String getUserAssetAndWorkspaceBaseFolder(String projectName, String projectId) {
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		if( !(baseFolder.endsWith("/") || baseFolder.endsWith("\\")) ) {
			baseFolder += DIR_SEPARATOR;
		}
		
		String baseProjectFolder = Utility.normalizePath(baseFolder + Constants.USER_FOLDER + DIR_SEPARATOR 
				+ SmssUtilities.getUniqueName(projectName, projectId) + DIR_SEPARATOR + Constants.APP_ROOT_FOLDER );

		File baseAppFolderFile = new File(baseProjectFolder);
		if(!baseAppFolderFile.exists()) {
			baseAppFolderFile.mkdir();
			// if you are creating this.. there is a possibility we need to fix this engine
			rehomeDB(projectName, projectId, baseProjectFolder);
		}
		// try to see if there is a version folder and if so move it into app_root
		return baseProjectFolder;
	}
	
}
