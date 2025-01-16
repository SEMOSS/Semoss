package prerna.util.git.reactors;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;

public class CloneProjectReactor extends AbstractReactor {

	public CloneProjectReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey() /* old project id */,
				ReactorKeysEnum.SPACE.getKey() };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String OldProjectID = this.keyValue.get(this.keysToGet[0]);
		String space = this.keyValue.get(this.keysToGet[1]);
		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}
		// Check whether the user has permission to clone the application
		boolean isAdmin = SecurityAdminUtils.userIsAdmin(user);
		if (!isAdmin) {
			boolean isOwner = SecurityProjectUtils.userIsOwner(user, OldProjectID);
			if (!isOwner) {
				throw new IllegalArgumentException(
						"Project " + OldProjectID + "user does not have access to Clone this App.");
			}
		}

		String oldProjectAssetFolder = AssetUtility.getProjectAssetFolder(OldProjectID);
		String newProjectAssetFolder = AssetUtility.getAssetBasePath(this.insight, space, true) + "/version/assets/";

		File newProjectFolder = new File(newProjectAssetFolder);
		// Copy the Asset folder from the old project to the new project
		try {
			copyDirectory(new File(oldProjectAssetFolder), new File(newProjectAssetFolder));
		} catch (IOException e) {
			// Handle the exception by throwing a runtime exception with a message
			throw new RuntimeException(
					"Failed to clone the app from " + oldProjectAssetFolder + " to " + newProjectAssetFolder, e);
		}

		if (ClusterUtil.IS_CLUSTER) {
			// is it in the user space?
			if (AssetUtility.USER_SPACE_KEY.equalsIgnoreCase(space)) {
				AuthProvider provider = user.getPrimaryLogin();
				String projectId = user.getAssetProjectId(provider);
				if (projectId != null && !(projectId.isEmpty())) {
					ClusterUtil.pushUserWorkspace(projectId, true);
				}
				// is it in the insight space of a saved insight?
			} else if (space == null || space.trim().isEmpty() || space.equals(AssetUtility.INSIGHT_SPACE_KEY)) {
				if (this.insight.isSavedInsight()) {
					IProject project = Utility.getProject(this.insight.getProjectId());
					ClusterUtil.pushProjectFolder(project, newProjectFolder.getParent());
				}
				// this is in the project space where space = project id
			} else {
				IProject project = Utility.getProject(space);
				ClusterUtil.pushProjectFolder(project, newProjectFolder.getParent());
			}
		}

		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	private void copyDirectory(File source, File destination) throws IOException {
		if (source.isDirectory()) {
			// Create the destination directory if it doesn't exist
			if (!destination.exists()) {
				destination.mkdirs();
			}

			// List all files and directories in the source directory
			String[] children = source.list();
			if (children != null) {
				for (String child : children) {
					// Recursively copy each child
					copyDirectory(new File(source, child), new File(destination, child));
				}
			}
		} else {
			// If it's a file, copy it
			Files.copy(source.toPath(), destination.toPath(), StandardCopyOption.REPLACE_EXISTING);
		}
	}
}
