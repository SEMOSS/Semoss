package prerna.util;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.om.Insight;
import prerna.project.api.IProject;

/**
 * A resolved asset "space": the folder that a reactor's relative file paths are
 * anchored to, plus what is needed to version and replicate a write into it.
 *
 * <p>
 * Accepts the same {@code space} values as {@link AssetUtility#getRootFolderPath}
 * (the convention shared by SaveAsset, BrowseAsset, DeleteAsset, ...) and adds
 * room ids:
 * <ul>
 * <li>{@code null}, empty or {@code "insight"} - the current insight folder. When
 * the insight is bound to a room this is the room folder.</li>
 * <li>{@code "user"} - the logged in user's asset project ({@code app_root}, so
 * relative paths normally start with {@code version/assets/}).</li>
 * <li>a project id - that project's {@code app_root}.</li>
 * <li>a room id the user owns and that is active - that room's folder.</li>
 * </ul>
 * Project ids are tried before room ids, so the room lookup only costs a query
 * when the value is not a project the user can access.
 */
public final class AssetSpace {

	private static final Logger classLogger = LogManager.getLogger(AssetSpace.class);

	public enum Kind {
		USER, PROJECT, ROOM, INSIGHT
	}

	private final Kind kind;
	private final String id;
	private final String rootFolder;
	private final String gitFolder;
	private final IProject project;

	private AssetSpace(Kind kind, String id, String rootFolder, String gitFolder, IProject project) {
		this.kind = kind;
		this.id = id;
		this.rootFolder = Utility.normalizePath(rootFolder).replace("\\", "/");
		this.gitFolder = gitFolder == null ? null : Utility.normalizePath(gitFolder).replace("\\", "/");
		this.project = project;
	}

	/**
	 * Resolves a space key for the current insight and user, enforcing view or edit
	 * permission as requested.
	 *
	 * @param in           the current insight
	 * @param space        the space key (see class documentation)
	 * @param editRequired {@code true} when the caller will write into the space
	 * @return the resolved space
	 * @throws IllegalArgumentException if the space does not exist or the user lacks
	 *                                  the required permission
	 */
	public static AssetSpace resolve(Insight in, String space, boolean editRequired) {
		User user = in.getUser();
		String key = space == null ? "" : space.trim();

		if (key.isEmpty() || AssetUtility.INSIGHT_SPACE_KEY.equalsIgnoreCase(key)) {
			if (editRequired && in.isSavedInsight()
					&& !SecurityInsightUtils.userCanEditInsight(user, in.getProjectId(), in.getRdbmsId())) {
				throw new IllegalArgumentException("User does not have permission for this insight");
			}
			String root = in.getInsightFolder();
			if (in.getRoomId() != null) {
				return new AssetSpace(Kind.ROOM, in.getRoomId(), root, null, null);
			}
			IProject insightProject = in.isSavedInsight() ? Utility.getProject(in.getProjectId()) : null;
			return new AssetSpace(Kind.INSIGHT, in.getInsightId(), root, null, insightProject);
		}

		if (AssetUtility.USER_SPACE_KEY.equalsIgnoreCase(key)) {
			if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
				throw new IllegalArgumentException("Must be logged in to access user specific assets");
			}
			// loads (and in a cluster pulls, once per pod) the user asset project
			IProject assetProject = user.getAssetProject();
			if (assetProject == null) {
				throw new IllegalArgumentException("Unable to find user asset app");
			}
			String root = AssetUtility.getUserAssetAppRootFolder(assetProject.getProjectName(),
					assetProject.getProjectId());
			String git = AssetUtility.getUserAssetVersionFolder(assetProject.getProjectName(),
					assetProject.getProjectId());
			return new AssetSpace(Kind.USER, assetProject.getProjectId(), root, git, assetProject);
		}

		boolean projectAllowed = editRequired ? SecurityProjectUtils.userCanEditProject(user, key)
				: SecurityProjectUtils.userCanViewProject(user, key);
		if (projectAllowed) {
			IProject targetProject = Utility.getProject(key);
			if (targetProject == null) {
				throw new IllegalArgumentException("Unable to load project " + key);
			}
			String root = AssetUtility.getProjectAppRootFolder(targetProject.getProjectName(),
					targetProject.getProjectId());
			String git = AssetUtility.getProjectVersionFolder(targetProject.getProjectName(),
					targetProject.getProjectId());
			return new AssetSpace(Kind.PROJECT, targetProject.getProjectId(), root, git, targetProject);
		}

		AccessToken token = user.getPrimaryLoginToken();
		String userId = token == null ? null : token.getId();
		if (userId != null && !ModelInferenceLogsUtils.getUserActiveRooms(key, userId).isEmpty()) {
			Room room = RoomUtils.getOrLoadRoom(key, in);
			if (room == null || room.getRoomFolderPath() == null) {
				throw new IllegalArgumentException("Room " + key + " could not be loaded");
			}
			return new AssetSpace(Kind.ROOM, key, room.getRoomFolderPath(), null, null);
		}

		throw new IllegalArgumentException(
				"Space '" + key + "' is not a project or active room the user has permission for");
	}

	public Kind getKind() {
		return this.kind;
	}

	/** Project id, room id or insight id depending on {@link #getKind()}. */
	public String getId() {
		return this.id;
	}

	public String getRootFolder() {
		return this.rootFolder;
	}

	/** The git working directory, or {@code null} for spaces that are not versioned. */
	public String getGitFolder() {
		return this.gitFolder;
	}

	public boolean isGitBacked() {
		return this.gitFolder != null;
	}

	public IProject getProject() {
		return this.project;
	}

	/** The value a caller would pass as {@code space} to address this space. */
	public String label() {
		switch (this.kind) {
		case USER:
			return AssetUtility.USER_SPACE_KEY.toLowerCase();
		case INSIGHT:
			return AssetUtility.INSIGHT_SPACE_KEY.toLowerCase();
		default:
			return this.id;
		}
	}

	/**
	 * Resolves a caller supplied relative path to an absolute path inside this
	 * space. Rejects empty paths, {@code .} and {@code ..} segments, and anything
	 * that would escape the root folder.
	 *
	 * @param relativePath the path relative to the space root
	 * @return the absolute, normalized path (forward slashes)
	 */
	public String resolvePath(String relativePath) {
		if (relativePath == null || relativePath.trim().isEmpty()) {
			throw new IllegalArgumentException("Must pass a file path");
		}
		String normalized = Utility.normalizePath(relativePath.trim());
		while (normalized.startsWith("/")) {
			normalized = normalized.substring(1);
		}
		if (normalized.isEmpty()) {
			throw new IllegalArgumentException("A file path is required, not the space root");
		}
		for (String part : normalized.split("/")) {
			if (part.equals("..") || part.equals(".") || part.isEmpty()) {
				throw new IllegalArgumentException("File path must not contain '.' or '..' segments");
			}
		}
		Path root = Paths.get(this.rootFolder).toAbsolutePath().normalize();
		Path target = root.resolve(normalized).normalize();
		if (!target.startsWith(root)) {
			throw new IllegalArgumentException("File path must stay within the space");
		}
		return target.toString().replace("\\", "/");
	}

	/** The path relative to the space root, as a caller would pass it. */
	public String relativePath(String absolutePath) {
		Path root = Paths.get(this.rootFolder).toAbsolutePath().normalize();
		return root.relativize(Paths.get(absolutePath).toAbsolutePath().normalize()).toString().replace("\\", "/");
	}

	/**
	 * The path relative to the git working directory, for staging. Only valid when
	 * {@link #isGitBacked()}.
	 */
	public String gitRelativePath(String absolutePath) {
		if (this.gitFolder == null) {
			throw new IllegalStateException("Space is not git backed");
		}
		Path git = Paths.get(this.gitFolder).toAbsolutePath().normalize();
		return git.relativize(Paths.get(absolutePath).toAbsolutePath().normalize()).toString().replace("\\", "/");
	}

	/**
	 * Replicates a write to cloud storage when running in a cluster. No-op
	 * otherwise, and for unsaved insights.
	 *
	 * @param absoluteChangedPath the file that was written
	 */
	public void pushToCloud(String absoluteChangedPath) {
		if (!ClusterUtil.IS_CLUSTER) {
			return;
		}
		switch (this.kind) {
		case USER:
			ClusterUtil.pushUserAsset(this.id);
			break;
		case PROJECT:
			ClusterUtil.pushProjectFolder(this.project, new File(absoluteChangedPath).getParent());
			break;
		case ROOM:
			ClusterUtil.pushRoom(this.id);
			break;
		case INSIGHT:
			if (this.project != null) {
				ClusterUtil.pushProjectFolder(this.project, new File(absoluteChangedPath).getParent());
			} else {
				classLogger.debug("Unsaved insight {} is not replicated to cloud storage", this.id);
			}
			break;
		default:
			break;
		}
	}
}
