package prerna.util.git.reactors;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.EngineSyncUtility;
import prerna.util.EngineUtility;
import prerna.util.FileSystemUtil;
import prerna.util.ProjectSyncUtility;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

/**
 * Copies one file between any two asset locations in a single server side call.
 *
 * <pre>
 * Copy(source, sourceType, sourceFilePath, target, targetType, targetFilePath, override, comment)
 * </pre>
 *
 * Types are explicit (INSIGHT, ROOM, PROJECT, ENGINE, USER) so no id has to be
 * guessed. File paths are relative to the location's assets folder. Read access
 * is required on the source, edit access on the target. PROJECT, ENGINE and USER
 * targets are git backed: the copy is committed once and the whole copy + commit
 * + cloud push runs under that project's or engine's lock. INSIGHT and ROOM
 * targets are plain folders. Only files are copied; directories are rejected.
 */
public class CopyReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CopyReactor.class);

	private static final String SOURCE = "source";
	private static final String SOURCE_TYPE = "sourceType";
	private static final String SOURCE_FILE_PATH = "sourceFilePath";
	private static final String TARGET = "target";
	private static final String TARGET_TYPE = "targetType";
	private static final String TARGET_FILE_PATH = "targetFilePath";
	private static final String OVERRIDE = ReactorKeysEnum.OVERRIDE.getKey();
	private static final String COMMENT = ReactorKeysEnum.COMMENT_KEY.getKey();

	private enum SpaceType {
		INSIGHT, ROOM, PROJECT, ENGINE, USER;

		static SpaceType parse(String key, String value) {
			if (value == null || value.trim().isEmpty()) {
				throw new IllegalArgumentException("Must pass " + key + " as one of " + Arrays.toString(values()));
			}
			try {
				return valueOf(value.trim().toUpperCase());
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException(
						"Unknown " + key + " '" + value + "'. Expected one of " + Arrays.toString(values()));
			}
		}
	}

	/** A resolved type + id: where relative paths live and how writes are recorded. */
	private static final class Location {
		SpaceType type;
		String id;
		String assetFolder;
		/** git working directory, or null when the location is not versioned */
		String gitFolder;
		IProject project;
		IEngine engine;
	}

	public CopyReactor() {
		this.keysToGet = new String[] { SOURCE, SOURCE_TYPE, SOURCE_FILE_PATH, TARGET, TARGET_TYPE, TARGET_FILE_PATH,
				OVERRIDE, COMMENT };
		this.keyRequired = new int[] { 0, 1, 1, 0, 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		SpaceType sourceType = SpaceType.parse(SOURCE_TYPE, this.keyValue.get(SOURCE_TYPE));
		SpaceType targetType = SpaceType.parse(TARGET_TYPE, this.keyValue.get(TARGET_TYPE));
		String sourceRelative = relativePath(SOURCE_FILE_PATH, this.keyValue.get(SOURCE_FILE_PATH));
		String targetRelative = relativePath(TARGET_FILE_PATH, this.keyValue.get(TARGET_FILE_PATH));

		Location source = resolve(user, sourceType, this.keyValue.get(SOURCE), false);
		Location target = resolve(user, targetType, this.keyValue.get(TARGET), true);
		String sourceAbsolute = absolutePath(source.assetFolder, sourceRelative);
		String targetAbsolute = absolutePath(target.assetFolder, targetRelative);

		boolean override = getBoolean(OVERRIDE, false);
		String comment = this.keyValue.get(COMMENT);
		if (comment == null || comment.trim().isEmpty()) {
			comment = "copy: " + sourceRelative + " (" + sourceType + ") to " + targetRelative + " (" + targetType + ")";
		}

		// Serialize with everything else that commits to or syncs this folder.
		ReentrantLock lock = lockFor(target);
		if (lock != null) {
			lock.lock();
		}
		try {
			long size = FileSystemUtil.copyResolvedFile(sourceAbsolute, targetAbsolute, override);

			if (target.gitFolder != null) {
				// Version history on asset folders is bookkeeping. The file is on disk;
				// a git problem must not turn a successful copy into a failed pixel.
				try {
					List<String> toAdd = new ArrayList<>();
					toAdd.add(Constants.ASSETS_FOLDER + "/" + targetRelative);
					GitRepoUtils.addSpecificFiles(target.gitFolder, toAdd);
					AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
					GitRepoUtils.commitAddedFiles(target.gitFolder, comment, accessToken.getResolvedUsername(),
							accessToken.getEmail());
				} catch (RuntimeException e) {
					classLogger.error("Copied {} into {} {} but could not record it in git", targetRelative, targetType,
							target.id, e);
				}
			}

			push(target, targetAbsolute);

			Map<String, Object> result = new HashMap<>();
			result.put(TARGET_TYPE, targetType.name());
			if (target.id != null) {
				result.put(TARGET, target.id);
			}
			result.put(TARGET_FILE_PATH, targetRelative);
			result.put("size", size);
			result.put("versioned", target.gitFolder != null);
			return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
		} finally {
			if (lock != null) {
				lock.unlock();
			}
		}
	}

	private Location resolve(User user, SpaceType type, String id, boolean editRequired) {
		Location location = new Location();
		location.type = type;
		String access = editRequired ? "edit" : "view";
		switch (type) {
		case INSIGHT: {
			if (editRequired && this.insight.isSavedInsight() && !SecurityInsightUtils.userCanEditInsight(user,
					this.insight.getProjectId(), this.insight.getRdbmsId())) {
				throw new IllegalArgumentException("User does not have permission to edit this insight");
			}
			location.id = this.insight.getInsightId();
			location.assetFolder = this.insight.getInsightFolder();
			if (this.insight.isSavedInsight()) {
				location.project = Utility.getProject(this.insight.getProjectId());
			}
			return location;
		}
		case ROOM: {
			requireId(type, id);
			AccessToken token = user.getPrimaryLoginToken();
			String userId = token == null ? null : token.getId();
			if (userId == null || ModelInferenceLogsUtils.getUserActiveRooms(id, userId).isEmpty()) {
				throw new IllegalArgumentException(
						"Room " + id + " does not exist, is not active, or the user is not its owner");
			}
			Room room = RoomUtils.getOrLoadRoom(id, this.insight);
			if (room == null || room.getRoomFolderPath() == null) {
				throw new IllegalArgumentException("Room " + id + " could not be loaded");
			}
			location.id = id;
			location.assetFolder = room.getRoomFolderPath();
			return location;
		}
		case PROJECT: {
			requireId(type, id);
			boolean allowed = editRequired ? SecurityProjectUtils.userCanEditProject(user, id)
					: SecurityProjectUtils.userCanViewProject(user, id);
			if (!allowed) {
				throw new IllegalArgumentException(
						"Project " + id + " does not exist or user does not have " + access + " access");
			}
			IProject project = Utility.getProject(id);
			if (project == null) {
				throw new IllegalArgumentException("Project " + id + " could not be loaded");
			}
			location.id = project.getProjectId();
			location.project = project;
			location.assetFolder = AssetUtility.getProjectAssetsFolder(project.getProjectName(),
					project.getProjectId());
			location.gitFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(),
					project.getProjectId());
			return location;
		}
		case ENGINE: {
			requireId(type, id);
			boolean allowed = editRequired ? SecurityEngineUtils.userCanEditEngine(user, id)
					: SecurityEngineUtils.userCanViewEngine(user, id);
			if (!allowed) {
				throw new IllegalArgumentException(
						"Engine " + id + " does not exist or user does not have " + access + " access");
			}
			IEngine engine = Utility.getEngine(id);
			if (engine == null) {
				throw new IllegalArgumentException("Engine " + id + " could not be loaded");
			}
			location.id = engine.getEngineId();
			location.engine = engine;
			location.assetFolder = EngineUtility.getSpecificEngineAssetsFolder(engine.getCatalogType(),
					engine.getEngineId(), engine.getEngineName());
			location.gitFolder = EngineUtility.getSpecificEngineVersionFolder(engine.getCatalogType(),
					engine.getEngineId(), engine.getEngineName());
			return location;
		}
		case USER: {
			IProject assetProject = user.getAssetProject();
			if (assetProject == null) {
				throw new IllegalArgumentException("Unable to find user asset app");
			}
			location.id = assetProject.getProjectId();
			location.project = assetProject;
			location.assetFolder = AssetUtility.getUserAssetFolder(assetProject.getProjectName(),
					assetProject.getProjectId());
			location.gitFolder = AssetUtility.getUserAssetVersionFolder(assetProject.getProjectName(),
					assetProject.getProjectId());
			return location;
		}
		default:
			throw new IllegalArgumentException("Unsupported type " + type);
		}
	}

	private static void requireId(SpaceType type, String id) {
		if (id == null || id.trim().isEmpty()) {
			throw new IllegalArgumentException("Must pass the " + type.name().toLowerCase() + " id for type " + type);
		}
	}

	private static ReentrantLock lockFor(Location target) {
		switch (target.type) {
		case PROJECT:
		case USER:
			return ProjectSyncUtility.getProjectLock(target.id);
		case ENGINE:
			return EngineSyncUtility.getEngineLock(target.id);
		default:
			return null;
		}
	}

	private void push(Location target, String absoluteFile) {
		String folder = new File(absoluteFile).getParent();
		switch (target.type) {
		case USER:
			ClusterUtil.pushUserAsset(target.id);
			break;
		case PROJECT:
			ClusterUtil.pushProjectFolder(target.project, folder);
			break;
		case ENGINE:
			ClusterUtil.pushEngineFolder(target.engine, folder);
			break;
		case ROOM:
			ClusterUtil.pushRoom(target.id);
			break;
		case INSIGHT:
			if (this.insight.getRoomId() != null) {
				ClusterUtil.pushRoom(this.insight.getRoomId());
			} else if (target.project != null) {
				ClusterUtil.pushProjectFolder(target.project, folder);
			}
			break;
		default:
			break;
		}
	}

	/** Normalizes a caller path: forward slashes, no leading slash, no '.' or '..' segments. */
	private static String relativePath(String key, String value) {
		if (value == null || value.trim().isEmpty()) {
			throw new IllegalArgumentException("Must pass " + key);
		}
		String normalized = Utility.normalizePath(value.trim());
		while (normalized.startsWith("/")) {
			normalized = normalized.substring(1);
		}
		if (normalized.isEmpty()) {
			throw new IllegalArgumentException(key + " must name a file, not the assets root");
		}
		for (String part : normalized.split("/")) {
			if (part.isEmpty() || part.equals(".") || part.equals("..")) {
				throw new IllegalArgumentException(key + " must not contain '.' or '..' segments");
			}
		}
		return normalized;
	}

	/** Resolves against the assets folder and refuses anything that escapes it. */
	private static String absolutePath(String assetFolder, String relativePath) {
		Path root = Paths.get(Utility.normalizePath(assetFolder)).toAbsolutePath().normalize();
		Path resolved = root.resolve(relativePath).normalize();
		if (!resolved.startsWith(root)) {
			throw new IllegalArgumentException("File path must stay within the assets folder");
		}
		return resolved.toString().replace("\\", "/");
	}

	@Override
	public String getReactorDescription() {
		return "Copy a single file between asset locations (insight, room, project, engine, user). "
				+ "Requires view access on the source and edit access on the target. Project, engine and user targets "
				+ "are committed to git and replicated to cloud storage in a cluster.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		switch (key) {
		case SOURCE:
			return "Id of the source location: room id, project id, or engine id. Ignored for INSIGHT (the current insight) and USER (the logged in user).";
		case SOURCE_TYPE:
			return "One of INSIGHT, ROOM, PROJECT, ENGINE, USER.";
		case SOURCE_FILE_PATH:
			return "File to copy, relative to the source location's assets folder.";
		case TARGET:
			return "Id of the target location: room id, project id, or engine id. Ignored for INSIGHT and USER.";
		case TARGET_TYPE:
			return "One of INSIGHT, ROOM, PROJECT, ENGINE, USER.";
		case TARGET_FILE_PATH:
			return "Destination path, relative to the target location's assets folder. Parent folders are created as needed.";
		default:
			if (key.equals(OVERRIDE)) {
				return "Whether to replace an existing destination file. Defaults to false, in which case an existing destination is an error.";
			} else if (key.equals(COMMENT)) {
				return "Git commit message used when the target is a project, engine, or user location.";
			}
			return super.getDescriptionForKey(key);
		}
	}

	@Override
	public JSONObject getMcpProperties() {
		JSONObject properties = super.getMcpProperties();
		properties.getJSONObject(OVERRIDE).put("default", false);
		return properties;
	}

	@Override
	protected MCP_KEY_TYPE getKeyTypeForMCP(String key) {
		if (key.equals(OVERRIDE)) {
			return MCP_KEY_TYPE.BOOLEAN;
		}
		return super.getKeyTypeForMCP(key);
	}

}
