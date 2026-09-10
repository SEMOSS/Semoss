package prerna.util.git.reactors;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetSpace;
import prerna.util.FileSystemUtil;
import prerna.util.ProjectSyncUtility;
import prerna.util.git.GitRepoUtils;

/**
 * Copies one file from any asset space into any other in a single server side
 * call: read permission on the source, edit permission on the target, one git
 * commit when the target is versioned (user or project space), one cloud push,
 * all under the target project's lock so a cluster sync cannot interleave.
 */
public class CopyAssetReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CopyAssetReactor.class);

	private static final String OVERRIDE = ReactorKeysEnum.OVERRIDE.getKey();

	public CopyAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey(),
				ReactorKeysEnum.TARGET_PATH.getKey(), ReactorKeysEnum.TARGET_SPACE.getKey(), OVERRIDE,
				ReactorKeysEnum.COMMENT_KEY.getKey() };
		this.keyRequired = new int[] { 1, 0, 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String sourcePath = this.keyValue.get(this.keysToGet[0]);
		String targetPath = this.keyValue.get(this.keysToGet[2]);
		if (sourcePath == null || sourcePath.trim().isEmpty() || targetPath == null || targetPath.trim().isEmpty()) {
			throw new IllegalArgumentException("Must pass both filePath and targetPath");
		}

		AssetSpace source = AssetSpace.resolve(this.insight, this.keyValue.get(this.keysToGet[1]), false);
		AssetSpace target = AssetSpace.resolve(this.insight, this.keyValue.get(this.keysToGet[3]), true);
		String sourceAbsolute = source.resolvePath(sourcePath);
		String targetAbsolute = target.resolvePath(targetPath);
		boolean override = getBoolean(OVERRIDE, false);

		String comment = this.keyValue.get(this.keysToGet[5]);
		if (comment == null || comment.trim().isEmpty()) {
			comment = "copy: " + source.relativePath(sourceAbsolute) + " (" + source.label() + ") to "
					+ target.relativePath(targetAbsolute) + " (" + target.label() + ")";
		}

		// Serialize with everything else that commits or syncs this project's folder.
		ReentrantLock lock = target.isGitBacked() ? ProjectSyncUtility.getProjectLock(target.getId()) : null;
		if (lock != null) {
			lock.lock();
		}
		try {
			long size = FileSystemUtil.copyResolvedFile(sourceAbsolute, targetAbsolute, override);

			if (target.isGitBacked()) {
				// Version control on asset folders is bookkeeping. The file is on disk;
				// a git problem must not turn a successful copy into a failed pixel.
				try {
					AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
					GitRepoUtils.addFilesRelativeToRepo(target.getGitFolder(),
							Arrays.asList(target.gitRelativePath(targetAbsolute)));
					GitRepoUtils.commitAddedFiles(target.getGitFolder(), comment, accessToken.getResolvedUsername(),
							accessToken.getEmail());
				} catch (RuntimeException e) {
					classLogger.error("Copied {} into space {} but could not record it in git", targetPath,
							target.label(), e);
				}
			}

			target.pushToCloud(targetAbsolute);

			Map<String, Object> result = new HashMap<>();
			result.put("space", target.label());
			result.put("filePath", target.relativePath(targetAbsolute));
			result.put("size", size);
			result.put("versioned", target.isGitBacked());
			return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
		} finally {
			if (lock != null) {
				lock.unlock();
			}
		}
	}

	@Override
	public String getReactorDescription() {
		return "Copy a single file from one asset space (insight, room, user, or project) into another. "
				+ "The target is committed to git when it is a user or project space and replicated to cloud storage in a cluster.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "The source file, relative to the source space. For user and project spaces this normally starts with 'version/assets/'.";
		} else if (key.equals(ReactorKeysEnum.SPACE.getKey())) {
			return "The source space: omit or 'insight' for the current insight (or its room), 'user' for the user's space, a project id, or a room id the user owns.";
		} else if (key.equals(ReactorKeysEnum.TARGET_PATH.getKey())) {
			return "The destination file, relative to the target space. Parent folders are created as needed.";
		} else if (key.equals(ReactorKeysEnum.TARGET_SPACE.getKey())) {
			return "The target space, same values as space. The user needs edit permission on it.";
		} else if (key.equals(OVERRIDE)) {
			return "Whether to replace an existing destination file. Defaults to false, in which case an existing destination is an error.";
		} else if (key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
			return "Git commit message used when the target space is versioned.";
		}
		return super.getDescriptionForKey(key);
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
