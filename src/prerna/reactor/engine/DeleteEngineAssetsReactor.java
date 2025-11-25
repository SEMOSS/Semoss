package prerna.reactor.engine;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.git.GitDestroyer;
import prerna.util.git.GitRepoUtils;

public class DeleteEngineAssetsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(DeleteEngineAssetsReactor.class);

	public DeleteEngineAssetsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.FILE_PATH.getKey(),
				ReactorKeysEnum.COMMENT_KEY.getKey() };
		this.keyRequired = new int[] { 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String engineId = this.keyValue.get(this.keysToGet[0]);
		if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Engine " + engineId + " does not exist or user does not have access to edit assets.");
		}
		IEngine engine = Utility.getEngine(engineId);

		String versionGitFolder = EngineUtility.getSpecificEngineVersionFolder(engine.getCatalogType(),
				engine.getEngineId(), engine.getEngineName());
		String assetFolder = EngineUtility.getSpecificEngineAssetsFolder(engine.getCatalogType(), engine.getEngineId(),
				engine.getEngineName());

		// Retrieve all file names and contents
		// get the list of file paths to delete
		List<String> filePaths = getNounAsStringList(this.keysToGet[1]);
		if (filePaths == null) {
			filePaths = new ArrayList<>();
		}
		if (filePaths.isEmpty()) {
			File[] allFilesInAssets = new File(assetFolder).listFiles();
			for (File f : allFilesInAssets) {
				filePaths.add(f.getName());
			}
		}

		String comment = this.keyValue.get(this.keysToGet[2]);
		if (comment == null) {
			comment = "remove: DeleteEngineAssets executed";
		}

		// Prepare to collect Git relative paths and actual File objects
		List<String> gitRelativeFilePaths = new ArrayList<>();
		List<File> deletedFiles = new ArrayList<>();

		// iterate each provided path and delete it
		for (String rawPath : filePaths) {
			String inputFilePath = Utility.normalizePath(rawPath.trim());
			if (inputFilePath == null || inputFilePath.isEmpty()) {
				continue;
			}

			String realFilePath = assetFolder + "/" + inputFilePath;
			realFilePath = realFilePath.replace("\\", "/");
			File realFile = new File(realFilePath);
			if (!realFile.exists()) {
				classLogger.warn("Cannot find the folder/file at path {}. Skipping.", inputFilePath);
				continue;
			}

			if (realFile.isDirectory()) {
				try {
					FileUtils.deleteDirectory(realFile);
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
					throw new IllegalArgumentException(
							"Error occurred trying to delete folder at path " + inputFilePath);
				}
			} else {
				try {
					FileUtils.forceDelete(realFile);
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
					throw new IllegalArgumentException("Error occurred trying to delete file at path " + inputFilePath);
				}
			}

			// Collect for Git and cluster sync
			gitRelativeFilePaths.add(Constants.ASSETS_FOLDER + "/" + inputFilePath);
			deletedFiles.add(realFile);
		}

		if (deletedFiles.isEmpty()) {
			throw new IllegalArgumentException("Could not find any of the files passed in to delete");
		}

		// Get the user's email
		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
		String email = accessToken.getEmail();
		String author = accessToken.getUsername();

		GitDestroyer.removeSpecificFiles(versionGitFolder, true, gitRelativeFilePaths);
		// commit it
		GitRepoUtils.commitAddedFiles(versionGitFolder, comment, author, email);
		// handle synchronization to the cloud
		ClusterUtil.pushEngineFolder(engine, assetFolder);

		NounMetadata retNoun = NounMetadata.getSuccessNounMessage("Success!");
		return retNoun;
	}

	@Override
	public String getReactorDescription() {
		return "Delete a single or multiple files in the engine folder";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The unique id for the engine";
		} else if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return """
					Names of the file(s) to delete. This relative path should assume the prefix of '/version/assets/' and not include the prefix in the string value.
					If no value passed in, all files in '/version/assets/' will be deleted.";
					""";
		} else if (key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
			return "Comment to add while removing the files within the git repository for the engine";
		}
		return super.getDescriptionForKey(key);
	}

}
