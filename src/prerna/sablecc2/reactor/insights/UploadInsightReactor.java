package prerna.sablecc2.reactor.insights;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.Logger;
import org.h2.store.fs.FileUtils;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.engine.impl.InsightAdministrator;
import prerna.om.MosfetFile;
import prerna.project.api.IProject;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.ZipUtils;
import prerna.util.git.GitRepoUtils;
import prerna.util.upload.UploadInputUtility;

public class UploadInsightReactor extends AbstractInsightReactor {
	
	// TODO: change to project from database
	
	private static final String CLASS_NAME = UploadInsightReactor.class.getName();

	public UploadInsightReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = this.getLogger(CLASS_NAME);
		// get inputs
		String zipFilePath = UploadInputUtility.getFilePath(this.store, this.insight);
		String projectId = getProject();

		// security
		User user = this.insight.getUser();
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}
		if (!SecurityEngineUtils.userCanEditEngine(user, projectId)) {
			throw new IllegalArgumentException("User does not have permission to add insights in the proejct");
		}
		// Get the user's email
		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
		String email = accessToken.getEmail();
		String author = accessToken.getUsername();

		Map<String, List<String>> filesAdded = new HashMap<>();
		List<String> fileList = new Vector<>();
		String mosfetFileLoc = null;
		File mosfetFile = null;
		IProject project = Utility.getProject(projectId);
		String versionFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId);
		// unzip asset to db folder
		try {
			filesAdded = ZipUtils.unzip(zipFilePath, versionFolder);
			fileList = filesAdded.get("FILE");
			for (String filePath : fileList) {
				if (filePath.endsWith(MosfetFile.RECIPE_FILE)) {
					mosfetFileLoc = versionFolder + DIR_SEPARATOR + filePath;
					mosfetFile = new File(Utility.normalizePath(mosfetFileLoc));
					// check if the file exists
					if (!mosfetFile.exists()) {
						// invalid file need to delete the files unzipped
						mosfetFileLoc = null;
					}
					break;
				}
			}

			// delete the files if we were unable to find the mosfet file
			if (mosfetFileLoc == null) {
				for (String filePath : fileList) {
					FileUtils.delete(versionFolder + DIR_SEPARATOR + filePath);
				}
				List<String> dirList = filesAdded.get("DIR");
				for (String filePath : dirList) {
					FileUtils.deleteRecursive(versionFolder + DIR_SEPARATOR + filePath, true);
				}
				SemossPixelException exception = new SemossPixelException(
						NounMetadata.getErrorNounMessage("Unable to find " + MosfetFile.RECIPE_FILE + " file."));
				exception.setContinueThreadOfExecution(false);
				throw exception;
			}

		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
			SemossPixelException exception = new SemossPixelException(
					NounMetadata.getErrorNounMessage("Unable to unzip files."));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		// get insight mosfet to register new insight
		// TODO we are assuming the insight is uploaded to the same app
		// TODO we can resync to use new app
		MosfetFile mosfet;
		try {
			mosfet = MosfetFile.generateFromFile(mosfetFile);
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
			SemossPixelException exception = new SemossPixelException(
					NounMetadata.getErrorNounMessage("Unable to load the mosfet file."));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		// add the recipe to the insights database
		InsightAdministrator admin = new InsightAdministrator(project.getInsightDatabase());

		String insightName = mosfet.getInsightName();
		int step = 1;
		logger.info(step + ") Add insight " + Utility.cleanLogString(insightName) + " to rdbms store...");
		String newInsightId = mosfet.getRdbmsId();
		String layout = mosfet.getLayout();
		List<String> pixelRecipeToSave = mosfet.getRecipe();
		boolean hidden = mosfet.isGlobal();
		boolean cacheable = mosfet.isCacheable();
		int cacheMinutes = mosfet.getCacheMinutes();
		String cacheCron = mosfet.getCacheCron();
		LocalDateTime cachedOn = mosfet.getCachedOn();
		boolean cacheEncrypt = mosfet.isCacheEncrypt();
		String schemaName = mosfet.getSchemaName();
		
		String newRdbmsId = admin.addInsight(newInsightId, insightName, layout, pixelRecipeToSave, hidden, 
				cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, schemaName);
		logger.info(step + ") Done...");
		step++;

		// add file to git
		logger.info(step + ") Adding insight to git...");
		String gitFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId);
		GitRepoUtils.addSpecificFiles(gitFolder, fileList);
		// commit it
		String comment = "Adding " + insightName + " insight.";
		GitRepoUtils.commitAddedFiles(gitFolder, comment, author, email);
		logger.info(step + ") Done...");
		step++;

		logger.info(step + ") Regsiter insight...");
		SecurityInsightUtils.addInsight(projectId, newInsightId, insightName, true, 
				layout, cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, 
				pixelRecipeToSave, schemaName);
		if (this.insight.getUser() != null) {
			SecurityInsightUtils.addUserInsightCreator(this.insight.getUser(), projectId, newInsightId);
		}
		logger.info(step + ") Done...");
		step++;

//		ClusterUtil.reactorPushInsightDB(projectId);

		Map<String, Object> returnMap = new HashMap<>();
		returnMap.put("name", insightName);
		returnMap.put("app_insight_id", newRdbmsId);
		returnMap.put("app_name", project.getProjectName());
		returnMap.put("app_id", projectId);
		returnMap.put("recipe", pixelRecipeToSave);
		NounMetadata noun = new NounMetadata(returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.SAVE_INSIGHT);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully added new insight."));
		return noun;
	}

}
