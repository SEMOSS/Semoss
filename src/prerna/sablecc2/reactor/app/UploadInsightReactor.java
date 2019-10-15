package prerna.sablecc2.reactor.app;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.h2.store.fs.FileUtils;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.impl.InsightAdministrator;
import prerna.om.MosfetFile;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.app.upload.UploadInputUtility;
import prerna.sablecc2.reactor.insights.AbstractInsightReactor;
import prerna.util.AssetUtility;
import prerna.util.Utility;
import prerna.util.ZipUtils;
import prerna.util.git.GitRepoUtils;

public class UploadInsightReactor extends AbstractInsightReactor {
	private static final String CLASS_NAME = UploadInsightReactor.class.getName();

	public UploadInsightReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.FILE_PATH.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = this.getLogger(CLASS_NAME);
		// get inputs
		String zipFilePath = UploadInputUtility.getFilePath(this.store, this.insight);
		String appId = getApp();
		String author = null;
		String email = null;

		// security
		if (AbstractSecurityUtils.securityEnabled()) {
			User user = this.insight.getUser();
			if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
				throwAnonymousUserError();
			}
			if (!SecurityAppUtils.userCanEditEngine(user, appId)) {
				throw new IllegalArgumentException("User does not have permission to add insights in the app");
			}
			// Get the user's email
			AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
			email = accessToken.getEmail();
			author = accessToken.getUsername();
		}

		Map<String, List<String>> filesAdded = new HashMap<>();
		List<String> fileList = new Vector<>();
		String mosfetFileLoc = null;
		File mosfetFile = null;
		IEngine app = Utility.getEngine(appId);
		String versionFolder = AssetUtility.getAppAssetVersionFolder(app.getEngineName(), appId);
		// unzip asset to db folder
		try {
			filesAdded = ZipUtils.unzip(zipFilePath, versionFolder);
			fileList = filesAdded.get("FILE");
			for (String filePath : fileList) {
				if (filePath.endsWith(MosfetFile.RECIPE_FILE)) {
					mosfetFileLoc = versionFolder + DIR_SEPARATOR + filePath;
					mosfetFile = new File(mosfetFileLoc);
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
			e.printStackTrace();
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
			e.printStackTrace();
			SemossPixelException exception = new SemossPixelException(
					NounMetadata.getErrorNounMessage("Unable to load the mosfet file."));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		// add the recipe to the insights database
		InsightAdministrator admin = new InsightAdministrator(app.getInsightDatabase());

		String insightName = mosfet.getInsightName();
		int step = 1;
		logger.info(step + ") Add insight " + insightName + " to rdbms store...");
		String newInsightId = mosfet.getRdbmsId();
		String layout = mosfet.getLayout();
		String[] pixelRecipeToSave = mosfet.getRecipe();
		boolean hidden = false;
		String newRdbmsId = admin.addInsight(newInsightId, insightName, layout, pixelRecipeToSave, hidden);
		logger.info(step + ") Done...");
		step++;

		// add file to git
		logger.info(step + ") Adding insight to git...");
		String gitFolder = AssetUtility.getAppAssetVersionFolder(app.getEngineName(), app.getEngineId());
		GitRepoUtils.addSpecificFiles(gitFolder, fileList);
		// commit it
		String comment = "Adding " + insightName + " insight.";
		GitRepoUtils.commitAddedFiles(gitFolder, comment, author, email);
		logger.info(step + ") Done...");
		step++;

		logger.info(step + ") Regsiter insight...");
		SecurityInsightUtils.addInsight(appId, newInsightId, insightName, true, layout);
		if (this.insight.getUser() != null) {
			SecurityInsightUtils.addUserInsightCreator(this.insight.getUser(), appId, newInsightId);
		}
		logger.info(step + ") Done...");
		step++;

		ClusterUtil.reactorPushApp(appId);

		Map<String, Object> returnMap = new HashMap<String, Object>();
		returnMap.put("name", insightName);
		returnMap.put("app_insight_id", newRdbmsId);
		returnMap.put("app_name", app.getEngineName());
		returnMap.put("app_id", app.getEngineId());
		returnMap.put("recipe", pixelRecipeToSave);
		NounMetadata noun = new NounMetadata(returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.SAVE_INSIGHT);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully added new insight."));
		return noun;
	}

}
