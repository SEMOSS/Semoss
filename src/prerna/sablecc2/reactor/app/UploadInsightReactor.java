package prerna.sablecc2.reactor.app;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.impl.InsightAdministrator;
import prerna.engine.impl.SmssUtilities;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.app.upload.UploadInputUtility;
import prerna.sablecc2.reactor.insights.AbstractInsightReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.MosfetSyncHelper;
import prerna.util.Utility;
import prerna.util.ZipUtils;

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

		// security
		if (AbstractSecurityUtils.securityEnabled()) {
			if (AbstractSecurityUtils.anonymousUsersEnabled() && this.insight.getUser().isAnonymous()) {
				throwAnonymousUserError();
			}

			if (!SecurityAppUtils.userCanEditEngine(this.insight.getUser(), appId)) {
				throw new IllegalArgumentException("User does not have permission to add insights in the app");
			}
		}

		String mosfetFileLoc = null;
		IEngine app = Utility.getEngine(appId);
		String versionFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + "db"
				+ DIR_SEPARATOR + SmssUtilities.getUniqueName(app.getEngineName(), appId) + DIR_SEPARATOR + "version" + DIR_SEPARATOR;
		// unzip asset to db folder
		try {
			Map<String, List<String>> filesAdded = ZipUtils.unzip(zipFilePath, versionFolder);
			List<String> list = filesAdded.get("DIR");
			// get the insight folder name
			String insightFolderName = list.get(0);
			mosfetFileLoc = versionFolder + insightFolderName + ".mosfet";

		} catch (IOException e) {
			e.printStackTrace();
			SemossPixelException exception = new SemossPixelException(NounMetadata.getErrorNounMessage("Unable to unzip files."));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		File mosfetFile = new File(mosfetFileLoc);
		// get insight mosfet to register new insight
		// TODO we are assuming the insight is uploaded to the same app
		// TODO we can resync to use new app
		Map<String, Object> mosfet = MosfetSyncHelper.getMosfitMap(mosfetFile);

		// add the recipe to the insights database
		InsightAdministrator admin = new InsightAdministrator(app.getInsightDatabase());

		String insightName = mosfet.get(MosfetSyncHelper.INSIGHT_NAME_KEY).toString();
		logger.info("1) Add insight " + insightName + " to rdbms store...");
		String newInsightId = mosfet.get(MosfetSyncHelper.RDBMS_ID_KEY).toString();;
		String layout = mosfet.get(MosfetSyncHelper.LAYOUT_KEY).toString();
		String recipeToSave = mosfet.get(MosfetSyncHelper.RECIPE_KEY).toString();
		String[] pixelRecipeToSave = new String[] { recipeToSave };
		boolean hidden = false;
		String newRdbmsId = admin.addInsight(newInsightId, insightName, layout, pixelRecipeToSave, hidden);
		logger.info("1) Done...");

	
		logger.info("2) Regsiter insight...");
		SecurityInsightUtils.addInsight(appId, newInsightId, insightName, true, layout);
		if (this.insight.getUser() != null) {
			SecurityInsightUtils.addUserInsightCreator(this.insight.getUser(), appId, newInsightId);
		}
		logger.info("2) Done...");
		

		ClusterUtil.reactorPushApp(appId);

		Map<String, Object> returnMap = new HashMap<String, Object>();
		returnMap.put("name", insightName);
		returnMap.put("app_insight_id", newRdbmsId);
		returnMap.put("app_name", app.getEngineName());
		returnMap.put("app_id", app.getEngineId());
		returnMap.put("recipe", recipeToSave);
		NounMetadata noun = new NounMetadata(returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.SAVE_INSIGHT);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully added new insight."));
		return noun;

	}

}
