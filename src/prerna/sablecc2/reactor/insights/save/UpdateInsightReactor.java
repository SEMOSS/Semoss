package prerna.sablecc2.reactor.insights.save;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.cache.InsightCacheUtility;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.impl.InsightAdministrator;
import prerna.engine.impl.SmssUtilities;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.om.MosfetFile;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.insights.AbstractInsightReactor;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.MosfetSyncHelper;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;
import prerna.util.git.GitUtils;

public class UpdateInsightReactor extends AbstractInsightReactor {

	private static final Logger logger = LogManager.getLogger(UpdateInsightReactor.class);
	private static final String CLASS_NAME = UpdateInsightReactor.class.getName();

	public UpdateInsightReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.INSIGHT_NAME.getKey(), 
				ReactorKeysEnum.ID.getKey(), ReactorKeysEnum.LAYOUT_KEY.getKey(), HIDDEN_KEY, 
				ReactorKeysEnum.RECIPE.getKey(), ReactorKeysEnum.DESCRIPTION.getKey(), 
				ReactorKeysEnum.TAGS.getKey(), ReactorKeysEnum.PARAM_KEY.getKey(), 
				ReactorKeysEnum.PIPELINE.getKey(), ReactorKeysEnum.IMAGE.getKey(), ENCODED_KEY};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = this.getLogger(CLASS_NAME);

		String appId = getApp();
		// need to know what we are updating
		String existingId = getRdbmsId();
		
		// security
		if(AbstractSecurityUtils.securityEnabled()) {
			if(AbstractSecurityUtils.anonymousUsersEnabled() && this.insight.getUser().isAnonymous()) {
				throwAnonymousUserError();
			}
			
			if(!SecurityInsightUtils.userCanEditInsight(this.insight.getUser(), appId, existingId)) {
				throw new IllegalArgumentException("User does not have permission to edit this insight");
			}
		}
		
		String insightName = getInsightName();
		if(insightName == null || insightName.isEmpty()) {
			throw new IllegalArgumentException("Need to define the insight name");
		}
		List<String> recipeToSave = getRecipe();
		// saving an empty recipe?
		if (recipeToSave == null || recipeToSave.isEmpty()) {
			recipeToSave = this.insight.getPixelList().getPixelRecipe();
			recipeToSave.addAll(PixelUtility.getMetaInsightRecipeSteps(this.insight));
		} else {
			// default for recipe encoded when no key is passed is true
			if(recipeEncoded()) {
				recipeToSave = decodeRecipe(recipeToSave);
			}
		}

		String layout = getLayout();
		boolean hidden = getHidden();
		List<Map<String, Object>> params = getParams();
		Map pipeline = getPipeline();

		// get an updated recipe if there are files used
		// and save the files in the correct location
		recipeToSave = saveFilesInInsight(recipeToSave, appId, existingId);
		
		if(params != null && !params.isEmpty()) {
			recipeToSave = getParamRecipe(recipeToSave, params, insightName);
		}
		
		IEngine engine = Utility.getEngine(appId);
		if(engine == null) {
			// we may have the alias
			engine = Utility.getEngine(MasterDatabaseUtility.testEngineIdIfAlias(appId));
			if(engine == null) {
				throw new IllegalArgumentException("Cannot find app = " + appId);
			}
		}
		
		//Pull the insights db again incase someone just saved something 
		ClusterUtil.reactorPullInsightsDB(appId);
		ClusterUtil.reactorPullFolder(engine, AssetUtility.getAppAssetVersionFolder(engine.getEngineName(), appId));

		// add the recipe to the insights database
		InsightAdministrator admin = new InsightAdministrator(engine.getInsightDatabase());

		// update insight db
		logger.info("1) Updating insight in rdbms");
		admin.updateInsight(existingId, insightName, layout, recipeToSave, hidden);
		logger.info("1) Done");
		
		String description = getDescription();
		List<String> tags = getTags();
		
		if(!hidden) {
			logger.info("2) Updated registered insight...");
			editRegisteredInsightAndMetadata(engine, existingId, insightName, layout, description, tags);
			logger.info("2) Done...");
		}
		
		// update recipe text file
		logger.info("3) Update Mosfet file for collaboration");
		updateRecipeFile(engine.getEngineId(), engine.getEngineName(), 
				existingId, insightName, layout, IMAGE_NAME, recipeToSave, hidden, description, tags);
		logger.info("3) Done");
		
		// write pipeline
		if(pipeline != null && !pipeline.isEmpty()) {
			logger.info("4) Add pipeline to file...");
			writePipelineToFile(engine.getEngineId(), engine.getEngineName(), existingId, pipeline);
			logger.info("4) Done...");
		}
		
		String base64Image = getImage();
		if(base64Image != null && !base64Image.trim().isEmpty()) {
			storeImageFromPng(base64Image, existingId, engine.getEngineId(), engine.getEngineName());
		}
		
		// update the workspace cache for the saved insight
		this.insight.setEngineId(engine.getEngineId());
		this.insight.setInsightName(insightName);

		// delete the cache
		// NOTE ::: We already pulled above, so we will not pull again to delete the cache
		InsightCacheUtility.deleteCache(engine.getEngineId(), engine.getEngineName(), existingId, false);
		// push back to the cluster
		ClusterUtil.reactorPushInsightDB(appId);
		ClusterUtil.reactorPushFolder(engine, AssetUtility.getAppAssetVersionFolder(engine.getEngineName(), appId));
		
		Map<String, Object> returnMap = new HashMap<String, Object>();
		returnMap.put("name", insightName);
		returnMap.put("app_insight_id", existingId);
		returnMap.put("app_name", engine.getEngineName());
		returnMap.put("app_id", engine.getEngineId());
		NounMetadata noun = new NounMetadata(returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.SAVE_INSIGHT);
		return noun;
	}

	/**
	 * Edit an existing insight saved within security database
	 * @param appId
	 * @param existingRdbmsId
	 * @param insightName
	 * @param layout
	 * @param description
	 * @param tags
	 */
	private void editRegisteredInsightAndMetadata(IEngine engine, String existingRdbmsId, String insightName, String layout, String description, List<String> tags) {
		String appId = engine.getEngineId();
		SecurityInsightUtils.updateInsight(appId, existingRdbmsId, insightName, true, layout);
		InsightAdministrator admin = new InsightAdministrator(engine.getInsightDatabase());
		if(description != null) {
			admin.updateInsightDescription(existingRdbmsId, description);
			SecurityInsightUtils.updateInsightDescription(appId, existingRdbmsId, description);
		}
		if(tags != null) {
			admin.updateInsightTags(existingRdbmsId, tags);
			SecurityInsightUtils.updateInsightTags(appId, existingRdbmsId, tags);
		}
	}
	
	/**
	 * Update recipe: delete the old file and save as new
	 * 
	 * @param engineName
	 * @param rdbmsID
	 * @param recipeToSave
	 */
	protected void updateRecipeFile(String appId, String appName, String rdbmsID, String insightName, 
			String layout, String imageName, List<String> recipeToSave, boolean hidden, String description, List<String> tags) {
		String recipeLocation = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) 
				+ DIR_SEPARATOR + Constants.DB + DIR_SEPARATOR + SmssUtilities.getUniqueName(appName, appId) + DIR_SEPARATOR + "version" 
				+ DIR_SEPARATOR + rdbmsID + DIR_SEPARATOR + MosfetFile.RECIPE_FILE;
		// update the mosfet
		try {
			MosfetSyncHelper.updateMosfitFile(new File(recipeLocation), appId, appName, rdbmsID, insightName,
					layout, imageName, recipeToSave, hidden, description, tags);
			// add to git
			String gitFolder = AssetUtility.getAppAssetVersionFolder(appName, appId);
			List<String> files = new Vector<>();
			files.add(rdbmsID + DIR_SEPARATOR + MosfetFile.RECIPE_FILE);		
			GitRepoUtils.addSpecificFiles(gitFolder, files);
			GitRepoUtils.commitAddedFiles(gitFolder, GitUtils.getDateMessage("Changed " + insightName + " recipe on"));
		} catch (Exception e) {
			UpdateInsightReactor.logger.error(Constants.STACKTRACE, e);
		}
	}
	
}
