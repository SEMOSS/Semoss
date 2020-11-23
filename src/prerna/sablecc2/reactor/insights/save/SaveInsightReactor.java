package prerna.sablecc2.reactor.insights.save;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.cache.InsightCacheUtility;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.impl.InsightAdministrator;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.insights.AbstractInsightReactor;
import prerna.util.AssetUtility;
import prerna.util.MosfetSyncHelper;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;
import prerna.util.git.GitUtils;

public class SaveInsightReactor extends AbstractInsightReactor {

	private static final String CLASS_NAME = SaveInsightReactor.class.getName();
	
	public SaveInsightReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.INSIGHT_NAME.getKey(), 
				ReactorKeysEnum.LAYOUT_KEY.getKey(), HIDDEN_KEY, ReactorKeysEnum.RECIPE.getKey(), 
				ReactorKeysEnum.PARAM_KEY.getKey(), ReactorKeysEnum.DESCRIPTION.getKey(), 
				ReactorKeysEnum.TAGS.getKey(), ReactorKeysEnum.PIPELINE.getKey(), 
				ReactorKeysEnum.IMAGE.getKey(), ENCODED_KEY};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = this.getLogger(CLASS_NAME);
		// get the recipe for the insight
		// need the engine name and id that has the recipe
		String appId = getApp();
		User user = this.insight.getUser();
		String author = null;
		String email = null;
		
		// security
		if(AbstractSecurityUtils.securityEnabled()) {
			if(AbstractSecurityUtils.anonymousUsersEnabled() && this.insight.getUser().isAnonymous()) {
				throwAnonymousUserError();
			}
			
			if(!SecurityAppUtils.userCanEditEngine(this.insight.getUser(), appId)) {
				throw new IllegalArgumentException("User does not have permission to add insights in the app");
			}
			// Get the user's email
			AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
			email = accessToken.getEmail();
			author = accessToken.getUsername();
		}
		
		String insightName = getInsightName();
		if(insightName == null || insightName.isEmpty()) {
			throw new IllegalArgumentException("Need to define the insight name");
		}
		List<String> recipeToSave = getRecipe();
		String layout = getLayout();
		boolean hidden = getHidden();
		Boolean cacheable = getUserDefinedCacheable();
		if(cacheable == null) {
			cacheable = true;
		}
		List<Map<String, Object>> params = getParams();
		Map pipeline = getPipeline();

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
		// get the new insight id
		String newInsightId = UUID.randomUUID().toString();
		// get an updated recipe if there are files used
		// and save the files in the correct location
		try {
			recipeToSave = saveFilesInInsight(recipeToSave, appId, newInsightId);
		} catch(Exception e) {
			throw new IllegalArgumentException("An error occured trying to identify file based sources to parameterize. The source error message is: " + e.getMessage(), e);
		}
		if(params != null && !params.isEmpty()) {
			try {
				recipeToSave = getParamRecipe(recipeToSave, params, insightName);
			} catch(Exception e) {
				throw new IllegalArgumentException("An error occured trying to parameterize the insight recipe. The source error message is: " + e.getMessage(), e);
			}
		}
		
		int stepCounter = 1;
		// add the recipe to the insights database
		InsightAdministrator admin = new InsightAdministrator(engine.getInsightDatabase());
		logger.info(stepCounter + ") Add insight " + insightName + " to rdbms store...");
		String newRdbmsId = admin.addInsight(newInsightId, insightName, layout, recipeToSave, hidden, cacheable);
		logger.info(stepCounter +") Done...");
		stepCounter++;

		String description = getDescription();
		List<String> tags = getTags();
		
		if(!hidden) {
			logger.info(stepCounter + ") Regsiter insight...");
			registerInsightAndMetadata(engine, newRdbmsId, insightName, layout, description, tags);
			logger.info(stepCounter + ") Done...");
		} else {
			logger.info(stepCounter + ") Insight is hidden ... do not add to solr");
		}
		stepCounter++;
		
		//write recipe to file
		logger.info(stepCounter + ") Add recipe to file...");
		try {
			MosfetSyncHelper.makeMosfitFile(engine.getEngineId(), engine.getEngineName(), 
					newRdbmsId, insightName, layout, recipeToSave, hidden, description, tags);
		} catch (IOException e) {
			e.printStackTrace();
		}
		logger.info(stepCounter + ") Done...");
		stepCounter++;
		
		// Move assets to new insight folder
		File tempInsightFolder = new File(this.insight.getInsightFolder());
		File newInsightFolder = new File(AssetUtility.getAppAssetVersionFolder(engine.getEngineName(), engine.getEngineId()) + DIR_SEPARATOR + newRdbmsId);
	    try {
			logger.info(stepCounter + ") Moving assets...");
			FileUtils.copyDirectory(tempInsightFolder, newInsightFolder);
			logger.info(stepCounter + ") Done...");
		} catch (IOException e) {
			logger.info(stepCounter + ") Unable to move assets...");
		}
	    stepCounter++;
	    // delete the cache folder for the new insight
	 	InsightCacheUtility.deleteCache(engine.getEngineId(), engine.getEngineName(), newRdbmsId, false);

		// write pipeline
		if(pipeline != null && !pipeline.isEmpty()) {
			logger.info(stepCounter + ") Add pipeline to file...");
			writePipelineToFile(engine.getEngineId(), engine.getEngineName(), newRdbmsId, pipeline);
			logger.info(stepCounter + ") Done...");
			stepCounter++;
		}
	
		// get base 64 image string and write to file
		String base64Image = getImage();
		if(base64Image != null && !base64Image.trim().isEmpty()) {
			logger.info(stepCounter + ") Storing insight image...");
			storeImageFromPng(base64Image, newRdbmsId, engine.getEngineId(), engine.getEngineName());
			logger.info(stepCounter + ") Done...");
			stepCounter++;
		}

		// adding insight files to git
		Stream<Path> walk = null;
		try {
			String folder = AssetUtility.getAppAssetVersionFolder(engine.getEngineName(), appId);
			// grab relative file paths
			walk = Files.walk(Paths.get(newInsightFolder.toURI()));
			List<String> files = walk
					.map(x -> newInsightId + DIR_SEPARATOR
							+ newInsightFolder.toURI().relativize(new File(x.toString()).toURI()).getPath().toString())
					.collect(Collectors.toList());
			files.remove(""); // removing empty path
			logger.info(stepCounter + ") Adding insight to git...");
			GitRepoUtils.addSpecificFiles(folder, files);
			GitRepoUtils.commitAddedFiles(folder, GitUtils.getDateMessage("Saved "+ insightName +" insight on"), author, email);
			logger.info(stepCounter + ") Done...");
		} catch (IOException e) {
			logger.info(stepCounter + ") Unable to add insight to git...");
			e.printStackTrace();
		} finally {
			if(walk != null) {
				walk.close();
			}
		}
		stepCounter++;
		
		// update the workspace cache for the saved insight
		this.insight.setEngineId(engine.getEngineId());
		this.insight.setEngineName(engine.getEngineName());
		this.insight.setRdbmsId(newRdbmsId);
		this.insight.setInsightName(insightName);
		// this is to reset it
		this.insight.setInsightFolder(null);
		this.insight.setAppFolder(null);
		
		ClusterUtil.reactorPushInsightDB(appId);
		ClusterUtil.reactorPushFolder(engine, AssetUtility.getAppAssetVersionFolder(engine.getEngineName(), appId));

		Map<String, Object> returnMap = new HashMap<String, Object>();
		returnMap.put("name", insightName);
		returnMap.put("app_insight_id", newRdbmsId);
		returnMap.put("app_name", engine.getEngineName());
		returnMap.put("app_id", engine.getEngineId());
		returnMap.put("recipe", recipeToSave);
		NounMetadata noun = new NounMetadata(returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.SAVE_INSIGHT);
		return noun;
	}
	
	/**
	 * Save a new insight within security database
	 * @param appId
	 * @param insightIdToSave
	 * @param insightName
	 * @param layout
	 * @param description
	 * @param tags
	 */
	private void registerInsightAndMetadata(IEngine engine, String insightIdToSave, String insightName, String layout, String description, List<String> tags) {
		String appId = engine.getEngineId();
		// TODO: INSIGHTS ARE ALWAYS GLOBAL!!!
		SecurityInsightUtils.addInsight(appId, insightIdToSave, insightName, true, layout);
		if(this.insight.getUser() != null) {
			SecurityInsightUtils.addUserInsightCreator(this.insight.getUser(), appId, insightIdToSave);
		}
		InsightAdministrator admin = new InsightAdministrator(engine.getInsightDatabase());
		if(description != null) {
			admin.updateInsightDescription(insightIdToSave, description);
			SecurityInsightUtils.updateInsightDescription(appId, insightIdToSave, description);
		}
		if(tags != null) {
			admin.updateInsightTags(insightIdToSave, tags);
			SecurityInsightUtils.updateInsightTags(appId, insightIdToSave, tags);
		}
	}
}
