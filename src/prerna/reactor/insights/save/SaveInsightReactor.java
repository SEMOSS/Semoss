package prerna.reactor.insights.save;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cache.InsightCacheUtility;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.impl.InsightAdministrator;
import prerna.om.PixelList;
import prerna.project.api.IProject;
import prerna.query.parsers.ParamStruct;
import prerna.reactor.insights.AbstractInsightReactor;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.UserTrackingUtils;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.MosfetSyncHelper;
import prerna.util.Utility;
import prerna.util.git.GitPushUtils;
import prerna.util.git.GitRepoUtils;
import prerna.util.git.GitUtils;
import prerna.util.insight.InsightUtility;

public class SaveInsightReactor extends AbstractInsightReactor {

	private static final Logger logger = LogManager.getLogger(SaveInsightReactor.class);
	private static final String CLASS_NAME = SaveInsightReactor.class.getName();
	
	public SaveInsightReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.INSIGHT_NAME.getKey(), 
				ReactorKeysEnum.LAYOUT_KEY.getKey(), GLOBAL_KEY, ReactorKeysEnum.RECIPE.getKey(), 
				ReactorKeysEnum.PARAM_KEY.getKey(), ReactorKeysEnum.DESCRIPTION.getKey(), 
				ReactorKeysEnum.TAGS.getKey(), ReactorKeysEnum.IMAGE.getKey(), 
				ENCODED_KEY, CACHEABLE, CACHE_MINUTES, CACHE_ENCRYPT, SCHEMA_NAME};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = this.getLogger(CLASS_NAME);
		// get the recipe for the insight
		// need the engine name and id that has the recipe
		boolean savingThisInsight = false;
		boolean optimizeRecipe = true;
		String projectId = getProject();
		
		User user = this.insight.getUser();
		// security
		if(AbstractSecurityUtils.anonymousUsersEnabled() && this.insight.getUser().isAnonymous()) {
			throwAnonymousUserError();
		}
		if(!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("User does not have permission to add insights in the project");
		}
		
		// Get the user's email
		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
		String email = accessToken.getEmail();
		String author = accessToken.getUsername();
		
		String insightName = getInsightName();
		if(insightName == null || (insightName = insightName.trim()).isEmpty()) {
			throw new IllegalArgumentException("Need to define the insight name");
		}
		
		if(SecurityInsightUtils.insightNameExists(projectId, insightName) != null) {
			throw new IllegalArgumentException("Insight name already exists");
		}
		
		String schemaName = getUserSchemaNameOrDefaultAndValidate(projectId, insightName);
		
		PixelList insightPixelList = null;
		List<String> recipeToSave = getRecipe();
		List<String> recipeIds = null;
		List<String> additionalSteps = null;
		List<ParamStruct> params = null;
		Set<String> queriedDatabaseIds = null;

		String layout = getLayout();
		boolean global = getGlobal();
		if(global && AbstractSecurityUtils.adminOnlyInsightSetPublic()) {
			if(!SecurityAdminUtils.userIsAdmin(user)) {
				throw new IllegalArgumentException("Only an admin can set an insight as public");
			}
		}
		Boolean cacheable = getUserDefinedCacheable();
		if(cacheable == null) {
			cacheable = Utility.getApplicationCacheInsight();
		}
		Integer cacheMinutes = getUserDefinedCacheMinutes();
		if(cacheMinutes == null) {
			cacheMinutes = Utility.getApplicationCacheInsightMinutes();
		}
		String cacheCron = getUserDefinedCacheCron();
		if(cacheCron == null || cacheCron.isEmpty()) {
			cacheCron = Utility.getApplicationCacheCron();
		}
		Boolean cacheEncrypt = getUserDefinedCacheEncrypt();
		if(cacheEncrypt == null) {
			cacheEncrypt = Utility.getApplicationCacheEncrypt();
		}
		// we cache on open insight, not on save
		ZonedDateTime cachedOn = null;
		
		// saving an empty recipe?
		if (recipeToSave == null || recipeToSave.isEmpty()) {
			savingThisInsight = true;
			if(optimizeRecipe) {
				// optimize the recipe
				insightPixelList = PixelUtility.getOptimizedPixelList(this.insight);
			} else {
				// remove unnecessary pixels to start
				insightPixelList = this.insight.getPixelList();
			}
			
			recipeToSave = insightPixelList.getPixelRecipe();
			recipeIds = insightPixelList.getPixelIds();
			
			// now add the additional pixel steps on save
			int counter = 0;
			// make sure we pass the correct insight pixel list
			// for the optimized pixel or the regular one
			additionalSteps = PixelUtility.getMetaInsightRecipeSteps(this.insight, insightPixelList);
			for(String step : additionalSteps) {
				recipeToSave.add(step);
				// in case we are saving a not run recipe like forms
				if(recipeIds != null) {
					recipeIds.add(counter++ + "_additionalStep");
				}
			}
			params = InsightUtility.getInsightParams(this.insight);
			queriedDatabaseIds = this.insight.getQueriedDatabaseIds();
		} else {
			// default for recipe encoded when no key is passed is true
			if(recipeEncoded()) {
				recipeToSave = decodeRecipe(recipeToSave);
			}
			queriedDatabaseIds = PixelUtility.getDatabaseIds(user, recipeToSave);
		}
		
		IProject project = Utility.getProject(projectId);
		
		// pull the insights db again incase someone just saved something 
		ClusterUtil.pullProjectFolder(project, AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId));
		
		// get an updated recipe if there are files used
		// and save the files in the correct location
		// get the new insight id
		String newInsightId = UUID.randomUUID().toString();
		if(insightPixelList != null) {
			try {
				// if we are saving a saved insight as another insight
				// do not delete the file from this insight
				if(saveFilesInInsight(insightPixelList, projectId, project.getProjectName(), newInsightId, !this.insight.isSavedInsight(), this.insight.getPixelList())) {
					// need to pull the new saved recipe
					recipeToSave = insightPixelList.getPixelRecipe();
				}
			} catch(Exception e) {
				throw new IllegalArgumentException("An error occurred trying to identify file based sources to parameterize. The source error message is: " + e.getMessage(), e);
			}
		}
		
		// get an updated recipe if there are files used
		// and save the files in the correct location
		
		if(params != null && !params.isEmpty()) {
			try {
				recipeToSave = PixelUtility.parameterizeRecipe(this.insight, recipeToSave, recipeIds, params, insightName);
			} catch(Exception e) {
				throw new IllegalArgumentException("An error occurred trying to parameterize the insight recipe. The source error message is: " + e.getMessage(), e);
			}
		}
		
		// to append preApplied parameters to the save recipe
		recipeToSave = PixelUtility.appendPreAppliedParameter(this.insight, recipeToSave);

		int stepCounter = 1;
		// add the recipe to the insights database
		InsightAdministrator admin = new InsightAdministrator(project.getInsightDatabase());
		logger.info(stepCounter + ") Add insight " + insightName + " to rdbms store...");
		admin.addInsight(newInsightId, insightName, layout, recipeToSave, global, cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, schemaName);
		logger.info(stepCounter +") Done...");
		stepCounter++;

		String description = getDescription();
		List<String> tags = getTags();
		
		logger.info(stepCounter + ") Regsiter insight...");
		registerInsightAndMetadata(project, newInsightId, insightName, layout, global, 
				cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, 
				recipeToSave, description, tags, this.insight.getVarStore().getFrames(), 
				queriedDatabaseIds, schemaName);
		logger.info(stepCounter + ") Done...");
		stepCounter++;
		
		// Move assets to new insight folder
		File tempInsightFolder = new File(this.insight.getInsightFolder());
		File newInsightFolder = new File(AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId) + DIR_SEPARATOR + newInsightId);
		if(tempInsightFolder.exists()) {
			try {
				logger.info(stepCounter + ") Moving assets...");
				FileUtils.copyDirectory(tempInsightFolder, newInsightFolder);
				logger.info(stepCounter + ") Done...");
			} catch (IOException e) {
				SaveInsightReactor.logger.error(Constants.STACKTRACE, e);
				logger.info(stepCounter + ") Unable to move assets...");
			}
		} else {
			logger.info(stepCounter + ") No asset folder exists to move...");
		}
	    stepCounter++;
	    // delete the cache folder for the new insight
	 	InsightCacheUtility.deleteCache(project.getProjectId(), project.getProjectName(), newInsightId, null, false);

	 	// write recipe to file
	 	// force = true to delete any existing mosfet files that were pulled from asset folder
		logger.info(stepCounter + ") Add recipe to file...");
		try {
			MosfetSyncHelper.makeMosfitFile(project.getProjectId(), project.getProjectName(), 
					newInsightId, insightName, layout, recipeToSave, global, 
					cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, 
					description, tags, schemaName, true);
		} catch (IOException e) {
			SaveInsightReactor.logger.error(Constants.STACKTRACE, e);
			logger.info(stepCounter + ") Unable to save recipe file...");
		}
		logger.info(stepCounter + ") Done...");
		stepCounter++;
	 	
		// get file we are saving as an image
		String imageFile = getImage();
		if(imageFile != null && !imageFile.trim().isEmpty()) {
			logger.info(stepCounter + ") Storing insight image...");
			storeImageFromFile(imageFile, newInsightId, project.getProjectId(), project.getProjectName());
			logger.info(stepCounter + ") Done...");
			stepCounter++;
		}

		// adding insight files to git
		Stream<Path> walk = null;
		try {
			String projectVersion = AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId);
			// grab relative file paths
			walk = Files.walk(Paths.get(newInsightFolder.toURI()));
			List<String> files = walk
					.map(x -> newInsightId + DIR_SEPARATOR
							+ newInsightFolder.toURI().relativize(new File(x.toString()).toURI()).getPath().toString())
					.collect(Collectors.toList());
			files.remove(""); // removing empty path
			logger.info(stepCounter + ") Adding insight to git...");
			GitRepoUtils.addSpecificFiles(projectVersion, files);
			GitRepoUtils.commitAddedFiles(projectVersion, GitUtils.getDateMessage("Saved insight '" + newInsightId + "' ("+ insightName + ") on"), author, email);
			AuthProvider projectGitProvider = project.getGitProvider();
			if(user != null && projectGitProvider != null && user.getAccessToken(projectGitProvider) != null) {
				List<Map<String, String>> remotes = GitRepoUtils.listConfigRemotes(projectVersion);
				if(remotes != null && !remotes.isEmpty()) {
					AccessToken userToken = user.getAccessToken(projectGitProvider);
					String token = userToken.getAccess_token();
					for(Map<String, String> thisRemote : remotes) {
						GitPushUtils.push(projectVersion, thisRemote.get("url"), null, token, projectGitProvider, 1);
					}
				}
			}
			logger.info(stepCounter + ") Done...");
		} catch (Exception e) {
			SaveInsightReactor.logger.error(Constants.STACKTRACE, e);
			logger.info(stepCounter + ") Unable to add insight to git...");
		} finally {
			if(walk != null) {
				walk.close();
			}
		}
		stepCounter++;
		
		// update the workspace cache for the saved insight
		this.insight.setProjectId(projectId);
		this.insight.setProjectName(project.getProjectName());
		this.insight.setRdbmsId(newInsightId);
		this.insight.setInsightName(insightName);
		// this is to reset it
		this.insight.setInsightFolder(null);
		this.insight.setAppFolder(null);
		
		// add to the users opened insights
		if(savingThisInsight && this.insight.getUser() != null) {
			this.insight.getUser().addOpenInsight(projectId, newInsightId, this.insight.getInsightId());
		}
		
		// push only the insight folder
		ClusterUtil.pushProjectFolder(project, AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId), newInsightId);

		Map<String, Object> returnMap = new HashMap<String, Object>();
		// TODO: delete app_ and only send project_
		returnMap.put("app_insight_id", newInsightId);
		returnMap.put("app_name", project.getProjectName());
		returnMap.put("app_id", projectId);
		
		returnMap.put("name", insightName);
		returnMap.put("project_insight_id", newInsightId);
		returnMap.put("project_name", project.getProjectName());
		returnMap.put("project_id", projectId);
		returnMap.put("recipe", recipeToSave);
		returnMap.put("cacheable", cacheable);
		returnMap.put("cacheMinutes", cacheMinutes);
		returnMap.put("cacheCron", cacheCron);
		returnMap.put("cacheEncrypt", cacheEncrypt);
		returnMap.put("isPublic", global);
		NounMetadata noun = new NounMetadata(returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.SAVE_INSIGHT);
		return noun;
	}
	
	/**
	 * Save a new insight within security database
	 * @param project
	 * @param insightIdToSave
	 * @param insightName
	 * @param layout
	 * @param global
	 * @param cacheable
	 * @param cacheMinutes
	 * @param cacheCron
	 * @param cachedOn
	 * @param cacheEncrypt
	 * @param recipe
	 * @param description
	 * @param tags
	 * @param insightFrames
	 * @param queriedDatabaseIds
	 * @param schemaName
	 */
	private void registerInsightAndMetadata(IProject project, String insightIdToSave, String insightName, String layout, boolean global,
			boolean cacheable, int cacheMinutes, String cacheCron, ZonedDateTime cachedOn, boolean cacheEncrypt, 
			List<String> recipe, String description, List<String> tags, Set<ITableDataFrame> insightFrames, Set<String> queriedDatabaseIds, String schemaName) {
		String projectId = project.getProjectId();
		SecurityInsightUtils.addInsight(projectId, insightIdToSave, insightName, global, layout, 
				cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, recipe, schemaName);
		if(this.insight.getUser() != null) {
			SecurityInsightUtils.addUserInsightCreator(this.insight.getUser(), projectId, insightIdToSave);
		}
		InsightAdministrator admin = new InsightAdministrator(project.getInsightDatabase());
		if(description != null) {
			admin.updateInsightDescription(insightIdToSave, description);
			SecurityInsightUtils.updateInsightDescription(projectId, insightIdToSave, description);
		}
		if(tags != null) {
			admin.updateInsightTags(insightIdToSave, tags);
			SecurityInsightUtils.updateInsightTags(projectId, insightIdToSave, tags);
		}
		if(insightFrames != null && !insightFrames.isEmpty()) {
			SecurityInsightUtils.updateInsightFrames(projectId, insightIdToSave, insightFrames);
		}
		// keep track of any engines used
		UserTrackingUtils.addEngineUsage(queriedDatabaseIds, insightIdToSave, projectId);
	}
}
