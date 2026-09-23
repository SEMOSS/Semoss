/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.insights.save;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.cache.InsightCacheUtility;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.impl.InsightAdministrator;
import prerna.om.MosfetFile;
import prerna.om.PixelList;
import prerna.project.api.IProject;
import prerna.query.parsers.ParamStruct;
import prerna.reactor.agent.mcp.MCPUtility;
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

public class UpdateInsightReactor extends AbstractInsightReactor {

	private static final Logger classLogger = LogManager.getLogger(UpdateInsightReactor.class);
	private static final String CLASS_NAME = UpdateInsightReactor.class.getName();

	public UpdateInsightReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.INSIGHT_NAME.getKey(),
				ReactorKeysEnum.ID.getKey(), ReactorKeysEnum.LAYOUT_KEY.getKey(), ReactorKeysEnum.GLOBAL.getKey(),
				ReactorKeysEnum.RECIPE.getKey(), ReactorKeysEnum.DESCRIPTION.getKey(), ReactorKeysEnum.TAGS.getKey(),
				ReactorKeysEnum.PARAM_KEY.getKey(), ReactorKeysEnum.IMAGE.getKey(), CACHEABLE, CACHE_MINUTES,
				SCHEMA_NAME };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = this.getLogger(CLASS_NAME);
		boolean optimizeRecipe = true;
		String projectId = getProject();
		// need to know what we are updating
		String existingId = getRdbmsId();

		User user = this.insight.getUser();
		// security
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		if (!SecurityInsightUtils.userCanEditInsight(user, projectId, existingId)) {
			throw new IllegalArgumentException("User does not have permission to edit this insight");
		}

		// Get the user's email
		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
		String email = accessToken.getEmail();
		String author = accessToken.getUsername();

		String insightName = getInsightName();
		if (insightName == null || (insightName = insightName.trim()).isEmpty()) {
			throw new IllegalArgumentException("Need to define the insight name");
		}

		if (SecurityInsightUtils.insightNameExistsMinusId(projectId, insightName, existingId)) {
			throw new IllegalArgumentException("Insight name already exists");
		}

		String schemaName = getUserSchemaNameOrExistingSchemaName(projectId, existingId);
		if (schemaName == null) {
			// if null, use the insight name
			schemaName = getUserSchemaNameOrDefaultAndValidate(projectId, insightName);
		}

		PixelList insightPixelList = null;
		List<String> recipeToSave = getRecipe();
		List<String> recipeIds = null;
		List<String> additionalSteps = null;
		List<ParamStruct> params = null;
		Set<String> queriedDatabaseIds = null;

		// saving an empty recipe?
		if (recipeToSave == null || recipeToSave.isEmpty()) {
			if (optimizeRecipe) {
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
			for (String step : additionalSteps) {
				recipeToSave.add(step);
				// in case we are saving a not run recipe like forms
				if (recipeIds != null) {
					recipeIds.add(counter++ + "_additionalStep");
				}
			}
			params = InsightUtility.getInsightParams(this.insight);
			queriedDatabaseIds = this.insight.getQueriedDatabaseIds();
		} else {
			queriedDatabaseIds = PixelUtility.getDatabaseIds(user, recipeToSave);
		}

		IProject project = Utility.getProject(projectId);

		// WE DO NOT NEED TO SAVE THE FILES
		// THEY SHOULD ALREADY BE LOADED INTO THE INSIGHT SPACE
		// SINCE THIS IS AN EXISTING INSIGHTX

		// get an updated recipe if there are files used
		// and save the files in the correct location
		if (insightPixelList != null) {
			try {
				// we will delete and move the files used in this insight space to the data
				// folder
				if (saveFilesInInsight(insightPixelList, projectId, project.getProjectName(), existingId, true,
						this.insight.getPixelList())) {
					// need to pull the new saved recipe
					recipeToSave = insightPixelList.getPixelRecipe();
				}
			} catch (Exception e) {
				throw new IllegalArgumentException(
						"An error occurred trying to identify file based sources to parameterize. The source error message is: "
								+ e.getMessage(),
						e);
			}
		}

		String layout = getLayout();
		boolean global = getGlobal();
		if (global && AbstractSecurityUtils.adminOnlyInsightSetPublic()) {
			if (!SecurityAdminUtils.userIsAdmin(user)) {
				throw new IllegalArgumentException("Only an admin can set an insight as public");
			}
		}
		Boolean cacheable = getUserDefinedCacheable();
		Integer cacheMinutes = getUserDefinedCacheMinutes();
		Boolean cacheEncrypt = getUserDefinedCacheEncrypt();
		String cacheCron = getUserDefinedCacheCron();
		// keep current insight cache
		// or set to the new user inputs
		if (cacheable == null) {
			cacheable = this.insight.isCacheable();
		} else {
			this.insight.setCacheable(cacheable);
		}
		if (cacheMinutes == null) {
			cacheMinutes = this.insight.getCacheMinutes();
		} else {
			this.insight.setCacheMinutes(cacheMinutes);
		}
		if (cacheEncrypt == null) {
			cacheEncrypt = this.insight.isCacheEncrypt();
		} else {
			this.insight.setCacheEncrypt(cacheEncrypt);
		}
		if (cacheCron == null) {
			cacheCron = this.insight.getCacheCron();
		} else {
			this.insight.setCacheCron(cacheCron);
		}
		// we delete the cache cause its invalid on recipe updates
		ZonedDateTime cachedOn = null;

		if (params != null && !params.isEmpty()) {
			recipeToSave = PixelUtility.parameterizeRecipe(this.insight, recipeToSave, recipeIds, params, insightName);
		}

		// to append preApplied parameters to the save recipe
		recipeToSave = PixelUtility.appendPreAppliedParameter(this.insight, recipeToSave);

		// Pull the insights db again incase someone just saved something
		ClusterUtil.pullProjectFolder(project,
				AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId));

		// add the recipe to the insights database
		InsightAdministrator admin = new InsightAdministrator(project.getInsightDatabase());

		// update insight db
		int stepCounter = 1;
		logger.info(stepCounter + ") Updating insight in rdbms");
		admin.updateInsight(existingId, insightName, layout, recipeToSave, global, cacheable, cacheMinutes, cacheCron,
				cachedOn, cacheEncrypt, schemaName);
		logger.info(stepCounter + ") Done");
		stepCounter++;

		String description = getDescription();
		List<String> tags = getTags();

		logger.info(stepCounter + ") Updated registered insight...");
		editRegisteredInsightAndMetadata(project, existingId, insightName, layout, global, cacheable, cacheMinutes,
				cacheCron, cachedOn, cacheEncrypt, recipeToSave, description, tags,
				this.insight.getVarStore().getFrames(), queriedDatabaseIds, schemaName);
		logger.info(stepCounter + ") Done...");
		stepCounter++;

		// update recipe text file
		logger.info(stepCounter + ") Update Mosfet file for collaboration");
		try {
			MosfetSyncHelper.makeMosfitFile(projectId, project.getProjectName(), existingId, insightName, layout,
					recipeToSave, global, cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, description, tags,
					schemaName, true);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			logger.info(stepCounter + ") Unable to save recipe file...");
		}
		logger.info(stepCounter + ") Done...");
		stepCounter++;

		logger.info(stepCounter + ") Push updated mosfet to git");
		String projectVersion = AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId);
		List<String> files = new Vector<>();
		files.add(existingId + DIR_SEPARATOR + MosfetFile.RECIPE_FILE);
		GitRepoUtils.addSpecificFiles(projectVersion, files);
		GitRepoUtils.commitAddedFiles(projectVersion,
				GitUtils.getDateMessage("Update insight '" + existingId + "' (+" + insightName + ") recipe on"), author,
				email);
		AuthProvider projectGitProvider = project.getGitProvider();
		if (user != null && projectGitProvider != null && user.getAccessToken(projectGitProvider) != null) {
			List<Map<String, String>> remotes = GitRepoUtils.listConfigRemotes(projectVersion);
			if (remotes != null && !remotes.isEmpty()) {
				AccessToken userToken = user.getAccessToken(projectGitProvider);
				String token = userToken.getAccess_token();
				for (Map<String, String> thisRemote : remotes) {
					GitPushUtils.push(projectVersion, thisRemote.get("url"), null, token, projectGitProvider, 1);
				}
			}
		}
		logger.info(stepCounter + ") Done pushing to git");
		stepCounter++;

		// get file we are saving as an image
		String imageFile = getImage();
		if (imageFile != null && !imageFile.trim().isEmpty()) {
			logger.info(stepCounter + ") Storing insight image...");
			storeImageFromFile(imageFile, existingId, projectId, project.getProjectName());
			logger.info(stepCounter + ") Done...");
			stepCounter++;
		}

		// update the workspace cache for the saved insight
		this.insight.setProjectId(projectId);
		this.insight.setInsightName(insightName);

		// delete the cache
		// NOTE ::: We already pulled above, so we will not pull again to delete the
		// cache
		InsightCacheUtility.deleteCache(projectId, project.getProjectName(), existingId, null, false);
		// push only the insight folder
		ClusterUtil.pushProjectFolder(project,
				AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId), existingId);

		Map<String, Object> returnMap = new HashMap<String, Object>();
		// TODO: delete app_ and only send project_
		returnMap.put("app_insight_id", existingId);
		returnMap.put("app_name", project.getProjectName());
		returnMap.put("app_id", projectId);

		returnMap.put("name", insightName);
		returnMap.put("project_insight_id", existingId);
		returnMap.put("project_name", project.getProjectName());
		returnMap.put("project_id", projectId);
		returnMap.put("recipe", recipeToSave);
		returnMap.put("cacheable", cacheable);
		returnMap.put("cacheMinutes", cacheMinutes);
		returnMap.put("cacheCron", cacheCron);
		returnMap.put("cacheEncrypt", cacheEncrypt);
		returnMap.put("isPublic", global);
		NounMetadata noun = new NounMetadata(returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE,
				PixelOperationType.SAVE_INSIGHT);
		return noun;
	}

	/**
	 * Edit an existing insight saved within security database
	 * 
	 * @param project
	 * @param existingRdbmsId
	 * @param insightName
	 * @param layout
	 * @param global
	 * @param cacheable
	 * @param cacheableMinutes
	 * @param cacheCron
	 * @param cachedOn
	 * @param cacheEncrypt
	 * @param recipe
	 * @param description
	 * @param tags
	 * @param insightFrames
	 * @param queriedDatabaseIds
	 */
	private void editRegisteredInsightAndMetadata(IProject project, String existingRdbmsId, String insightName,
			String layout, boolean global, boolean cacheable, int cacheableMinutes, String cacheCron,
			ZonedDateTime cachedOn, boolean cacheEncrypt, List<String> recipe, String description, List<String> tags,
			Set<ITableDataFrame> insightFrames, Set<String> queriedDatabaseIds, String schemaName) {
		String projectId = project.getProjectId();
		SecurityInsightUtils.updateInsight(projectId, existingRdbmsId, insightName, global, layout, cacheable,
				cacheableMinutes, cacheCron, cachedOn, cacheEncrypt, recipe, schemaName);
		InsightAdministrator admin = new InsightAdministrator(project.getInsightDatabase());
		if (description != null) {
			admin.updateInsightDescription(existingRdbmsId, description);
			SecurityInsightUtils.updateInsightDescription(projectId, existingRdbmsId, description);
		}
		if (tags != null) {
			admin.updateInsightTags(existingRdbmsId, tags);
			SecurityInsightUtils.updateInsightTags(projectId, existingRdbmsId, tags);
		}
		if (insightFrames != null && !insightFrames.isEmpty()) {
			SecurityInsightUtils.updateInsightFrames(projectId, existingRdbmsId, insightFrames);
		}
		UserTrackingUtils.updateEngineUsage(queriedDatabaseIds, existingRdbmsId, projectId);
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		Map<String, String> meta = new HashMap<>();
		// default to auto execution for reactors
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.ASK.getValue());
		// sidebar to view default json for reactor input+output
		meta.put(MCPUtility.UI_DISPLAY_LOCATION, MCPUtility.MCPDisplayOption.SIDEBAR.getValue());
		return meta;
	}
}
