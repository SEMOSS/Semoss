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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.impl.InsightAdministrator;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.UserTrackingUtils;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.git.GitDestroyer;
import prerna.util.git.GitPushUtils;
import prerna.util.git.GitRepoUtils;
import prerna.util.git.GitUtils;

public class DeleteInsightReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(DeleteInsightReactor.class);

	public DeleteInsightReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.ID.getKey() };
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		organizeKeys();
		GenRowStruct projectGrs = this.store.getGenRowStruct(this.keysToGet[0]);
		if (projectGrs.isEmpty()) {
			throw new IllegalArgumentException("Must define the project to delete the insights from");
		}
		String projectId = projectGrs.get(0).toString();
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(user, projectId);
		if (!SecurityProjectUtils.userCanViewProject(user, projectId)) {
			throw new IllegalArgumentException(
					"Project " + projectId + " does not exist or user does not have access to the project");
		}
		// Get the user's email
		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
		String email = accessToken.getEmail();
		String author = accessToken.getUsername();

		IProject project = Utility.getProject(projectId);
		String projectName = project.getProjectName();

		InsightAdministrator admin = new InsightAdministrator(project.getInsightDatabase());
//		ClusterUtil.reactorPullInsightsDB(projectId);
		ClusterUtil.pullProjectFolder(project, AssetUtility.getProjectVersionFolder(projectName, projectId));

		GenRowStruct grs = this.store.getGenRowStruct(this.keysToGet[1]);
		int size = grs.size();
		for (int i = 0; i < size; i++) {
			String insightId = grs.get(i).toString();
			if (!SecurityInsightUtils.userCanEditInsight(user, projectId, insightId)) {
				throw new IllegalArgumentException("User does not have permission to edit this insight");
			}

			// delete from insights database
			try {
				admin.dropInsight(insightId);
			} catch (RuntimeException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}

			// delete insight folder
			String projectVersion = AssetUtility.getProjectVersionFolder(projectName, projectId);
			String insightFolderPath = projectVersion + DIR_SEPARATOR + insightId;
			File insightFolder = new File(insightFolderPath);
			try (Stream<Path> walk = Files.walk(Paths.get(insightFolder.toURI()))) {
				// grab relative file paths
				List<String> files = walk
						.map(x -> insightId + DIR_SEPARATOR
								+ insightFolder.toURI().relativize(new File(x.toString()).toURI()).getPath().toString())
						.collect(Collectors.toList());
				files.remove(""); // removing empty path
				GitDestroyer.removeSpecificFiles(projectVersion, true, files);
				GitRepoUtils.commitAddedFiles(projectVersion,
						GitUtils.getDateMessage("Deleted insight '" + insightId + "' on"), author, email);
				AuthProvider projectGitProvider = project.getGitProvider();
				if (user != null && projectGitProvider != null && user.getAccessToken(projectGitProvider) != null) {
					List<Map<String, String>> remotes = GitRepoUtils.listConfigRemotes(projectVersion);
					if (remotes != null && !remotes.isEmpty()) {
						AccessToken userToken = user.getAccessToken(projectGitProvider);
						String token = userToken.getAccess_token();
						for (Map<String, String> thisRemote : remotes) {
							GitPushUtils.push(projectVersion, thisRemote.get("url"), null, token, projectGitProvider,
									1);
						}
					}
				}
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				// delete folder
				try {
					FileUtils.deleteDirectory(insightFolder);
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}

			// now delete from security db
			SecurityInsightUtils.deleteInsight(projectId, insightId);

			// now delete from user tracking db
			UserTrackingUtils.deleteInsight(insightId, projectId);
		}

//		ClusterUtil.reactorPushInsightDB(projectId);
		ClusterUtil.pushProjectFolder(project, AssetUtility.getProjectVersionFolder(projectName, projectId));
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.DELETE_INSIGHT);
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
