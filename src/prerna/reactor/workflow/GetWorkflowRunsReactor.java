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
package prerna.reactor.workflow;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

/**
 * GetWorkflowRuns(project=["<appId>"])
 *
 * Returns the list of run summaries from
 * project/<appId>/app_root/version/assets/portals/runs/
 * sorted most-recent first, up to 50 entries.
 */
public class GetWorkflowRunsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetWorkflowRunsReactor.class);
	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();
	private static final int MAX_RUNS = 50;

	public GetWorkflowRunsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	@SuppressWarnings("unchecked")
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String projectId = this.keyValue.get(this.keysToGet[0]);

		if (!SecurityProjectUtils.userCanViewProject(user, projectId)) {
			NounMetadata noun = new NounMetadata(
				"User does not have permission to view project " + projectId,
				PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException ex = new SemossPixelException(noun);
			ex.setContinueThreadOfExecution(false);
			throw ex;
		}

		String portalsFolder = AssetUtility.getProjectPortalsFolder(projectId);
		File runsDir = new File(portalsFolder + "/runs");

		List<Map<String, Object>> runs = new ArrayList<>();

		if (runsDir.exists() && runsDir.isDirectory()) {
			File[] runFiles = runsDir.listFiles((dir, name) -> name.endsWith(".json"));
			if (runFiles != null && runFiles.length > 0) {
				// Sort newest first by filename (runId contains timestamp)
				Arrays.sort(runFiles, Comparator.comparing(File::getName).reversed());

				int count = 0;
				for (File f : runFiles) {
					if (count >= MAX_RUNS) break;
					try {
						String json = FileUtils.readFileToString(f, StandardCharsets.UTF_8);
						Map<String, Object> run = GSON.fromJson(json, Map.class);
						// Include a lightweight summary (omit full node output bodies)
						Map<String, Object> summary = new java.util.LinkedHashMap<>();
						summary.put("runId", run.get("runId"));
						summary.put("startedAt", run.get("startedAt"));
						summary.put("status", run.get("status"));
						Object nodeResults = run.get("nodeResults");
						summary.put("nodeCount", nodeResults instanceof List ? ((List<?>) nodeResults).size() : 0);
						runs.add(summary);
						count++;
					} catch (IOException | com.google.gson.JsonSyntaxException e) {
						classLogger.warn("Could not read run file " + f.getName() + ": " + e.getMessage());
					}
				}
			}
		}

		return new NounMetadata(runs, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}
}
