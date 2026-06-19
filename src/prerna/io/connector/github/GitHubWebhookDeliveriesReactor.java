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
package prerna.io.connector.github;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityExternalConnectorsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Returns the GitHub App's most recent webhook deliveries (event, response
 * status, time) for monitoring webhook health, optionally scoped to a single
 * project's installation.
 * <p>
 * Delegates to {@link GitHubAppClient#getRecentWebhookDeliveries(int)}. When a
 * {@code project} is supplied, its {@code GITHUB_PROJECT_LINK} row is used to
 * filter the returned deliveries down to that project's installation. GitHub
 * retains only a limited recent window of deliveries, so this is a live
 * diagnostics feed rather than an audit log.
 */
public class GitHubWebhookDeliveriesReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GitHubWebhookDeliveriesReactor.class);

	private static final String LIMIT = "limit";
	private static final int DEFAULT_LIMIT = 30;

	public GitHubWebhookDeliveriesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), LIMIT };
		this.keyRequired = new int[] { 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		int limit = DEFAULT_LIMIT;
		String limitValue = this.keyValue.get(LIMIT);
		if (limitValue != null && !limitValue.trim().isEmpty()) {
			try {
				limit = Integer.parseInt(limitValue.trim());
			} catch (NumberFormatException e) {
				throw new SemossPixelException("Invalid number for limit: '" + limitValue.trim() + "'.", e);
			}
		}

		// optional project filter -> scope deliveries to that project's installation
		Long installationFilter = null;
		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		if (projectId != null && !projectId.trim().isEmpty()) {
			projectId = projectId.trim();
			Map<String, Object> link = SecurityExternalConnectorsUtils.getGitHubProjectLink(projectId);
			if (link == null) {
				throw new SemossPixelException("Project " + projectId + " is not linked to a GitHub repository.");
			}
			Object installObj = link.get("installationId");
			if (installObj != null) {
				installationFilter = ((Number) installObj).longValue();
			}
		}

		try {
			List<Map<String, Object>> deliveries = GitHubAppClient.getRecentWebhookDeliveries(limit);
			if (installationFilter != null) {
				List<Map<String, Object>> filtered = new ArrayList<>();
				for (Map<String, Object> d : deliveries) {
					Object instId = d.get("installationId");
					if (instId instanceof Number && ((Number) instId).longValue() == installationFilter.longValue()) {
						filtered.add(d);
					}
				}
				deliveries = filtered;
			}

			Map<String, Object> result = new HashMap<>();
			result.put("count", deliveries.size());
			result.put("deliveries", deliveries);
			return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to retrieve GitHub webhook deliveries", e);
			throw new SemossPixelException("Failed to retrieve GitHub webhook deliveries: " + e.getMessage(), e);
		}
	}

	@Override
	public String getReactorDescription() {
		return "Return the GitHub App's most recent webhook deliveries (event, response status, time), optionally scoped to a project's installation, for monitoring webhook health.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.PROJECT.getKey().equals(key)) {
			return "Optional project id; when provided, only deliveries for that project's GitHub App installation are returned";
		} else if (LIMIT.equals(key)) {
			return "Optional maximum number of recent deliveries to return (default 30, max 100)";
		}
		return super.getDescriptionForKey(key);
	}

}
