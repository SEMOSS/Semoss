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
package prerna.io.connector.jira;

import org.javatuples.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

import java.util.Map;

public class JiraSearchReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraSearchReactor.class);

	private static final String JQL = "jql";
	private static final String NEXT_PAGE_TOKEN = "nextPageToken";
	private static final String MAX_RESULTS = "maxResults";

	public JiraSearchReactor() {
		this.keysToGet = new String[] { JQL, NEXT_PAGE_TOKEN, MAX_RESULTS };
		this.keyRequired = new int[] { 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();

			String jql = this.keyValue.get(JQL);
			if (jql == null || jql.trim().isEmpty()) {
				throw new SemossPixelException(
						"The jql parameter is required. Provide a valid JQL string, for example 'project = RTJ AND status = \"In Progress\" ORDER BY created DESC'.");
			}
			jql = jql.trim();

			String nextPageToken = this.keyValue.get(NEXT_PAGE_TOKEN);

			int maxResults = 50;
			String maxResultsRaw = this.keyValue.get(MAX_RESULTS);
			if (maxResultsRaw != null && !maxResultsRaw.trim().isEmpty()) {
				try {
					maxResults = Integer.parseInt(maxResultsRaw.trim());
				} catch (NumberFormatException e) {
					throw new SemossPixelException(
							"maxResults must be a number, got: '" + maxResultsRaw.trim() + "'.");
				}
			}

			User user = this.insight.getUser();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();

			Map<String, Object> result = JiraHelper.searchIssues(accessToken, baseUrl, jql, nextPageToken, maxResults);

			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error while searching Jira issues", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to search Jira issues", e);
			throw new SemossPixelException(
					"An error occurred while searching Jira issues. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Searches Jira issues using a JQL query string. Returns a paginated list of matching issues with their fields. Use nextPageToken from the response to fetch additional pages.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(JQL)) {
			return "Required JQL query string (for example, project = RTJ AND status = Open ORDER BY created DESC).";
		} else if (key.equals(NEXT_PAGE_TOKEN)) {
			return "Optional pagination token from a previous response for fetching the next page.";
		} else if (key.equals(MAX_RESULTS)) {
			return "Optional maximum issues per page, default 50, server-capped at 100.";
		}
		return super.getDescriptionForKey(key);
	}
}