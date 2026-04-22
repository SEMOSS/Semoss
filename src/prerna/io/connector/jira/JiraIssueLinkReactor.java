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

import java.util.Map;

import org.javatuples.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class JiraIssueLinkReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraIssueLinkReactor.class);

	private static final String ACTION = "action";
	private static final String LINK_TYPE = "linkType";
	private static final String INWARD_ISSUE = "inwardIssue";
	private static final String OUTWARD_ISSUE = "outwardIssue";
	private static final String LINK_ID = "linkId";

	public JiraIssueLinkReactor() {
		this.keysToGet = new String[] { ACTION, LINK_TYPE, INWARD_ISSUE, OUTWARD_ISSUE, LINK_ID };
		this.keyRequired = new int[] { 1, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String action = this.keyValue.get(ACTION);
			if (action == null || action.trim().isEmpty()) {
				throw new SemossPixelException("The action parameter is required. Valid values are: link, unlink.");
			}
			action = action.trim().toLowerCase();

			User user = this.insight.getUser();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();

			switch (action) {
			case "link": {
				String linkType = this.keyValue.get(LINK_TYPE);
				if (linkType == null || linkType.trim().isEmpty()) {
					throw new SemossPixelException("Link type (linkType) is required for the link action.");
				}
				String inwardIssue = JiraUtils.validateIssueKey(this.keyValue.get(INWARD_ISSUE));
				String outwardIssue = JiraUtils.validateIssueKey(this.keyValue.get(OUTWARD_ISSUE));
				if (inwardIssue.equalsIgnoreCase(outwardIssue)) {
					throw new SemossPixelException(
							"Cannot link an issue to itself. inwardIssue and outwardIssue must be different.");
				}
				Map<String, Object> result = JiraHelper.linkIssues(accessToken, baseUrl, linkType, inwardIssue, outwardIssue);
				return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			case "unlink": {
				String linkId = JiraUtils.validateNumericId(this.keyValue.get(LINK_ID), "linkId");
				Map<String, Object> result = JiraHelper.unlinkIssues(accessToken, baseUrl, linkId);
				return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			default:
				throw new SemossPixelException("Invalid action '" + action + "'. Valid values are: link, unlink.");
			}
		} catch (SemossPixelException e) {
			classLogger.error("Error in JiraIssueLinkReactor", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to execute JiraIssueLinkReactor", e);
			throw new SemossPixelException(
					"An error occurred in JiraIssueLinkReactor. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Manages links between Jira issues. Supports link and unlink operations.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ACTION)) {
			return "Required operation to perform. Valid values: link - creates a link between two issues (linkType, inwardIssue, outwardIssue required). unlink - removes an existing link (linkId required).";
		} else if (key.equals(LINK_TYPE)) {
			return "Exact link type name (for example, Blocks, Relates, Duplicate). Required for link action.";
		} else if (key.equals(INWARD_ISSUE)) {
			return "Jira issue key for the inward side of the link (for example, RTJ-123). Required for link action.";
		} else if (key.equals(OUTWARD_ISSUE)) {
			return "Jira issue key for the outward side of the link (for example, RTJ-456). Required for link action.";
		} else if (key.equals(LINK_ID)) {
			return "Numeric issue link id to remove. Required for unlink action.";
		}
		return super.getDescriptionForKey(key);
	}
}
