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

import java.util.List;
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

public class JiraGetCreateFieldsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraGetCreateFieldsReactor.class);

	private static final String PROJECT = "project";
	private static final String ISSUETYPEID = "issuetypeid";

	public JiraGetCreateFieldsReactor() {
		this.keysToGet = new String[] { PROJECT, ISSUETYPEID };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			User user = this.insight.getUser();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();

			String projectKey = JiraUtils.validateProjectKey(this.keyValue.get(PROJECT));
			String issueTypeId = JiraUtils.validateNumericId(this.keyValue.get(ISSUETYPEID), "issuetypeid");

			List<Map<String, Object>> fields = JiraHelper.getCreateMetaFields(accessToken, baseUrl, projectKey, issueTypeId);
			return new NounMetadata(fields, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error in JiraGetCreateFieldsReactor", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to get Jira fields", e);
			throw new SemossPixelException(
					"An error occurred while getting Jira fields. Error: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Returns available fields for a given Jira project and issue type. Each field includes fieldId, name, required, hasDefaultValue, schema (with type indicating value structure such as string, project, priority, user, date, array), and allowedValues listing valid options for enum-style fields.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(PROJECT)) {
			return "Required Jira project key in uppercase (for example, RTJ).";
		} else if (key.equals(ISSUETYPEID)) {
			return "Required numeric issue type id.";
		}
		return super.getDescriptionForKey(key);
	}
}
