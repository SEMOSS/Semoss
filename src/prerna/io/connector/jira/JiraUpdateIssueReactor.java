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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.javatuples.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class JiraUpdateIssueReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraUpdateIssueReactor.class);

	private static final String JIRAID = "jiraid";
	private static final String PARAM_MAP = "paramMap";

	public JiraUpdateIssueReactor() {
		this.keysToGet = new String[] { JIRAID, PARAM_MAP };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String jiraId = JiraUtils.validateIssueKey(this.keyValue.get(JIRAID));

			User user = this.insight.getUser();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();

			Map<String, Object> inputMap = getInputFieldMap();
			if (inputMap == null || inputMap.isEmpty()) {
				throw new SemossPixelException(
						"paramMap is required and must be passed as a JSON object / MAP");
			}

			Map<String, Object> fieldMap = new HashMap<>(inputMap);

			String statusValue = extractStringValue(fieldMap.remove("status"));
			String transitionValue = extractStringValue(fieldMap.remove("transition"));

			boolean hasFields = !fieldMap.isEmpty();
			boolean hasTransition = transitionValue != null || statusValue != null;

			if (!hasFields && !hasTransition) {
				throw new SemossPixelException("No editable fields or status/transition provided.");
			}
			if (hasFields) {
				JiraHelper.updateIssueFromMap(accessToken, baseUrl, jiraId, fieldMap);
			}
			if (transitionValue != null) {
				if (transitionValue.chars().allMatch(Character::isDigit)) {
					JiraHelper.transitionIssueById(accessToken, baseUrl, jiraId, transitionValue);
				} else {
					JiraHelper.transitionIssue(accessToken, baseUrl, jiraId, transitionValue);
				}
			} else if (statusValue != null) {
				JiraHelper.transitionIssue(accessToken, baseUrl, jiraId, statusValue);
			}

			Map<String, Object> result = new HashMap<>();
			result.put("jiraid", jiraId);
			result.put("success", true);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error while updating a Jira issue", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to update a Jira issue", e);
			throw new SemossPixelException(
					"An error occurred while updating the Jira issue. Error message: " + e.getMessage());
		}
	}

	@SuppressWarnings("unchecked")
	private String extractStringValue(Object value) {
		if (value instanceof String && !((String) value).trim().isEmpty()) {
			return ((String) value).trim();
		}
		if (value instanceof Map) {
			Object nameVal = ((Map<?, ?>) value).get("name");
			if (nameVal instanceof String && !((String) nameVal).trim().isEmpty()) {
				return ((String) nameVal).trim();
			}
			Object idVal = ((Map<?, ?>) value).get("id");
			if (idVal != null && !idVal.toString().trim().isEmpty()) {
				return idVal.toString().trim();
			}
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> getInputFieldMap() {
		GenRowStruct grs = this.store.getNoun(PARAM_MAP);
		if (grs != null && !grs.isEmpty()) {
			List<NounMetadata> mapNouns = grs.getNounsOfType(PixelDataType.MAP);
			if (mapNouns != null && !mapNouns.isEmpty()) {
				return (Map<String, Object>) mapNouns.get(0).getValue();
			}
		}
		List<NounMetadata> mapNouns = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (mapNouns != null && !mapNouns.isEmpty()) {
			return (Map<String, Object>) mapNouns.get(0).getValue();
		}
		return null;
	}

	@Override
	public String getReactorDescription() {
		return "Updates an existing Jira issue. Only accepts editable issue fields as returned by JiraGetUpdateFields, plus status or transition for workflow changes.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(JIRAID)) {
			return "Required Jira issue key in KEY-NUMBER format (for example, RTJ-123).";
		} else if (key.equals(PARAM_MAP)) {
			return "Required object / MAP of editable field key-value pairs as returned by JiraGetUpdateFields. Pass paramMap as a raw JSON object / MAP, not as a quoted JSON string. Also accepts status (target status name) or transition (transition id or name) for workflow changes.";
		}
		return super.getDescriptionForKey(key);
	}
}