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
package prerna.reactor.automation;

import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.IReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * Returns a Pixel template string and description for a custom reactor in a project.
 *
 * <p>Pixel: {@code GetReactorSignature(project=["appId"], reactor=["ReactorName"])}
 *
 * <p>Returns a JSON object with:
 * <ul>
 *   <li>{@code template}  - a filled Pixel call showing each param placeholder, e.g.
 *       {@code MyReactor(requiredKey=[""], optionalKey="")}</li>
 *   <li>{@code description}  - the reactor's one-line description, or empty string if none</li>
 *   <li>{@code hasParams}  - boolean, false when the reactor declares no keys</li>
 * </ul>
 *
 * <p>On any failure (project not found, reactor not found, bad metadata) returns a minimal
 * fallback object with {@code template="ReactorName()"} so the caller can still populate the field.
 */
public final class GetReactorSignatureReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetReactorSignatureReactor.class);

	private static final String REACTOR_KEY = "reactor";

	public GetReactorSignatureReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), REACTOR_KEY };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in.");
		}

		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		String reactorName = this.keyValue.get(REACTOR_KEY);

		projectId = SecurityProjectUtils.testUserProjectIdForAlias(user, projectId);
		if (!SecurityProjectUtils.userCanViewProject(user, projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access.");
		}
		if (reactorName == null || reactorName.trim().isEmpty()) {
			throw new IllegalArgumentException("A reactor name is required.");
		}
		reactorName = reactorName.trim();

		JSONObject result = buildSignature(projectId, reactorName);
		return new NounMetadata(result.toString(), PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}

	private static JSONObject buildSignature(String projectId, String reactorName) {
		JSONObject result = new JSONObject();
		result.put("reactorName", reactorName);

		IProject project = Utility.getProject(projectId);
		if (project == null) {
			classLogger.warn("GetReactorSignature: project {} not found", projectId);
			return fallback(result, reactorName);
		}

		IReactor reactor;
		try {
			reactor = project.getReactor(reactorName);
		} catch (Exception e) {
			classLogger.warn("GetReactorSignature: could not load reactor {} from project {}", reactorName, projectId, e);
			return fallback(result, reactorName);
		}
		if (reactor == null) {
			return fallback(result, reactorName);
		}

		// Description  - best-effort
		String description = "";
		try {
			String d = reactor.getReactorDescription();
			if (d != null && !d.isBlank()) description = d.trim();
		} catch (Exception e) {
			classLogger.warn("GetReactorSignature: getReactorDescription() failed for {}", reactorName, e);
		}
		result.put("description", description);

		// Parameter metadata via asMcpTool()
		try {
			JSONObject tool = reactor.asMcpTool();
			JSONObject inputSchema = tool.optJSONObject("inputSchema");
			if (inputSchema == null) {
				return fallback(result, reactorName);
			}

			JSONObject properties = inputSchema.optJSONObject("properties");
			JSONArray required = inputSchema.optJSONArray("required");

			if (properties == null || properties.isEmpty()) {
				result.put("template", reactorName + "()");
				result.put("hasParams", false);
				return result;
			}

			// Build set of required key names for O(1) lookup
			Set<String> requiredKeys = new HashSet<>();
			if (required != null) {
				for (int i = 0; i < required.length(); i++) {
					requiredKeys.add(required.getString(i));
				}
			}

			// Build per-param metadata and required-only template
			StringBuilder template = new StringBuilder(reactorName).append("(");
			JSONArray params = new JSONArray();
			boolean firstRequired = true;

			// Required params first (for template ordering)
			for (String key : properties.keySet()) {
				if (!requiredKeys.contains(key)) continue;
				if (!firstRequired) template.append(", ");
				template.append(key).append("=[\"\"]");
				firstRequired = false;

				JSONObject prop = properties.optJSONObject(key);
				params.put(buildParamMeta(key, prop, true));
			}
			// Then optional params (not in template, but included in params list)
			for (String key : properties.keySet()) {
				if (requiredKeys.contains(key)) continue;
				JSONObject prop = properties.optJSONObject(key);
				params.put(buildParamMeta(key, prop, false));
			}
			template.append(")");

			result.put("template", template.toString());
			result.put("hasParams", !properties.isEmpty());
			result.put("params", params);
		} catch (Exception e) {
			classLogger.warn("GetReactorSignature: asMcpTool() failed for {}", reactorName, e);
			return fallback(result, reactorName);
		}

		return result;
	}

	private static JSONObject buildParamMeta(String key, JSONObject prop, boolean required) {
		JSONObject meta = new JSONObject();
		meta.put("name", key);
		meta.put("required", required);
		if (prop != null) {
			String type = prop.optString("type", "string");
			meta.put("type", type);
			String desc = prop.optString("description", "");
			// Suppress the default placeholder description  - it adds no value
			if (!desc.isBlank() && !desc.equals("No description present")) {
				meta.put("description", desc);
			}
		} else {
			meta.put("type", "string");
		}
		return meta;
	}

	private static JSONObject fallback(JSONObject base, String reactorName) {
		base.put("template", reactorName + "()");
		base.put("description", "");
		base.put("hasParams", false);
		return base;
	}

	@Override
	public String getReactorDescription() {
		return "Returns the Pixel call template and description for a custom reactor in a project.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (REACTOR_KEY.equals(key)) return "Name of the reactor to inspect (without 'Reactor' suffix).";
		if (ReactorKeysEnum.PROJECT.getKey().equals(key)) return "Project ID containing the reactor.";
		return super.getDescriptionForKey(key);
	}
}
