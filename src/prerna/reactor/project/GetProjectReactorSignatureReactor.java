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
package prerna.reactor.project;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

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
 * Returns safe input metadata for one project-specific custom reactor.
 *
 * <p>
 * The reactor first applies standard project alias and view-permission checks, then derives the
 * parameter contract from the target reactor's MCP schema. It does not execute the project reactor.
 *
 * <p>Pixel: {@code GetProjectReactorSignature(project=["appId"], reactor=["ReactorName"])}
 */
public class GetProjectReactorSignatureReactor extends AbstractReactor {

	private static final String REACTOR_KEY = "reactor";

	public GetProjectReactorSignatureReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), REACTOR_KEY };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		String reactorName = this.keyValue.get(REACTOR_KEY);
		if (projectId == null || projectId.isBlank()) {
			throw new IllegalArgumentException("Must provide a project id.");
		}
		if (reactorName == null || reactorName.isBlank()) {
			throw new IllegalArgumentException("Must provide a reactor name.");
		}

		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if (!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access to the project.");
		}

		IProject project = Utility.getProject(projectId);
		if (project == null) {
			throw new IllegalArgumentException("Project is not loaded: " + projectId);
		}
		IReactor reactor = project.getReactor(reactorName);
		if (reactor == null) {
			throw new IllegalArgumentException("Project does not contain reactor: " + reactorName);
		}

		JSONObject tool = reactor.asMcpTool();
		JSONObject schema = tool.optJSONObject("inputSchema");
		JSONObject properties = schema != null ? schema.optJSONObject("properties") : null;
		JSONArray required = schema != null ? schema.optJSONArray("required") : null;
		List<Map<String, Object>> params = new ArrayList<>();
		if (properties != null) {
			for (String name : properties.keySet()) {
				JSONObject property = properties.optJSONObject(name);
				Map<String, Object> param = new LinkedHashMap<>();
				param.put("name", name);
				param.put("type", property != null ? property.optString("type", "string") : "string");
				param.put("required", required != null && required.toList().contains(name));
				if (property != null && property.has("description")) {
					param.put("description", property.optString("description"));
				}
				params.add(param);
			}
		}

		Map<String, Object> output = new LinkedHashMap<>();
		output.put("description", reactor.getReactorDescription());
		output.put("params", params);
		output.put("template", template(reactorName, params));
		return new NounMetadata(output, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private static String template(String reactorName, List<Map<String, Object>> params) {
		StringBuilder result = new StringBuilder(reactorName).append("(");
		for (int index = 0; index < params.size(); index++) {
			if (index > 0) {
				result.append(", ");
			}
			result.append(params.get(index).get("name")).append("=[\"\"]");
		}
		return result.append(")").toString();
	}

	@Override
	public String getReactorDescription() {
		return "Returns parameter metadata for a project-specific custom reactor.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.PROJECT.getKey().equals(key)) {
			return "The project ID or alias containing the custom reactor.";
		}
		if (REACTOR_KEY.equals(key)) {
			return "Exact name of the project-specific reactor to inspect without executing it.";
		}
		return super.getDescriptionForKey(key);
	}
}
