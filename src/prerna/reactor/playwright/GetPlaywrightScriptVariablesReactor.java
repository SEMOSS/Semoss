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
package prerna.reactor.playwright;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetPlaywrightScriptVariablesReactor extends AbstractReactor {

	private final static String SCRIPT_KEY = "Script";

	public GetPlaywrightScriptVariablesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), SCRIPT_KEY };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String projectId = this.keyValue.get(this.keysToGet[0]);
		String fileName = this.keyValue.get(this.keysToGet[1]);
		if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must input a project id");
		}
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if (!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access to view the project");
		}

		if (fileName == null || fileName.trim().isEmpty()) {
			throw new IllegalArgumentException("File name cannot be null or empty");
		}

		if (!fileName.toLowerCase().endsWith(".json")) {
			fileName += ".json";
		}

		Path recordingsDir = PlaywrightUtility.initRecordingsDir(projectId);
		Path scriptPath = recordingsDir.resolve(fileName);

		File scriptFile = scriptPath.toFile();

		if (!scriptFile.exists()) {
			throw new IllegalArgumentException("Script file not found: " + fileName + " in recordings folder");
		}

		List<VariableRecord> variables = new ArrayList<>();

		try (FileReader reader = new FileReader(scriptFile)) {
			JSONTokener tokener = new JSONTokener(reader);
			JSONObject jsonObject = new JSONObject(tokener);

			if (jsonObject.has("meta")) {
				JSONObject meta = jsonObject.getJSONObject("meta");

				if (meta.has("title")) {
					String title = meta.optString("title", "");
					if (!title.isEmpty()) {
						variables.add(new VariableRecord("title", title));
					}
				}

				if (meta.has("description")) {
					String desc = meta.optString("description", "");
					if (!desc.isEmpty()) {
						variables.add(new VariableRecord("description", desc));
					}
				}
			}

			if (jsonObject.has("steps")) {
				JSONArray steps = jsonObject.getJSONArray("steps");

				for (int i = 0; i < steps.length(); i++) {
					JSONObject step = steps.getJSONObject(i);

					if (step.has("type")) {
						String type = step.getString("type");
						boolean isPassword = step.getBoolean("isPassword");

						// process TYPE or VARIABLE steps
						if ("TYPE".equals(type) || "VARIABLE".equals(type)) {
							// Extract label and text
							String label = step.optString("label", null);
							String text = step.optString("text", null);

							if (label != null && !label.trim().isEmpty() && text != null) {
								variables.add(new VariableRecord(label, text, isPassword));
							}
						}
					}
				}
			}
		} catch (IOException e) {
			throw new IllegalArgumentException("Error reading script file: " + fileName, e);
		} catch (Exception e) {
			throw new IllegalArgumentException("Error parsing JSON from script file: " + fileName, e);
		}

		return new NounMetadata(variables, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
		return "Extracts variables (TYPE or VARIABLE steps) from a Playwright script JSON file, returning them as a list of VariableRecord objects.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(SCRIPT_KEY)) {
			return "The name of the JSON file (e.g., 'script-1.json') located in the recordings folder.";
		}

		return super.getDescriptionForKey(key);
	}
}