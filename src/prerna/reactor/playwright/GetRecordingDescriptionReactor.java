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

import org.json.JSONObject;
import org.json.JSONTokener;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Reactor to retrieve the description of a Playwright recording file.
 * This reactor reads a recording JSON file and returns the description
 * from the metadata.
 */
public class GetRecordingDescriptionReactor extends AbstractReactor {

	private final static String SCRIPT_KEY = "Script";

	public GetRecordingDescriptionReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), SCRIPT_KEY };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String projectId = this.keyValue.get(this.keysToGet[0]);
		String fileName = this.keyValue.get(this.keysToGet[1]);

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

		String description = "";

		try (FileReader reader = new FileReader(scriptFile)) {
			JSONTokener tokener = new JSONTokener(reader);
			JSONObject jsonObject = new JSONObject(tokener);

			if (jsonObject.has("meta")) {
				JSONObject meta = jsonObject.getJSONObject("meta");

				if (meta.has("description")) {
					description = meta.optString("description", "");
				}
			}
		} catch (IOException e) {
			throw new IllegalArgumentException("Error reading script file: " + fileName, e);
		} catch (Exception e) {
			throw new IllegalArgumentException("Error parsing JSON from script file: " + fileName, e);
		}

		return new NounMetadata(description, PixelDataType.CONST_STRING);
	}

	@Override
	public String getReactorDescription() {
		return "Retrieves the description of a Playwright recording file from its metadata.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(SCRIPT_KEY)) {
			return "The name of the JSON file (e.g., 'script-1.json') located in the recordings folder.";
		}

		return super.getDescriptionForKey(key);
	}
}

