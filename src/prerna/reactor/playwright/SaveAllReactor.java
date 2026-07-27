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

import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SaveAllReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SaveAllReactor.class);

	ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

	public SaveAllReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), "sessionId", "name", "title", "description",
				"intent" };
		this.keyRequired = new int[] { 1, 1, 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String projectId = this.keyValue.get(this.keysToGet[0]);
		String sessionId = this.keyValue.get(this.keysToGet[1]);
		String name = this.keyValue.get(this.keysToGet[2]);
		String title = this.keyValue.get(this.keysToGet[3]);
		String desc = this.keyValue.get(this.keysToGet[4]);
		String intent = this.keyValue.get(this.keysToGet[5]);
		if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must input a project id");
		}
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if (!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access to edit the project");
		}

		// Build meta with timestamps
		long now = System.currentTimeMillis();

		// Try to preserve createdAt if file already exists
		String base = PlaywrightUtility.sanitizeFilename(
				name == null || name.isBlank() ? ("script-" + PlaywrightUtility.generateTimestamp()) : name);
		Path file = PlaywrightUtility.initRecordingsDir(projectId)
				.resolve(base.endsWith(".json") ? base : (base + ".json"));

		RecordingMeta existingMeta = null;
		if (Files.exists(file)) {
			try {
				StepsEnvelope existing = json.readValue(file.toFile(), StepsEnvelope.class);
				existingMeta = existing.meta();
			} catch (Exception e) {
				classLogger.warn("Unable to read existing recording metadata from '{}'; creating fresh metadata", file,
						e);
			}
		}

		RecordingMeta newMeta = new RecordingMeta(
				(existingMeta != null && existingMeta.id() != null) ? existingMeta.id() : sessionId, title, desc,
				(existingMeta != null && existingMeta.createdAt() != null) ? existingMeta.createdAt() : now, now,
				intent);

		PlaywrightSession playwrightSession = this.insight.getUser().getPlaywrightSession(sessionId);

		StepsEnvelope env = new StepsEnvelope("1.0", newMeta, playwrightSession.history.steps());

		// TODO: shouldn't be returning the full path
		String filePath = null;
		try {
			json.writeValue(file.toFile(), env);
			filePath = file.toAbsolutePath().toString();
		} catch (Exception e) {
			throw new RuntimeException("Failed to save script to: " + file, e);
		}

		return new NounMetadata(filePath, PixelDataType.CONST_STRING);
	}

	@Override
	public String getReactorDescription() {
		return "Reactor that replays step that is in order to run by given , sesionId, tabId, and fileName";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("sessionId")) {
			return "The id of the current session of the playwright";
		} else if (key.equals("name")) {
			return "the name of the recorded file";
		} else if (key.equals("description")) {
			return "The description of the recorded file";
		} else if (key.equals("title")) {
			return "The title of the recorded file";
		} else if (key.equals("intent")) {
			return "The intention or the purpose of the recorded file";
		}
		return super.getDescriptionForKey(key);
	}
}
