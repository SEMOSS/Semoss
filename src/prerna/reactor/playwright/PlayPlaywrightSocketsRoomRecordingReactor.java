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

import java.util.LinkedHashMap;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Opens the Playwright Sockets portal to resolve and replay a room recording. */
public class PlayPlaywrightSocketsRoomRecordingReactor extends AbstractReactor {

	private static final String RECORDING_NAME_HINT = "recording_name_hint";
	private static final String RECORDING_FILE = "recording_file";
	private static final String PROJECT_ID = "project_id";
	private static final String START_URL = "start_url";

	public PlayPlaywrightSocketsRoomRecordingReactor() {
		this.keysToGet = new String[] { RECORDING_NAME_HINT, RECORDING_FILE, PROJECT_ID, START_URL };
		this.keyRequired = new int[] { 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String hint = trim(this.keyValue.get(RECORDING_NAME_HINT));
		String fileName = trim(this.keyValue.get(RECORDING_FILE));
		if (hint.isEmpty() && fileName.isEmpty()) {
			throw new IllegalArgumentException(
					"recording_name_hint or recording_file is required. Ask the user which room recording to play.");
		}

		String startUrl = trim(this.keyValue.get(START_URL));
		if (!startUrl.isEmpty()) {
			startUrl = OpenPlaywrightSocketsRoomRecordingReactor.normalizeUrl(startUrl);
		}

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("status", "ui_required");
		result.put("mode", "play_recording");
		result.put("recording_name_hint", hint);
		result.put("recording_file", fileName);
		result.put("project_id", trim(this.keyValue.get(PROJECT_ID)));
		result.put("start_url", startUrl);
		result.put("instructions",
				"The Playwright Sockets UI will open, find the closest matching recording in the current Playground room, and replay it. An accessible project recording may be used as a fallback.");
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private static String trim(String value) {
		return value == null ? "" : value.trim();
	}

	@Override
	public String getReactorDescription() {
		return "Opens Playwright Sockets and replays the current Playground room recording that best matches a natural-language description or filename.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (RECORDING_NAME_HINT.equals(key)) {
			return "Natural-language description of the Playground room recording, such as google.com or login flow.";
		}
		if (RECORDING_FILE.equals(key)) {
			return "Optional exact recording JSON filename.";
		}
		if (PROJECT_ID.equals(key)) {
			return "Optional accessible Playwright project to search as a fallback.";
		}
		if (START_URL.equals(key)) {
			return "Optional URL override to open before replaying the recording.";
		}
		return super.getDescriptionForKey(key);
	}
}
