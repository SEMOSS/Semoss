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

/** Opens the Playwright Sockets portal in recording mode. */
public class OpenPlaywrightSocketsRoomRecordingReactor extends AbstractReactor {

	private static final String START_URL = "start_url";
	private static final String RECORDING_NAME_HINT = "recording_name_hint";
	private static final String CAPTURE_FULL_PAGE_AT_END = "capture_full_page_at_end";

	public OpenPlaywrightSocketsRoomRecordingReactor() {
		this.keysToGet = new String[] { START_URL, RECORDING_NAME_HINT, CAPTURE_FULL_PAGE_AT_END };
		this.keyRequired = new int[] { 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String startUrl = normalizeUrl(this.keyValue.get(START_URL));
		String recordingNameHint = trim(this.keyValue.get(RECORDING_NAME_HINT));

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("status", "ui_required");
		result.put("start_url", startUrl);
		result.put("recording_name_hint", recordingNameHint);
		result.put(CAPTURE_FULL_PAGE_AT_END,
				Boolean.parseBoolean(trim(this.keyValue.get(CAPTURE_FULL_PAGE_AT_END))));
		result.put("instructions",
				"The Playwright Sockets UI will open, start recording, and save the recording to the current Playground room when returned. Native browser downloads are captured automatically and saved as individual assets under /browser-downloads/<run-id>/; the response returns saved insight paths and any per-file errors.");
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	static String normalizeUrl(String value) {
		String normalized = trim(value);
		if (normalized.isEmpty()) {
			throw new IllegalArgumentException("start_url is required. Ask the user for a URL before calling this tool.");
		}
		if (!normalized.matches("^[A-Za-z][A-Za-z0-9+.-]*://.*$")) {
			normalized = "https://" + normalized;
		}
		return normalized;
	}

	private static String trim(String value) {
		return value == null ? "" : value.trim();
	}

	@Override
	public String getReactorDescription() {
		return "Opens the Playwright Sockets remote browser, navigates to a URL, and starts a recording that can be saved to the current Playground room. Native browser downloads are automatically saved as individual assets in the current insight and returned as metadata and insight paths.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (START_URL.equals(key)) {
			return "URL to open. Bare hostnames such as google.com are prefixed with https://.";
		}
		if (RECORDING_NAME_HINT.equals(key)) {
			return "Optional short name or description for the recording.";
		}
		if (CAPTURE_FULL_PAGE_AT_END.equals(key)) {
			return "When returning the recording to Playground, auto-scroll the final page and return its rendered text as context. Best effort; capture errors do not fail the save.";
		}
		return super.getDescriptionForKey(key);
	}
}
