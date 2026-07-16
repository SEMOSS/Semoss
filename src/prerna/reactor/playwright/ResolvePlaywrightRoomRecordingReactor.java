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

import java.nio.file.Path;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

/** Resolves the closest matching Playwright recording in an authorized room. */
public class ResolvePlaywrightRoomRecordingReactor extends AbstractReactor {

	private static final String RECORDING_NAME_HINT = "recording_name_hint";
	private static final String RECORDING_FILE = "recording_file";
	private static final String PROJECT_ID = "project_id";

	public ResolvePlaywrightRoomRecordingReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ROOM_ID.getKey(), RECORDING_NAME_HINT, RECORDING_FILE,
				PROJECT_ID };
		this.keyRequired = new int[] { 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String roomId = trim(this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey()));
		String hint = trim(this.keyValue.get(RECORDING_NAME_HINT));
		String recordingFile = trim(this.keyValue.get(RECORDING_FILE));
		String projectId = trim(this.keyValue.get(PROJECT_ID));

		if (roomId.isEmpty()) {
			throw new IllegalArgumentException("roomId is required");
		}
		if (!roomId.equals(this.insight.getRoomId())) {
			throw new IllegalArgumentException(
					"The insight is not bound to the requested Playground room. Run SetRoomForInsight first.");
		}
		if (hint.isEmpty() && recordingFile.isEmpty()) {
			throw new IllegalArgumentException("recording_name_hint or recording_file is required");
		}

		Path roomFolder = Path.of(this.insight.getInsightFolder()).toAbsolutePath().normalize();
		Path projectRecordingsFolder = null;
		if (!projectId.isEmpty()) {
			User user = this.insight.getUser();
			if (!SecurityProjectUtils.userCanViewProject(user, projectId)) {
				throw new IllegalArgumentException("Project does not exist or user does not have access to the project");
			}
			projectRecordingsFolder = Path.of(AssetUtility.getProjectAssetsFolder(projectId),
					PlaywrightUtility.RECORDINGS_FOLDER_NAME).toAbsolutePath().normalize();
		}

		Map<String, Object> result = new PlaywrightRecordingCatalogService().resolve(roomFolder,
				projectRecordingsFolder, projectId, hint, recordingFile);
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private static String trim(String value) {
		return value == null ? "" : value.trim();
	}

	@Override
	public String getReactorDescription() {
		return "Finds the Playwright recording in the current Playground room that best matches a natural-language description or filename, with an accessible project as an optional fallback.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.ROOM_ID.getKey().equals(key)) {
			return "Active Playground room ID. The current insight must already be bound to this room.";
		}
		if (RECORDING_NAME_HINT.equals(key)) {
			return "Natural-language description of the desired room recording.";
		}
		if (RECORDING_FILE.equals(key)) {
			return "Optional exact recording JSON filename.";
		}
		if (PROJECT_ID.equals(key)) {
			return "Optional accessible project whose recordings are searched as a fallback.";
		}
		return super.getDescriptionForKey(key);
	}
}
