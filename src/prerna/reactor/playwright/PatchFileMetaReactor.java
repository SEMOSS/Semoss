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

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class PatchFileMetaReactor extends AbstractReactor {

	public PatchFileMetaReactor() {
		this.keysToGet = new String[] { "name", ReactorKeysEnum.PARAM_VALUES_MAP.getKey(),
				ReactorKeysEnum.PROJECT.getKey() };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String nameOrPath = this.keyValue.get(this.keysToGet[0]);
		Map<String, String> paramValues = getMap();
		String projectId = this.keyValue.get(this.keysToGet[2]);
		if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must input a project id");
		}
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if (!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException(
					"Project does not exist or user does not have access to edit the project");
		}

		MetaPatch patch = MetaPatch.fromMap(paramValues);

		StepsEnvelope env = PlaywrightUtility.loadStepsFromFile(projectId, nameOrPath);
		RecordingMeta old = env.meta();
		long now = System.currentTimeMillis();

		String id = old != null && old.id() != null ? old.id() : java.util.UUID.randomUUID().toString();
		String title = patch.title() != null ? patch.title() : (old != null ? old.title() : null);
		String desc = patch.description() != null ? patch.description() : (old != null ? old.description() : null);
		String intent = patch.intent() != null ? patch.intent() : (old != null ? old.intent() : null);
		Long created = (old != null && old.createdAt() != null) ? old.createdAt() : now; // set if missing
		Long updated = now;

		StepsEnvelope updatedEnv = new StepsEnvelope(env.version(),
				new RecordingMeta(id, title, desc, created, updated, intent), env.steps());

		Path file = nameOrPath.contains(FileSystems.getDefault().getSeparator()) ? Paths.get(nameOrPath)
				: PlaywrightUtility.initRecordingsDir(projectId)
						.resolve(nameOrPath.endsWith(".json") ? nameOrPath : nameOrPath + ".json");

		RecordingMeta meta = null;
		try {
			PlaywrightUtility.writeStepsEnvelope(file.toFile(), updatedEnv);
			meta = updatedEnv.meta();
		} catch (Exception e) {
			throw new RuntimeException("Failed to write: " + file, e);
		}

		return new NounMetadata(meta, PixelDataType.MAP);
	}

	private Map<String, String> getMap() {
		GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		Map<String, String> output = new HashMap<>();
		if (mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if (mapInputs != null && !mapInputs.isEmpty()) {
				for (int i = 0; i < mapInputs.size(); i++) {
					output.putAll((Map<? extends String, ? extends String>) mapInputs.get(i).getValue());
				}
				return output;
			}
		}
		return null;
	}

}
