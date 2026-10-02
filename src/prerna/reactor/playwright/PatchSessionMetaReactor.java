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

import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class PatchSessionMetaReactor extends AbstractReactor {

	public PatchSessionMetaReactor() {
		this.keysToGet = new String[] { "sessionId", ReactorKeysEnum.PARAM_VALUES_MAP.getKey() };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String sessionId = this.keyValue.get(this.keysToGet[0]);
		Map<String, Object> paramValues = getMap(this.keysToGet[1]);

		MetaPatch patch = MetaPatch.fromMap(paramValues);

		PlaywrightSession playwrightSession = this.insight.getUser().getPlaywrightSession(sessionId);
		RecordingMeta old = playwrightSession.history.meta();
		long now = System.currentTimeMillis();

		String id = old != null && old.id() != null ? old.id() : java.util.UUID.randomUUID().toString();
		String title = patch.title() != null ? patch.title() : (old != null ? old.title() : null);
		String desc = patch.description() != null ? patch.description() : (old != null ? old.description() : null);
		String intent = patch.intent() != null ? patch.intent() : (old != null ? old.intent() : null);
		Long created = old != null ? old.createdAt() : null; // keep null during recording
		Long updated = now; // bump updatedAt on edit

		RecordingMeta meta = new RecordingMeta(id, title, desc, created, updated, intent);
		playwrightSession.history = new StepsEnvelope(playwrightSession.history.version(), meta,
				playwrightSession.history.steps());

		return new NounMetadata(meta, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
		return "Reactor that allow the Recorder app to update the title and the description for a recorded file";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("sessionId")) {
			return "The id of the current session of the playwright";
		}

		return super.getDescriptionForKey(key);
	}

}
