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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.playwright;

import java.nio.file.Path;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Returns compact room recording summaries for model-assisted selection. */
public class FindPlaywrightRoomRecordingsReactor extends AbstractReactor {

	private static final String QUERY = "query";
	private static final String MAX_CANDIDATES = "max_candidates";

	public FindPlaywrightRoomRecordingsReactor() {
		this.keysToGet = new String[] { QUERY, MAX_CANDIDATES };
		this.keyRequired = new int[] { 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String roomId = this.insight.getRoomId();
		if (roomId == null || roomId.isBlank()) {
			throw new IllegalArgumentException("The current insight is not bound to a Playground room");
		}

		Path roomFolder = Path.of(this.insight.getInsightFolder()).toAbsolutePath().normalize();
		Map<String, Object> result = new PlaywrightRecordingCatalogService().findRoomRecordings(roomFolder,
				trim(this.keyValue.get(QUERY)), parseLimit(this.keyValue.get(MAX_CANDIDATES)));
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private static int parseLimit(String value) {
		if (value == null || value.isBlank()) {
			return 20;
		}
		try {
			return Integer.parseInt(value.trim());
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("max_candidates must be an integer");
		}
	}

	private static String trim(String value) {
		return value == null ? "" : value.trim();
	}

	@Override
	public String getReactorDescription() {
		return "Lists and summarizes Playwright recordings in the current Playground room so the model can identify the closest match before replaying one.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (QUERY.equals(key)) {
			return "Natural-language description of the recording to find. Leave empty to inspect all recent room recordings.";
		}
		if (MAX_CANDIDATES.equals(key)) {
			return "Maximum recording summaries to return, from 1 to 50. Defaults to 20.";
		}
		return super.getDescriptionForKey(key);
	}
}
