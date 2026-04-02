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
package prerna.reactor.prompt;

import java.util.List;
import java.util.Map;

import prerna.prompt.PromptUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Returns distinct metadata values with usage counts for the specified
 * metakeys.
 *
 * Pixel usage: GetPromptMetaValues(metaKeys=["department", "region"]);
 *
 * Parameters: metaKeys (List of String, required) - List of metakeys to
 * retrieve values for
 *
 * Returns: CUSTOM_DATA_STRUCTURE - a list of maps, each containing:
 *
 * Per-entry fields: metakey (String) - The metadata key name metavalue (String)
 * - A distinct value for that key count (int) - Number of prompts with this
 * key-value pair
 */
public class GetPromptMetaValuesReactor extends AbstractReactor {

	public GetPromptMetaValuesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.META_KEYS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		String userId = this.insight.getUserId();
		if (userId == null || userId.isEmpty()) {
			throw new IllegalArgumentException("User is not properly logged in.");
		}

		organizeKeys();
		List<Map<String, Object>> ret = PromptUtils
				.getAvailableMetaValues(getListValues(ReactorKeysEnum.META_KEYS.getKey()));
		return new NounMetadata(ret, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

	private List<String> getListValues(String key) {
		GenRowStruct grs = this.store.getGenRowStruct(key);
		if (grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues();
		}
		return this.curRow.getAllStrValues();
	}
}
