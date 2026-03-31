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
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Lists prompts visible to the current user (global prompts + user's own
 * prompts), with optional filtering and pagination.
 *
 * Pixel usage: ListPrompt(limit="10", offset="0", metaFilters=[{"department":
 * "engineering"}]);
 *
 * Parameters: limit (String, optional) - Maximum number of results offset
 * (String, optional) - Pagination offset filters (GenRowFilters, optional) -
 * Additional column-level filters metaFilters (Map of String to Object,
 * optional) - Filter by metadata key-value pairs
 *
 * Returns: MAP - a list of prompt maps, each containing:
 *
 * Per-prompt fields: id (String) - UUID of the prompt title (String) - Prompt
 * name context (String) - The prompt text/template version (int) - Version
 * number (0-based, incremented on update) intent (String) - Description of the
 * prompt's purpose created_by (String) - User ID of the prompt creator
 * date_created (String) - ISO timestamp of creation global (boolean) - Whether
 * the prompt is visible to all users tags (List of String) - Tags for
 * categorization metaKeys (Map of String to List of String) - Metadata
 * key-value pairs
 */
public class ListPromptReactor extends AbstractReactor {

	public ListPromptReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey(),
				ReactorKeysEnum.FILTERS.getKey(), ReactorKeysEnum.META_FILTERS.getKey(), };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String userId = this.insight.getUserId();
		if (userId == null || userId.isEmpty()) {
			throw new IllegalArgumentException("User is not properly logged in.");
		}

		GenRowFilters filters = getFilters();
		String limit = this.keyValue.get(this.keysToGet[0]);
		String offset = this.keyValue.get(this.keysToGet[1]);
		Map<String, Object> promptMetadataFilter = getMetaMap();
		List<Map<String, Object>> response = PromptUtils.getPrompts(this.insight.getUser(), filters,
				promptMetadataFilter, limit, offset);

		return new NounMetadata(response, PixelDataType.MAP);
	}

	protected GenRowFilters getFilters() {
		GenRowStruct inputsGRS = this.store.getGenRowStruct(ReactorKeysEnum.FILTERS.getKey());
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			NounMetadata filterNoun = inputsGRS.getNoun(0);
			SelectQueryStruct qs = (SelectQueryStruct) filterNoun.getValue();
			GenRowFilters filters = qs.getCombinedFilters();
			return filters;
		}
		return null;
	}

	private Map<String, Object> getMetaMap() {
		GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.META_FILTERS.getKey());
		if (mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if (mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Object>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, Object>) mapInputs.get(0).getValue();
		}
		return null;
	}
}
