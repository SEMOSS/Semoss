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
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Retrieves a single prompt by ID with access control. Only returns the prompt
 * if it is global or created by the requesting user.
 *
 * Pixel usage: GetPromptWithVersions(promptId="<uuid>");
 *
 * Parameters: promptId (String, required) - UUID of the prompt to retrieve
 *
 * Returns: MAP - a map containing prompt details, tags, and metadata. Returns
 * an empty map if the prompt is not found or the user lacks access.
 *
 * Return fields: id (String) - UUID of the prompt title (String) - Prompt name
 * context (String) - The prompt text/template version (int) - Version number
 * (0-based, incremented on update) intent (String) - Description of the
 * prompt's purpose created_by (String) - User ID of the prompt creator
 * date_created (String) - ISO timestamp of creation global (boolean) - Whether
 * the prompt is visible to all users tags (List of String) - Tags for
 * categorization metaKeys (Map of String to List of String) - Metadata
 * key-value pairs
 */
public class GetPromptWithVersionsReactor extends AbstractReactor {

	public GetPromptWithVersionsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROMPT_ID.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String userId = this.insight.getUserId();
		if (userId == null || userId.isEmpty()) {
			throw new IllegalArgumentException("User is not properly logged in.");
		}

		String promptID = this.keyValue.get(ReactorKeysEnum.PROMPT_ID.getKey());
		if (promptID == null || promptID.isEmpty()) {
			throw new IllegalArgumentException("PROMPT ID must be passed in to get details for a specific prompt");
		}

		List<Map<String, Object>> promptDetails = PromptUtils.getPromptWithVersioning(promptID, this.insight.getUser());
		return new NounMetadata(promptDetails, PixelDataType.VECTOR);
	}

}
