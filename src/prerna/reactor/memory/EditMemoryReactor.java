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
package prerna.reactor.memory;

import java.util.Map;

import prerna.auth.User;
import prerna.engine.impl.model.inferencetracking.MemoryUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Updates a memory's content/metadata. Only the memory's creator may edit it
 * (see MemoryUtils#editMemory); a MEMORY_AUDIT row is recorded with the prior
 * content/metadata before the update is applied.
 */
public class EditMemoryReactor extends AbstractReactor {

	public EditMemoryReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.MEMORY_ID.getKey(), ReactorKeysEnum.CONTENT.getKey(),
				ReactorKeysEnum.METADATA.getKey() };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public String getReactorDescription() {
		return """
				Updates a memory's content and/or metadata. Only the memory's creator may \
				edit it; the prior content/metadata is recorded in the memory's audit trail \
				before the update is applied.\
				""";
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}
		String userId = user.getPrimaryLoginToken().getId();

		String memoryId = this.keyValue.get(ReactorKeysEnum.MEMORY_ID.getKey());
		String newContent = this.keyValue.get(ReactorKeysEnum.CONTENT.getKey());
		Map<String, Object> metadata = getMapFromKeyOrCurRow(ReactorKeysEnum.METADATA.getKey());
		if (metadata != null && metadata.isEmpty()) {
			// distinguish "no metadata key passed" (leave unchanged) from "passed empty" -
			// treat an explicitly empty map the same as not passed, since MemoryUtils
			// uses null to mean "leave metadata unchanged"
			metadata = null;
		}

		boolean updated = MemoryUtils.editMemory(memoryId, userId, newContent, metadata);
		return new NounMetadata(updated, PixelDataType.BOOLEAN);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.MEMORY_ID.getKey())) {
			return "Id of the memory to edit";
		}
		if (key.equals(ReactorKeysEnum.CONTENT.getKey())) {
			return "Replacement memory content";
		}
		if (key.equals(ReactorKeysEnum.METADATA.getKey())) {
			return "Replacement metadata map (omit to leave metadata unchanged)";
		}
		return super.getDescriptionForKey(key);
	}

}
